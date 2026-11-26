import { GatewayError } from "@gateway/contracts";
import type { CommitTerminalInput } from "@gateway/ledger";
import { canonical, normalizedNumber, type ReservationMeasurement } from "@gateway/kernel";
import { numberLiteral } from "@gateway/protocol";
import { sha256 } from "@gateway/runtime";
import { resolveEffectiveCustomerPrice, type EffectiveCustomerPrice } from "../pricing.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { InferenceRequest } from "./types.ts";
import type { InferenceTelemetry } from "./telemetry.ts";

export function mapHoldError(code: string, message: string): GatewayError {
  if (code === "insufficient_balance" || code === "key_spend_limit_exceeded") {
    return new GatewayError(402, code, message);
  }
  if (code === "wallet_frozen" || code === "wallet_not_found") {
    return new GatewayError(403, code, message);
  }
  if (code === "idempotency_conflict") return new GatewayError(409, code, message);
  if (code === "ledger_busy") return new GatewayError(503, code, message);
  if (code === "supplier_unavailable" || code === "supplier_insufficient_balance") {
    return new GatewayError(503, code, message);
  }
  return new GatewayError(400, code, message);
}

type ChargeDetails = Pick<
  CommitTerminalInput,
  "usage" | "plugin_audit" | "charge_policy" | "moderation_blocked"
>;

type AttemptQuote = {
  candidate: HydratedCandidate;
  customer: EffectiveCustomerPrice;
  customerAtoms: string;
  supplierAtoms: string;
};

type Idempotency = { key: string; fingerprint: string; legacy_fingerprint: string };

export class InferenceAccounting {
  private heldCustomerAtoms = 0n;
  private hold: "none" | "held" | "settled" | "deferred" = "none";
  private finalizing: Promise<boolean> | undefined;
  private dispatched = false;
  private _unknownAttempt = false;
  private providerRequestIds: Map<string, string> | undefined;

  constructor(
    private readonly request: InferenceRequest,
    private readonly telemetry: InferenceTelemetry,
  ) {}

  private async finalize(mode: "release" | "unknown"): Promise<boolean> {
    const { ledger } = this.request.deps;
    const { requestId } = this.request;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        if (mode === "release") await ledger.releaseHold(requestId);
        else await ledger.markUnknown(requestId, this.providerRequestIds);
        this.hold = "settled";
        return true;
      } catch {
        if (attempt === 0) await Bun.sleep(15);
      }
    }
    this.hold = "deferred";
    return false;
  }

  finalizeHold(mode: "release" | "unknown"): Promise<boolean> {
    if (this.hold === "none" || this.hold === "settled") return Promise.resolve(true);
    return (this.finalizing ??= this.finalize(mode).finally(() => {
      this.finalizing = undefined;
    }));
  }

  private async fingerprintRequest(
    key: string,
    policyBody: Record<string, unknown>,
  ): Promise<Idempotency> {
    const original = await fingerprintInference(this.request, this.request.clientBody);
    const legacy =
      this.request.multipartEntries || policyBody === this.request.clientBody
        ? original
        : await fingerprintInference(this.request, policyBody);
    return { key, fingerprint: `v1:${original}`, legacy_fingerprint: legacy };
  }

  identifyIdempotency(
    header: string | null,
    policyBody: Record<string, unknown>,
  ): Promise<Idempotency> | null {
    const key = header?.trim();
    return key ? this.fingerprintRequest(key, policyBody) : null;
  }

  async findReplay(idempotency: Idempotency) {
    const { deps, billingAccount, operation } = this.request;
    const prior = await deps.ledger.findIdempotentHold(
      billingAccount.id,
      operation,
      idempotency.key,
      idempotency,
    );
    if (prior && !prior.matches) {
      throw mapHoldError(
        "idempotency_conflict",
        "Idempotency key reused with different request fingerprint",
      );
    }
    return prior;
  }

  customerPrice(candidate: HydratedCandidate) {
    const { snap, billingAccount, model, operation } = this.request;
    return resolveEffectiveCustomerPrice(
      snap,
      billingAccount.id,
      model.name,
      operation,
      candidate.billingPrice,
    );
  }

  quote(
    candidate: HydratedCandidate,
    customer: EffectiveCustomerPrice,
    measurement: ReservationMeasurement,
  ): AttemptQuote {
    const { state, snap, outputBound } = this.request;
    return {
      candidate,
      customer,
      customerAtoms: state.quoteForPrice(
        snap.epoch,
        customer.price,
        measurement,
        outputBound,
        customer.cacheKey,
      ),
      supplierAtoms: state.quoteForPrice(snap.epoch, candidate.costPrice, measurement, outputBound),
    };
  }

  async acquire(attemptId: string, quote: AttemptQuote, idempotency: Idempotency | null) {
    const {
      deps,
      state,
      requestId,
      operation,
      billingAccount,
      key,
      model,
      isStream,
      maxStreamDurationMs,
    } = this.request;
    const { candidate, customer, customerAtoms, supplierAtoms } = quote;
    const result = await deps.ledger.acquireHold({
      request_id: requestId,
      attempt_id: attemptId,
      billing_account_id: billingAccount.id,
      api_key_id: key.id,
      enforce_key_spend_limit: key.spend_limit_atoms !== null,
      pricing_override_id: customer.overrideId,
      discount_numerator: customer.discountNumerator,
      discount_denominator: customer.discountDenominator,
      currency: billingAccount.currency,
      operation,
      model: model.name,
      idempotency_key: idempotency?.key ?? null,
      fingerprint: idempotency?.fingerprint ?? "",
      ...(idempotency ? { legacy_fingerprint: idempotency.legacy_fingerprint } : {}),
      customer_atoms: customerAtoms,
      supplier_account_id: candidate.account.id,
      supplier_cost_version_id: candidate.costPrice.id,
      customer_price_version_id: customer.price.id,
      supplier_channel_id: candidate.channel.id,
      supplier_currency: candidate.costPrice.currency,
      supplier_atoms: supplierAtoms,
      expires_at: state.cachedExpiresAt(
        deps.config.holdTtlMs + (isStream ? maxStreamDurationMs : 0),
      ),
    });
    if (result.ok && !result.replayed) {
      this.hold = "held";
      this.heldCustomerAtoms = BigInt(result.held_atoms);
    }
    return result;
  }

  async switchSupplier(
    previous: { attemptId: string; outcome: "FAILED" | "UNKNOWN" },
    nextAttemptId: string,
    quote: AttemptQuote,
  ) {
    const { deps, requestId } = this.request;
    const { candidate, customer, customerAtoms, supplierAtoms } = quote;
    const providerRequestId = this.providerRequestIds?.get(previous.attemptId);
    const result = await deps.ledger.switchSupplierAttempt({
      request_id: requestId,
      failed_attempt_id: previous.attemptId,
      failed_outcome: previous.outcome,
      next_attempt_id: nextAttemptId,
      next_account_id: candidate.account.id,
      next_channel_id: candidate.channel.id,
      next_currency: candidate.costPrice.currency,
      next_supplier_atoms: supplierAtoms,
      next_supplier_cost_version_id: candidate.costPrice.id,
      next_customer_price_version_id: customer.price.id,
      next_customer_price_adjustment: {
        override_id: customer.overrideId,
        discount_numerator: customer.discountNumerator,
        discount_denominator: customer.discountDenominator,
      },
      ...(providerRequestId ? { failed_provider_request_id: providerRequestId } : {}),
      ...(BigInt(customerAtoms) > this.heldCustomerAtoms
        ? { required_customer_atoms: customerAtoms }
        : {}),
    });
    if (result.ok) this.heldCustomerAtoms = BigInt(result.held_atoms);
    return result;
  }

  setDispatched(value: boolean): void {
    this.dispatched = value;
  }

  noteUnknownAttempt(): void {
    this._unknownAttempt = true;
  }

  get unknownAttempt(): boolean {
    return this._unknownAttempt;
  }

  async rememberProviderRequestId(attemptId: string, id: string): Promise<void> {
    (this.providerRequestIds ??= new Map()).set(attemptId, id);
    await this.request.deps.ledger
      .rememberProviderRequestId(this.request.requestId, attemptId, id)
      .catch(() => undefined);
  }

  finishFailedRequest(): Promise<boolean> {
    return this.finalizeHold(this._unknownAttempt ? "unknown" : "release");
  }

  rejectOutstandingHold(): Promise<boolean> | undefined {
    if (this.hold === "held") {
      return this.finalizeHold(this.dispatched || this._unknownAttempt ? "unknown" : "release");
    }
  }

  bindSettlement(
    attemptId: string,
    candidate: HydratedCandidate,
    customer: EffectiveCustomerPrice,
    chars: number | undefined,
    pluginSignal: AbortSignal,
  ) {
    return async (details: ChargeDetails) => {
      const { c, deps, state, requestId, billingAccount, key } = this.request;
      pluginSignal.throwIfAborted();
      if (c.req.raw.signal.aborted) {
        throw new GatewayError(499, "client_aborted", "Client disconnected");
      }
      const input: CommitTerminalInput = {
        request_id: requestId,
        billing_account_id: billingAccount.id,
        selected_attempt_id: attemptId,
        channel_id: candidate.channel.id,
        billing_price_id: customer.price.id,
        cost_version_id: candidate.costPrice.id,
        api_key_id: key.id,
        pricing_override_id: customer.overrideId,
        discount_numerator: customer.discountNumerator,
        discount_denominator: customer.discountDenominator,
        billing_pricing: state.pricingOf(customer.price),
        billing_basis: customer.price.basis,
        supplier_basis: candidate.costPrice.basis,
        supplier_pricing: state.pricingOf(candidate.costPrice),
        supplier_currency: candidate.costPrice.currency,
        usage: details.usage,
      };
      if (chars !== undefined) input.input_characters = String(chars);
      if (details.plugin_audit !== undefined) input.plugin_audit = details.plugin_audit;
      if (details.charge_policy !== undefined) input.charge_policy = details.charge_policy;
      if (details.moderation_blocked !== undefined)
        input.moderation_blocked = details.moderation_blocked;
      const result = await deps.ledger.commitTerminal(input).catch(() => null);
      if (
        result?.ok === false &&
        (result.code === "invalid_upstream_usage" || result.code === "usage_alias_conflict")
      ) {
        throw new GatewayError(502, result.code, result.message);
      }
      if (!result?.ok) {
        deps.metrics.ledgerCommitFailures++;
        throw new GatewayError(503, "ledger_commit_failed", "Ledger could not confirm settlement");
      }
      this.hold = "settled";
      this.telemetry.requestTrace?.recordSettlement(attemptId);
      return result;
    };
  }
}

export function createInferenceAccounting(
  request: InferenceRequest,
  telemetry: InferenceTelemetry,
): InferenceAccounting {
  return new InferenceAccounting(request, telemetry);
}

async function fingerprintInference(
  request: InferenceRequest,
  firstBody: Record<string, unknown>,
): Promise<string> {
  const { multipartEntries, operation, billingAccount, model } = request;
  const body = multipartEntries
    ? await Promise.all(
        multipartEntries.map(async ([field, value]) => [
          field,
          typeof value === "string"
            ? value
            : {
                filename: value.name,
                type: value.type,
                sha256: new Bun.CryptoHasher("sha256")
                  .update(await value.arrayBuffer())
                  .digest("hex"),
              },
        ]),
      )
    : firstBody;
  return sha256(
    canonical(
      { billing_account_id: billingAccount.id, operation, model: model.name, body },
      (parent, key) => {
        const literal = numberLiteral(parent, key);
        if (!literal) return undefined;
        const rounded = String(Number(literal));
        if (literal === rounded) return undefined;
        if (!Number.isFinite(Number(literal))) return literal;
        const exact = normalizedNumber(literal);
        const value = normalizedNumber(rounded);
        return exact.digits === value.digits &&
          exact.exponent === value.exponent &&
          exact.negative === value.negative
          ? undefined
          : literal;
      },
    ),
  );
}

export type AttemptSettlement = ReturnType<InferenceAccounting["bindSettlement"]>;
