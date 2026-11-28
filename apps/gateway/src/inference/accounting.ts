import { GatewayError } from "@gateway/contracts";
import type { CommitTerminalInput } from "@gateway/ledger";
import {
  canonical,
  normalizedNumber,
  quoteSnapshotReservation,
  type ReservationMeasurement,
} from "@gateway/kernel";
import { numberLiteral } from "@gateway/protocol";
import { sha256 } from "@gateway/runtime";
import { resolveAttemptPricing, type EffectiveCustomerPrice } from "../pricing.ts";
import { inspectMeteredRequestSignals } from "../request-metering.ts";
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
  "usage" | "plugin_audit" | "charge_policy" | "moderation_blocked" | "async_task"
> & {
  allowAfterClientAbort?: boolean;
};

type AttemptQuote = {
  candidate: HydratedCandidate;
  customer: EffectiveCustomerPrice;
  customerAtoms: string;
  supplierAtoms: string;
};

type Idempotency = { key: string; fingerprint: string; legacy_fingerprint: string };

export class InferenceAccounting {
  private heldCustomerAtoms = 0n;
  private heldSupplierAtoms = 0n;
  private hold: "none" | "held" | "settled" | "deferred" = "none";
  private finalizing: Promise<boolean> | undefined;
  private dispatched = false;
  private _unknownAttempt = false;
  private providerRequestIds: Map<string, string> | undefined;
  private _ratedAt: string | undefined;

  constructor(
    private readonly request: InferenceRequest,
    private readonly telemetry: InferenceTelemetry,
  ) {}

  private ratedAt(): string {
    return (this._ratedAt ??= new Date().toISOString());
  }

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
    identified?: Idempotency,
  ): Promise<Idempotency> {
    const original =
      identified?.legacy_fingerprint ??
      (await fingerprintInference(this.request, this.request.clientBody));
    const legacy =
      this.request.multipartEntries || policyBody === this.request.clientBody
        ? original
        : await fingerprintInference(this.request, policyBody);
    return { key, fingerprint: `v1:${original}`, legacy_fingerprint: legacy };
  }

  identifyIdempotency(
    header: string | null,
    policyBody: Record<string, unknown>,
    identified?: Idempotency,
  ): Promise<Idempotency> | null {
    const key = header?.trim();
    return key ? this.fingerprintRequest(key, policyBody, identified) : null;
  }

  async findReplay(idempotency: Idempotency, versionedOnly = false) {
    const { deps, billingAccount, operation } = this.request;
    const prior = await deps.ledger.findIdempotentHold(
      billingAccount.id,
      operation,
      idempotency.key,
      idempotency,
      versionedOnly,
    );
    if (prior && !prior.matches) {
      throw mapHoldError(
        "idempotency_conflict",
        "Idempotency key reused with different request fingerprint",
      );
    }
    return prior;
  }

  customerPrice(candidate: HydratedCandidate, body?: Record<string, unknown>) {
    const { snap, billingAccount, model, operation, clientBody, isStream } = this.request;
    const signals = inspectMeteredRequestSignals(body ?? clientBody, operation);
    return resolveAttemptPricing({
      snap,
      billingAccount,
      modelName: model.name,
      operation,
      candidate,
      ratedAt: () => this.ratedAt(),
      signals,
      isStream,
    });
  }

  quote(
    candidate: HydratedCandidate,
    customer: EffectiveCustomerPrice,
    measurement: ReservationMeasurement,
  ): AttemptQuote {
    const { state, snap, outputBound } = this.request;
    if (customer.customerSnapshot || customer.supplierSnapshot) {
      return {
        candidate,
        customer,
        customerAtoms: customer.customerSnapshot
          ? quoteSnapshotReservation(customer.customerSnapshot, measurement, outputBound)
              .amount_atoms
          : state.quoteForPrice(
              snap.epoch,
              customer.price,
              measurement,
              outputBound,
              customer.cacheKey,
            ),
        supplierAtoms: customer.supplierSnapshot
          ? quoteSnapshotReservation(customer.supplierSnapshot, measurement, outputBound)
              .amount_atoms
          : state.quoteForPrice(snap.epoch, candidate.costPrice, measurement, outputBound),
      };
    }
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

  private holdExpiresAt(candidate: HydratedCandidate): string {
    const { deps, state, isStream, maxStreamDurationMs } = this.request;
    const unaryGraceMs = !isStream ? (deps.config.unaryClientAbortGraceMs ?? 600_000) : 0;
    const effectiveAttemptMs =
      candidate.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs;
    const minUnaryHoldMs = unaryGraceMs > 0 ? effectiveAttemptMs + 30_000 : 0;
    return state.cachedExpiresAt(
      Math.max(deps.config.holdTtlMs, minUnaryHoldMs) + (isStream ? maxStreamDurationMs : 0),
    );
  }

  async acquire(attemptId: string, quote: AttemptQuote, idempotency: Idempotency | null) {
    const { deps, requestId, operation, billingAccount, key, model } = this.request;
    const { candidate, customer, customerAtoms, supplierAtoms } = quote;
    const result = await deps.ledger.acquireHold({
      request_id: requestId,
      attempt_id: attemptId,
      billing_account_id: billingAccount.id,
      api_key_id: key.id,
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
      supplier_cost_version_id: customer.supplierSnapshot?.rate_card_id ?? candidate.costPrice.id,
      customer_price_version_id: customer.customerSnapshot?.rate_card_id ?? customer.price.id,
      ...(customer.customerSnapshot
        ? { customer_pricing_snapshot: customer.customerSnapshot }
        : {}),
      ...(customer.supplierSnapshot
        ? { supplier_pricing_snapshot: customer.supplierSnapshot }
        : {}),
      supplier_channel_id: candidate.channel.id,
      supplier_currency: customer.supplierSnapshot?.currency ?? candidate.costPrice.currency,
      supplier_atoms: supplierAtoms,
      expires_at: this.holdExpiresAt(candidate),
    });
    if (result.ok && !result.replayed) {
      this.hold = "held";
      this.heldCustomerAtoms = BigInt(result.held_atoms);
      this.heldSupplierAtoms = BigInt(supplierAtoms);
    }
    return result;
  }

  get isSettled(): boolean {
    return this.hold === "settled";
  }

  get heldLimits(): { customerAtoms: bigint; supplierAtoms: bigint } {
    return {
      customerAtoms: this.heldCustomerAtoms,
      supplierAtoms: this.heldSupplierAtoms,
    };
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
      next_currency: customer.supplierSnapshot?.currency ?? candidate.costPrice.currency,
      next_supplier_atoms: supplierAtoms,
      next_supplier_cost_version_id:
        customer.supplierSnapshot?.rate_card_id ?? candidate.costPrice.id,
      next_customer_price_version_id: customer.customerSnapshot?.rate_card_id ?? customer.price.id,
      next_customer_price_adjustment: {
        override_id: customer.overrideId,
        discount_numerator: customer.discountNumerator,
        discount_denominator: customer.discountDenominator,
      },
      ...(customer.customerSnapshot
        ? { next_customer_pricing_snapshot: customer.customerSnapshot }
        : {}),
      ...(customer.supplierSnapshot
        ? { next_supplier_pricing_snapshot: customer.supplierSnapshot }
        : {}),
      expires_at: this.holdExpiresAt(candidate),
      ...(providerRequestId ? { failed_provider_request_id: providerRequestId } : {}),
      ...(BigInt(customerAtoms) > this.heldCustomerAtoms
        ? { required_customer_atoms: customerAtoms }
        : {}),
    });
    if (result.ok) {
      this.heldCustomerAtoms = BigInt(result.held_atoms);
      this.heldSupplierAtoms = BigInt(supplierAtoms);
    }
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
      if (!details.allowAfterClientAbort && c.req.raw.signal.aborted) {
        throw new GatewayError(499, "client_aborted", "Client disconnected");
      }
      const input: CommitTerminalInput = {
        request_id: requestId,
        billing_account_id: billingAccount.id,
        selected_attempt_id: attemptId,
        channel_id: candidate.channel.id,
        billing_price_id: customer.customerSnapshot?.rate_card_id ?? customer.price.id,
        cost_version_id: customer.supplierSnapshot?.rate_card_id ?? candidate.costPrice.id,
        api_key_id: key.id,
        pricing_override_id: customer.overrideId,
        discount_numerator: customer.discountNumerator,
        discount_denominator: customer.discountDenominator,
        billing_pricing: customer.customerSnapshot
          ? {
              currency: customer.customerSnapshot.currency,
              rates: customer.customerSnapshot.effective_card.rates,
            }
          : state.pricingOf(customer.price),
        billing_basis: customer.customerSnapshot?.basis ?? customer.price.basis,
        supplier_basis: customer.supplierSnapshot?.basis ?? candidate.costPrice.basis,
        supplier_pricing: customer.supplierSnapshot
          ? {
              currency: customer.supplierSnapshot.currency,
              rates: customer.supplierSnapshot.effective_card.rates,
            }
          : state.pricingOf(candidate.costPrice),
        supplier_currency: customer.supplierSnapshot?.currency ?? candidate.costPrice.currency,
        ...(customer.customerSnapshot
          ? { customer_pricing_snapshot: customer.customerSnapshot }
          : {}),
        ...(customer.supplierSnapshot
          ? { supplier_pricing_snapshot: customer.supplierSnapshot }
          : {}),
        usage: details.usage,
      };
      if (chars !== undefined) input.input_characters = String(chars);
      if (details.plugin_audit !== undefined) input.plugin_audit = details.plugin_audit;
      if (details.charge_policy !== undefined) input.charge_policy = details.charge_policy;
      if (details.moderation_blocked !== undefined)
        input.moderation_blocked = details.moderation_blocked;
      if (details.async_task !== undefined) input.async_task = details.async_task;
      const result = await deps.ledger.commitTerminal(input).catch(() => null);
      if (
        result?.ok === false &&
        (result.code === "invalid_upstream_usage" || result.code === "usage_alias_conflict")
      ) {
        this.telemetry.noteRoutingInvalidUsage();
        throw new GatewayError(502, result.code, result.message);
      }
      if (!result?.ok) {
        deps.metrics.ledgerCommitFailures++;
        throw new GatewayError(503, "ledger_commit_failed", "Ledger could not confirm settlement");
      }
      this.hold = "settled";
      this.telemetry.observeRoutingSettledUsage(details.usage.output_tokens);
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
