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

export function createInferenceAccounting(
  request: InferenceRequest,
  telemetry: InferenceTelemetry,
) {
  const { deps, state, requestId, snap, operation, billingAccount, key, model } = request;
  const { ledger } = deps;
  let heldCustomerAtoms = 0n;
  let hold: "none" | "held" | "settled" | "deferred" = "none";
  let finalizing: Promise<boolean> | undefined;
  let dispatched = false;
  let unknownAttempt = false;
  let providerRequestIds: Map<string, string> | undefined;

  async function finalize(mode: "release" | "unknown"): Promise<boolean> {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        if (mode === "release") await ledger.releaseHold(requestId);
        else await ledger.markUnknown(requestId, providerRequestIds);
        hold = "settled";
        return true;
      } catch {
        if (attempt === 0) await Bun.sleep(15);
      }
    }
    hold = "deferred";
    return false;
  }

  function finalizeHold(mode: "release" | "unknown"): Promise<boolean> {
    if (hold === "none" || hold === "settled") return Promise.resolve(true);
    return (finalizing ??= finalize(mode).finally(() => {
      finalizing = undefined;
    }));
  }

  async function fingerprintRequest(
    key: string,
    policyBody: Record<string, unknown>,
  ): Promise<Idempotency> {
    const original = await fingerprintInference(request, request.clientBody);
    const legacy =
      request.multipartEntries || policyBody === request.clientBody
        ? original
        : await fingerprintInference(request, policyBody);
    return { key, fingerprint: `v1:${original}`, legacy_fingerprint: legacy };
  }

  return {
    identifyIdempotency(
      header: string | null,
      policyBody: Record<string, unknown>,
    ): Promise<Idempotency> | null {
      const key = header?.trim();
      return key ? fingerprintRequest(key, policyBody) : null;
    },
    async findReplay(idempotency: Idempotency) {
      const prior = await ledger.findIdempotentHold(
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
    },
    customerPrice(candidate: HydratedCandidate) {
      return resolveEffectiveCustomerPrice(
        snap,
        billingAccount.id,
        model.name,
        operation,
        candidate.billingPrice,
      );
    },
    quote(
      candidate: HydratedCandidate,
      customer: EffectiveCustomerPrice,
      measurement: ReservationMeasurement,
    ): AttemptQuote {
      return {
        candidate,
        customer,
        customerAtoms: state.quoteForPrice(
          snap.epoch,
          customer.price,
          measurement,
          request.outputBound,
          customer.cacheKey,
        ),
        supplierAtoms: state.quoteForPrice(
          snap.epoch,
          candidate.costPrice,
          measurement,
          request.outputBound,
        ),
      };
    },
    async acquire(attemptId: string, quote: AttemptQuote, idempotency: Idempotency | null) {
      const { candidate, customer, customerAtoms, supplierAtoms } = quote;
      const result = await ledger.acquireHold({
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
          deps.config.holdTtlMs + (request.isStream ? request.maxStreamDurationMs : 0),
        ),
      });
      if (result.ok && !result.replayed) {
        hold = "held";
        heldCustomerAtoms = BigInt(result.held_atoms);
      }
      return result;
    },
    async switchSupplier(
      previous: { attemptId: string; outcome: "FAILED" | "UNKNOWN" },
      nextAttemptId: string,
      quote: AttemptQuote,
    ) {
      const { candidate, customer, customerAtoms, supplierAtoms } = quote;
      const providerRequestId = providerRequestIds?.get(previous.attemptId);
      const result = await ledger.switchSupplierAttempt({
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
        ...(BigInt(customerAtoms) > heldCustomerAtoms
          ? { required_customer_atoms: customerAtoms }
          : {}),
      });
      if (result.ok) heldCustomerAtoms = BigInt(result.held_atoms);
      return result;
    },
    setDispatched(value: boolean) {
      dispatched = value;
    },
    noteUnknownAttempt() {
      unknownAttempt = true;
    },
    get unknownAttempt() {
      return unknownAttempt;
    },
    async rememberProviderRequestId(attemptId: string, id: string) {
      (providerRequestIds ??= new Map()).set(attemptId, id);
      await ledger.rememberProviderRequestId(requestId, attemptId, id).catch(() => undefined);
    },
    finalizeHold,
    finishFailedRequest() {
      return finalizeHold(unknownAttempt ? "unknown" : "release");
    },
    rejectOutstandingHold() {
      if (hold === "held")
        return finalizeHold(dispatched || unknownAttempt ? "unknown" : "release");
    },
    bindSettlement(
      attemptId: string,
      candidate: HydratedCandidate,
      customer: EffectiveCustomerPrice,
      chars: number | undefined,
      pluginSignal: AbortSignal,
    ) {
      return async (details: ChargeDetails) => {
        pluginSignal.throwIfAborted();
        if (request.c.req.raw.signal.aborted) {
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
        const result = await ledger.commitTerminal(input).catch(() => null);
        if (
          result?.ok === false &&
          (result.code === "invalid_upstream_usage" || result.code === "usage_alias_conflict")
        ) {
          throw new GatewayError(502, result.code, result.message);
        }
        if (!result?.ok) {
          deps.metrics.ledgerCommitFailures++;
          throw new GatewayError(
            503,
            "ledger_commit_failed",
            "Ledger could not confirm settlement",
          );
        }
        hold = "settled";
        telemetry.requestTrace?.recordSettlement(attemptId);
        return result;
      };
    },
  };
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

export type InferenceAccounting = ReturnType<typeof createInferenceAccounting>;
export type AttemptSettlement = ReturnType<InferenceAccounting["bindSettlement"]>;
