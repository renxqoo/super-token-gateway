import { GatewayError } from "@gateway/contracts";
import type { AdaptiveRoutingPolicy, Operation } from "@gateway/kernel";
import type { RouteRetryPolicy } from "@gateway/ledger";
import type { HydratedCandidate } from "../proxy-state.ts";

export type AttemptOutcome = "FAILED" | "UNKNOWN";

export type FailoverPolicy = {
  requireExecutionEvidence: boolean;
  supplierReserveLimit?: NonNullable<AdaptiveRoutingPolicy["supplier_reserve_limit"]>;
};

export const defaultRouteAttempts = 3;

export function routeAttemptTimeoutMs(retry: RouteRetryPolicy, defaultTimeoutMs: number): number {
  return retry.attempt_timeout_ms ?? defaultTimeoutMs;
}

export function attemptLimit(
  { requireExecutionEvidence }: FailoverPolicy,
  retry: RouteRetryPolicy,
  candidates: number,
): number {
  const configured = Math.max(1, retry.max_attempts ?? defaultRouteAttempts);
  return Math.min(candidates, requireExecutionEvidence ? Math.min(2, configured) : configured);
}

export function attemptTimeoutWithinBudget(
  routeTimeoutMs: number,
  remainingMs: number,
  healthyBackups: number,
): number {
  if (healthyBackups === 0) return Math.min(routeTimeoutMs, remainingMs);
  return Math.min(
    routeTimeoutMs,
    Math.max(Math.min(100, remainingMs), Math.floor(remainingMs / (healthyBackups + 1))),
  );
}

export function allowsFallback(
  options: {
    requireExecutionEvidence: boolean;
    operation: Operation;
    outcome: AttemptOutcome;
    videoAdmissionRetained: boolean;
    retryOnAmbiguous: boolean | undefined;
  },
  consumeAmbiguousRetry: () => boolean,
): boolean {
  const { requireExecutionEvidence, operation, outcome, videoAdmissionRetained, retryOnAmbiguous } =
    options;
  if (videoAdmissionRetained) return false;
  if (outcome === "FAILED") return true;
  return (
    !requireExecutionEvidence &&
    operation !== "videos" &&
    Boolean(retryOnAmbiguous) &&
    consumeAmbiguousRetry()
  );
}

export function rejectedAttemptOutcome(
  notExecuted: boolean,
  requiresExecutionEvidence: boolean,
): AttemptOutcome {
  return notExecuted || !requiresExecutionEvidence ? "FAILED" : "UNKNOWN";
}

export function supplierReconciliation({
  route,
  channel,
}: Pick<HydratedCandidate, "route" | "channel">) {
  const providerIdHeader =
    route.adapter_overrides.reconciliation_response_id_header ??
    channel.adapter_config.reconciliation_response_id_header;
  const requiresEvidence =
    route.adapter_overrides.reconciliation_evidence_required ??
    channel.adapter_config.reconciliation_evidence_required ??
    Boolean(providerIdHeader);
  return { providerIdHeader, requiresEvidence };
}

export function enforceSupplierReserveLimit(
  { supplierReserveLimit: limit }: FailoverPolicy,
  currency: string,
  atoms: string,
): void {
  if (limit && (limit.currency !== currency || BigInt(atoms) > BigInt(limit.atoms))) {
    throw new GatewayError(
      503,
      "supplier_reserve_limit",
      "No permitted supplier reservation at the configured currency and limit",
    );
  }
}
