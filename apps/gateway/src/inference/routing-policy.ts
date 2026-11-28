import type { AdaptiveRoutingPolicy, Operation } from "@gateway/kernel";
import type { ModelRecord } from "@gateway/ledger";
import type { FailoverPolicy } from "./failover-policy.ts";

type AdaptiveOrderingPolicy = Pick<AdaptiveRoutingPolicy, "mode" | "first_content_target_ms"> & {
  requireUniformRetryPolicy: boolean;
};

export type InferenceRoutingPolicy = {
  ordering: AdaptiveOrderingPolicy | undefined;
  failover: FailoverPolicy;
};

const defaultOrdering: AdaptiveOrderingPolicy = {
  mode: "active",
  first_content_target_ms: 1500,
  requireUniformRetryPolicy: true,
};

const baselinePolicy: InferenceRoutingPolicy = {
  ordering: undefined,
  failover: { requireExecutionEvidence: false },
};

const weightedChatPolicy: InferenceRoutingPolicy = { ...baselinePolicy, ordering: defaultOrdering };

export function resolveInferenceRoutingPolicy(
  model: Pick<ModelRecord, "routing_strategy" | "adaptive_routing">,
  operation: Operation,
): InferenceRoutingPolicy {
  if (operation !== "chat" || model.adaptive_routing === false) return baselinePolicy;
  const configured = model.adaptive_routing;
  if (!configured)
    return model.routing_strategy === "WEIGHTED" ? weightedChatPolicy : baselinePolicy;
  const requireExecutionEvidence = configured.execution_policy === "protected";
  return {
    ordering: {
      mode: configured.mode,
      first_content_target_ms: configured.first_content_target_ms,
      requireUniformRetryPolicy: !requireExecutionEvidence,
    },
    failover: {
      requireExecutionEvidence,
      ...(configured.supplier_reserve_limit
        ? { supplierReserveLimit: configured.supplier_reserve_limit }
        : {}),
    },
  };
}
