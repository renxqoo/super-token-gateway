import type { GatewayError } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";

import type { PluginAuditEntry } from "@gateway/ledger";
import type { FastUpstreamResult } from "@gateway/runtime";
import type { EffectiveCustomerPrice } from "../pricing.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { TraceAttempt } from "../trace-types.ts";
import type { PreparedUpstreamRequest } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import type { InferenceTelemetry } from "./telemetry.ts";
import type { InferenceResources, AttemptLifetime } from "./resources.ts";
import type { InferenceAccounting, AttemptSettlement } from "./accounting.ts";

export type RetryAttempt = { outcome: "FAILED" | "UNKNOWN"; error: GatewayError };
export type AttemptResponse = {
  request: InferenceRequest;
  resources: InferenceResources;
  telemetry: InferenceTelemetry;
  accounting: InferenceAccounting;
  attempt: AttemptLifetime;
  candidate: HydratedCandidate;
  customer: EffectiveCustomerPrice;
  prepared: PreparedUpstreamRequest;
  pluginSignal: AbortSignal;
  upstreamResp: Response | null;
  fastResult: FastUpstreamResult;
  upstreamStatus: number;
  upstreamContentType: string;
  isUpstreamSse: boolean;
  isUpstreamJson: boolean;
  isFakeJsonOnStream: boolean;
  latencyMs: number;
  startedAt: number;
  chars: number | undefined;
  authorizedImages: number | undefined;
  preRequestAudit: PluginAuditEntry[];
  settle: AttemptSettlement;
  rejectedOutcome: (notExecuted?: boolean) => RetryAttempt["outcome"];
  releaseBackups: () => void;
  releaseProbe: () => void;
  completeProbe: () => void;
  observeAttemptSuccess: (latencyMs: number) => void;
  observeAttemptFailure: (
    kind: "retryable" | "rate_limit" | "auth_error",
    retryAfterMs?: number,
    latencyMs?: number,
  ) => void;
  describeFailure: ((body?: unknown) => TraceAttempt["providerError"]) | undefined;
  describeEventFailure: ((data: string) => TraceAttempt["providerError"]) | undefined;
};

export type StreamResponse = {
  context: AttemptResponse;
  tracedUpstreamProtocol: Protocol | null;
  streamSignal: AbortSignal;
  settle: AttemptSettlement;
  clientCancelled: () => boolean;
  markProbeDone: () => void;
  confirmStreamProbe: () => void;
};
