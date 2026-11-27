import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import type { ChannelRecord } from "@gateway/ledger";
import { formatProtocolError } from "@gateway/protocol";
import { IngressDenialError } from "../public-ingress.ts";
import type { TraceContext } from "../observability.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import type { InferenceContext } from "./types.ts";

export function supplierReconciliation({ route, channel }: HydratedCandidate) {
  const providerIdHeader =
    route.adapter_overrides.reconciliation_response_id_header ??
    channel.adapter_config.reconciliation_response_id_header;
  const requiresEvidence =
    route.adapter_overrides.reconciliation_evidence_required ??
    channel.adapter_config.reconciliation_evidence_required ??
    Boolean(providerIdHeader);
  return { providerIdHeader, requiresEvidence };
}

export function getHeaderValue(
  headers: Headers | Record<string, string>,
  name: string,
): string | null {
  if (headers instanceof Headers) return headers.get(name);
  return headers[name] ?? null;
}

export function isRetryableStatus(
  status: number,
): "rate_limit" | "auth_error" | "retryable" | null {
  if (status === 429) return "rate_limit";
  if (status === 401 || status === 403) return "auth_error";
  if (status === 500 || status === 502 || status === 503 || status === 504 || status === 529) {
    return "retryable";
  }
  return null;
}

export function responseHeaders(
  requestId: string,
  trace: TraceContext,
  extra: Record<string, string> = {},
): Record<string, string> {
  return {
    "x-request-id": requestId,
    "x-trace-id": trace.traceId,
    traceparent: trace.traceparent,
    ...extra,
  };
}

const safeChannelHeaderNames = new WeakMap<Pick<ChannelRecord, "id" | "name">, string>();

export function channelJsonHeaders(
  requestId: string,
  trace: TraceContext,
  channel: Pick<ChannelRecord, "id" | "name">,
  droppedFields: readonly string[],
): Record<string, string> {
  let name = safeChannelHeaderNames.get(channel);
  if (name === undefined) {
    name = /[^\x20-\x7e\xa0-\xff]/.test(channel.name) ? channel.id : channel.name;
    safeChannelHeaderNames.set(channel, name);
  }
  if (droppedFields.length === 0) {
    return {
      "x-request-id": requestId,
      "x-trace-id": trace.traceId,
      traceparent: trace.traceparent,
      "x-gateway-channel": name,
      "content-type": "application/json",
    };
  }
  return {
    "x-request-id": requestId,
    "x-trace-id": trace.traceId,
    traceparent: trace.traceparent,
    "x-gateway-channel": name,
    "content-type": "application/json",
    "x-gateway-dropped-fields": droppedFields.join(","),
  };
}

export function channelResponseHeaders(
  requestId: string,
  trace: TraceContext,
  channel: Pick<ChannelRecord, "id" | "name">,
  extra: Record<string, string>,
): Record<string, string> {
  let name = safeChannelHeaderNames.get(channel);
  if (name === undefined) {
    name = /[^\x20-\x7e\xa0-\xff]/.test(channel.name) ? channel.id : channel.name;
    safeChannelHeaderNames.set(channel, name);
  }
  const out: Record<string, string> = {
    "x-request-id": requestId,
    "x-trace-id": trace.traceId,
    traceparent: trace.traceparent,
    "x-gateway-channel": name,
  };
  for (const k in extra) out[k] = extra[k]!;
  return out;
}

export function errorJson(
  c: InferenceContext,
  operation: Operation,
  err: unknown,
  requestId: string,
  trace: TraceContext,
): Response {
  const gw =
    err instanceof GatewayError
      ? err
      : new GatewayError(500, "internal_error", "Internal gateway error");
  const extraHeaders: Record<string, string> =
    gw instanceof IngressDenialError
      ? { "retry-after": gw.retryAfter }
      : gw.code === "plugin_capacity_unavailable" ||
          gw.code === "provider_capacity_unavailable" ||
          gw.code === "request_body_capacity_unavailable" ||
          gw.code === "upstream_capacity"
        ? { "retry-after": "1" }
        : gw.status === 408 || gw.status === 413 || gw.code === "overloaded"
          ? { connection: "close" }
          : {};
  return c.json(
    formatProtocolError(operation, gw.status, gw.code, gw.message, requestId),
    gw.status as 400,
    responseHeaders(requestId, trace, extraHeaders),
  );
}

export function sanitizeHttpErrorStatus(status: number | undefined, fallback = 400): number {
  return typeof status === "number" && Number.isInteger(status) && status >= 400 && status <= 599
    ? status
    : fallback;
}
