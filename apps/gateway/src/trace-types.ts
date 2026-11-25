export type TransportTiming = {
  dns_start?: number;
  dns_end?: number;
  first_wait?: number;
  last_wait?: number;
  wait_ms: number;
  dial_start?: number;
  tcp_ready?: number;
  tls_ready?: number;
  connection_ready?: number;
  connection_reused: boolean;
  dial_count: number;
  request_written?: number;
  response_headers?: number;
};

export type TraceAttempt = {
  id: string;
  number: number;
  channel: string;
  start: number;
  end?: number;
  status?: number;
  error?: string;
  failureStage?: ChannelFailureStage;
  failureKind?: ChannelFailureKind;
  providerRequestId?: string;
  providerError?: { code: string; summary: string };
  firstText?: number;
  transport: TransportTiming;
};

export type ChannelFailureStage =
  | "pre_dispatch"
  | "request_prepare"
  | "transport"
  | "http_response"
  | "response_body"
  | "stream_prelude"
  | "stream";

export type ChannelFailureKind =
  | "dns"
  | "tls"
  | "connect"
  | "interrupted"
  | "timeout"
  | "capacity"
  | "supplier_funding"
  | "rate_limit"
  | "auth_error"
  | "client_error"
  | "retryable"
  | "protocol"
  | "other";

export type TraceRecord = {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  requestId: string;
  projectId: string;
  model: string | null;
  operation: string;
  status: number;
  firstTextTracked: boolean;
  error: string | null;
  origin: number;
  start: number;
  end: number;
  settledAttemptId?: string;
  failedAttempts: number;
  enqueued?: number;
  attempts: TraceAttempt[];
  omittedAttempts: number;
  admission?: TransportTiming;
};

export type SpanValue = { stringValue: string } | { intValue: string } | { boolValue: boolean };

export type StoredSpan = {
  trace_id: string;
  span_id: string;
  request_id: string;
  project_id: string | null;
  source: "gateway" | "client";
  start_ms: number;
  span: {
    traceId: string;
    spanId: string;
    parentSpanId?: string;
    name: string;
    kind: number;
    startTimeUnixNano: string;
    endTimeUnixNano: string;
    attributes: { key: string; value: SpanValue }[];
    events: { name: string; timeUnixNano: string }[];
    status: { code: number };
  };
};
