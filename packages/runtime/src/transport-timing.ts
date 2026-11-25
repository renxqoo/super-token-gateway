export type UpstreamPhase =
  | "dns_start"
  | "dns_end"
  | "wait_start"
  | "wait_end"
  | "dial_start"
  | "tcp_ready"
  | "tls_ready"
  | "connection_ready"
  | "connection_reused"
  | "request_written"
  | "response_headers";

export type UpstreamTimingObserver = (phase: UpstreamPhase, at: number) => void;

export function markUpstream(observer: UpstreamTimingObserver | undefined, phase: UpstreamPhase) {
  if (!observer) return;
  try {
    observer(phase, performance.now());
  } catch {
    return;
  }
}
