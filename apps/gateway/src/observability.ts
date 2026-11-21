export type TraceContext = {
  traceId: string;
  spanId: string;
  flags: string;
  traceparent: string;
};

const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));
const randBuf = new Uint8Array(24);

function randomHex(byteLength: number): string {
  const slice = randBuf.subarray(0, byteLength);
  crypto.getRandomValues(slice);
  let out = "";
  for (let i = 0; i < byteLength; i++) out += HEX[slice[i]!]!;
  return out;
}

export function parseOrCreateTrace(headerValue: string | undefined): TraceContext {
  const match = headerValue ? TRACEPARENT_RE.exec(headerValue.trim().toLowerCase()) : null;
  const traceId =
    match && match[1] !== "00000000000000000000000000000000" ? match[1]! : randomHex(16);
  const flags = match ? match[3]! : "01";
  const spanId = randomHex(8);
  return {
    traceId,
    spanId,
    flags,
    traceparent: `00-${traceId}-${spanId}-${flags}`,
  };
}

const DEFAULT_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 15000];

class Histogram {
  private readonly counts: number[];
  private sum = 0;
  private count = 0;

  constructor(private readonly buckets: readonly number[] = DEFAULT_BUCKETS_MS) {
    this.counts = Array.from({ length: buckets.length }, () => 0);
  }

  observe(value: number): void {
    const v = Number.isFinite(value) && value >= 0 ? value : 0;
    this.sum += v;
    this.count++;
    for (let i = 0; i < this.buckets.length; i++) {
      if (v <= this.buckets[i]!) this.counts[i]!++;
    }
  }

  render(name: string, help: string): string[] {
    const lines = [`# HELP ${name} ${help}`, `# TYPE ${name} histogram`];
    for (let i = 0; i < this.buckets.length; i++) {
      lines.push(`${name}_bucket{le="${this.buckets[i]}"} ${this.counts[i]}`);
    }
    lines.push(`${name}_bucket{le="+Inf"} ${this.count}`);
    lines.push(`${name}_sum ${this.sum.toFixed(3)}`);
    lines.push(`${name}_count ${this.count}`);
    return lines;
  }
}

function escapeLabel(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll('"', '\\"').replaceAll("\n", " ");
}

export type AccessLogRecord = {
  trace_id: string;
  request_id: string;
  project_id: string | null;
  model: string | null;
  operation: string;
  stream: boolean;
  channel: string | null;
  status: number;
  attempts: number;
  failover: boolean;
  moderation_blocked: boolean;
  duration_ms: number;
  ttft_ms: number | null;
  error_code: string | null;
};

export class GatewayMetrics {
  requestsTotal = 0;
  failoversTotal = 0;
  moderationBlocksTotal = 0;
  ledgerCommitFailures = 0;
  leaseRenewalFailures = 0;
  adminAuditFailures = 0;
  activeStreams = 0;
  private readonly httpCounters = new Map<string, number>();
  private readonly upstreamErrorCounters = new Map<string, number>();
  private readonly requestDuration = new Histogram();
  private readonly ttftDuration = new Histogram();

  constructor(private readonly accessLogEnabled: boolean) {}

  observeRequest(record: AccessLogRecord): void {
    const statusClass = `${Math.floor(record.status / 100)}xx`;
    const key = JSON.stringify([record.operation, statusClass, record.channel ?? "none"]);
    this.httpCounters.set(key, (this.httpCounters.get(key) ?? 0) + 1);
    this.requestDuration.observe(record.duration_ms);
    if (record.ttft_ms !== null) {
      this.ttftDuration.observe(record.ttft_ms);
    }
    if (this.accessLogEnabled) {
      console.log(
        JSON.stringify({
          ts: new Date().toISOString(),
          level: record.status >= 500 ? "error" : "info",
          service: "gateway",
          ...record,
          duration_ms: Number(record.duration_ms.toFixed(2)),
          ttft_ms: record.ttft_ms === null ? null : Number(record.ttft_ms.toFixed(2)),
        }),
      );
    }
  }

  recordUpstreamError(channel: string, kind: string): void {
    const key = JSON.stringify([channel, kind]);
    this.upstreamErrorCounters.set(key, (this.upstreamErrorCounters.get(key) ?? 0) + 1);
  }

  renderPrometheus(gauges: {
    inFlight: number;
    configEpoch: number;
    openCircuits: number;
    redisConnected: boolean;
    redisRequired: boolean;
    ready: boolean;
    snapshotVerifiedAt: number;
  }): string {
    const lines = [
      "# TYPE gateway_requests_total counter",
      `gateway_requests_total ${this.requestsTotal}`,
      "# TYPE gateway_failovers_total counter",
      `gateway_failovers_total ${this.failoversTotal}`,
      "# TYPE gateway_moderation_blocks_total counter",
      `gateway_moderation_blocks_total ${this.moderationBlocksTotal}`,
      "# TYPE gateway_ledger_commit_failures_total counter",
      `gateway_ledger_commit_failures_total ${this.ledgerCommitFailures}`,
      "# TYPE gateway_lease_renewal_failures_total counter",
      `gateway_lease_renewal_failures_total ${this.leaseRenewalFailures}`,
      "# TYPE gateway_admin_audit_failures_total counter",
      `gateway_admin_audit_failures_total ${this.adminAuditFailures}`,
      "# TYPE gateway_active_streams gauge",
      `gateway_active_streams ${this.activeStreams}`,
      "# TYPE gateway_ready gauge",
      `gateway_ready ${gauges.ready ? 1 : 0}`,
      "# TYPE gateway_snapshot_verified_timestamp_seconds gauge",
      `gateway_snapshot_verified_timestamp_seconds ${gauges.snapshotVerifiedAt}`,
      "# TYPE gateway_cluster_redis_required gauge",
      `gateway_cluster_redis_required ${gauges.redisRequired ? 1 : 0}`,
      "# TYPE gateway_in_flight gauge",
      `gateway_in_flight ${gauges.inFlight}`,
      "# TYPE gateway_config_epoch gauge",
      `gateway_config_epoch ${gauges.configEpoch}`,
      "# TYPE gateway_open_circuits gauge",
      `gateway_open_circuits ${gauges.openCircuits}`,
      "# TYPE gateway_cluster_redis_connected gauge",
      `gateway_cluster_redis_connected ${gauges.redisConnected ? 1 : 0}`,
      "# TYPE gateway_http_requests_total counter",
    ];
    for (const [key, val] of this.httpCounters) {
      const [op, statusClass, channel] = JSON.parse(key) as [string, string, string];
      lines.push(
        `gateway_http_requests_total{operation="${escapeLabel(op!)}",status_class="${escapeLabel(statusClass!)}",channel="${escapeLabel(channel!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_upstream_errors_total counter");
    for (const [key, val] of this.upstreamErrorCounters) {
      const [channel, kind] = JSON.parse(key) as [string, string];
      lines.push(
        `gateway_upstream_errors_total{channel="${escapeLabel(channel!)}",kind="${escapeLabel(kind!)}"} ${val}`,
      );
    }
    lines.push(
      ...this.requestDuration.render(
        "gateway_request_duration_ms",
        "End-to-end request duration in milliseconds",
      ),
    );
    lines.push(
      ...this.ttftDuration.render(
        "gateway_ttft_ms",
        "Time to first upstream response header in milliseconds",
      ),
    );
    return `${lines.join("\n")}\n`;
  }
}
