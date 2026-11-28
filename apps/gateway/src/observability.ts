import { createRoutingMetrics } from "./routing-metrics.ts";
import {
  bumpLabeled,
  escapeLabel,
  Histogram,
  STREAM_BUCKETS_SECONDS,
  type LabeledCounter,
} from "./prometheus.ts";
import type {
  PluginObservation,
  WasmExecutorStats,
  WebhookExecutorStats,
} from "@gateway/plugin-engine";
import type { FirstTextTraceRecord } from "./first-text-trace.ts";
import type {
  ChannelFailureKind,
  ChannelFailureStage,
  TraceAttempt,
  TransportTiming,
} from "./trace-types.ts";

export type TraceContext = {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  flags: string;
  traceparent: string;
};

const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const TRACE_POOL = Buffer.allocUnsafe(4096);
let traceHex = "";
let traceHexOffset = 8192;

function nextTraceHexOffset(chars: number): number {
  if (traceHexOffset + chars > 8192) {
    crypto.getRandomValues(TRACE_POOL);
    traceHex = TRACE_POOL.toString("hex");
    traceHexOffset = 0;
  }
  const offset = traceHexOffset;
  traceHexOffset += chars;
  return offset;
}

export function parseOrCreateTrace(headerValue: string | undefined): TraceContext {
  const match = headerValue ? TRACEPARENT_RE.exec(headerValue.trim().toLowerCase()) : null;
  let traceId: string;
  let spanId: string;
  const parent =
    match && match[1] !== "00000000000000000000000000000000" && match[2] !== "0000000000000000"
      ? match
      : null;
  if (parent) {
    traceId = parent[1]!;
    const offset = nextTraceHexOffset(16);
    spanId = traceHex.slice(offset, offset + 16);
  } else {
    const offset = nextTraceHexOffset(48);
    traceId = traceHex.slice(offset, offset + 32);
    spanId = traceHex.slice(offset + 32, offset + 48);
  }
  const flags = parent ? parent[3]! : "01";
  return {
    traceId,
    spanId,
    ...(parent ? { parentSpanId: parent[2]! } : {}),
    flags,
    traceparent: `00-${traceId}-${spanId}-${flags}`,
  };
}

const PLUGIN_BUCKETS_MS = [0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 2000];

function statusClassOf(status: number): string {
  if (status >= 200 && status < 300) return "2xx";
  if (status >= 400 && status < 500) return "4xx";
  if (status >= 500 && status < 600) return "5xx";
  return `${Math.floor(status / 100)}xx`;
}

export type AccessLogRecord = {
  trace_id: string;
  request_id: string;
  billing_account_id: string | null;
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
  first_text_trace?: FirstTextTraceRecord;
  usage_source?: "provider" | "estimated";
  error_code: string | null;
};

const maxAccessLogBytes = 16 * 1024;

type TenantAbsorbWindow = {
  windowStartMs: number;
  count: number;
  atoms: bigint;
};

type AmbiguousRetryWindow = {
  windowStartMs: number;
  count: number;
};

const STREAM_PHASE_NAMES = [
  "gateway_prepare",
  "dns",
  "connection_wait",
  "tcp",
  "tls",
  "request_write",
  "upstream_headers",
  "upstream_first_text",
  "gateway_first_text_relay",
  "gateway_first_text",
] as const;

type StreamPhaseCache = {
  stages: (Histogram | undefined)[];
  outcomes: Map<string, LabeledCounter<readonly [string, string]>>;
};

export class GatewayMetrics {
  requestsTotal = 0;
  failoversTotal = 0;
  ambiguousRetriesCappedTotal = 0;
  moderationBlocksTotal = 0;
  estimatedStreamSettlements = 0;
  ledgerCommitFailures = 0;
  leaseRenewalFailures = 0;
  adminAuditFailures = 0;
  activeStreams = 0;
  waitingFirstText = 0;
  traceCaptureFailures = 0;
  webhookAdmissionRejections = 0;
  wasmAdmissionRejections = 0;
  pluginIntegrityFailures = 0;
  pluginPlatformAbsorbTotal = 0;
  pluginPlatformAbsorbAtoms = 0n;
  snapshotDiverged = false;
  private readonly httpCounters = new Map<
    string,
    LabeledCounter<readonly [string, string, string]>
  >();
  private lastHttpOp = "";
  private lastHttpStatus = -1;
  private lastHttpChannel: string | null = null;
  private lastHttpCounter: LabeledCounter<readonly [string, string, string]> | undefined;
  private readonly upstreamErrorCounters = new Map<
    string,
    LabeledCounter<readonly [string, string]>
  >();
  private readonly channelFailureCounters = new Map<
    string,
    LabeledCounter<readonly [string, ChannelFailureStage, ChannelFailureKind]>
  >();
  private readonly channelFailureOverflow = new Map<
    string,
    LabeledCounter<readonly [ChannelFailureStage, ChannelFailureKind]>
  >();
  private readonly pluginInvocations = new Map<
    string,
    LabeledCounter<readonly [string, string, string, string]>
  >();
  private readonly pluginFaults = new Map<
    string,
    LabeledCounter<readonly [string, string, string]>
  >();
  private readonly pluginCapacityRejections = new Map<
    string,
    LabeledCounter<readonly [string, string, string]>
  >();
  private readonly pluginFailOpen = new Map<string, LabeledCounter<readonly [string, string]>>();
  private readonly protocolFieldDropped = new Map<
    string,
    LabeledCounter<readonly [string, string, string]>
  >();
  private readonly protocolFieldRejected = new Map<
    string,
    LabeledCounter<readonly [string, string, string]>
  >();
  private readonly tenantAbsorbWindows = new Map<string, TenantAbsorbWindow>();
  private readonly ambiguousRetryWindows = new Map<string, AmbiguousRetryWindow>();
  private readonly requestDuration = new Histogram();
  private readonly ttftDuration = new Histogram();
  private readonly pluginDuration = new Histogram(PLUGIN_BUCKETS_MS);
  private readonly streamPhases = new Map<string, Histogram>();
  private readonly streamPhaseSets = new Map<string, StreamPhaseCache>();
  private readonly firstTextOutcomes = new Map<string, LabeledCounter<readonly [string, string]>>();
  readonly routing = createRoutingMetrics();

  constructor(private readonly accessLogEnabled: boolean) {}

  get isAccessLogEnabled(): boolean {
    return this.accessLogEnabled;
  }

  private observeStage(
    cache: StreamPhaseCache,
    operation: string,
    index: number,
    from: number | undefined,
    to: number | undefined,
  ): void {
    if (from === undefined || to === undefined || to < from) return;
    let h = cache.stages[index];
    if (!h) {
      const key = `${operation}\0${STREAM_PHASE_NAMES[index]!}`;
      h = this.streamPhases.get(key);
      if (!h) {
        h = new Histogram(STREAM_BUCKETS_SECONDS);
        this.streamPhases.set(key, h);
      }
      cache.stages[index] = h;
    }
    h.observe((to - from) / 1000);
  }

  observeStreamPhases(
    operation: string,
    status: number,
    point: {
      requestStart: number;
      attempt: TraceAttempt | undefined;
      enqueued: number | undefined;
      admission: TransportTiming | undefined;
    },
  ): void {
    const { requestStart, attempt, enqueued, admission } = point;
    const transport = attempt?.transport ?? admission;
    let cache = this.streamPhaseSets.get(operation);
    if (!cache) {
      cache = {
        stages: Array.from({ length: STREAM_PHASE_NAMES.length }, () => undefined),
        outcomes: new Map(),
      };
      this.streamPhaseSets.set(operation, cache);
    }
    const outcome =
      enqueued !== undefined
        ? "sent"
        : status === 499
          ? "cancelled"
          : status >= 400
            ? "failed"
            : "no_text";
    let outcomeCounter = cache.outcomes.get(outcome);
    if (outcomeCounter) {
      outcomeCounter.count++;
    } else {
      const key = `${operation}\0${outcome}`;
      outcomeCounter = this.firstTextOutcomes.get(key);
      if (outcomeCounter) {
        outcomeCounter.count++;
        cache.outcomes.set(outcome, outcomeCounter);
      } else if (this.firstTextOutcomes.size < 2048) {
        outcomeCounter = { labels: [operation, outcome] as const, count: 1 };
        this.firstTextOutcomes.set(key, outcomeCounter);
        cache.outcomes.set(outcome, outcomeCounter);
      }
    }
    this.observeStage(cache, operation, 0, requestStart, attempt?.start);
    if (transport) {
      this.observeStage(cache, operation, 1, transport.dns_start, transport.dns_end);
      if (transport.first_wait !== undefined) {
        this.observeStage(cache, operation, 2, 0, transport.wait_ms);
      }
      this.observeStage(cache, operation, 3, transport.dial_start, transport.tcp_ready);
      this.observeStage(cache, operation, 4, transport.tcp_ready, transport.tls_ready);
      this.observeStage(cache, operation, 5, transport.connection_ready, transport.request_written);
      this.observeStage(cache, operation, 6, transport.request_written, transport.response_headers);
      this.observeStage(cache, operation, 7, transport.response_headers, attempt?.firstText);
    }
    this.observeStage(cache, operation, 8, attempt?.firstText, enqueued);
    this.observeStage(cache, operation, 9, requestStart, enqueued);
  }

  observeRequestFast(
    operation: string,
    status: number,
    channel: string | null,
    durationMs: number,
    ttftMs: number | null,
  ): void {
    if (
      this.lastHttpCounter !== undefined &&
      this.lastHttpOp === operation &&
      this.lastHttpStatus === status &&
      this.lastHttpChannel === channel
    ) {
      this.lastHttpCounter.count++;
    } else {
      const statusClass = statusClassOf(status);
      const ch = channel ?? "none";
      const key = `${operation}\0${statusClass}\0${ch}`;
      let entry = this.httpCounters.get(key);
      if (entry !== undefined) {
        entry.count++;
      } else if (this.httpCounters.size < 2048) {
        entry = { labels: [operation, statusClass, ch] as const, count: 1 };
        this.httpCounters.set(key, entry);
      }
      if (entry !== undefined) {
        this.lastHttpOp = operation;
        this.lastHttpStatus = status;
        this.lastHttpChannel = channel;
        this.lastHttpCounter = entry;
      }
    }
    this.requestDuration.observe(durationMs);
    if (ttftMs !== null) {
      this.ttftDuration.observe(ttftMs);
    }
  }

  observeRequest(record: AccessLogRecord): void {
    this.observeRequestFast(
      record.operation,
      record.status,
      record.channel,
      record.duration_ms,
      record.ttft_ms,
    );
    if (this.accessLogEnabled) {
      const ts = new Date().toISOString();
      const level = record.status >= 500 ? "error" : "info";
      const line = JSON.stringify({
        ts,
        level,
        service: "gateway",
        ...record,
        duration_ms: Number(record.duration_ms.toFixed(2)),
        ttft_ms: record.ttft_ms === null ? null : Number(record.ttft_ms.toFixed(2)),
      });
      if (Buffer.byteLength(line) <= maxAccessLogBytes) {
        console.log(line);
      } else {
        console.log(
          JSON.stringify({
            ts,
            level,
            service: "gateway",
            request_id: record.request_id,
            trace_id: record.trace_id,
            operation: record.operation,
            status: record.status,
            log_truncated: true,
          }),
        );
      }
    }
  }

  recordUpstreamError(channel: string, kind: string): void {
    const key = `${channel}\0${kind}`;
    bumpLabeled(this.upstreamErrorCounters, key, () => [channel, kind] as const);
  }

  recordChannelFailure(
    channel: string,
    stage: ChannelFailureStage,
    kind: ChannelFailureKind,
  ): void {
    const recorded = bumpLabeled(
      this.channelFailureCounters,
      `${channel}\0${stage}\0${kind}`,
      () => [channel, stage, kind] as const,
    );
    if (!recorded)
      bumpLabeled(this.channelFailureOverflow, `${stage}\0${kind}`, () => [stage, kind] as const);
  }

  observePlugin(event: PluginObservation): void {
    this.pluginDuration.observe(event.latency_ms);
    const action = event.blocked ? "BLOCK" : "PASS";
    bumpLabeled(
      this.pluginInvocations,
      `${event.plugin_id}\0${event.hook}\0${event.outcome_kind}\0${action}`,
      () => [event.plugin_id, event.hook, event.outcome_kind, action] as const,
    );
    if (event.fail_open) {
      bumpLabeled(
        this.pluginFailOpen,
        `${event.plugin_id}\0${event.hook}`,
        () => [event.plugin_id, event.hook] as const,
      );
    }
    if (event.outcome_kind === "plugin_fault") {
      const ruleCode = event.rule_code ?? "plugin_fault";
      bumpLabeled(
        this.pluginFaults,
        `${event.plugin_id}\0${event.hook}\0${ruleCode}`,
        () => [event.plugin_id, event.hook, ruleCode] as const,
      );
    } else if (event.outcome_kind === "host_capacity") {
      const ruleCode = event.rule_code ?? "host_capacity";
      bumpLabeled(
        this.pluginCapacityRejections,
        `${event.plugin_id}\0${event.hook}\0${ruleCode}`,
        () => [event.plugin_id, event.hook, ruleCode] as const,
      );
    }
  }

  recordPluginIntegrityFailure(): void {
    this.pluginIntegrityFailures++;
  }

  recordSnapshotDiverged(): void {
    this.snapshotDiverged = true;
  }

  isTenantPlatformAbsorbExceeded(
    billingAccountId: string,
    maxCountPerMinute: number,
    maxAtomsPerMinute?: string,
  ): boolean {
    const entry = this.tenantAbsorbWindows.get(billingAccountId);
    if (!entry) return false;
    if (performance.now() - entry.windowStartMs >= 60_000) {
      this.tenantAbsorbWindows.delete(billingAccountId);
      return false;
    }
    if (entry.count >= maxCountPerMinute) return true;
    if (maxAtomsPerMinute && entry.atoms >= BigInt(maxAtomsPerMinute)) return true;
    return false;
  }

  recordPlatformAbsorb(billingAccountId: string, supplierAtoms: string): void {
    const atoms = BigInt(supplierAtoms || "0");
    this.pluginPlatformAbsorbTotal++;
    this.pluginPlatformAbsorbAtoms += atoms;
    const now = performance.now();
    const existing = this.tenantAbsorbWindows.get(billingAccountId);
    if (!existing || now - existing.windowStartMs >= 60_000) {
      if (this.tenantAbsorbWindows.size >= 4096) this.tenantAbsorbWindows.clear();
      this.tenantAbsorbWindows.set(billingAccountId, { windowStartMs: now, count: 1, atoms });
    } else {
      existing.count++;
      existing.atoms += atoms;
    }
  }

  recordProtocolFieldDropped(source: string, target: string, field: string): void {
    bumpLabeled(
      this.protocolFieldDropped,
      `${source}\0${target}\0${field}`,
      () => [source, target, field] as const,
    );
  }

  recordProtocolFieldRejected(source: string, target: string, field: string): void {
    bumpLabeled(
      this.protocolFieldRejected,
      `${source}\0${target}\0${field}`,
      () => [source, target, field] as const,
    );
  }

  tryConsumeAmbiguousRetry(channelId: string, maxPerMinute: number): boolean {
    if (maxPerMinute <= 0) {
      this.ambiguousRetriesCappedTotal++;
      return false;
    }
    const now = performance.now();
    const existing = this.ambiguousRetryWindows.get(channelId);
    if (!existing || now - existing.windowStartMs >= 60_000) {
      if (this.ambiguousRetryWindows.size >= 4096) this.ambiguousRetryWindows.clear();
      this.ambiguousRetryWindows.set(channelId, { windowStartMs: now, count: 1 });
      return true;
    }
    if (existing.count >= maxPerMinute) {
      this.ambiguousRetriesCappedTotal++;
      return false;
    }
    existing.count++;
    return true;
  }

  renderPrometheus(gauges: {
    inFlight: number;
    configEpoch: number;
    openCircuits: number;
    redisConnected: boolean;
    redisRequired: boolean;
    ready: boolean;
    snapshotVerifiedAt: number;
    snapshotDiverged?: boolean;
    wasmRuntime?: string;
    contentWasmStats?: WasmExecutorStats | null;
    providerWasmStats?: WasmExecutorStats | null;
    webhookStats?: WebhookExecutorStats | null;
  }): string {
    const wasmRuntime = gauges.wasmRuntime ?? "wasmtime";
    const lines = [
      "# TYPE gateway_requests_total counter",
      `gateway_requests_total ${this.requestsTotal}`,
      "# TYPE gateway_failovers_total counter",
      `gateway_failovers_total ${this.failoversTotal}`,
      "# TYPE gateway_ambiguous_retries_capped_total counter",
      `gateway_ambiguous_retries_capped_total ${this.ambiguousRetriesCappedTotal}`,
      "# TYPE gateway_moderation_blocks_total counter",
      `gateway_moderation_blocks_total ${this.moderationBlocksTotal}`,
      "# TYPE gateway_ledger_commit_failures_total counter",
      `gateway_ledger_commit_failures_total ${this.ledgerCommitFailures}`,
      "# TYPE gateway_lease_renewal_failures_total counter",
      `gateway_lease_renewal_failures_total ${this.leaseRenewalFailures}`,
      "# TYPE gateway_admin_audit_failures_total counter",
      `gateway_admin_audit_failures_total ${this.adminAuditFailures}`,
      "# TYPE gateway_plugin_integrity_failures_total counter",
      `gateway_plugin_integrity_failures_total ${this.pluginIntegrityFailures}`,
      "# TYPE gateway_plugin_platform_absorb_total counter",
      `gateway_plugin_platform_absorb_total ${this.pluginPlatformAbsorbTotal}`,
      "# TYPE gateway_plugin_platform_absorb_atoms_total counter",
      `gateway_plugin_platform_absorb_atoms_total ${this.pluginPlatformAbsorbAtoms.toString()}`,
      "# TYPE gateway_active_streams gauge",
      `gateway_active_streams ${this.activeStreams}`,
      "# TYPE gateway_ready gauge",
      `gateway_ready ${gauges.ready ? 1 : 0}`,
      "# TYPE gateway_snapshot_diverged gauge",
      `gateway_snapshot_diverged ${gauges.snapshotDiverged || this.snapshotDiverged ? 1 : 0}`,
      "# TYPE gateway_snapshot_verified_timestamp_seconds gauge",
      `gateway_snapshot_verified_timestamp_seconds ${gauges.snapshotVerifiedAt}`,
      "# TYPE gateway_wasm_runtime_info gauge",
      `gateway_wasm_runtime_info{runtime="${escapeLabel(wasmRuntime)}"} 1`,
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
    ];
    const pools: [string, WasmExecutorStats | null | undefined][] = [
      ["content", gauges.contentWasmStats],
      ["provider", gauges.providerWasmStats],
    ];
    const activePools = pools.filter((entry): entry is [string, WasmExecutorStats] =>
      Boolean(entry[1]),
    );
    if (activePools.length > 0) {
      lines.push("# TYPE gateway_wasm_pool_active gauge");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_active{pool="${pool}"} ${st.active}`);
      }
      lines.push("# TYPE gateway_wasm_pool_queued gauge");
      for (const [pool, st] of activePools) {
        lines.push(
          `gateway_wasm_pool_queued{pool="${pool}",priority="ingress"} ${st.queuedIngress}`,
        );
        lines.push(`gateway_wasm_pool_queued{pool="${pool}",priority="egress"} ${st.queuedEgress}`);
      }
      lines.push("# TYPE gateway_wasm_pool_rejected_total counter");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_rejected_total{pool="${pool}"} ${st.rejectedTotal}`);
      }
      lines.push("# TYPE gateway_wasm_pool_shed_total counter");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_shed_total{pool="${pool}"} ${st.shedTotal}`);
      }
      lines.push("# TYPE gateway_wasm_pool_timeouts_total counter");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_timeouts_total{pool="${pool}"} ${st.timeoutTotal}`);
      }
      lines.push("# TYPE gateway_wasm_pool_traps_total counter");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_traps_total{pool="${pool}"} ${st.trapTotal}`);
      }
      lines.push("# TYPE gateway_wasm_pool_compiles_total counter");
      for (const [pool, st] of activePools) {
        lines.push(`gateway_wasm_pool_compiles_total{pool="${pool}"} ${st.compileTotal}`);
      }
    }
    if (gauges.webhookStats) {
      const entries = Object.values(gauges.webhookStats);
      const totalInFlight = entries.reduce((sum, item) => sum + item.inFlight, 0);
      const openWebhookCircuits = entries.filter((item) => item.open).length;
      lines.push(
        "# TYPE gateway_webhook_in_flight gauge",
        `gateway_webhook_in_flight ${totalInFlight}`,
        "# TYPE gateway_webhook_open_circuits gauge",
        `gateway_webhook_open_circuits ${openWebhookCircuits}`,
      );
    }
    lines.push("# TYPE gateway_http_requests_total counter");
    for (const entry of this.httpCounters.values()) {
      const [op, statusClass, channel] = entry.labels;
      lines.push(
        `gateway_http_requests_total{operation="${escapeLabel(op)}",status_class="${escapeLabel(statusClass)}",channel="${escapeLabel(channel)}"} ${entry.count}`,
      );
    }
    lines.push(...this.routing.render());
    lines.push("# TYPE gateway_upstream_errors_total counter");
    for (const entry of this.upstreamErrorCounters.values()) {
      const [channel, kind] = entry.labels;
      lines.push(
        `gateway_upstream_errors_total{channel="${escapeLabel(channel)}",kind="${escapeLabel(kind)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_channel_failures_total counter");
    for (const {
      labels: [channel, stage, kind],
      count,
    } of this.channelFailureCounters.values()) {
      lines.push(
        `gateway_channel_failures_total{channel="${escapeLabel(channel)}",stage="${stage}",kind="${kind}"} ${count}`,
      );
    }
    lines.push("# TYPE gateway_channel_failure_overflow_total counter");
    for (const {
      labels: [stage, kind],
      count,
    } of this.channelFailureOverflow.values()) {
      lines.push(
        `gateway_channel_failure_overflow_total{stage="${stage}",kind="${kind}"} ${count}`,
      );
    }
    lines.push("# TYPE gateway_plugin_invocations_total counter");
    for (const entry of this.pluginInvocations.values()) {
      const [plugin, hook, outcomeKind, action] = entry.labels;
      lines.push(
        `gateway_plugin_invocations_total{plugin="${escapeLabel(plugin)}",hook="${escapeLabel(hook)}",outcome_kind="${escapeLabel(outcomeKind)}",action="${escapeLabel(action)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_plugin_faults_total counter");
    for (const entry of this.pluginFaults.values()) {
      const [plugin, hook, ruleCode] = entry.labels;
      lines.push(
        `gateway_plugin_faults_total{plugin="${escapeLabel(plugin)}",hook="${escapeLabel(hook)}",rule_code="${escapeLabel(ruleCode)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_plugin_capacity_rejections_total counter");
    for (const entry of this.pluginCapacityRejections.values()) {
      const [plugin, hook, ruleCode] = entry.labels;
      lines.push(
        `gateway_plugin_capacity_rejections_total{plugin="${escapeLabel(plugin)}",hook="${escapeLabel(hook)}",rule_code="${escapeLabel(ruleCode)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_plugin_fail_open_total counter");
    for (const entry of this.pluginFailOpen.values()) {
      const [plugin, hook] = entry.labels;
      lines.push(
        `gateway_plugin_fail_open_total{plugin="${escapeLabel(plugin)}",hook="${escapeLabel(hook)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_protocol_field_dropped_total counter");
    for (const entry of this.protocolFieldDropped.values()) {
      const [source, target, field] = entry.labels;
      lines.push(
        `gateway_protocol_field_dropped_total{source="${escapeLabel(source)}",target="${escapeLabel(target)}",field="${escapeLabel(field)}"} ${entry.count}`,
      );
    }
    lines.push("# TYPE gateway_protocol_field_rejected_total counter");
    for (const entry of this.protocolFieldRejected.values()) {
      const [source, target, field] = entry.labels;
      lines.push(
        `gateway_protocol_field_rejected_total{source="${escapeLabel(source)}",target="${escapeLabel(target)}",field="${escapeLabel(field)}"} ${entry.count}`,
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
    lines.push(
      ...this.pluginDuration.render(
        "gateway_plugin_duration_ms",
        "Plugin hook execution duration in milliseconds",
      ),
    );
    lines.push(
      "# HELP gateway_sse_phase_seconds Measured streaming phase duration in seconds",
      "# TYPE gateway_sse_phase_seconds histogram",
    );
    for (const [key, histogram] of this.streamPhases) {
      const [operation, stage] = key.split("\0");
      lines.push(
        ...histogram
          .render("gateway_sse_phase_seconds", "", `operation="${operation}",stage="${stage}"`)
          .slice(2),
      );
    }
    lines.push("# TYPE gateway_sse_first_text_total counter");
    for (const {
      labels: [operation, outcome],
      count,
    } of this.firstTextOutcomes.values()) {
      lines.push(
        `gateway_sse_first_text_total{operation="${operation}",outcome="${outcome}"} ${count}`,
      );
    }
    lines.push(
      "# TYPE gateway_sse_waiting_first_text gauge",
      `gateway_sse_waiting_first_text ${this.waitingFirstText}`,
      "# TYPE gateway_estimated_stream_settlements_total counter",
      `gateway_estimated_stream_settlements_total ${this.estimatedStreamSettlements}`,
      `gateway_trace_capture_failures_total ${this.traceCaptureFailures}`,
      `gateway_webhook_admission_rejections_total ${this.webhookAdmissionRejections}`,
      `gateway_wasm_admission_rejections_total ${this.wasmAdmissionRejections}`,
    );
    return `${lines.join("\n")}\n`;
  }
}
