import type {
  PluginObservation,
  WasmExecutorStats,
  WebhookExecutorStats,
} from "@gateway/plugin-engine";

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
const PLUGIN_BUCKETS_MS = [0.25, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 2000];

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

function bumpCounter(map: Map<string, number>, key: string, maxEntries = 2048): void {
  const existing = map.get(key);
  if (existing !== undefined) {
    map.set(key, existing + 1);
    return;
  }
  if (map.size >= maxEntries) return;
  map.set(key, 1);
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

type TenantAbsorbWindow = {
  windowStartMs: number;
  count: number;
  atoms: bigint;
};

export class GatewayMetrics {
  requestsTotal = 0;
  failoversTotal = 0;
  moderationBlocksTotal = 0;
  ledgerCommitFailures = 0;
  leaseRenewalFailures = 0;
  adminAuditFailures = 0;
  activeStreams = 0;
  pluginIntegrityFailures = 0;
  pluginPlatformAbsorbTotal = 0;
  pluginPlatformAbsorbAtoms = 0n;
  snapshotDiverged = false;
  private readonly httpCounters = new Map<string, number>();
  private readonly upstreamErrorCounters = new Map<string, number>();
  private readonly pluginInvocations = new Map<string, number>();
  private readonly pluginFaults = new Map<string, number>();
  private readonly pluginCapacityRejections = new Map<string, number>();
  private readonly pluginFailOpen = new Map<string, number>();
  private readonly protocolFieldDropped = new Map<string, number>();
  private readonly protocolFieldRejected = new Map<string, number>();
  private readonly tenantAbsorbWindows = new Map<string, TenantAbsorbWindow>();
  private readonly requestDuration = new Histogram();
  private readonly ttftDuration = new Histogram();
  private readonly pluginDuration = new Histogram(PLUGIN_BUCKETS_MS);

  constructor(private readonly accessLogEnabled: boolean) {}

  observeRequest(record: AccessLogRecord): void {
    const statusClass = `${Math.floor(record.status / 100)}xx`;
    const key = JSON.stringify([record.operation, statusClass, record.channel ?? "none"]);
    bumpCounter(this.httpCounters, key);
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
    bumpCounter(this.upstreamErrorCounters, key);
  }

  observePlugin(event: PluginObservation): void {
    this.pluginDuration.observe(event.latency_ms);
    bumpCounter(
      this.pluginInvocations,
      JSON.stringify([
        event.plugin_id,
        event.hook,
        event.outcome_kind,
        event.blocked ? "BLOCK" : "PASS",
      ]),
    );
    if (event.fail_open) {
      bumpCounter(this.pluginFailOpen, JSON.stringify([event.plugin_id, event.hook]));
    }
    if (event.outcome_kind === "plugin_fault") {
      bumpCounter(
        this.pluginFaults,
        JSON.stringify([event.plugin_id, event.hook, event.rule_code ?? "plugin_fault"]),
      );
    } else if (event.outcome_kind === "host_capacity") {
      bumpCounter(
        this.pluginCapacityRejections,
        JSON.stringify([event.plugin_id, event.hook, event.rule_code ?? "host_capacity"]),
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
    projectId: string,
    maxCountPerMinute: number,
    maxAtomsPerMinute?: string,
  ): boolean {
    const entry = this.tenantAbsorbWindows.get(projectId);
    if (!entry) return false;
    if (performance.now() - entry.windowStartMs >= 60_000) {
      this.tenantAbsorbWindows.delete(projectId);
      return false;
    }
    if (entry.count >= maxCountPerMinute) return true;
    if (maxAtomsPerMinute && entry.atoms >= BigInt(maxAtomsPerMinute)) return true;
    return false;
  }

  recordPlatformAbsorb(projectId: string, supplierAtoms: string): void {
    const atoms = BigInt(supplierAtoms || "0");
    this.pluginPlatformAbsorbTotal++;
    this.pluginPlatformAbsorbAtoms += atoms;
    const now = performance.now();
    const existing = this.tenantAbsorbWindows.get(projectId);
    if (!existing || now - existing.windowStartMs >= 60_000) {
      if (this.tenantAbsorbWindows.size >= 4096) this.tenantAbsorbWindows.clear();
      this.tenantAbsorbWindows.set(projectId, { windowStartMs: now, count: 1, atoms });
    } else {
      existing.count++;
      existing.atoms += atoms;
    }
  }

  recordProtocolFieldDropped(source: string, target: string, field: string): void {
    bumpCounter(this.protocolFieldDropped, JSON.stringify([source, target, field]));
  }

  recordProtocolFieldRejected(source: string, target: string, field: string): void {
    bumpCounter(this.protocolFieldRejected, JSON.stringify([source, target, field]));
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
    lines.push("# TYPE gateway_plugin_invocations_total counter");
    for (const [key, val] of this.pluginInvocations) {
      const [plugin, hook, outcomeKind, action] = JSON.parse(key) as [
        string,
        string,
        string,
        string,
      ];
      lines.push(
        `gateway_plugin_invocations_total{plugin="${escapeLabel(plugin!)}",hook="${escapeLabel(hook!)}",outcome_kind="${escapeLabel(outcomeKind!)}",action="${escapeLabel(action!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_plugin_faults_total counter");
    for (const [key, val] of this.pluginFaults) {
      const [plugin, hook, ruleCode] = JSON.parse(key) as [string, string, string];
      lines.push(
        `gateway_plugin_faults_total{plugin="${escapeLabel(plugin!)}",hook="${escapeLabel(hook!)}",rule_code="${escapeLabel(ruleCode!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_plugin_capacity_rejections_total counter");
    for (const [key, val] of this.pluginCapacityRejections) {
      const [plugin, hook, ruleCode] = JSON.parse(key) as [string, string, string];
      lines.push(
        `gateway_plugin_capacity_rejections_total{plugin="${escapeLabel(plugin!)}",hook="${escapeLabel(hook!)}",rule_code="${escapeLabel(ruleCode!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_plugin_fail_open_total counter");
    for (const [key, val] of this.pluginFailOpen) {
      const [plugin, hook] = JSON.parse(key) as [string, string];
      lines.push(
        `gateway_plugin_fail_open_total{plugin="${escapeLabel(plugin!)}",hook="${escapeLabel(hook!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_protocol_field_dropped_total counter");
    for (const [key, val] of this.protocolFieldDropped) {
      const [source, target, field] = JSON.parse(key) as [string, string, string];
      lines.push(
        `gateway_protocol_field_dropped_total{source="${escapeLabel(source!)}",target="${escapeLabel(target!)}",field="${escapeLabel(field!)}"} ${val}`,
      );
    }
    lines.push("# TYPE gateway_protocol_field_rejected_total counter");
    for (const [key, val] of this.protocolFieldRejected) {
      const [source, target, field] = JSON.parse(key) as [string, string, string];
      lines.push(
        `gateway_protocol_field_rejected_total{source="${escapeLabel(source!)}",target="${escapeLabel(target!)}",field="${escapeLabel(field!)}"} ${val}`,
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
    return `${lines.join("\n")}\n`;
  }
}
