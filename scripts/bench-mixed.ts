import type { Agent } from "node:http";
import type { SQL } from "bun";
import { postJsonKeepAlive } from "./bench-http.ts";
import { createMixedSchedule } from "./mixed-schedule.ts";

const policyInput = "fixture alice@example.com";
const policyOutput = "fixture [REDACTED_BY_WEBHOOK]";
const wasmOutput = "fixture [REDACTED_EMAIL]";

const cases = [
  { name: "json", stream: false, expected: "mixed JSON" },
  { name: "sse", stream: true, expected: "mixed SSE" },
  { name: "wasm", stream: false, expected: wasmOutput },
  { name: "webhook", stream: true, expected: policyOutput },
] as const;

type InternalCall = (
  method: string,
  path: string,
  body?: unknown,
) => Promise<Record<string, unknown>>;

function modelName(name: string) {
  return `mixed-${name}`;
}

export async function seedMixedPlugins(options: {
  callInternal: InternalCall;
  mockPort: number;
  mockToken: string;
  channelId: unknown;
  billingPriceId: unknown;
  costVersionId: unknown;
}) {
  const { callInternal, mockPort, mockToken } = options;
  const wasm = await callInternal("POST", "/internal/v1/plugins/builtins/pii-guard/install", {
    manifest: { hooks: ["on_response"] },
  });
  const webhook = await callInternal("PUT", "/internal/v1/plugins", {
    name: "mixed-webhook",
    version: "1",
    kind: "WEBHOOK",
    webhook_config: {
      url: `http://127.0.0.1:${mockPort}/__control/mixed-policy`,
      headers: { authorization: `Bearer ${mockToken}` },
      timeout_ms: 2000,
      stream_mode: "HOLD_UNTIL_APPROVED",
      failure_policy: "FAIL_CLOSED",
    },
    manifest: { hooks: ["on_stream_end"] },
  });
  await callInternal("POST", "/internal/v1/plugin-releases", {
    id: crypto.randomUUID(),
    expected_epoch: webhook.config_epoch,
    targets: [{ plugin_id: wasm.id }, { plugin_id: webhook.id }],
    samples: {
      [String(wasm.id)]: { text: policyInput },
      [String(webhook.id)]: {
        webhook_reply: { action: "TRANSFORM", text: policyOutput },
      },
    },
  });

  for (const fixture of cases) {
    const model = modelName(fixture.name);
    await callInternal("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 64,
    });
    const pluginId =
      fixture.name === "wasm" ? wasm.id : fixture.name === "webhook" ? webhook.id : null;
    await callInternal("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: options.channelId,
      upstream_model: model,
      operations: ["chat"],
      billing_price_id: options.billingPriceId,
      cost_version_id: options.costVersionId,
      ...(pluginId ? { plugin_pipeline: [{ plugin_id: pluginId, priority: 0 }] } : {}),
    });
    const configured = await fetch(`http://127.0.0.1:${mockPort}/__control/scenarios`, {
      method: "POST",
      headers: { authorization: `Bearer ${mockToken}`, "content-type": "application/json" },
      body: JSON.stringify({
        model,
        scenario: {
          output: pluginId ? policyInput : fixture.expected,
          ...(fixture.stream ? { chunkDelayMs: 10 } : {}),
        },
      }),
    });
    if (!configured.ok) throw new Error(`Could not set the mixed fixture for ${model}`);
  }
  return { wasm: String(wasm.id), webhook: String(webhook.id) };
}

function histogram() {
  const bins = new Uint32Array(16_001);
  let samples = 0;
  return {
    observe(ms: number) {
      bins[Math.min(Math.ceil(ms), bins.length - 1)]!++;
      samples++;
    },
    p99() {
      if (!samples) return 0;
      let remaining = Math.ceil(samples * 0.99);
      return bins.findIndex((count) => (remaining -= count) <= 0);
    },
  };
}

export async function runMixedSoak(options: {
  agent: Agent;
  ports: readonly number[];
  keys: readonly string[];
  seconds: number;
  targetRps: number;
  concurrency: number;
  processes: Record<string, number>;
}) {
  const { agent, ports, keys, seconds, targetRps, concurrency, processes } = options;
  const lanes = [0, 0, 0, 0, 1, 1, 2, 3];
  const byCase = cases.map(({ name, stream, expected }) => ({
    name,
    stream,
    expected,
    completed: 0,
    errors: 0,
    latency: histogram(),
    payload: Buffer.from(
      JSON.stringify({
        model: modelName(name),
        stream,
        messages: [{ role: "user", content: "mixed fixture" }],
        max_tokens: 32,
      }),
    ),
  }));
  const start = performance.now();
  const schedule = createMixedSchedule(seconds, targetRps, concurrency);
  const dispatchLag = histogram();
  const serviceLatency = histogram();
  const latency = histogram();
  let windowHistogram = histogram();
  let completed = 0;
  let errors = 0;
  let afterWindow = 0;
  let lastDispatchMs = 0;
  let lastSample = start;
  let lastCompleted = 0;
  let lastErrors = 0;
  const errorKinds: Record<string, number> = {};
  let previousErrorKinds: Record<string, number> = {};
  const pending = new Set<Promise<void>>();
  const samples: {
    elapsed_sec: number;
    rps: number;
    errors: number;
    p99_ms: number;
    error_kinds: Record<string, number>;
    processes: Record<string, { rss_mb: number; cpu_pct: number }>;
  }[] = [];

  async function resources() {
    const observed: Record<string, { rss_mb: number; cpu_pct: number }> = {};
    const proc = Bun.spawn(
      ["ps", "-o", "pid=,rss=,%cpu=", "-p", Object.values(processes).join(",")],
      {
        stdout: "pipe",
        stderr: "ignore",
      },
    );
    const [output] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
    for (const line of output.trim().split("\n")) {
      const [pid, rss, cpu] = line.trim().split(/\s+/).map(Number);
      const role = Object.keys(processes).find((name) => processes[name] === pid);
      if (role && rss !== undefined && cpu !== undefined)
        observed[role] = { rss_mb: Number((rss / 1024).toFixed(2)), cpu_pct: cpu };
    }
    return observed;
  }

  const sampling: Promise<void>[] = [];
  function sample(now: number) {
    const reading = {
      elapsed_sec: Number(((now - start) / 1000).toFixed(1)),
      rps: Number(
        (((completed - lastCompleted) * 1000) / Math.max(1, now - lastSample)).toFixed(1),
      ),
      errors: errors - lastErrors,
      p99_ms: windowHistogram.p99(),
      error_kinds: Object.fromEntries(
        Object.entries(errorKinds).flatMap(([kind, count]) => {
          const delta = count - (previousErrorKinds[kind] ?? 0);
          return delta ? [[kind, delta]] : [];
        }),
      ),
    };
    sampling.push(
      resources().then((observed) => {
        const row = { ...reading, processes: observed };
        samples.push(row);
        console.error(JSON.stringify({ event: "mixed_progress", ...row }));
      }),
    );
    lastSample = now;
    lastCompleted = completed;
    lastErrors = errors;
    previousErrorKinds = { ...errorKinds };
    windowHistogram = histogram();
  }

  while (!schedule.done(performance.now() - start)) {
    const now = performance.now();
    const { first, end } = schedule.take(now - start, pending.size);
    for (let index = first; index < end; index++) {
      const fixture = byCase[lanes[index % lanes.length]!]!;
      const port = ports[Math.floor(index / lanes.length) % ports.length]!;
      const key = keys[(index + Math.floor(index / lanes.length)) % keys.length]!;
      const started = performance.now();
      const plannedAt = start + schedule.plannedAt(index);
      lastDispatchMs = started - start;
      if (lastDispatchMs > seconds * 1000) afterWindow++;
      dispatchLag.observe(started - plannedAt);
      const task = postJsonKeepAlive(
        agent,
        port,
        key,
        fixture.payload,
        fixture.stream,
        fixture.expected,
      )
        .then((result) => {
          const finished = performance.now();
          serviceLatency.observe(finished - started);
          const elapsed = finished - plannedAt;
          latency.observe(elapsed);
          windowHistogram.observe(elapsed);
          fixture.latency.observe(elapsed);
          if (result.ok) {
            completed++;
            fixture.completed++;
          } else {
            errors++;
            fixture.errors++;
            const kind =
              result.status === 0
                ? "transport"
                : result.status === 200
                  ? "protocol_or_plugin_output"
                  : `http_${result.status}`;
            errorKinds[kind] = (errorKinds[kind] ?? 0) + 1;
          }
        })
        .finally(() => {
          pending.delete(task);
        });
      pending.add(task);
    }
    if (now - lastSample >= 60_000) sample(now);
    await Bun.sleep(1);
  }
  const { total, started, maxDebt } = schedule.status();
  const unsent = total - started;
  if (unsent) {
    for (let index = started; index < total; index++)
      byCase[lanes[index % lanes.length]!]!.errors++;
    errors += unsent;
    errorKinds.unsent_at_deadline = unsent;
  }
  const dispatchEnded = performance.now();
  await Promise.all(pending);
  const ended = performance.now();
  sample(ended);
  await Promise.all(sampling);
  samples.sort((a, b) => a.elapsed_sec - b.elapsed_sec);
  const elapsedSec = (ended - start) / 1000;
  return {
    phase: `${ports.length}node_mixed_${seconds}s`,
    concurrency,
    target_rps: targetRps,
    completed,
    errors,
    error_rate_pct: (errors * 100) / Math.max(1, completed + errors),
    scheduled: total,
    started,
    unsent,
    starts_after_window: afterWindow,
    last_dispatch_ms: Number(lastDispatchMs.toFixed(3)),
    max_dispatch_debt: maxDebt,
    dispatch_lag_p99_ms: dispatchLag.p99(),
    service_p99_ms: serviceLatency.p99(),
    drain_ms: Number((ended - dispatchEnded).toFixed(3)),
    elapsed_sec: Number(elapsedSec.toFixed(3)),
    rps: Number((completed / elapsedSec).toFixed(2)),
    p99_ms: latency.p99(),
    error_kinds: errorKinds,
    lanes: byCase.map((fixture) => ({
      name: fixture.name,
      completed: fixture.completed,
      errors: fixture.errors,
      p99_ms: fixture.latency.p99(),
    })),
    resources: samples,
  };
}

export type MixedChargeCheck = {
  model: string;
  charged: number;
  wrong_atoms_or_usage: number;
  wrong_plugin: number;
  customer_atoms: string;
  supplier_atoms: string;
};

export async function verifyMixedCharges(
  db: SQL,
  plugins: { wasm: string; webhook: string },
): Promise<MixedChargeCheck[]> {
  return db<MixedChargeCheck[]>`
    SELECT model, count(*)::int AS charged,
      count(*) FILTER (WHERE customer_atoms <> 185 OR funded_atoms <> 185 OR supplier_cost_atoms <> 37
        OR debt_atoms <> 0 OR refunded_atoms <> 0 OR debt_refunded_atoms <> 0
        OR currency <> 'USD' OR supplier_currency <> 'USD' 
        OR usage->>'input_tokens' IS DISTINCT FROM '7'
        OR usage->>'cached_input_tokens' IS DISTINCT FROM '3'
        OR usage->>'output_tokens' IS DISTINCT FROM '5')::int AS wrong_atoms_or_usage,
      count(*) FILTER (WHERE CASE model
        WHEN 'mixed-wasm' THEN NOT (plugin_audit @> ${JSON.stringify([{ plugin_id: plugins.wasm, action: "TRANSFORM" }])}::text::jsonb)
        WHEN 'mixed-webhook' THEN NOT (plugin_audit @> ${JSON.stringify([{ plugin_id: plugins.webhook, action: "TRANSFORM" }])}::text::jsonb)
        ELSE plugin_audit <> '[]'::jsonb END)::int AS wrong_plugin,
      sum(customer_atoms)::text AS customer_atoms,
      sum(supplier_cost_atoms)::text AS supplier_atoms
    FROM public.billing_charges GROUP BY model ORDER BY model
  `;
}

export async function verifyMixedBalances(db: SQL) {
  const [proof] = await db<
    {
      wallet_mismatches: number;
      supplier_cell_mismatches: number;
      missing_supplier_attempts: number;
    }[]
  >`
    WITH
      wallet_totals AS (
        SELECT wallet_id, SUM(customer_atoms) AS spent
        FROM public.billing_charges GROUP BY wallet_id
      ),
      supplier_totals AS (
        SELECT (item->>'account_id')::uuid AS account_id, item->>'currency' AS currency,
          (item->>'cell')::smallint AS cell, SUM(c.supplier_cost_atoms) AS spent, count(*) AS charges
        FROM public.billing_charges c
        JOIN public.billing_holds h ON h.request_id = c.request_id
        CROSS JOIN LATERAL jsonb_array_elements(h.supplier_holds) item
        WHERE item->>'attempt_id' = c.selected_attempt_id::text
        GROUP BY 1, 2, 3
      )
    SELECT
      (SELECT count(*)::int FROM public.wallets w FULL JOIN wallet_totals t ON t.wallet_id = w.id
        WHERE w.id IS NULL OR w.spent_atoms <> COALESCE(t.spent, 0)) AS wallet_mismatches,
      (SELECT count(*)::int FROM public.supplier_cells s FULL JOIN supplier_totals t USING (account_id, currency, cell)
        WHERE s.account_id IS NULL OR s.spent_atoms <> COALESCE(t.spent, 0)) AS supplier_cell_mismatches,
      ((SELECT count(*) FROM public.billing_charges)
        - COALESCE((SELECT SUM(charges) FROM supplier_totals), 0))::int AS missing_supplier_attempts
  `;
  if (!proof) throw new Error("Missing mixed wallet and supplier cell evidence");
  return proof;
}
