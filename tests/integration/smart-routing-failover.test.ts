import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll, expect, test } from "bun:test";
import type { SQL } from "bun";
import { createDatabase, migrate, type AdapterOverrides } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";
import { createTraceStore } from "../../apps/gateway/src/trace-storage.ts";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const testDbName = `gateway_sr_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${testDbName}`;
const OPERATOR_TOKEN = "sr-admin-token-0123456789abcdef000000000000000";

let adminDb: SQL;
let db: SQL;
beforeAll(async () => {
  adminDb = createDatabase(adminUrl.toString(), 2);
  await adminDb.unsafe(`CREATE DATABASE ${testDbName}`);
  db = createDatabase(targetUrl.toString(), 16);
  await migrate(db);
});
afterAll(async () => {
  await db?.close();
  if (adminDb) {
    await adminDb.unsafe(
      `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${testDbName}' AND pid <> pg_backend_pid()`,
    );
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${testDbName}`);
    await adminDb.close();
  }
});

function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: targetUrl.toString(),
    dbMaxConnections: 16,
    keyPepper: "sr-pepper-secret-0123456789abcdef",
    encryptionKey: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    internalCredentials: localInternalCredentials(OPERATOR_TOKEN),
    allowPrivateUpstreams: true,
    privateUpstreamCidrs: ["127.0.0.0/8"],
    trustedProxyCidrs: [],
    maxInFlight: 500,
    maxBodyBytes: 2 * 1024 * 1024,
    holdTtlMs: 30_000,
    attemptTimeoutMs: 3000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 32,
    ...overrides,
  };
}

async function internalCall(
  app: { fetch: (req: Request) => Response | Promise<Response> },
  method: string,
  path: string,
  body?: unknown,
) {
  const res = await app.fetch(
    new Request(`http://internal.local${path}`, {
      method,
      headers: { authorization: `Bearer ${OPERATOR_TOKEN}`, "content-type": "application/json" },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    }),
  );
  return { status: res.status, data: (await res.json()) as Record<string, unknown> };
}

type UpstreamMode = {
  status?: number;
  headerDelayMs?: number;
  refuse?: boolean;
  truncate?: boolean;
  streamFrames?: string;
  unaryBody?: object;
  headers?: Record<string, string>;
};
type FakeUpstream = {
  url: string;
  calls: () => number;
  set: (mode: UpstreamMode) => void;
  label: string;
  server: ReturnType<typeof Bun.serve>;
};

function fakeUpstream(label: string, initial: UpstreamMode = {}): FakeUpstream {
  let mode = initial;
  let calls = 0;
  const sse = [
    `data: {"id":"x","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n`,
    `data: {"id":"x","model":"m","choices":[{"index":0,"delta":{"content":"${label}"},"finish_reason":null}]}\n\n`,
    `data: {"id":"x","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}\n\n`,
    `data: [DONE]\n\n`,
  ].join("");
  const json = JSON.stringify({
    id: "x",
    object: "chat.completion",
    model: "m",
    choices: [{ index: 0, message: { role: "assistant", content: label }, finish_reason: "stop" }],
    usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
  });
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      calls++;
      if (mode.headerDelayMs) await Bun.sleep(mode.headerDelayMs);
      if (mode.status)
        return Response.json(
          mode.unaryBody ?? { error: { message: `${label} failure`, type: "mock" } },
          {
            status: mode.status,
            headers: {
              ...(mode.status === 429 ? { "retry-after": "1" } : {}),
              ...mode.headers,
            },
          },
        );
      if (mode.streamFrames !== undefined) {
        return new Response(mode.streamFrames, {
          headers: { "content-type": "text/event-stream" },
        });
      }
      if (mode.unaryBody) return Response.json(mode.unaryBody);
      const body = (await req.json().catch(() => ({}))) as { stream?: boolean };
      if (body.stream) {
        const frames = mode.truncate ? sse.split("\n\n").slice(0, 2).join("\n\n") + "\n\n" : sse;
        return new Response(frames, { headers: { "content-type": "text/event-stream" } });
      }
      return new Response(json, { headers: { "content-type": "application/json" } });
    },
  });
  return {
    label,
    url: server.url.origin,
    server,
    calls: () => calls,
    set: (m) => {
      mode = m;
    },
  };
}

type Wallet = { available_atoms: string; reserved_atoms: string; spent_atoms: string };

async function setup(
  runtime: ReturnType<typeof createGatewayRuntime>,
  ups: FakeUpstream[],
  opts: {
    strategy?: "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY";
    priority?: number[];
    weights?: number[];
    maxAttempts?: number;
    operation?: "chat" | "speech" | "images";
    adapterOverrides?: AdapterOverrides;
    cachedInputPrice?: boolean;
  } = {},
) {
  const call = async (method: string, path: string, body?: unknown) => {
    const r = await internalCall(runtime.internalApp, method, path, body);
    expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
    return r.data as Record<string, unknown>;
  };
  const tag = crypto.randomUUID().slice(0, 6);
  const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
    name: `sr-${tag}`,
    currency: "USD",
    rpm: 10_000_000,
    tpm: 100_000_000,
    concurrency: 5000,
  });
  const billingAccountId = String(billingAccount.id);
  await call("POST", `/internal/v1/wallets/${billingAccountId}/credits`, {
    currency: "USD",
    external_ref: `sr-${tag}`,
    amount_atoms: "1000000000",
    kind: "CASH",
  });
  const key = await call("PUT", "/internal/v1/api-keys", {
    billing_account_id: billingAccountId,
    name: "sr",
  });
  const account = await call("PUT", "/internal/v1/upstream-accounts", { name: `sr-${tag}` });
  await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
    currency: "USD",
    external_ref: `sr-sup-${tag}`,
    amount_atoms: "1000000000",
  });
  const channels = [];
  for (const [i, u] of ups.entries()) {
    channels.push(
      await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: "openai",
        name: `${u.label}-${tag}`,
        base_url: `${u.url}/openai`,
        api_key: "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      }),
    );
    void i;
  }
  const price =
    opts.operation === "images"
      ? {
          currency: "USD",
          rates: { images: { numerator: "5", denominator: "1" } },
          ceilings: { images: "10" },
        }
      : {
          currency: "USD",
          rates: {
            input_tokens: { numerator: "1", denominator: "1" },
            output_tokens: { numerator: "1", denominator: "1" },
            ...(opts.cachedInputPrice
              ? { cached_input_tokens: { numerator: "2", denominator: "1" } }
              : {}),
          },
          ceilings: {
            input_tokens: "100",
            output_tokens: "100",
            ...(opts.cachedInputPrice ? { cached_input_tokens: "100" } : {}),
          },
        };
  const customer = await call("POST", "/internal/v1/prices", { side: "CUSTOMER", ...price });
  const supplier = await call("POST", "/internal/v1/prices", { side: "SUPPLIER", ...price });
  const model = `sr-${tag}`;
  const operation = opts.operation ?? "chat";
  await call("PUT", "/internal/v1/models", {
    name: model,
    operations: [operation],
    max_output_tokens: 50,
    routing_strategy: opts.strategy ?? "ROUND_ROBIN",
  });
  for (const [i, ch] of channels.entries()) {
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: ch.id,
      upstream_model: "m",
      operations: [operation],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
      priority: opts.priority?.[i] ?? 0,
      weight: opts.weights?.[i] ?? 1,
      retry_policy: { max_attempts: opts.maxAttempts ?? 3 },
      ...(opts.adapterOverrides ? { adapter_overrides: opts.adapterOverrides } : {}),
    });
  }
  const wallet = async (): Promise<Wallet> => {
    const r = await internalCall(
      runtime.internalApp,
      "GET",
      `/internal/v1/wallets/${billingAccountId}`,
    );
    expect(r.status).toBe(200);
    return r.data as unknown as Wallet;
  };
  const chat = (stream = false, signal?: AbortSignal) =>
    runtime.publicApp.fetch(
      new Request("http://gateway.local/v1/chat/completions", {
        method: "POST",
        ...(signal ? { signal } : {}),
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model,
          stream,
          messages: [{ role: "user", content: "hi" }],
          max_tokens: 20,
        }),
      }),
    );
  const speech = () =>
    runtime.fetchPublic(
      new Request("http://gateway.local/v1/audio/speech", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model,
          input: "Read this text",
          voice: "alloy",
          stream_format: "sse",
        }),
      }),
    );
  const images = (n: number, stream: boolean) =>
    runtime.fetchPublic(
      new Request("http://gateway.local/v1/images/generations", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
        body: JSON.stringify({ model, prompt: "Render a fixture", n, stream }),
      }),
    );
  const health = async () => {
    const r = await internalCall(runtime.internalApp, "GET", "/internal/v1/health/channels");
    const rows = r.data.channels as {
      channel_id: string;
      state: string;
      consecutive_failures: number;
      ewma_latency_ms?: number;
      cooldown_until_ms?: number;
    }[];
    return Object.fromEntries(rows.map((row) => [row.channel_id, row]));
  };
  const charges = async () => {
    const rows =
      await db`SELECT count(*)::int AS n, count(DISTINCT request_id)::int AS u, coalesce(sum(customer_atoms),0)::text AS atoms FROM public.billing_charges WHERE billing_account_id = ${billingAccountId}`;
    return rows[0] as unknown as { n: number; u: number; atoms: string };
  };
  const holdsByState = async () => {
    const rows = await db<
      { state: string; n: number }[]
    >`SELECT state, count(*)::int AS n FROM public.billing_holds WHERE billing_account_id = ${billingAccountId} GROUP BY state`;
    return Object.fromEntries(rows.map((r: { state: string; n: number }) => [r.state, r.n]));
  };
  const resolveUnknownHolds = async () => {
    const rows = await db<{ request_id: string }[]>`
      SELECT request_id FROM public.billing_holds WHERE billing_account_id = ${billingAccountId} AND state = 'UNKNOWN'
    `;
    for (const { request_id } of rows) {
      expect(
        await runtime.ledger.resolveRequest({
          request_id,
          action: "RELEASE",
          actor: "fixture",
          reason: "Verified simulated upstream fault",
        }),
      ).toMatchObject({ state: "RELEASED" });
    }
  };
  const audit = async () =>
    (await internalCall(runtime.internalApp, "GET", "/internal/v1/audit/conservation")).data;
  const settle = async () => {
    for (let i = 0; i < 100 && runtime.lifecycle.size > 0; i++) await Bun.sleep(10);
  };
  return {
    billingAccountId,
    channels,
    model,
    wallet,
    chat,
    speech,
    images,
    health,
    charges,
    holdsByState,
    resolveUnknownHolds,
    audit,
    settle,
    customer,
  };
}

test("media SSE previously exposed success on incomplete or unmetered streams", async () => {
  const up = fakeUpstream("media-terminal");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { speech, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up], {
      operation: "speech",
    });
    const delta = 'data: {"type":"speech.audio.delta","audio":"YWJj"}\n\n';
    const usage = 'data: {"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\n';
    up.set({
      streamFrames:
        delta + usage + 'event: speech.audio.done\ndata: {"type":"speech.audio.done"}\n\n',
    });
    const complete = await speech();
    expect(complete.status).toBe(200);
    const completedWire = await complete.text();
    expect(completedWire).toContain("event: speech.audio.done");
    expect(completedWire).not.toContain("event: error");
    up.set({ streamFrames: delta + usage + "data: [DONE]\n\n" });
    const done = await speech();
    expect((await done.text()).trimEnd()).toEndWith("data: [DONE]");
    for (const [frames, code] of [
      [delta + "data: [DONE]\n\n", "invalid_upstream_usage"],
      [delta + usage + 'data: {"type":"speech.segment.done"}\n\n', "stream_interrupted"],
      [delta + usage + "event: error\n\ndata: [DONE]\n\n", "upstream_stream_error"],
      [delta + usage + "event: error\ndata: [DONE]\n\n", "upstream_stream_error"],
    ] as const) {
      up.set({ streamFrames: frames });
      const response = await speech();
      expect(response.status).toBe(200);
      const wire = await response.text();
      expect(wire).toContain("speech.audio.delta");
      expect(wire).toContain("event: error");
      expect(wire).toContain(code);
      expect(wire).not.toContain("[DONE]");
    }
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "30" });
    expect(await holdsByState()).toEqual({ SETTLED: 2, UNKNOWN: 4 });
    expect(up.calls()).toBe(6);
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("image settlement previously lost delivered image units when upstream also reported tokens", async () => {
  const up = fakeUpstream("image-metering");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { images, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up], {
      operation: "images",
      adapterOverrides: { usage_aliases: [{ target: "images", source_path: "meter.images" }] },
    });
    up.set({
      unaryBody: {
        data: [{ b64_json: "YWJj" }, { url: "https://provider.test/output.png" }],
        usage: { input_tokens: 10, output_tokens: 5 },
      },
    });
    const unary = await images(3, false);
    expect(unary.status).toBe(200);
    await unary.json();
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "10" });

    up.set({
      streamFrames:
        'data: {"type":"image_generation.partial_image","b64_json":"YWJj"}\n\ndata: {"type":"image_generation.completed","b64_json":"ZGVm","usage":{"input_tokens":10,"output_tokens":5}}\n\n',
    });
    const streamed = await images(1, true);
    expect(streamed.status).toBe(200);
    expect(await streamed.text()).toContain('"type":"image_generation.completed"');
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "15" });

    up.set({ unaryBody: { data: [{ b64_json: "YWJj" }, { b64_json: "ZGVm" }] } });
    const excess = await images(1, false);
    expect(excess.status).toBe(502);
    expect(await excess.json()).toMatchObject({ error: { code: "invalid_upstream_usage" } });

    up.set({ unaryBody: { data: [{ b64_json: "  " }], meter: { images: 1 } } });
    const conflicting = await images(1, false);
    expect(conflicting.status).toBe(502);
    expect(await conflicting.json()).toMatchObject({ error: { code: "usage_alias_conflict" } });

    const repeatedFrame = 'data: {"type":"image_generation.completed","b64_json":"ZGVm"}\n\n';
    up.set({ streamFrames: repeatedFrame.repeat(2) });
    const repeated = await images(2, true);
    const repeatedWire = await repeated.text();
    expect(repeatedWire).toContain("invalid_upstream_usage");
    expect(repeatedWire).not.toContain("image_generation.completed");

    up.set({
      streamFrames:
        'data: {"type":"image_generation.completed","usage":{"input_tokens":10,"output_tokens":5}}\n\n',
    });
    const unmetered = await images(1, true);
    const wire = await unmetered.text();
    expect(wire).toContain("invalid_upstream_usage");
    expect(wire).not.toContain("image_generation.completed");
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "15" });
    expect(await holdsByState()).toEqual({ SETTLED: 2, UNKNOWN: 4 });
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("a new media model uses configurable terminal and metering rules without a Gateway restart", async () => {
  const up = fakeUpstream("media-declared");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { speech, charges, holdsByState, resolveUnknownHolds, channels, health } = await setup(
      runtime,
      [up],
      {
        operation: "speech",
        adapterOverrides: {
          media_stream_terminal: { path: "payload.state", equals: "ready" },
          usage_aliases: [
            { target: "input_tokens", source_path: "counts.read" },
            { target: "output_tokens", source_path: "counts.write" },
          ],
          error_rules: [
            { path: "payload.rejected", equals: "unsupported_input", retry_kind: "client_error" },
          ],
        },
      },
    );
    const media =
      'data: {"type":"synthesis.segment.done","audio":"YWJj","counts":{"read":7,"write":3}}\n\n';
    const final = 'data: {"payload":{"state":"ready"}}\n\n';
    up.set({ streamFrames: media + final });
    const good = await speech();
    expect(good.status).toBe(200);
    expect(await good.text()).toBe(media + final);
    const before = (await health())[String(channels[0]!.id)]!;

    up.set({
      streamFrames:
        media + 'event: error\ndata: {"payload":{"rejected":"unsupported_input"}}\n\n' + final,
    });
    const rejected = await speech();
    const rejectedWire = await rejected.text();
    expect(rejectedWire).toContain("upstream_stream_error");
    expect(rejectedWire).not.toContain(final);
    expect((await health())[String(channels[0]!.id)]!.consecutive_failures).toBe(
      before.consecutive_failures,
    );

    up.set({ streamFrames: media });
    const truncated = await speech();
    expect(await truncated.text()).toContain("stream_interrupted");
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "10" });
    expect(await holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 2 });
    await resolveUnknownHolds();

    const named = await setup(runtime, [up], {
      operation: "speech",
      adapterOverrides: {
        media_stream_terminal: { event: "vendor_finished" },
        usage_aliases: [
          { target: "input_tokens", source_path: "counts.read" },
          { target: "output_tokens", source_path: "counts.write" },
        ],
      },
    });
    for (const marker of [
      "event: vendor_finished\n\n",
      "event: vendor_finished\ndata: complete\n\n",
    ]) {
      up.set({ streamFrames: media + marker });
      const finished = await named.speech();
      expect(await finished.text()).toBe(media + marker);
    }
    expect(await named.charges()).toEqual({ n: 2, u: 2, atoms: "20" });
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("chat finish and cache usage sharing the final frame survive through response and ledger", async () => {
  const up = fakeUpstream("finish-with-usage", {
    streamFrames: [
      'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}\n\n',
      'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4}}}\n\n',
      "data: [DONE]\n\n",
    ].join(""),
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { chat, charges } = await setup(runtime, [up], { cachedInputPrice: true });
    const response = await chat(true);
    expect(response.status).toBe(200);
    const wire = await response.text();
    expect(wire).toContain('"finish_reason":"stop"');
    expect(wire.trimEnd()).toEndWith("data: [DONE]");
    const [charge] =
      await db`SELECT usage FROM public.billing_charges WHERE request_id = ${response.headers.get("x-request-id")}`;
    expect(charge?.usage).toEqual({
      input_tokens: "6",
      output_tokens: "5",
      cached_input_tokens: "4",
    });
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "19" });
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("symptom: cancelled or saturated inference waits create reservations before the dial is admitted", async () => {
  const upstream = fakeUpstream("dial", { headerDelayMs: 250 });
  const runtime = createGatewayRuntime({
    config: makeConfig({
      upstreamDial: { concurrency: 1, startsPerSecond: 1, maxQueueWaitMs: 65 },
    }),
    db,
  });
  const pending: Promise<Response>[] = [];
  try {
    await runtime.start();
    const fixture = await setup(runtime, [upstream]);
    const first = Promise.resolve(fixture.chat());
    pending.push(first);
    for (let i = 0; upstream.calls() === 0 && i < 100; i++) await Bun.sleep(5);
    expect(upstream.calls()).toBe(1);

    const cancellation = new AbortController();
    const cancelled = Promise.resolve(fixture.chat(false, cancellation.signal));
    const excess = Promise.resolve(fixture.chat());
    pending.push(cancelled, excess);
    await Bun.sleep(15);
    expect(await fixture.holdsByState()).toEqual({ HELD: 1 });
    cancellation.abort();
    const [aborted, saturated] = await Promise.all([cancelled, excess]);
    expect(aborted.status).toBe(499);
    expect(saturated.status).toBe(503);
    expect(saturated.headers.get("retry-after")).toBe("1");
    expect(await saturated.json()).toMatchObject({ error: { code: "upstream_capacity" } });
    expect((await first).status).toBe(200);
    expect(upstream.calls()).toBe(1);
    expect(await fixture.holdsByState()).toEqual({ SETTLED: 1 });
    expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "15" });
    expect(await fixture.audit()).toMatchObject({ ok: true, open_holds: 0, unknown_holds: 0 });
  } finally {
    await Promise.allSettled(pending);
    await runtime.close();
    upstream.server.stop(true);
  }
});

test("cascade: primary 503 -> secondary 429 -> tertiary healthy; one request succeeds through two failures with a single exact charge", async () => {
  const A = fakeUpstream("A", { status: 503 });
  const B = fakeUpstream("B", { status: 429 });
  const C = fakeUpstream("C");
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B, C], { priority: [20, 10, 0], strategy: "WEIGHTED" });
    const before = await ctx.wallet();
    const t0 = performance.now();
    const res = await ctx.chat();
    const elapsed = performance.now() - t0;
    expect(res.status).toBe(200);
    expect(res.headers.get("x-gateway-channel")?.startsWith("C-")).toBe(true);
    expect(
      ((await res.json()) as { choices: { message: { content: string } }[] }).choices[0]!.message
        .content,
    ).toBe("C");
    expect(elapsed).toBeLessThan(1500);
    expect(A.calls()).toBe(1);
    expect(B.calls()).toBe(1);
    expect(C.calls()).toBe(1);
    await ctx.settle();
    const after = await ctx.wallet();
    expect(BigInt(after.spent_atoms) - BigInt(before.spent_atoms)).toBe(15n);
    expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
    const c = await ctx.charges();
    expect(c.n).toBe(1);
    expect(c.u).toBe(1);
    expect(BigInt(c.atoms)).toBe(15n);
    expect(await ctx.holdsByState()).toEqual({ SETTLED: 1 });
    expect((await ctx.audit()).ok).toBe(true);
    const h = await ctx.health();
    expect(h[ctx.channels[0]!.id as string]!.consecutive_failures).toBe(1);
    expect(h[ctx.channels[1]!.id as string]!.consecutive_failures).toBe(0);
    expect(h[ctx.channels[2]!.id as string]!.state).toBe("CLOSED");
  } finally {
    await runtime.close();
    for (const u of [A, B, C]) u.server.stop(true);
  }
});

test("channel diagnostics retain failures through JSON and SSE retries and classify broken streams", async () => {
  const traceDbName = `trace_${testDbName}`;
  const traceUrl = new URL(adminUrl);
  traceUrl.pathname = `/${traceDbName}`;
  await adminDb.unsafe(`CREATE DATABASE ${traceDbName}`);
  const traceDb = createDatabase(traceUrl.toString(), 1);
  const traces = createTraceStore(traceUrl.toString(), 1000);
  const runtime = createGatewayRuntime({
    config: makeConfig({
      traceExport: {
        databaseUrl: traceUrl.toString(),
        sampleBasisPoints: 0,
        slowFirstTextMs: 60_000,
        maxAttempts: 4,
        errorBodyMaxBytes: 1024,
        maxRecords: 16,
        maxBytes: 65_536,
        maxPerBillingAccount: 8,
        priorityReserve: 4,
        batchSize: 4,
        batchDeadlineMs: 2000,
        flushMs: 20,
        timeoutMs: 1000,
        retryLimit: 0,
        retentionSeconds: 120,
        cleanupIntervalMs: 1000,
        cleanupBatch: 20,
      },
    }),
    db,
  });
  const readTrace = async (requestId: string) => {
    const deadline = performance.now() + 2000;
    let rows = await traces.read(requestId);
    while (!rows.length && performance.now() < deadline) {
      await Bun.sleep(20);
      rows = await traces.read(requestId);
    }
    return rows;
  };
  const attrs = (span: Awaited<ReturnType<typeof readTrace>>[number]["span"]) =>
    Object.fromEntries(span.attributes.map(({ key, value }) => [key, Object.values(value)[0]]));
  const summaries = { quota_exhausted: "Provider account quota exhausted" };
  try {
    await traceDb.unsafe(
      await Bun.file(new URL("../../infra/postgres/trace-schema.sql", import.meta.url)).text(),
    );
    await runtime.start();
    for (const scenario of ["http_body", "http_header", "sse"] as const) {
      const stream = scenario === "sse";
      const fromHeader = scenario === "http_header";
      const body = {
        error: {
          code: fromHeader ? "untrusted-provider-code" : "quota_exhausted",
          message: "private-provider-content".repeat(fromHeader ? 100 : 1),
        },
      };
      const unreachable = fakeUpstream("closed");
      unreachable.server.stop(true);
      const primary = fakeUpstream(
        "primary",
        stream
          ? { streamFrames: `data: ${JSON.stringify(body)}\n\n` }
          : {
              status: 429,
              unaryBody: body,
              headers: {
                "x-vendor-request-id": "vendor-429",
                ...(fromHeader ? { "x-vendor-error-code": "quota_exhausted" } : {}),
              },
            },
      );
      const backup = fakeUpstream("backup");
      try {
        const fixture = await setup(runtime, [unreachable, primary, backup], {
          priority: [20, 10, 0],
          strategy: "WEIGHTED",
          adapterOverrides: {
            error_diagnostics: {
              code_path: "error.code",
              code_header: "X-Vendor-Error-Code",
              request_id_header: "x-vendor-request-id",
              summaries,
            },
            error_rules: [
              { path: "error.code", equals: "quota_exhausted", retry_kind: "rate_limit" },
            ],
          },
        });
        const response = await fixture.chat(stream);
        expect(response.status).toBe(200);
        expect(await response.text()).toContain("backup");
        expect(primary.calls()).toBe(1);
        expect(backup.calls()).toBe(1);
        const requestId = response.headers.get("x-request-id")!;
        const rows = await readTrace(requestId);
        expect(rows).toHaveLength(4);
        const request = attrs(rows.find(({ span }) => span.name === "gateway.inference")!.span);
        const attempts = rows
          .filter(({ span }) => span.name === "gateway.upstream.attempt")
          .map(({ span }) => attrs(span))
          .sort(
            (left, right) =>
              Number(left["gateway.attempt.number"]) - Number(right["gateway.attempt.number"]),
          );
        expect(attempts[0]).toMatchObject({
          "gateway.failure.stage": "pre_dispatch",
          "error.type": "upstream_connect",
        });
        expect(attempts[1]).toMatchObject({
          "gateway.failure.stage": stream ? "stream_prelude" : "http_response",
          "http.response.status_code": stream ? "200" : "429",
          "gateway.provider.error_code": "quota_exhausted",
          "gateway.provider.error_summary": "Provider account quota exhausted",
          ...(stream ? {} : { "gateway.provider.request_id": "vendor-429" }),
        });
        expect(JSON.stringify(rows)).not.toContain("private-provider-content");
        expect(JSON.stringify(rows)).not.toContain("untrusted-provider-code");
        expect(attempts[2]?.["gateway.channel"]).toBe(fixture.channels[2]!.name);
        const [charge] =
          await db`SELECT selected_attempt_id, customer_atoms::text, supplier_cost_atoms::text
          FROM public.billing_charges WHERE request_id=${requestId}::uuid`;
        expect(charge).toEqual({
          selected_attempt_id: attempts[2]?.["gateway.attempt.id"],
          customer_atoms: "15",
          supplier_cost_atoms: "15",
        });
        expect(request["gateway.settlement.attempt_id"]).toBe(charge!.selected_attempt_id);
        const metrics = await (await runtime.metricsResponse()).text();
        expect(metrics).toContain(
          `gateway_channel_failures_total{channel="${fixture.channels[1]!.name}",stage="${stream ? "stream_prelude" : "http_response"}",kind="rate_limit"} 1`,
        );
        expect(await fixture.audit()).toMatchObject({ ok: true, open_holds: 0, unknown_holds: 0 });
      } finally {
        primary.server.stop(true);
        backup.server.stop(true);
      }
    }
    for (const code of ["provider_stream_error", "stream_truncated"]) {
      const textFrame = 'data: {"id":"x","choices":[{"index":0,"delta":{"content":"first"}}]}\n\n';
      const errorFrame =
        'event: error\ndata: {"error":{"code":"quota_exhausted","message":"private-provider-content"}}\n\n';
      const upstream = fakeUpstream("broken", {
        streamFrames: code === "provider_stream_error" ? textFrame + errorFrame : textFrame,
      });
      try {
        const fixture = await setup(runtime, [upstream], {
          adapterOverrides: {
            error_diagnostics: { code_path: "error.code", summaries },
          },
        });
        const before = await fixture.audit();
        const response = await fixture.chat(true);
        expect(response.status).toBe(200);
        expect(await response.text()).toContain(code);
        const requestId = response.headers.get("x-request-id")!;
        const rows = await readTrace(requestId);
        const attempt = rows.find(({ span }) => span.name === "gateway.upstream.attempt")!.span;
        expect(attempt.status.code).toBe(2);
        expect(attrs(attempt)).toMatchObject({
          "gateway.failure.stage": "stream",
          "gateway.failure.kind": "protocol",
          "error.type": code,
          ...(code === "provider_stream_error"
            ? { "gateway.provider.error_code": "quota_exhausted" }
            : {}),
        });
        expect(JSON.stringify(rows)).not.toContain("private-provider-content");
        expect(await fixture.charges()).toMatchObject({ n: 0 });
        expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
        expect(await fixture.audit()).toMatchObject({
          ok: true,
          open_holds: 0,
          unknown_holds: Number(before.unknown_holds) + 1,
        });
        const cleanup = await internalCall(
          runtime.internalApp,
          "POST",
          `/internal/v1/requests/${requestId}/resolve`,
          {
            action: "RELEASE",
            reason: "Isolated test provider confirmed no billable usage",
          },
        );
        expect(cleanup.status).toBe(200);
        expect(await fixture.holdsByState()).toEqual({ RELEASED: 1 });
      } finally {
        upstream.server.stop(true);
      }
    }
  } finally {
    await runtime.close();
    await traces.close();
    await traceDb.close();
    await adminDb.unsafe(`DROP DATABASE ${traceDbName} WITH (FORCE)`);
  }
});

test("latency: a slow-failing primary near the attempt timeout adds at most one timeout of latency before switching; the client never waits for a second slow channel", async () => {
  const A = fakeUpstream("A", { status: 503, headerDelayMs: 2500 });
  const B = fakeUpstream("B");
  const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
    const res = await ctx.chat();
    const elapsedLabel = res.headers.get("x-gateway-channel");
    expect(res.status).toBe(200);
    expect(elapsedLabel?.startsWith("B-")).toBe(true);
    await res.text();
    await ctx.settle();
    const slow = await ctx.health();
    expect(slow[ctx.channels[0]!.id as string]!.consecutive_failures).toBeGreaterThanOrEqual(1);

    A.set({ status: 503, headerDelayMs: 4000 });
    B.set({ status: 503, headerDelayMs: 4000 });
    const before2 = await ctx.wallet();
    const t0 = performance.now();
    const failing = await ctx.chat();
    const waited = performance.now() - t0;
    expect(failing.status).toBe(504);
    expect(((await failing.json()) as { error: { code: string } }).error.code).toBe(
      "upstream_timeout",
    );
    expect(waited).toBeGreaterThan(2800);
    expect(waited).toBeLessThan(7000);
    await ctx.settle();
    const during = await ctx.wallet();
    expect(BigInt(during.spent_atoms)).toBe(BigInt(before2.spent_atoms));
    const unk =
      await db`SELECT held_atoms::text AS h FROM public.billing_holds WHERE billing_account_id = ${ctx.billingAccountId} AND state = 'UNKNOWN'`;
    expect(unk.length).toBe(1);
    expect(BigInt(during.reserved_atoms) - BigInt(before2.reserved_atoms)).toBe(
      BigInt(unk[0]!.h as string),
    );
    expect(await ctx.charges()).toMatchObject({ n: 1, u: 1 });
    expect(await ctx.holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 1 });
    const a = await ctx.audit();
    expect(a.ok).toBe(true);
    expect(a.open_holds).toBe(0);
    expect(a.unknown_holds).toBe(1);
  } finally {
    await runtime.close();
    A.server.stop(true);
    B.server.stop(true);
  }
});

test("exhaustion: every channel down returns the last upstream status with no stranded hold, no charge, and no retry storm (each channel hit once)", async () => {
  const A = fakeUpstream("A", { status: 503 });
  const B = fakeUpstream("B", { status: 429 });
  const C = fakeUpstream("C", { status: 500 });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B, C], { priority: [20, 10, 0] });
    const before = await ctx.wallet();
    const res = await ctx.chat();
    expect(res.status).toBe(502);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("upstream_error");
    expect(A.calls()).toBe(1);
    expect(B.calls()).toBe(1);
    expect(C.calls()).toBe(1);
    await ctx.settle();
    for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++) await Bun.sleep(10);
    const after = await ctx.wallet();
    expect(after).toEqual(before);
    expect(await ctx.charges()).toMatchObject({ n: 0 });
    expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
    const a = await ctx.audit();
    expect(a.ok).toBe(true);
    expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
  } finally {
    await runtime.close();
    for (const u of [A, B, C]) u.server.stop(true);
  }
});

test("exhaustion with only 429s surfaces 429 upstream_rate_limited to the client", async () => {
  const A = fakeUpstream("A", { status: 429 });
  const B = fakeUpstream("B", { status: 429 });
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B]);
    const res = await ctx.chat();
    expect(res.status).toBe(429);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe(
      "upstream_rate_limited",
    );
    await ctx.settle();
    for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++) await Bun.sleep(10);
    expect(await ctx.charges()).toMatchObject({ n: 0 });
    expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
    expect((await ctx.audit()).ok).toBe(true);
  } finally {
    await runtime.close();
    A.server.stop(true);
    B.server.stop(true);
  }
});

test("dead channel: ECONNREFUSED on primary switches before dispatch, the customer is never charged for the dead attempt, and a later success closes the circuit via probe", async () => {
  const dead = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("x") });
  const deadPort = dead.port!;
  dead.stop(true);
  const B = fakeUpstream("B");
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const tag = crypto.randomUUID().slice(0, 6);
    const call = async (method: string, path: string, body?: unknown) => {
      const r = await internalCall(runtime.internalApp, method, path, body);
      expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
      return r.data as Record<string, unknown>;
    };
    const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
      name: `sr-dead-${tag}`,
      currency: "USD",
      rpm: 10_000_000,
      tpm: 100_000_000,
      concurrency: 5000,
    });
    await call("POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
      currency: "USD",
      external_ref: `d-${tag}`,
      amount_atoms: "1000000000",
      kind: "CASH",
    });
    const key = await call("PUT", "/internal/v1/api-keys", {
      billing_account_id: billingAccount.id,
      name: "k",
    });
    const account = await call("PUT", "/internal/v1/upstream-accounts", { name: `d-${tag}` });
    await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
      currency: "USD",
      external_ref: `ds-${tag}`,
      amount_atoms: "1000000000",
    });
    const deadCh = await call("PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: `dead-${tag}`,
      base_url: `http://127.0.0.1:${deadPort}/openai`,
      api_key: "k",
      rpm: 10_000_000,
      concurrency: 10_000,
    });
    const liveCh = await call("PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: `live-${tag}`,
      base_url: `${B.url}/openai`,
      api_key: "k",
      rpm: 10_000_000,
      concurrency: 10_000,
    });
    const customer = await call("POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "100", output_tokens: "100" },
    });
    const supplier = await call("POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "100", output_tokens: "100" },
    });
    const model = `sr-dead-${tag}`;
    await call("PUT", "/internal/v1/models", {
      name: model,
      operations: ["chat"],
      max_output_tokens: 50,
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: deadCh.id,
      upstream_model: "m",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
      priority: 10,
      retry_policy: { max_attempts: 2 },
    });
    await call("PUT", "/internal/v1/routes", {
      model_name: model,
      channel_id: liveCh.id,
      upstream_model: "m",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
      priority: 0,
      retry_policy: { max_attempts: 2 },
    });
    const chat = () =>
      runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model,
            stream: false,
            messages: [{ role: "user", content: "x" }],
            max_tokens: 20,
          }),
        }),
      );
    const t0 = performance.now();
    const res = await chat();
    const sw = performance.now() - t0;
    expect(res.status).toBe(200);
    expect(res.headers.get("x-gateway-channel")).toBe(String(liveCh.name));
    expect(sw).toBeLessThan(1500);
    await res.text();
    for (let i = 0; i < 100 && runtime.lifecycle.size > 0; i++) await Bun.sleep(10);
    const charges =
      await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id = ${billingAccount.id}`;
    expect(charges[0]!.n).toBe(1);
    const hold =
      await db`SELECT supplier_holds FROM public.billing_holds WHERE billing_account_id = ${billingAccount.id}`;
    const sup = hold[0]!.supplier_holds as { channel_id: string; state: string }[];
    expect(sup.some((x) => x.channel_id === String(deadCh.id) && x.state === "RELEASED")).toBe(
      true,
    );
    expect(sup.some((x) => x.channel_id === String(liveCh.id) && x.state === "SETTLED")).toBe(true);
    const w = (
      await internalCall(runtime.internalApp, "GET", `/internal/v1/wallets/${billingAccount.id}`)
    ).data as { spent_atoms: string; reserved_atoms: string };
    expect(BigInt(w.spent_atoms)).toBe(15n);
    expect(BigInt(w.reserved_atoms)).toBe(0n);
  } finally {
    await runtime.close();
    B.server.stop(true);
  }
});

test("post-dispatch: upstream dies after SSE first byte -> no failover, hold UNKNOWN, no charge, zero duplicate upstream calls", async () => {
  const A = fakeUpstream("A", { truncate: true });
  const B = fakeUpstream("B");
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
    const res = await ctx.chat(true);
    expect(res.status).toBe(200);
    const text = await res.text();
    expect(text).not.toContain("[DONE]");
    expect(text).toContain("event: error");
    expect(A.calls()).toBe(1);
    expect(B.calls()).toBe(0);
    const requestId = res.headers.get("x-request-id")!;
    for (let i = 0; i < 100; i++) {
      const rows = await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
      if (rows[0]?.state === "UNKNOWN") break;
      await Bun.sleep(10);
    }
    expect(runtime.lifecycle.size).toBe(0);
    expect(await ctx.charges()).toMatchObject({ n: 0 });
    expect(await ctx.holdsByState()).toEqual({ UNKNOWN: 1 });
    expect((await ctx.audit()).ok).toBe(true);
  } finally {
    await runtime.close();
    A.server.stop(true);
    B.server.stop(true);
  }
});

test("LEAST_LATENCY: after at most two samples the slow channel is deprioritised and all remaining traffic goes to the fast peer", async () => {
  const slow = fakeUpstream("slow", { headerDelayMs: 400 });
  const fast = fakeUpstream("fast");
  const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [slow, fast], { strategy: "LEAST_LATENCY", priority: [0, 0] });
    const picked = { slow: 0, fast: 0 };
    for (let i = 0; i < 12; i++) {
      const r = await ctx.chat();
      expect(r.status).toBe(200);
      picked[r.headers.get("x-gateway-channel")?.startsWith("fast-") ? "fast" : "slow"]++;
      await r.text();
    }
    await ctx.settle();
    expect(picked.slow).toBeLessThanOrEqual(2);
    expect(picked.fast).toBeGreaterThanOrEqual(10);
    const h = await ctx.health();
    const fastRow = h[String(ctx.channels[1]!.id)]!;
    const slowRow = h[String(ctx.channels[0]!.id)];
    expect(fastRow.state).toBe("CLOSED");
    expect(fastRow.ewma_latency_ms).toBeLessThan(slowRow?.ewma_latency_ms ?? Infinity);
    expect(await ctx.charges()).toMatchObject({ n: 12, u: 12 });
    expect((await ctx.audit()).ok).toBe(true);
  } finally {
    await runtime.close();
    slow.server.stop(true);
    fast.server.stop(true);
  }
});

test("429 cooldown: a rate-limited channel is skipped for the retry-after window and receives no further calls until the cooldown passes", async () => {
  const A = fakeUpstream("A", { status: 429 });
  const B = fakeUpstream("B");
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
    const first = await ctx.chat();
    expect(first.status).toBe(200);
    expect(first.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
    await first.text();
    for (let i = 0; i < 10; i++) {
      const r = await ctx.chat();
      expect(r.status).toBe(200);
      expect(r.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
      await r.text();
    }
    await ctx.settle();
    expect(A.calls()).toBe(1);
    expect(B.calls()).toBe(11);
    const h = await ctx.health();
    const aRow = h[String(ctx.channels[0]!.id)]!;
    expect(aRow.cooldown_until_ms!).toBeGreaterThan(performance.now());
    expect(await ctx.charges()).toMatchObject({ n: 11, u: 11 });
    expect((await ctx.audit()).ok).toBe(true);
  } finally {
    await runtime.close();
    A.server.stop(true);
    B.server.stop(true);
  }
});
