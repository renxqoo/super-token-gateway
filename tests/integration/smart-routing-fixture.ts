import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { afterAll, beforeAll, expect } from "bun:test";
import type { SQL } from "bun";
import type { ReservationPrice } from "@gateway/kernel";
import { createDatabase, migrate, type AdapterOverrides } from "@gateway/ledger";
import { createGatewayRuntime, type GatewayConfig } from "@gateway/gateway";

const baseDbUrl = process.env.DATABASE_URL;

if (!baseDbUrl) throw new Error("DATABASE_URL is required");

export const testDbName = `gateway_sr_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;

export const adminUrl = new URL(baseDbUrl);

adminUrl.pathname = "/postgres";

const targetUrl = new URL(baseDbUrl);

targetUrl.pathname = `/${testDbName}`;

const OPERATOR_TOKEN = "sr-admin-token-0123456789abcdef000000000000000";

export let adminDb: SQL;

export let db: SQL;

export function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
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

export async function internalCall(
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
  respond?: (request: Request) => Response | Promise<Response>;
  status?: number;
  headerDelayMs?: number;
  refuse?: boolean;
  truncate?: boolean;
  streamFrames?: string | ReadableStream<Uint8Array>;
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

export function fakeUpstream(label: string, initial: UpstreamMode = {}): FakeUpstream {
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
      if (mode.respond) return mode.respond(req);
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
type TestPrice = Pick<ReservationPrice, "rates" | "ceilings">;

export async function setup(
  runtime: ReturnType<typeof createGatewayRuntime>,
  ups: FakeUpstream[],
  opts: {
    strategy?: "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY";
    priority?: number[];
    weights?: number[];
    maxAttempts?: number;
    billingConcurrency?: number;
    upstreamKeys?: readonly string[];
    operation?: "chat" | "speech" | "images" | "image_edits";
    providerKind?: "openai" | "anthropic";
    providerPluginId?: string;
    adapterOverrides?: AdapterOverrides | ((index: number) => AdapterOverrides | undefined);
    cachedInputPrice?: boolean;
    prices?: { customer: TestPrice; supplier: TestPrice };
    contentPluginId?: string;
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
    concurrency: opts.billingConcurrency ?? 5000,
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
  const providerKind = opts.providerKind ?? "openai";
  for (const [index, u] of ups.entries()) {
    channels.push(
      await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: providerKind,
        ...(opts.providerPluginId ? { provider_plugin_id: opts.providerPluginId } : {}),
        name: `${u.label}-${tag}`,
        base_url: `${u.url}/${providerKind}`,
        api_key: opts.upstreamKeys?.[index] ?? "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      }),
    );
  }
  const price =
    opts.operation === "images" || opts.operation === "image_edits"
      ? {
          rates: { images: { numerator: "5", denominator: "1" } },
          ceilings: { images: "10" },
        }
      : {
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
  const customer = await call("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    ...(opts.prices?.customer ?? price),
  });
  const supplier = await call("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    ...(opts.prices?.supplier ?? price),
  });
  const model = `sr-${tag}`;
  const operation = opts.operation ?? "chat";
  await call("PUT", "/internal/v1/models", {
    name: model,
    operations: [operation],
    max_output_tokens: 50,
    routing_strategy: opts.strategy ?? "ROUND_ROBIN",
  });
  for (const [i, ch] of channels.entries()) {
    const adapterOverrides =
      typeof opts.adapterOverrides === "function"
        ? opts.adapterOverrides(i)
        : opts.adapterOverrides;
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
      ...(adapterOverrides ? { adapter_overrides: adapterOverrides } : {}),
      ...(opts.contentPluginId
        ? { plugin_pipeline: [{ plugin_id: opts.contentPluginId, priority: 1 }] }
        : {}),
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
  const editImage = (form: FormData) => {
    form.set("model", model);
    return runtime.fetchPublic(
      new Request("http://gateway.local/v1/images/edits", {
        method: "POST",
        headers: { authorization: `Bearer ${key.key}` },
        body: form,
      }),
    );
  };
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
    editImage,
    health,
    charges,
    holdsByState,
    resolveUnknownHolds,
    audit,
    settle,
    customer,
  };
}

export function registerRoutingFixture() {
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
}
