import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { SQL } from "bun";
import { createDatabase, migrate } from "@gateway/storage";
import type { Operation, Protocol, ProviderKind } from "@gateway/kernel";
import { loadConfig } from "../../apps/gateway/src/config.ts";
import { assemble } from "../../apps/gateway/src/assembly.ts";
import { createApp } from "../../apps/gateway/src/app.ts";
import type { Dependencies } from "../../apps/gateway/src/context.ts";
import { BillingWorker } from "../../apps/gateway/src/billing-worker.ts";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import type { Scenario } from "../../apps/mock-providers/src/app.ts";

const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 16);
const database = `gateway_test_${suffix}`;
const namespace = `test:${suffix}`;
let adminDb: SQL;
let deps: Dependencies;
let billing: BillingWorker;
let gateway: ReturnType<typeof Bun.serve>;
let mockServer: ReturnType<typeof Bun.serve>;
let origin: string;
let projectId: string;
let key: string;
let otherKey: string;
const providers: Partial<Record<ProviderKind, string>> = {};
const mock = createMockApp("test-control-secret");
const adminKey = "test-admin-" + "a".repeat(40);
const json = async (path: string, body?: unknown, token = adminKey, method = "POST") => {
  const response = await fetch(`${origin}${path}`, {
    method,
    headers: { authorization: `Bearer ${String(token)}`, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const value = (await response.json()) as Record<string, unknown>;
  const requestId = response.headers.get("x-gateway-request-id");
  if (response.ok && requestId) await awaitUsage(requestId);
  return { response, value };
};
async function awaitUsage(requestId: string, count = 1): Promise<void> {
  await waitFor(async () => {
    const rows = await deps.store
      .db`SELECT q.state,(SELECT count(*)::int FROM usage_events WHERE request_id=q.id) AS n,(SELECT count(*)::int FROM attempts WHERE request_id=q.id AND evidence_id IS NULL) AS unseen FROM gateway_requests q WHERE id=${requestId}`;
    return (
      rows[0]?.state === "SUCCEEDED" && Number(rows[0]?.n ?? 0) >= count && rows[0]?.unseen === 0
    );
  });
}
async function awaitModelUsage(model: string, count = 1): Promise<void> {
  await waitFor(
    async () =>
      Number(
        (
          await deps.store
            .db`SELECT count(*)::int AS n FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model}`
        )[0]?.n ?? 0,
      ) >= count,
  );
}
async function awaitResourceFinal(id: string): Promise<void> {
  await waitFor(
    async () =>
      (
        await deps.store
          .db`SELECT 1 FROM usage_events e JOIN resources r ON r.attempt_id=e.attempt_id WHERE r.id=${id} AND e.source='async_final' AND e.finality='FINAL'`
      ).length > 0,
  );
}
async function configureModel(
  kind: ProviderKind,
  scenario: Scenario = {},
  caps: Operation[] = ["chat", "responses", "messages", "gemini"],
): Promise<string> {
  const name = `model-${crypto.randomUUID()}`;
  mock.scenarios.set(name, scenario);
  let result = await json("/admin/models", { name, operations: caps, max_output_tokens: 32768 });
  expect(result.response.status).toBe(201);
  result = await json(`/admin/models/${name}/routes`, {
    provider_id: providers[kind],
    upstream_model: name,
    operations: caps,
    priority: 10,
    weight: 1,
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "1" },
      },
    },
  });
  expect(result.response.status).toBe(201);
  for (const operation of caps)
    await deps.funds.setTariff({
      model: name,
      operation,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      ceilings: { requests: "1" },
      basis: "REQUEST_COMPLETION",
    });
  return name;
}
function payload(protocol: Protocol, model: string, stream: boolean): Record<string, unknown> {
  if (protocol === "messages")
    return { model, messages: [{ role: "user", content: "test input" }], max_tokens: 32, stream };
  if (protocol === "responses")
    return { model, input: "test input", max_output_tokens: 32, stream };
  if (protocol === "gemini")
    return {
      contents: [{ role: "user", parts: [{ text: "test input" }] }],
      generationConfig: { maxOutputTokens: 32 },
    };
  return {
    model,
    messages: [{ role: "user", content: "test input" }],
    max_completion_tokens: 32,
    stream,
  };
}
function pathFor(protocol: Protocol, model: string, stream: boolean): string {
  return protocol === "chat"
    ? "/v1/chat/completions"
    : protocol === "messages"
      ? "/v1/messages"
      : protocol === "responses"
        ? "/v1/responses"
        : `/v1beta/models/${model}:${stream ? "streamGenerateContent" : "generateContent"}`;
}

beforeAll(async () => {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("Run integration tests with .env.local");
  adminDb = createDatabase(base);
  await adminDb.unsafe(`CREATE DATABASE ${database}`);
  const url = new URL(base);
  url.pathname = `/${database}`;
  const db = createDatabase(url.toString());
  await migrate(db);
  await db.close();
  mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch, idleTimeout: 0 });
  deps = await assemble(
    loadConfig({
      ...process.env,
      NODE_ENV: "test",
      DATABASE_URL: url.toString(),
      ADMIN_KEY: adminKey,
      ENCRYPTION_KEY: "11".repeat(32),
      KEY_PEPPER: "p".repeat(40),
      REDIS_NAMESPACE: namespace,
      ALLOW_PRIVATE_UPSTREAM: "true",
      TOTAL_TIMEOUT_MS: "5000",
      HEADERS_TIMEOUT_MS: "1000",
      STREAM_IDLE_MS: "1000",
      DOWNSTREAM_STALL_MS: "1000",
    }),
  );
  const createAppForTests = createApp(deps);
  gateway = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: (request, server) =>
      createAppForTests.fetch(request, { peerAddress: server.requestIP(request)?.address }),
    idleTimeout: 0,
  });
  origin = `http://127.0.0.1:${gateway.port}`;
  const project = await json("/admin/projects", {
    name: "integration",
    rpm: 100000,
    tpm: 100000000,
    concurrency: 1000,
  });
  expect(project.response.status).toBe(201);
  projectId = String(project.value.id);
  await deps.funds.credit(projectId, {
    currency: "USD",
    external_ref: "test-funding",
    amount_atoms: "100000000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  billing = new BillingWorker(deps);
  await billing.start();
  const credential = await json(`/admin/projects/${projectId}/keys`, { name: "integration" });
  key = String(credential.value.key);
  const other = await json("/admin/projects", { name: "foreign" });
  const foreign = await json(`/admin/projects/${other.value.id}/keys`, { name: "foreign" });
  otherKey = String(foreign.value.key);
  for (const kind of ["openai", "anthropic", "gemini", "deepseek", "ollama"] as const) {
    const result = await json("/admin/providers", {
      name: kind,
      kind,
      base_url: `http://127.0.0.1:${mockServer.port}/${kind}/${kind === "gemini" ? "v1beta" : "v1"}`,
      api_key: `fixture-${kind}-secret`,
    });
    expect(result.response.status).toBe(201);
    providers[kind] = String(result.value.id);
  }
});
afterAll(async () => {
  if (deps) {
    await deps.lifecycle.drain(6000);
    await billing?.close();
    let cursor = "0";
    do {
      const result = (await deps.limits.command("SCAN", [
        cursor,
        "MATCH",
        `${namespace}:*`,
        "COUNT",
        "1000",
      ])) as [string, string[]];
      cursor = result[0];
      if (result[1].length) await deps.limits.command("DEL", result[1]);
    } while (cursor !== "0");
    deps.usage.close();
    const queueRedis = new Bun.RedisClient(deps.config.QUEUE_REDIS_URL);
    try {
      let cursor = "0";
      do {
        const [next, keys] = (await queueRedis.send("SCAN", [
          cursor,
          "MATCH",
          `{${namespace}:usage}:*`,
          "COUNT",
          "1000",
        ])) as [string, string[]];
        cursor = next;
        if (keys.length) await queueRedis.send("UNLINK", keys);
      } while (cursor !== "0");
    } finally {
      queueRedis.close();
    }
    deps.limits.close();
    await deps.store.db.close();
  }
  if (gateway) await gateway.stop(true);
  if (mockServer) await mockServer.stop(true);
  if (adminDb) {
    await adminDb.unsafe(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await adminDb.close();
  }
});

describe("real HTTP protocol matrix", () => {
  for (const kind of ["openai", "anthropic", "gemini", "deepseek", "ollama"] as const)
    for (const protocol of ["chat", "responses", "messages", "gemini"] as const)
      for (const stream of [false, true])
        test(`${kind} -> ${protocol} ${stream ? "SSE" : "JSON"}`, async () => {
          const model = await configureModel(kind, { chunkBytes: stream ? 1 : 16384 });
          const response = await fetch(`${origin}${pathFor(protocol, model, stream)}`, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify(payload(protocol, model, stream)),
          });
          expect(response.status).toBe(200);
          const body = await response.text();
          expect(body).toContain("世界");
          if (stream) {
            expect(body).not.toContain("gateway_stream_error");
            if (protocol === "chat") expect(body.match(/data: \[DONE\]/g)?.length).toBe(1);
            if (protocol === "responses")
              expect(body.match(/event: response.completed/g)?.length).toBe(1);
          }
          const requestId = response.headers.get("x-gateway-request-id");
          expect(requestId).toBeTruthy();
          await awaitUsage(String(requestId));
          const events = await deps.store.db<
            { usage: Record<string, string>; finality: string; cost: string }[]
          >`SELECT usage,finality,cost_atoms::text AS cost FROM usage_events WHERE request_id=${requestId}`;
          expect(events.length).toBe(1);
          expect(events[0]?.finality).toBe("FINAL");
          expect(events[0]?.usage.input_tokens).toBe("7");
          expect(events[0]?.usage.output_tokens).toBe("5");
          expect(events[0]?.usage.cached_input_tokens).toBe("3");
          expect(events[0]?.cost).toBe("59");
        });
});

test("100 simultaneous retries execute one physical call and reject payload conflict", async () => {
  const model = await configureModel("openai");
  const intent = crypto.randomUUID();
  const send = (content = "same") =>
    fetch(`${origin}/v1/chat/completions`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${key}`,
        "content-type": "application/json",
        "idempotency-key": intent,
      },
      body: JSON.stringify({ model, messages: [{ role: "user", content }] }),
    });
  const responses = await Promise.all(Array.from({ length: 100 }, () => send()));
  expect(responses.filter((r) => r.status === 200).length).toBe(1);
  expect(responses.filter((r) => r.status === 409).length).toBe(99);
  await Promise.all(responses.map((r) => r.arrayBuffer()));
  expect((await send("changed")).status).toBe(409);
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    1,
  );
});

test("availability-first retry preserves unknown attempt and independent usage", async () => {
  const model = await configureModel("openai", { status: 503, failFirst: 1 });
  const result = await json("/v1/chat/completions", payload("chat", model, false), key);
  expect(result.response.status).toBe(200);
  await awaitModelUsage(model, 2);
  const rows = await deps.store.db<
    { state: string }[]
  >`SELECT state FROM attempts WHERE upstream_model=${model} ORDER BY ordinal`;
  expect(rows.map((row) => row.state)).toEqual(["UNKNOWN", "SUCCEEDED"]);
  expect(
    (
      await deps.store
        .db`SELECT e.id FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model}`
    ).length,
  ).toBe(2);
});

test("truncated converted stream retains partial usage and never emits success", async () => {
  const model = await configureModel("gemini", { truncate: true });
  const response = await fetch(`${origin}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify(payload("chat", model, true)),
  });
  const body = await response.text();
  expect(body).toContain("gateway_stream_error");
  expect(body).not.toContain("[DONE]");
  await awaitModelUsage(model);
  const rows = await deps.store.db<
    { state: string }[]
  >`SELECT state FROM attempts WHERE upstream_model=${model}`;
  expect(rows.length).toBe(1);
  expect(["PARTIAL", "UNKNOWN"]).toContain(rows[0]?.state ?? "");
});

test("Files, Batch, images, embeddings, video and audio operate through the real gateway", async () => {
  const caps: Operation[] = [
    "files",
    "batches",
    "images",
    "embeddings",
    "videos",
    "speech",
    "transcription",
    "translation",
    "image_edits",
    "moderations",
    "completions",
    "chat",
  ];
  const model = await configureModel("openai", {}, caps);
  const form = new FormData();
  form.set("model", model);
  form.set("purpose", "batch");
  form.set(
    "file",
    new File(
      [
        JSON.stringify({
          custom_id: "1",
          method: "POST",
          url: "/v1/chat/completions",
          body: { model, messages: [{ role: "user", content: "input" }] },
        }),
      ],
      "input.jsonl",
    ),
  );
  const upload = await fetch(`${origin}/v1/files`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(upload.status).toBe(200);
  const file = (await upload.json()) as { id: string };
  expect(file.id).toStartWith("file_");
  const foreign = await fetch(`${origin}/v1/files/${file.id}/content`, {
    headers: { authorization: `Bearer ${otherKey}` },
  });
  expect(foreign.status).toBe(404);
  const batch = await json(
    "/v1/batches",
    { model, input_file_id: file.id, endpoint: "/v1/chat/completions", completion_window: "24h" },
    key,
  );
  expect(batch.response.status).toBe(200);
  const poll = await json(`/v1/batches/${batch.value.id}`, undefined, key, "GET");
  expect(poll.value.status).toBe("completed");
  expect(String(poll.value.output_file_id)).toStartWith("file_");
  const content = await fetch(`${origin}/v1/files/${poll.value.output_file_id}/content`, {
    headers: { authorization: `Bearer ${key}` },
  });
  expect(await content.text()).toContain("mock-batch-output-content");
  for (const [path, body] of [
    ["/v1/embeddings", { model, input: "x" }],
    ["/v1/images/generations", { model, prompt: "x" }],
    ["/v1/videos", { model, prompt: "x" }],
    ["/v1/moderations", { model, input: "x" }],
    ["/v1/completions", { model, prompt: "x" }],
  ] as const)
    expect((await json(path, body, key)).response.status).toBe(200);
  const audio = await fetch(`${origin}/v1/audio/speech`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({ model, input: "hello", voice: "alloy" }),
  });
  expect(audio.headers.get("x-gateway-request-id")).toBeTruthy();
  expect(await audio.text()).toBe("mock-generated-audio-content");
  for (const path of ["/v1/audio/transcriptions", "/v1/audio/translations", "/v1/images/edits"]) {
    const data = new FormData();
    data.set("model", model);
    data.set("file", new File(["input"], "input.wav"));
    const result = await fetch(`${origin}${path}`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}` },
      body: data,
    });
    expect(result.status).toBe(200);
    await result.arrayBuffer();
  }
});

test("metadata-only events are replayable with fenced leases and immutable evidence", async () => {
  const model = await configureModel("openai");
  await json("/v1/chat/completions", payload("chat", model, false), key);
  await waitFor(async () => Object.values(await deps.usage.producer.stats()).every((n) => n === 0));
  const consumer = await json("/admin/consumers", { name: `consumer-${suffix}`, replay: true });
  expect(consumer.response.status).toBe(201);
  const consumerId = String(consumer.value.id);
  const claimed = await deps.store.claimEvents(consumerId, 500);
  expect(claimed.events.length).toBeGreaterThan(0);
  expect(
    await deps.store.ackEvents(
      consumerId,
      crypto.randomUUID(),
      claimed.events.map((e) => e.id),
    ),
  ).toBe(0);
  expect(
    await deps.store.ackEvents(
      consumerId,
      claimed.lease_token,
      claimed.events.map((e) => e.id),
    ),
  ).toBe(claimed.events.length);
  const next = await deps.store.claimEvents(consumerId, 500);
  expect(next.events.length).toBe(0);
  const first = claimed.events[0];
  if (!first) throw new Error("Missing event");
  await expect(
    deps.store.db`UPDATE usage_events SET cost_atoms=999 WHERE id=${first.id}`,
  ).rejects.toThrow();
  await expect(
    deps.store
      .db`INSERT INTO usage_events(id,project_id,request_id,attempt_id,source,finality,usage,cost_atoms,currency) VALUES(${crypto.randomUUID()},${first.project_id},${first.request_id},${first.attempt_id},'poison','FINAL',${{ content: "forbidden" }},0,'USD')`,
  ).rejects.toThrow();
});

test("LLM response canary never enters any PostgreSQL table", async () => {
  const canary = `llm-output-${crypto.randomUUID()}`;
  const model = await configureModel("openai", { output: canary });
  const result = await json("/v1/chat/completions", payload("chat", model, false), key);
  expect(JSON.stringify(result.value)).toContain(canary);
  const tables = await deps.store.db<
    { tablename: string }[]
  >`SELECT tablename FROM pg_tables WHERE schemaname='public'`;
  for (const table of tables) {
    if (!/^[a-z_]+$/.test(table.tablename)) throw new Error("Unexpected table identifier");
    const rows = await deps.store.db.unsafe(
      `SELECT row_to_json(t)::text AS body FROM ${table.tablename} t`,
    );
    expect(JSON.stringify(rows)).not.toContain(canary);
  }
});

test("revoked project key is rejected immediately", async () => {
  const created = await json(`/admin/projects/${projectId}/keys`, { name: "revocable" });
  const token = String(created.value.key);
  expect((await json("/v1/models", undefined, token, "GET")).response.status).toBe(200);
  await json(`/admin/keys/${created.value.id}`, undefined, adminKey, "DELETE");
  expect((await json("/v1/models", undefined, token, "GET")).response.status).toBe(401);
});

async function waitFor(predicate: () => boolean | Promise<boolean>, timeout = 3000): Promise<void> {
  const end = Date.now() + timeout;
  while (!(await predicate())) {
    if (Date.now() > end) throw new Error("Condition timed out");
    await Bun.sleep(20);
  }
}
const sendChat = (
  model: string,
  extra: Record<string, unknown> = {},
  token = key,
  signal?: AbortSignal,
) =>
  fetch(`${origin}/v1/chat/completions`, {
    method: "POST",
    headers: { authorization: `Bearer ${String(token)}`, "content-type": "application/json" },
    body: JSON.stringify({ ...payload("chat", model, false), ...extra }),
    ...(signal ? { signal } : {}),
  });

describe("function calls survive real transport conversion", () => {
  for (const kind of ["openai", "anthropic", "gemini", "deepseek", "ollama"] as const)
    for (const protocol of ["chat", "responses", "messages", "gemini"] as const)
      for (const stream of [false, true])
        test(`${kind} function -> ${protocol} ${stream ? "SSE" : "JSON"}`, async () => {
          const model = await configureModel(kind, { tools: true, chunkBytes: 1 });
          const response = await fetch(`${origin}${pathFor(protocol, model, stream)}`, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify(payload(protocol, model, stream)),
          });
          expect(response.status).toBe(200);
          const body = await response.text();
          expect(body).toContain("weather");
          expect(body).toContain("北京");
          expect(body).not.toContain("gateway_stream_error");
          if (stream && protocol === "messages") {
            const starts = [...body.matchAll(/event: content_block_start\ndata: ([^\n]+)/g)].map(
              (m) => JSON.parse(m[1] ?? "{}").index,
            );
            expect(starts).toEqual([0]);
            expect(body.match(/event: content_block_stop/g)?.length).toBe(1);
          }
        });
});

test("native media SSE records image count and exact transcription duration", async () => {
  const model = await configureModel("openai", { chunkBytes: 1 }, [
    "images",
    "image_edits",
    "transcription",
    "speech",
  ]);
  for (const operation of ["images", "transcription", "speech"]) {
    let response: Response;
    if (operation === "transcription") {
      const form = new FormData();
      form.set("model", model);
      form.set("stream", "true");
      form.set("file", new File(["input"], "audio.wav"));
      response = await fetch(`${origin}/v1/audio/transcriptions`, {
        method: "POST",
        headers: { authorization: `Bearer ${key}` },
        body: form,
      });
    } else
      response = await fetch(
        `${origin}${operation === "images" ? "/v1/images/generations" : "/v1/audio/speech"}`,
        {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model,
            prompt: "x",
            input: "hello",
            ...(operation === "images" ? { stream: true } : { stream_format: "sse" }),
          }),
        },
      );
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).not.toContain("gateway_stream_error");
    expect(body).toContain(
      operation === "images"
        ? "image_generation.completed"
        : operation === "speech"
          ? "speech.audio.done"
          : "transcript.text.done",
    );
    await awaitUsage(String(response.headers.get("x-gateway-request-id")));
    const rows = await deps.store
      .db`SELECT usage,finality FROM usage_events WHERE request_id=${response.headers.get("x-gateway-request-id")}`;
    expect(rows.length).toBe(1);
    expect(rows[0]?.finality).toBe("FINAL");
    if (operation === "images") expect(rows[0]?.usage.images).toBe("1");
    if (operation === "transcription") expect(rows[0]?.usage.audio_seconds).toBe("0.125");
  }
});

test("model-restricted keys cannot inspect another model's requests, usage or resources", async () => {
  const allowed = await configureModel("openai");
  const hidden = await configureModel("openai");
  const a = await json("/v1/responses", payload("responses", allowed, false), key);
  const b = await json("/v1/responses", payload("responses", hidden, false), key);
  const scoped = await json(`/admin/projects/${projectId}/keys`, {
    name: "scoped",
    models: [allowed],
  });
  const token = String(scoped.value.key);
  expect((await json(`/v1/responses/${b.value.id}`, undefined, token, "GET")).response.status).toBe(
    404,
  );
  expect(
    (
      await json(
        `/v1/requests/${b.response.headers.get("x-gateway-request-id")}`,
        undefined,
        token,
        "GET",
      )
    ).response.status,
  ).toBe(404);
  expect((await json(`/v1/responses/${a.value.id}`, undefined, token, "GET")).response.status).toBe(
    200,
  );
  const listed = await json("/v1/responses", undefined, token, "GET");
  expect(JSON.stringify(listed.value)).not.toContain(String(b.value.id));
  const usage = await json("/v1/usage", undefined, token, "GET");
  expect(JSON.stringify(usage.value)).not.toContain(
    String(b.response.headers.get("x-gateway-request-id")),
  );
});

test("Responses previous IDs are owned, streaming IDs stable and background usage recovers without user polling", async () => {
  const model = await configureModel("openai");
  const created = await json("/v1/responses", payload("responses", model, false), key);
  expect(
    (
      await json(
        "/v1/responses",
        { previous_response_id: created.value.id, input: "next" },
        otherKey,
      )
    ).response.status,
  ).toBe(404);
  expect(
    (await json("/v1/responses", { previous_response_id: created.value.id, input: "next" }, key))
      .response.status,
  ).toBe(200);
  const stream = await fetch(`${origin}/v1/responses`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify(payload("responses", model, true)),
  });
  const body = await stream.text();
  const frames = body
    .split("\n")
    .filter((line) => line.startsWith("data: {"))
    .map((line) => JSON.parse(line.slice(6)));
  const ids = frames.flatMap((frame) => (frame.response?.id ? [frame.response.id] : []));
  expect(ids.length).toBeGreaterThanOrEqual(2);
  expect(new Set(ids).size).toBe(1);
  expect(ids[0]).toStartWith("response_");
  expect((await json(`/v1/responses/${ids[0]}`, undefined, key, "GET")).response.status).toBe(200);
  const background = await json(
    "/v1/responses",
    { ...payload("responses", model, false), background: true },
    key,
  );
  expect(background.value.status).toBe("queued");
  const { recoveryTick } = await import("../../apps/gateway/src/recovery.ts");
  await recoveryTick(deps);
  await awaitResourceFinal(String(background.value.id));
  const evidence = await deps.store
    .db`SELECT e.source,e.finality,e.usage,e.cost_complete FROM usage_events e JOIN resources r ON r.attempt_id=e.attempt_id WHERE r.id=${background.value.id} ORDER BY e.seq`;
  expect(evidence.map((row: { finality: string }) => row.finality)).toEqual(["FINAL", "FINAL"]);
  expect(evidence[0]?.usage).toEqual({ requests: "1" });
  expect(evidence[0]?.cost_complete).toBe(false);
  expect(evidence[1]?.usage.input_tokens).toBe("7");
});

test("async queue acceptance failure remains pollable and retries idempotently", async () => {
  const model = await configureModel("openai");
  const created = await json(
    "/v1/responses",
    { ...payload("responses", model, false), background: true },
    key,
  );
  const original = deps.usage.producer.publish.bind(deps.usage.producer);
  deps.usage.producer.publish = async (input) => {
    if (input.source === "async_final") throw new Error("injected commit fault");
    return original(input);
  };
  try {
    expect(
      (await json(`/v1/responses/${created.value.id}`, undefined, key, "GET")).response.status,
    ).toBeGreaterThanOrEqual(500);
  } finally {
    deps.usage.producer.publish = original;
  }
  expect((await deps.store.resource(String(created.value.id), projectId)).state).toBe("ACTIVE");
  expect(
    (await json(`/v1/responses/${created.value.id}`, undefined, key, "GET")).response.status,
  ).toBe(200);
  expect(
    (await json(`/v1/responses/${created.value.id}`, undefined, key, "GET")).response.status,
  ).toBe(200);
  const rows = await deps.store
    .db`SELECT e.id FROM usage_events e JOIN resources r ON r.attempt_id=e.attempt_id WHERE r.id=${created.value.id} AND e.source='async_final'`;
  expect(rows.length).toBe(1);
});

test("durable queue acceptance failure withholds stream terminal and does not execute twice", async () => {
  const model = await configureModel("openai");
  const original = deps.usage.producer.publish.bind(deps.usage.producer);
  deps.usage.producer.publish = async () => {
    throw new Error("injected database loss");
  };
  let body = "";
  try {
    body = await (await sendChat(model, { stream: true })).text();
  } finally {
    deps.usage.producer.publish = original;
  }
  expect(body).toContain("gateway_stream_error");
  expect(body).not.toContain("[DONE]");
  expect(body).not.toContain('"finish_reason":"stop"');
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    1,
  );
});

test("known usage survives a conversion rejection and output-free failover", async () => {
  const model = await configureModel("gemini", { thoughtSignature: true });
  await json(`/admin/models/${model}/routes`, {
    provider_id: providers.openai,
    upstream_model: model,
    operations: ["chat"],
    priority: 0,
    weight: 1,
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
      },
    },
  });
  const response = await sendChat(model);
  expect(response.status).toBe(200);
  await response.text();
  await awaitUsage(String(response.headers.get("x-gateway-request-id")), 2);
  const rows = await deps.store
    .db`SELECT e.finality,e.usage FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model} ORDER BY a.ordinal`;
  expect(rows.length).toBe(2);
  expect(rows.map((row: { finality: string }) => row.finality)).toEqual(["FINAL", "FINAL"]);
});

test("non-JSON transcription output is relayed once without being parsed or persisted", async () => {
  const model = await configureModel("openai", {}, ["transcription"]);
  const form = new FormData();
  form.set("model", model);
  form.set("response_format", "srt");
  form.set("file", new File(["input"], "audio.wav"));
  const response = await fetch(`${origin}/v1/audio/transcriptions`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(response.status).toBe(200);
  expect(await response.text()).toBe("mock-transcribed-output-content");
  await awaitModelUsage(model);
  const rows = await deps.store
    .db`SELECT e.finality FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model}`;
  expect(rows.length).toBe(1);
  expect(rows[0]?.finality).toBe("UNKNOWN");
});

test("client cancellation releases project/provider concurrency and bounds upstream activity", async () => {
  const model = await configureModel("openai", { repeat: 5000, chunkBytes: 128, chunkDelayMs: 2 });
  const controller = new AbortController();
  const response = await sendChat(model, { stream: true }, key, controller.signal);
  const reader = response.body?.getReader();
  expect(reader).toBeTruthy();
  await reader?.read();
  controller.abort();
  await reader?.cancel().catch(() => undefined);
  await waitFor(() => deps.lifecycle.size === 0);
  expect(deps.lifecycle.size).toBe(0);
  expect(
    Number(await deps.limits.command("ZCARD", [`${namespace}:{project:${projectId}}:active`])),
  ).toBe(0);
  await waitFor(() => mock.stats.active === 0);
  expect(mock.stats.cancellations).toBeGreaterThan(0);
});

test("oversized output policies reject before any physical provider call", async () => {
  const model = await configureModel("openai");
  const response = await sendChat(model, { max_completion_tokens: 32769 });
  expect(response.status).toBe(422);
  await response.text();
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    0,
  );
});

test("all provider-backed resource fields enforce ownership, including Gemini native", async () => {
  const model = await configureModel("openai");
  const denied = await json(
    "/v1/responses",
    {
      model,
      input: "x",
      tools: [
        {
          type: "code_interpreter",
          container: { type: "auto", file_ids: ["file-raw-provider-secret"] },
        },
      ],
    },
    key,
  );
  expect(denied.response.status).toBe(404);
  const geminiModel = await configureModel("gemini");
  for (const extra of [
    { cachedContent: "cachedContents/foreign" },
    { contents: [{ parts: [{ fileData: { fileUri: "files/foreign", mimeType: "text/plain" } }] }] },
  ]) {
    const response = await fetch(`${origin}/v1beta/models/${geminiModel}:generateContent`, {
      method: "POST",
      headers: { "x-goog-api-key": key, "content-type": "application/json" },
      body: JSON.stringify({ ...payload("gemini", geminiModel, false), ...extra }),
    });
    expect(response.status).toBe(422);
    await response.text();
  }
});

test("precise numeric literals survive resource rewriting, forwarding and idempotency", async () => {
  const { resolveFileReferences } = await import("../../apps/gateway/src/resources.ts");
  const { canonical, parseProviderJson, stringifyExact } = await import("@gateway/kernel");
  const a = (
    await resolveFileReferences(
      deps,
      projectId,
      parseProviderJson(
        '{"contents":[{"parts":[{"functionResponse":{"response":{"account":9007199254740993}}}]}]}',
      ) as Record<string, unknown>,
    )
  ).body;
  const b = (
    await resolveFileReferences(
      deps,
      projectId,
      parseProviderJson(
        '{"contents":[{"parts":[{"functionResponse":{"response":{"account":9007199254740992}}}]}]}',
      ) as Record<string, unknown>,
    )
  ).body;
  expect(stringifyExact(a)).toContain("9007199254740993");
  expect(canonical(a)).not.toBe(canonical(b));
  const model = await configureModel("openai");
  const intent = crypto.randomUUID();
  const send = (n: string) =>
    fetch(`${origin}/v1/chat/completions`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${key}`,
        "content-type": "application/json",
        "idempotency-key": intent,
      },
      body: `{"model":"${model}","messages":[{"role":"user","content":"x"}],"seed":${n}}`,
    });
  const first = await send("9007199254740993");
  expect(first.status).toBe(200);
  await first.text();
  const second = await send("9007199254740992");
  expect(second.status).toBe(409);
  expect(await second.text()).toContain("idempotency_conflict");
});

test("poll-final evidence cannot be downgraded by a later stream cancellation", async () => {
  const model = await configureModel("openai", { repeat: 5000, chunkBytes: 256, chunkDelayMs: 2 });
  const controller = new AbortController();
  const response = await fetch(`${origin}/v1/responses`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify(payload("responses", model, true)),
    signal: controller.signal,
  });
  const reader = response.body?.getReader();
  if (!reader) throw new Error("Missing stream");
  let prefix = "";
  while (!prefix.includes("response_")) {
    const chunk = await reader.read();
    prefix += new TextDecoder().decode(chunk.value);
  }
  const resourceId = /response_[a-f0-9]{32}/.exec(prefix)?.[0];
  expect(resourceId).toBeTruthy();
  const polled = await json(`/v1/responses/${resourceId}`, undefined, key, "GET");
  expect(polled.response.status).toBe(200);
  controller.abort();
  await reader.cancel().catch(() => undefined);
  await waitFor(() => deps.lifecycle.size === 0);
  const resource = await deps.store.resource(String(resourceId), projectId);
  const rows = await deps.store.db`SELECT state FROM attempts WHERE id=${resource.attempt_id}`;
  expect(rows[0]?.state).toBe("SUCCEEDED");
  const summary = await deps.store.tokenSummary(response.headers.get("x-gateway-request-id") ?? "");
  expect(summary).toEqual({ complete: true, tokens: 15 });
});

test("Batch output JSONL is metered after completion and cancellation without saving content", async () => {
  const model = await configureModel("openai", {}, ["chat", "files", "batches"]);
  const form = new FormData();
  form.set("purpose", "batch");
  form.set(
    "file",
    new File(
      [
        JSON.stringify({
          custom_id: "realistic",
          method: "POST",
          url: "/v1/chat/completions",
          body: { model, messages: [{ role: "user", content: "x" }] },
        }),
      ],
      "batch.jsonl",
    ),
  );
  const uploaded = await fetch(`${origin}/v1/files`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  const file = (await uploaded.json()) as { id: string };
  for (const cancel of [false, true]) {
    const created = await json(
      "/v1/batches",
      { input_file_id: file.id, endpoint: "/v1/chat/completions", completion_window: "24h" },
      key,
    );
    expect(created.response.status).toBe(200);
    if (cancel)
      expect((await json(`/v1/batches/${created.value.id}/cancel`, {}, key)).response.status).toBe(
        200,
      );
    const poll = await json(`/v1/batches/${created.value.id}`, undefined, key, "GET");
    expect(poll.value.usage).toBeUndefined();
    expect(poll.response.status).toBe(200);
    const resource = await deps.store.resource(String(created.value.id), projectId);
    await awaitResourceFinal(String(created.value.id));
    const rows = await deps.store
      .db`SELECT finality,usage FROM usage_events WHERE attempt_id=${resource.attempt_id} AND source='async_final'`;
    expect(rows.length).toBe(1);
    expect(rows[0]?.finality).toBe("FINAL");
    expect(rows[0]?.usage.input_tokens).toBe("10");
    expect(rows[0]?.usage.output_tokens).toBe("5");
    expect(JSON.stringify(rows)).not.toContain("mock-batch-output-content");
  }
});

test("pricing updates preserve dispatched attempt versions and configuration audit commits", async () => {
  const model = await configureModel("openai", { headerDelayMs: 150 });
  const routes = (await deps.store.routeDefinitions(model)) as { id: string; pricing_id: string }[];
  const route = routes[0];
  if (!route) throw new Error("Missing route");
  const pending = sendChat(model);
  await waitFor(
    async () =>
      (await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length === 1,
  );
  expect(
    (
      await json(
        `/admin/routes/${route.id}`,
        {
          pricing: {
            currency: "USD",
            rates: {
              input_tokens: { numerator: "100", denominator: "1" },
              output_tokens: { numerator: "100", denominator: "1" },
            },
          },
        },
        adminKey,
        "PATCH",
      )
    ).response.status,
  ).toBe(200);
  const first = await pending;
  await first.text();
  await awaitUsage(String(first.headers.get("x-gateway-request-id")));
  const firstCost = await deps.store
    .db`SELECT cost_atoms::text FROM usage_events WHERE request_id=${first.headers.get("x-gateway-request-id")}`;
  expect(firstCost[0]?.cost_atoms).toBe("59");
  const second = await sendChat(model);
  await second.text();
  await awaitUsage(String(second.headers.get("x-gateway-request-id")));
  const nextCost = await deps.store
    .db`SELECT cost_atoms::text FROM usage_events WHERE request_id=${second.headers.get("x-gateway-request-id")}`;
  expect(nextCost[0]?.cost_atoms).toBe("1200");
  const audit = await deps.store.db`SELECT id FROM audit_events WHERE action='routes.update'`;
  expect(audit.length).toBeGreaterThan(0);
});

test("Gemini SAFETY has the same content-filter meaning in JSON and SSE", async () => {
  const model = await configureModel("gemini", { finishReason: "SAFETY" });
  const jsonResponse = await sendChat(model);
  const value = (await jsonResponse.json()) as { choices: { finish_reason: string }[] };
  expect(value.choices[0]?.finish_reason).toBe("content_filter");
  const stream = await sendChat(model, { stream: true });
  const body = await stream.text();
  expect(body).toContain('"finish_reason":"content_filter"');
  expect(body).toContain("[DONE]");
  expect(body).not.toContain("gateway_stream_error");
});

test("distributed permits and half-open probes coordinate across two instances", async () => {
  const { Limits } = await import("@gateway/runtime");
  const second = new Limits(process.env.REDIS_URL ?? "", namespace);
  await second.redis.connect();
  const scope = `fixture:${crypto.randomUUID()}`;
  const provider = crypto.randomUUID();
  try {
    const permit = await deps.limits.enter(scope, "first", 100, 1, 1000, 200, 1000);
    await expect(second.enter(scope, "second", 100, 1, 1000, 200, 1000)).rejects.toThrow("limit");
    await permit.adjustTokens(20);
    await permit();
    const next = await second.enter(scope, "third", 100, 1, 1000, 200, 1000);
    await next.adjustTokens(20);
    await next();
    for (let i = 0; i < 3; i++) await deps.limits.observe(provider, true, 10, 30);
    expect((await second.health(provider)).open).toBe(true);
    await Bun.sleep(40);
    const probes = await Promise.all([
      deps.limits.probe(provider, 1000),
      second.probe(provider, 1000),
    ]);
    expect(probes.filter(Boolean).length).toBe(1);
    await second.observe(provider, false, 10);
    expect((await deps.limits.health(provider)).open).toBe(false);
  } finally {
    second.close();
  }
});

test("Redis TCP blackhole fails an established connection within command deadline", async () => {
  const { Limits } = await import("@gateway/runtime");
  const target = new URL(process.env.REDIS_URL ?? "");
  const sockets = new Set<{ terminate: () => void }>();
  let drop = false;
  const connections = new Map<unknown, Promise<Bun.TCPSocket>>();
  const blackhole = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(client) {
        sockets.add(client);
        connections.set(
          client,
          Bun.connect({
            hostname: target.hostname,
            port: Number(target.port || 6379),
            socket: {
              open(upstream) {
                sockets.add(upstream);
              },
              data(_, bytes) {
                client.write(bytes);
              },
              close(upstream) {
                sockets.delete(upstream);
              },
            },
          }),
        );
      },
      async data(client, bytes) {
        const copy = Buffer.from(bytes);
        const upstream = await connections.get(client);
        if (!drop) upstream?.write(copy);
      },
      close(client) {
        sockets.delete(client);
        void connections.get(client)?.then((socket) => socket.terminate());
        connections.delete(client);
      },
    },
  });
  const limits = new Limits(`redis://127.0.0.1:${blackhole.port}`, "blackhole", 100);
  try {
    await limits.redis.connect();
    expect(await limits.command("PING")).toBe("PONG");
    drop = true;
    const started = performance.now();
    await expect(limits.enter("scope", "request", 100, 10, 1000)).rejects.toThrow("unavailable");
    const elapsed = performance.now() - started;
    expect(elapsed).toBeGreaterThan(90);
    expect(elapsed).toBeLessThan(1200);
  } finally {
    limits.redis.close();
    for (const socket of sockets) socket.terminate();
    blackhole.stop(true);
  }
});

test("independent gateway instances share idempotency and admission without shared process state", async () => {
  const secondDeps = await assemble(deps.config);
  const second = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: createApp(secondDeps).fetch });
  const model = await configureModel("openai");
  const intent = crypto.randomUUID();
  const body = JSON.stringify(payload("chat", model, false));
  try {
    const responses = await Promise.all(
      Array.from({ length: 40 }, (_, i) =>
        fetch(`${i % 2 ? origin : `http://127.0.0.1:${second.port}`}/v1/chat/completions`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${key}`,
            "content-type": "application/json",
            "idempotency-key": intent,
          },
          body,
        }),
      ),
    );
    expect(responses.filter((r) => r.status === 200).length).toBe(1);
    expect(responses.filter((r) => r.status === 409).length).toBe(39);
    await Promise.all(responses.map((r) => r.text()));
    expect(
      (await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length,
    ).toBe(1);
  } finally {
    await second.stop(true);
    await secondDeps.lifecycle.drain(1000);
    secondDeps.usage.close();
    secondDeps.limits.close();
    await secondDeps.store.db.close();
  }
});

test("chunked management bodies obey byte and time budgets without Content-Length", async () => {
  const bytes = new TextEncoder().encode('{"name":"' + "x".repeat(70000) + '"}');
  const response = await fetch(`${origin}/admin/projects`, {
    method: "POST",
    headers: { authorization: `Bearer ${adminKey}`, "content-type": "application/json" },
    body: new ReadableStream({
      start(c) {
        c.enqueue(bytes);
        c.close();
      },
    }),
  });
  expect(response.status).toBe(413);
  await response.text();
  await waitFor(() => deps.memory.bufferedBytes === 0);
  expect(deps.memory.bufferedBytes).toBe(0);
});

test("batch validation rejects mixed models and duplicated item identities before dispatch", async () => {
  const model = await configureModel("openai", {}, ["files", "batches", "chat"]);
  const item = {
    custom_id: "1",
    method: "POST",
    url: "/v1/chat/completions",
    body: { model, messages: [{ role: "user", content: "x" }] },
  };
  for (const second of [
    item,
    { ...item, custom_id: "2", body: { ...item.body, model: "different" } },
  ]) {
    const form = new FormData();
    form.set("purpose", "batch");
    form.set(
      "file",
      new File([[item, second].map((value) => JSON.stringify(value)).join("\n")], "batch.jsonl"),
    );
    const response = await fetch(`${origin}/v1/files`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}` },
      body: form,
    });
    expect(response.status).toBe(400);
    await response.text();
  }
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    0,
  );
});

test("crash recovery appends UNKNOWN and later actual evidence supersedes without re-execution", async () => {
  const model = await configureModel("openai", { headerDelayMs: 200 });
  const pending = sendChat(model);
  await waitFor(
    async () =>
      (await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length === 1,
  );
  await deps.store
    .db`UPDATE attempts SET started_at=now()-interval '10 minutes' WHERE upstream_model=${model}`;
  await (await import("../../apps/gateway/src/recovery.ts")).recoverStale(deps);
  const response = await pending;
  expect(response.status).toBe(200);
  await response.text();
  await awaitModelUsage(model, 2);
  const rows = await deps.store
    .db`SELECT e.finality FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model} ORDER BY e.seq`;
  expect(rows.map((row: { finality: string }) => row.finality)).toEqual(["UNKNOWN", "FINAL"]);
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    1,
  );
});

test("event claims are disjoint, expired leases are fenced and registration cannot miss an in-flight commit", async () => {
  const model = await configureModel("openai");
  const result = await sendChat(model);
  await result.text();
  const attempt = (await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`)[0]
    ?.id as string;
  let unlock!: () => void;
  const gate = new Promise<void>((resolve) => {
    unlock = resolve;
  });
  let staged!: () => void;
  const ready = new Promise<void>((resolve) => {
    staged = resolve;
  });
  await awaitModelUsage(model);
  const previous = (
    await deps.store
      .db`SELECT usage FROM usage_events WHERE attempt_id=${attempt} ORDER BY seq DESC LIMIT 1`
  )[0]!.usage;
  const event = crypto.randomUUID();
  const writer = deps.store.db.begin(async (tx) => {
    await tx`SELECT record_usage_evidence(${attempt}::uuid,${event}::uuid,'fixture_late','SUCCEEDED','FINAL',${previous}::jsonb,59,true,NULL,NULL)`;
    staged();
    await gate;
  });
  await ready;
  let registered = false;
  const registration = deps.store.createConsumer(`race-${crypto.randomUUID()}`, true).then((id) => {
    registered = true;
    return id;
  });
  await Bun.sleep(30);
  expect(registered).toBe(false);
  unlock();
  await writer;
  const consumer = await registration;
  await deps.store.backfillConsumer(consumer);
  expect(
    (
      await deps.store
        .db`SELECT event_id FROM event_deliveries WHERE consumer_id=${consumer} AND event_id=${event}`
    ).length,
  ).toBe(1);
  const [a, b] = await Promise.all([
    deps.store.claimEvents(consumer, 500),
    deps.store.claimEvents(consumer, 500),
  ]);
  expect(a.events.length + b.events.length).toBeGreaterThan(0);
  const ids = [...a.events, ...b.events].map((value) => value.id);
  expect(new Set(ids).size).toBe(ids.length);
  const active = a.events.length ? a : b;
  await deps.store
    .db`UPDATE event_deliveries SET lease_until=now()-interval '1 second' WHERE consumer_id=${consumer}`;
  const renewed = await deps.store.claimEvents(consumer, 500);
  expect(renewed.events.length).toBe(ids.length);
  expect(await deps.store.ackEvents(consumer, active.lease_token, ids)).toBe(0);
  expect(await deps.store.ackEvents(consumer, renewed.lease_token, ids)).toBe(ids.length);
});

test("restricted production role can serve inference but cannot mutate usage or DDL", async () => {
  const role = `runtime_${suffix}`;
  const password = crypto.randomUUID().replaceAll("-", "");
  await deps.store.db.unsafe(
    await Bun.file(new URL("../../infra/runtime-role.sql", import.meta.url)).text(),
  );
  await adminDb.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
  await adminDb.unsafe(`GRANT gateway_runtime TO ${role}`);
  const url = new URL(deps.config.DATABASE_URL);
  url.username = role;
  url.password = password;
  const runtimeDeps = await assemble({ ...deps.config, DATABASE_URL: url.toString() });
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: createApp(runtimeDeps).fetch });
  try {
    const model = await configureModel("openai");
    const response = await fetch(`http://127.0.0.1:${server.port}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify(payload("chat", model, false)),
    });
    expect(response.status).toBe(200);
    await response.text();
    await expect(runtimeDeps.store.db`UPDATE usage_events SET cost_atoms=0`).rejects.toThrow();
    await expect(
      runtimeDeps.store.db`UPDATE wallets SET available_atoms=999999999`,
    ).rejects.toThrow();
    await expect(runtimeDeps.store.db`TRUNCATE money_journal`).rejects.toThrow();
    await expect(
      runtimeDeps.store.db`SELECT persist_usage_batch('[]'::jsonb)`,
    ).rejects.toMatchObject({ errno: "42501" });
    const tariffResponse = await fetch(`http://127.0.0.1:${server.port}/admin/tariffs`, {
      method: "POST",
      headers: { authorization: `Bearer ${adminKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model,
        operation: "chat",
        pricing: { currency: "USD", rates: { requests: { numerator: "2", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "REQUEST_COMPLETION",
      }),
    });
    const { recoveryTick } = await import("../../apps/gateway/src/recovery.ts");
    await recoveryTick(runtimeDeps);
    expect(tariffResponse.status).toBe(201);
    await tariffResponse.text();

    await runtimeDeps.store.db.begin(async (tx) => {
      await tx`CREATE TEMP TABLE wallets(LIKE public.wallets INCLUDING ALL) ON COMMIT DROP`;
      await tx.unsafe(
        "CREATE FUNCTION pg_temp.reject_shadow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'temporary shadow was executed';END $$",
      );
      await tx`CREATE TRIGGER reject_shadow BEFORE INSERT ON pg_temp.wallets FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_shadow()`;
      const result =
        await tx`SELECT public.credit_wallet(${projectId}::uuid,'USD',${`shadow-${suffix}`},1,'CASH',100,NULL,'{}','{}') AS result`;
      expect(result[0].result.wallet_id).toBeTruthy();
      const rows = await tx`SELECT count(*)::int AS n FROM pg_temp.wallets`;
      expect(rows[0].n).toBe(0);
    });
    await expect(runtimeDeps.store.db`TRUNCATE usage_events`).rejects.toThrow();
    await expect(runtimeDeps.store.db`DROP TABLE usage_events`).rejects.toThrow();
  } finally {
    await server.stop(true);
    await runtimeDeps.lifecycle.drain(1000);
    runtimeDeps.usage.close();
    runtimeDeps.limits.close();
    await runtimeDeps.store.db.close();
    await deps.store.db.unsafe(`DROP OWNED BY ${role}`);
    await adminDb.unsafe(`DROP ROLE ${role}`);
  }
});

test("completed Responses streams without usage remain recoverable until GET supplies evidence", async () => {
  const model = await configureModel("openai", { omitUsage: true, pollUsage: true });
  const response = await fetch(`${origin}/v1/responses`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify(payload("responses", model, true)),
  });
  const body = await response.text();
  expect(body).toContain("response.completed");
  const rows = await deps.store
    .db`SELECT id,state FROM resources WHERE attempt_id IN(SELECT id FROM attempts WHERE upstream_model=${model})`;
  expect(rows[0]?.state).toBe("PENDING_USAGE");
  const { recoveryTick } = await import("../../apps/gateway/src/recovery.ts");
  await waitFor(async () => {
    await recoveryTick(deps);
    return (await deps.store.resource(String(rows[0]?.id), projectId)).state === "COMPLETED";
  });
  await awaitResourceFinal(String(rows[0]?.id));
  const events = await deps.store
    .db`SELECT e.finality FROM usage_events e JOIN attempts a ON a.id=e.attempt_id WHERE a.upstream_model=${model} ORDER BY e.seq`;
  expect(events.map((row: { finality: string }) => row.finality)).toEqual(["UNKNOWN", "FINAL"]);
});

test("Redis admission reconnects after transport closure instead of staying permanently unavailable", async () => {
  const { Limits } = await import("@gateway/runtime");
  const limits = new Limits(process.env.REDIS_URL ?? "", `reconnect:${suffix}`);
  try {
    expect(await limits.command("PING")).toBe("PONG");
    limits.redis.close();
    await limits.command("PING").catch(() => undefined);
    await waitFor(async () => {
      try {
        return (await limits.command("PING")) === "PONG";
      } catch {
        return false;
      }
    }, 3000);
    const permit = await limits.enter("scope", "recovered", 10, 1, 1000);
    await permit();
  } finally {
    limits.close();
  }
});

test("HTTP project concurrency and RPM deny excess before dispatch and reconcile measured tokens", async () => {
  const model = await configureModel("openai", { headerDelayMs: 100 });
  const project = await json("/admin/projects", {
    name: "quota-boundary",
    rpm: 2,
    tpm: 2000,
    concurrency: 1,
  });
  await deps.funds.credit(String(project.value.id), {
    currency: "USD",
    external_ref: "test-funding",
    amount_atoms: "1000000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const created = await json(`/admin/projects/${project.value.id}/keys`, {
    name: "quota",
    models: [model],
  });
  const token = String(created.value.key);
  const calls = await Promise.all([sendChat(model, {}, token), sendChat(model, {}, token)]);
  expect(calls.map((r) => r.status).sort()).toEqual([200, 429]);
  await Promise.all(calls.map((r) => r.text()));
  const next = await sendChat(model, {}, token);
  expect(next.status).toBe(200);
  await next.text();
  const limited = await sendChat(model, {}, token);
  expect(limited.status).toBe(429);
  await limited.text();
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    2,
  );
});

test("streamed model content is absent from PG, admission Redis and queued payloads", async () => {
  const canary = `private-stream-${crypto.randomUUID()}`;
  const model = await configureModel("anthropic", { output: canary, chunkBytes: 1 });
  await billing.consumer.worker.pause();
  try {
    const response = await sendChat(model, { stream: true });
    expect(await response.text()).toContain(canary);
    await waitFor(() => deps.memory.bufferedBytes === 0);
    expect((await deps.usage.producer.stats()).waiting).toBeGreaterThan(0);
    const tables = await deps.store.db<
      { tablename: string }[]
    >`SELECT tablename FROM pg_tables WHERE schemaname='public'`;
    for (const table of tables) {
      expect(table.tablename).toMatch(/^[a-z_]+$/);
      const rows = await deps.store.db.unsafe(
        `SELECT row_to_json(t)::text AS row FROM ${table.tablename} t`,
      );
      expect(JSON.stringify(rows)).not.toContain(canary);
    }
    let queuedPayloads = 0;
    for (const [url, pattern] of [
      [deps.config.REDIS_URL, `${namespace}:*`],
      [deps.config.QUEUE_REDIS_URL, `{${namespace}:usage}:*`],
    ]) {
      const redis = new Bun.RedisClient(url!);
      try {
        let cursor = "0";
        do {
          const [next, keys] = (await redis.send("SCAN", [
            cursor,
            "MATCH",
            pattern!,
            "COUNT",
            "1000",
          ])) as [string, string[]];
          cursor = next;
          for (const key of keys) {
            const type = await redis.send("TYPE", [key]);
            const command =
              type === "hash"
                ? "HGETALL"
                : type === "zset"
                  ? "ZRANGE"
                  : type === "list"
                    ? "LRANGE"
                    : type === "set"
                      ? "SMEMBERS"
                      : type === "stream"
                        ? "XRANGE"
                        : "GET";
            const args =
              type === "zset" || type === "list"
                ? [key, "0", "-1"]
                : type === "stream"
                  ? [key, "-", "+"]
                  : [key];
            const value = await redis.send(command, args);
            expect(JSON.stringify(value)).not.toContain(canary);
            if (
              type === "hash" &&
              url === deps.config.QUEUE_REDIS_URL &&
              (await redis.send("HGET", [key, "data"]))
            )
              queuedPayloads++;
          }
        } while (cursor !== "0");
      } finally {
        redis.close();
      }
    }
    expect(queuedPayloads).toBeGreaterThan(0);
  } finally {
    await billing.consumer.worker.resume();
  }
});

test("resource keyset pagination applies authorization before limit and protects cursor ownership", async () => {
  const allowed = await configureModel("openai");
  const hidden = await configureModel("openai");
  const resourceIds: string[] = [];
  for (let i = 0; i < 3; i++) {
    const result = await json("/v1/responses", payload("responses", allowed, false), key);
    resourceIds.push(String(result.value.id));
  }
  await json("/v1/responses", payload("responses", hidden, false), key);
  const scoped = await json(`/admin/projects/${projectId}/keys`, {
    name: "paged",
    models: [allowed],
  });
  const token = String(scoped.value.key);
  const page1 = await json("/v1/responses?limit=2", undefined, token, "GET");
  expect(page1.value.has_more).toBe(true);
  const first = page1.value.data as { id: string }[];
  expect(first.length).toBe(2);
  const page2 = await json(
    `/v1/responses?limit=2&after=${page1.value.last_id}`,
    undefined,
    token,
    "GET",
  );
  expect(page2.value.has_more).toBe(false);
  const rest = page2.value.data as { id: string }[];
  expect(rest.length).toBe(1);
  expect(new Set([...first, ...rest].map((row) => row.id))).toEqual(new Set(resourceIds));
  expect(
    (await json(`/v1/responses?after=${first[0]?.id}`, undefined, otherKey, "GET")).response.status,
  ).toBe(404);
  expect(
    (await json("/v1/usage?before=9999999999999999999", undefined, key, "GET")).response.status,
  ).toBe(400);
});

test("Gemini blocked prompts without candidates preserve native feedback and convert to content filtering", async () => {
  const model = await configureModel("gemini", { promptBlocked: true });
  for (const protocol of ["chat", "gemini"] as const)
    for (const stream of [false, true]) {
      const response = await fetch(`${origin}${pathFor(protocol, model, stream)}`, {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify(payload(protocol, model, stream)),
      });
      expect(response.status).toBe(200);
      const text = await response.text();
      expect(text).toContain(protocol === "gemini" ? "SAFETY" : "content_filter");
      expect(text).not.toContain("gateway_stream_error");
    }
  expect((await deps.store.db`SELECT id FROM attempts WHERE upstream_model=${model}`).length).toBe(
    4,
  );
});

test("per-key CIDRs check transport IP and ignore spoofed forwarding headers", async () => {
  const model = await configureModel("openai");
  const allowed = await json(`/admin/projects/${projectId}/keys`, {
    name: "IP allowed",
    allowed_cidrs: ["127.0.0.1/32"],
  });
  const denied = await json(`/admin/projects/${projectId}/keys`, {
    name: "IP denied",
    allowed_cidrs: ["203.0.113.7/32"],
  });
  for (const [token, status] of [
    [allowed.value.key, 200],
    [denied.value.key, 403],
  ] as const) {
    const response = await fetch(`${origin}/v1/chat/completions`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${String(token)}`,
        "content-type": "application/json",
        "x-forwarded-for": "203.0.113.7",
      },
      body: JSON.stringify(payload("chat", model, false)),
    });
    expect(response.status).toBe(status);
    await response.text();
  }
});
test("technical organization/principal RBAC rejects cross-tenant keys and disabled membership", async () => {
  const org = await json("/admin/organizations", {
    external_ref: crypto.randomUUID(),
    name: "technical",
  });
  const principal = await json(`/admin/organizations/${org.value.id}/principals`, {
    external_ref: crypto.randomUUID(),
    role: "MEMBER",
  });
  const tenant = await json("/admin/projects", { name: "tenant", organization_id: org.value.id });
  await deps.funds.credit(String(tenant.value.id), {
    currency: "USD",
    external_ref: "funding",
    amount_atoms: "1000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const denied = await json(`/admin/projects/${projectId}/keys`, {
    name: "foreign principal",
    principal_id: principal.value.id,
  });
  expect(denied.response.status).toBe(403);
  const elevated = await json(`/admin/projects/${tenant.value.id}/keys`, {
    name: "escalation",
    principal_id: principal.value.id,
    role: "ADMIN",
  });
  expect(elevated.response.status).toBe(403);
  const created = await json(`/admin/projects/${tenant.value.id}/keys`, {
    name: "member",
    principal_id: principal.value.id,
  });
  expect(created.response.status).toBe(201);
  const model = await configureModel("openai");
  let response = await sendChat(model, {}, String(created.value.key));
  expect(response.status).toBe(200);
  await response.text();
  await json(`/admin/principals/${principal.value.id}`, { enabled: false }, adminKey, "PATCH");
  response = await sendChat(model, {}, String(created.value.key));
  expect(response.status).toBe(401);
  await response.text();
  const viewer = await json(`/admin/projects/${projectId}/keys`, {
    name: "viewer",
    role: "VIEWER",
  });
  response = await sendChat(model, {}, String(viewer.value.key));
  expect(response.status).toBe(403);
  await response.text();
  const listed = await json("/v1/project/keys", undefined, String(viewer.value.key), "GET");
  expect(listed.response.status).toBe(403);
});
test("round-robin rotates equal priorities and least-latency selects shared measurements", async () => {
  const { chooseRoutes } = await import("../../apps/gateway/src/routing.ts");
  const model = await configureModel("openai");
  await json(`/admin/models/${model}/routes`, {
    provider_id: providers.anthropic,
    upstream_model: model,
    operations: ["chat"],
    priority: 10,
    weight: 1,
    pricing: { currency: "USD", rates: {} },
  });
  await json(`/admin/models/${model}`, { routing_strategy: "ROUND_ROBIN" }, adminKey, "PATCH");
  const chosen: string[] = [];
  for (let i = 0; i < 6; i++)
    chosen.push((await chooseRoutes(deps, model, "chat"))[0]!.provider_id);
  expect(chosen[0]).not.toBe(chosen[1]);
  expect(chosen[0]).toBe(chosen[2]);
  expect(chosen[1]).toBe(chosen[3]);
  await deps.limits.command("DEL", [
    `${namespace}:health:${providers.openai}`,
    `${namespace}:health:${providers.anthropic}`,
  ]);
  await deps.limits.observe(String(providers.openai), false, 400);
  await deps.limits.observe(String(providers.anthropic), false, 10);
  await json(`/admin/models/${model}`, { routing_strategy: "LEAST_LATENCY" }, adminKey, "PATCH");
  expect((await chooseRoutes(deps, model, "chat"))[0]!.provider_id).toBe(
    String(providers.anthropic),
  );
});

test("native Gemini models, JSON and SSE enforce the same Key CIDRs before dispatch", async () => {
  const model = await configureModel("gemini");
  const denied = await json(`/admin/projects/${projectId}/keys`, {
    name: "Gemini IP denied",
    allowed_cidrs: ["203.0.113.7/32"],
  });
  const before = await deps.store
    .db`SELECT count(*)::int AS n FROM attempts WHERE upstream_model=${model}`;
  for (const path of [
    "/v1beta/models",
    ...[false, true].map((stream) => pathFor("gemini", model, stream)),
  ]) {
    const response = await fetch(`${origin}${path}`, {
      method: path === "/v1beta/models" ? "GET" : "POST",
      headers: {
        "x-goog-api-key": String(denied.value.key),
        "content-type": "application/json",
        "x-forwarded-for": "203.0.113.7",
      },
      ...(path === "/v1beta/models"
        ? {}
        : { body: JSON.stringify(payload("gemini", model, false)) }),
    });
    expect(response.status).toBe(403);
    await response.text();
  }
  const after = await deps.store
    .db`SELECT count(*)::int AS n FROM attempts WHERE upstream_model=${model}`;
  expect(after[0].n).toBe(before[0].n);
});

test("VIEWER and BILLING roles cannot delete or cancel upstream-owned resources", async () => {
  const model = await configureModel("openai", {}, ["chat", "files", "batches"]);
  const form = new FormData();
  form.set("purpose", "batch");
  form.set(
    "file",
    new File(
      [
        JSON.stringify({
          custom_id: "role",
          method: "POST",
          url: "/v1/chat/completions",
          body: { model, messages: [{ role: "user", content: "test" }] },
        }) + "\n",
      ],
      "role.jsonl",
    ),
  );
  const upload = await fetch(`${origin}/v1/files`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(upload.status).toBe(200);
  const resource = (await upload.json()) as { id: string };
  for (const role of ["VIEWER", "BILLING"]) {
    const created = await json(`/admin/projects/${projectId}/keys`, {
      name: `read-only ${role}`,
      role,
    });
    const token = String(created.value.key);
    for (const [method, path] of [
      ["DELETE", `/v1/files/${resource.id}`],
      ["POST", `/v1/batches/batch_${"a".repeat(32)}/cancel`],
    ] as const) {
      const result = await json(path, method === "POST" ? {} : undefined, token, method);
      expect(result.response.status).toBe(403);
    }
    expect((await deps.store.resource(resource.id, projectId)).state).not.toBe("DELETED");
  }
  const stillThere = await json(`/v1/files/${resource.id}`, undefined, key, "GET");
  expect(stillThere.response.status).toBe(200);
  void model;
});

test("queued known rejection releases its complete pre-hold after consumer resumes", async () => {
  const model = await configureModel("openai", { status: 400 });
  const p = await json("/admin/projects", { name: "rejected-wallet" });
  const pid = String(p.value.id);
  await deps.funds.credit(pid, {
    currency: "USD",
    external_ref: "fund",
    amount_atoms: "100",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const created = await json(`/admin/projects/${pid}/keys`, { name: "reject" });
  await billing.consumer.worker.pause();
  try {
    const response = await sendChat(model, {}, String(created.value.key));
    expect(response.status).toBe(400);
    await response.text();
    const holds = await deps.store
      .db`SELECT h.state,h.amount_atoms::text FROM billing_holds h JOIN gateway_requests q ON q.id=h.request_id WHERE q.project_id=${pid}`;
    expect(holds[0]).toMatchObject({ state: "HELD", amount_atoms: "1" });
    expect(
      ((await deps.funds.wallets(pid)) as { available_atoms: string }[])[0]?.available_atoms,
    ).toBe("99");
  } finally {
    billing.consumer.worker.resume();
  }
  await waitFor(
    async () =>
      (
        await deps.store
          .db`SELECT 1 FROM billing_holds h JOIN gateway_requests q ON q.id=h.request_id WHERE q.project_id=${pid} AND h.state='RELEASED'`
      ).length === 1,
  );
  expect(
    ((await deps.funds.wallets(pid)) as { available_atoms: string; reserved_atoms: string }[])[0],
  ).toMatchObject({ available_atoms: "100", reserved_atoms: "0" });
});

test("production worker role advances async resources and commits queued usage with financial settlement", async () => {
  const role = `worker_${suffix}`;
  const password = crypto.randomUUID().replaceAll("-", "");
  await deps.store.db.unsafe(
    await Bun.file(new URL("../../infra/runtime-role.sql", import.meta.url)).text(),
  );
  await adminDb.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
  await adminDb.unsafe(`GRANT gateway_worker TO ${role}`);
  const url = new URL(deps.config.DATABASE_URL);
  url.username = role;
  url.password = password;
  const workerDeps = await assemble({
    ...deps.config,
    DATABASE_URL: url.toString(),
    REDIS_NAMESPACE: `worker-role:${suffix}`,
    DB_POOL_SIZE: 4,
  });
  const consumer = new BillingWorker(workerDeps);
  try {
    await consumer.start();
    const model = await configureModel("openai");
    const background = await json(
      "/v1/responses",
      { ...payload("responses", model, false), background: true },
      key,
    );
    const resource = await deps.store.resource(String(background.value.id), projectId);
    await workerDeps.store.updateResource(resource.id, "PENDING_USAGE", "completed");
    await workerDeps.usage.publish({
      attemptId: resource.attempt_id!,
      source: "async_final",
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { input_tokens: "7", cached_input_tokens: "3", output_tokens: "5", requests: "1" },
    });
    const request = (
      await deps.store.db`SELECT request_id FROM attempts WHERE id=${resource.attempt_id}`
    )[0]!.request_id;
    await waitFor(async () => (await deps.funds.receipt(request, projectId)).charged_atoms === "1");
    expect((await deps.funds.receipt(request, projectId)).billing_outcome).toBe("SUCCESS");
  } finally {
    await consumer.close();
    workerDeps.usage.close();
    workerDeps.limits.close();
    await workerDeps.store.db.close();
    await deps.store.db.unsafe(`DROP OWNED BY ${role}`);
    await adminDb.unsafe(`DROP ROLE ${role}`);
  }
});

test("HTTP rejection retains numeric usage as partial evidence without storing its error body", async () => {
  const model = await configureModel("openai", { status: 400, errorUsage: true });
  const response = await sendChat(model);
  expect(response.status).toBe(400);
  await response.text();
  await awaitModelUsage(model);
  await waitFor(async () => {
    const rows = await deps.store
      .db`SELECT q.state FROM gateway_requests q JOIN attempts a ON a.request_id=q.id WHERE a.upstream_model=${model}`;
    return rows[0]?.state === "FAILED";
  });
  const rows = await deps.store
    .db`SELECT a.state,e.finality,e.usage,e.cost_atoms::text,e.cost_complete,q.id AS request_id FROM attempts a JOIN gateway_requests q ON q.id=a.request_id JOIN usage_events e ON e.id=a.evidence_id WHERE a.upstream_model=${model}`;
  expect(rows[0]).toMatchObject({
    state: "PARTIAL",
    finality: "PARTIAL",
    usage: { input_tokens: "7", cached_input_tokens: "3", output_tokens: "5" },
    cost_atoms: "59",
    cost_complete: false,
  });
  const receipt = await deps.funds.receipt(rows[0]!.request_id, projectId);
  expect(receipt.charged_atoms).toBeNull();
  expect(receipt.hold_state).toBe("RECONCILING");
  const events = await deps.store
    .db`SELECT row_to_json(e)::text AS value FROM usage_events e WHERE request_id=${rows[0]!.request_id}`;
  expect(JSON.stringify(events)).not.toContain("mock-secret-error-must-not-persist");
});

test("unreadable oversized HTTP errors remain unknown instead of releasing pre-holds", async () => {
  const model = await configureModel("openai", {
    status: 400,
    errorUsage: true,
    errorBodyBytes: 100000,
  });
  const response = await sendChat(model);
  expect(response.status).toBe(400);
  await response.text();
  await awaitModelUsage(model);
  await waitFor(async () => {
    const rows = await deps.store
      .db`SELECT q.state FROM gateway_requests q JOIN attempts a ON a.request_id=q.id WHERE a.upstream_model=${model}`;
    return rows[0]?.state === "FAILED";
  });
  const rows = await deps.store
    .db`SELECT a.state,a.request_id FROM attempts a WHERE a.upstream_model=${model}`;
  expect(rows[0]!.state).toBe("UNKNOWN");
  const receipt = await deps.funds.receipt(rows[0]!.request_id, projectId);
  expect(receipt.hold_state).toBe("RECONCILING");
  expect(receipt.charged_atoms).toBeNull();
  expect(BigInt(String(receipt.reserved_atoms))).toBeGreaterThan(0n);
});

test("local result finalization failure preserves known upstream cost without retrying generation", async () => {
  const model = await configureModel("openai");
  const original = deps.store.saveResource.bind(deps.store);
  deps.store.saveResource = async () => {
    throw new Error("injected resource metadata failure");
  };
  try {
    const response = await json("/v1/responses", payload("responses", model, false), key);
    expect(response.response.status).toBe(502);
    await awaitModelUsage(model);
    await waitFor(async () => {
      const rows = await deps.store
        .db`SELECT q.state FROM gateway_requests q JOIN attempts a ON a.request_id=q.id WHERE a.upstream_model=${model}`;
      return rows[0]?.state === "FAILED";
    });
    const rows = await deps.store
      .db`SELECT a.request_id,a.state,e.finality,e.cost_atoms::text,e.cost_complete FROM attempts a JOIN usage_events e ON e.id=a.evidence_id WHERE a.upstream_model=${model}`;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      state: "SUCCEEDED",
      finality: "FINAL",
      cost_atoms: "59",
      cost_complete: true,
    });
    const receipt = await deps.funds.receipt(rows[0]!.request_id, projectId);
    expect(receipt.hold_state).toBe("RELEASED");
    expect(receipt.charged_atoms).toBe("0");
  } finally {
    deps.store.saveResource = original;
  }
});

test("project key delegation inherits authority and cannot escape its principal or scope", async () => {
  const org = await json("/admin/organizations", {
    external_ref: crypto.randomUUID(),
    name: "delegation",
  });
  const principal = await json(`/admin/organizations/${org.value.id}/principals`, {
    external_ref: crypto.randomUUID(),
    role: "ADMIN",
  });
  const tenant = await json("/admin/projects", {
    name: "delegation",
    organization_id: org.value.id,
  });
  await deps.funds.credit(String(tenant.value.id), {
    currency: "USD",
    external_ref: "funding",
    amount_atoms: "1000",
    kind: "CASH",
    priority: 100,
    models: [],
    operations: [],
  });
  const model = await configureModel("openai");
  const other = await configureModel("openai");
  const expiry = new Date(Date.now() + 600000).toISOString();
  const parent = await json(`/admin/projects/${tenant.value.id}/keys`, {
    name: "restricted-admin",
    principal_id: principal.value.id,
    role: "ADMIN",
    models: [model],
    operations: ["chat"],
    allowed_cidrs: ["127.0.0.0/8"],
    expires_at: expiry,
    rpm: 100,
    tpm: 1000,
    concurrency: 10,
  });
  expect(parent.response.status).toBe(201);
  const created = await json("/v1/project/keys", { name: "inherited" }, String(parent.value.key));
  expect(created.response.status).toBe(201);
  expect(created.value.principal_id).toBe(principal.value.id);
  expect(created.value.models).toEqual([model]);
  expect(created.value.operations).toEqual(["chat"]);
  expect(created.value.allowed_cidrs).toEqual(["127.0.0.0/8"]);
  expect(new Date(String(created.value.expires_at)).getTime()).toBe(new Date(expiry).getTime());
  expect(created.value.rpm).toBe(100);
  expect(created.value.tpm).toBe(1000);
  expect(created.value.concurrency).toBe(10);
  for (const broaden of [
    { models: [other] },
    { operations: ["responses"] },
    { allowed_cidrs: ["0.0.0.0/0"] },
    { expires_at: new Date(Date.now() + 3600000).toISOString() },
    { rpm: 101 },
    { tpm: 1001 },
    { concurrency: 11 },
    { principal_id: crypto.randomUUID() },
  ]) {
    const denied = await json(
      "/v1/project/keys",
      { name: "escape", ...broaden },
      String(parent.value.key),
    );
    expect(denied.response.status).toBe(403);
  }
  const narrower = await json(
    "/v1/project/keys",
    { name: "narrower", allowed_cidrs: ["127.0.0.1/32"], rpm: 5 },
    String(parent.value.key),
  );
  expect(narrower.response.status).toBe(201);
  let response = await sendChat(model, {}, String(created.value.key));
  expect(response.status).toBe(200);
  await response.text();
  response = await sendChat(other, {}, String(created.value.key));
  expect(response.status).toBe(403);
  await response.text();
  await json(`/admin/principals/${principal.value.id}`, { enabled: false }, adminKey, "PATCH");
  for (const delegated of [created, narrower]) {
    response = await sendChat(model, {}, String(delegated.value.key));
    expect(response.status).toBe(401);
    await response.text();
  }
});

test("Batch rows with missing priced dimensions remain partial until every row is complete", async () => {
  const full = {
    prompt_tokens: 10,
    completion_tokens: 5,
    prompt_tokens_details: { cached_tokens: 0 },
  };
  const model = await configureModel(
    "openai",
    { batchRowUsage: [full, { prompt_tokens: 10, prompt_tokens_details: { cached_tokens: 0 } }] },
    ["files", "batches", "chat"],
  );
  await deps.funds.setTariff({
    model,
    operation: "batches",
    pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "3", denominator: "1" },
        output_tokens: { numerator: "7", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "1" },
      },
    },
    ceilings: { input_tokens: "100", output_tokens: "100", cached_input_tokens: "100" },
    basis: "PROVIDER_USAGE",
  });
  const form = new FormData();
  form.set("purpose", "batch");
  form.set(
    "file",
    new File(
      [
        Array.from({ length: 2 }, (_, i) =>
          JSON.stringify({
            custom_id: `item-${i + 1}`,
            method: "POST",
            url: "/v1/chat/completions",
            body: { model, messages: [{ role: "user", content: "x" }] },
          }),
        ).join("\n"),
      ],
      "batch.jsonl",
    ),
  );
  const uploaded = await fetch(`${origin}/v1/files`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(uploaded.status).toBe(200);
  const file = (await uploaded.json()) as { id: string };
  const created = await json(
    "/v1/batches",
    { input_file_id: file.id, endpoint: "/v1/chat/completions", completion_window: "24h" },
    key,
  );
  expect(created.response.status).toBe(200);
  expect(
    (await json(`/v1/batches/${created.value.id}`, undefined, key, "GET")).response.status,
  ).toBe(200);
  const resource = await deps.store.resource(String(created.value.id), projectId);
  await waitFor(
    async () =>
      Number(
        (
          await deps.store
            .db`SELECT count(*)::int AS n FROM usage_events WHERE attempt_id=${resource.attempt_id} AND usage?'input_tokens'`
        )[0].n,
      ) > 0,
  );
  const evidence = await deps.store.db<
    { finality: string; cost_complete: boolean }[]
  >`SELECT finality,cost_complete FROM usage_events WHERE attempt_id=${resource.attempt_id} AND usage?'input_tokens'`;
  expect(evidence.every((row) => row.finality === "PARTIAL" && row.cost_complete === false)).toBe(
    true,
  );
  const request = (
    await deps.store.db`SELECT request_id FROM attempts WHERE id=${resource.attempt_id}`
  )[0].request_id;
  expect((await deps.funds.receipt(request, projectId)).charged_atoms).toBeNull();
  expect((await deps.store.resource(String(created.value.id), projectId)).state).toBe(
    "PENDING_USAGE",
  );
  mock.scenarios.set(model, { batchRowUsage: [full, full] });
  await json(`/v1/batches/${created.value.id}`, undefined, key, "GET");
  await waitFor(
    async () => String((await deps.funds.receipt(request, projectId)).charged_atoms) === "130",
  );
  const final = await deps.store
    .db`SELECT usage,cost_atoms::text,cost_complete FROM usage_events WHERE attempt_id=${resource.attempt_id} AND source='async_final' AND finality='FINAL'`;
  expect(final).toHaveLength(1);
  expect(final[0].usage).toMatchObject({
    input_tokens: "20",
    output_tokens: "10",
    cached_input_tokens: "0",
  });
  expect(final[0].cost_atoms).toBe("130");
  expect(final[0].cost_complete).toBe(true);
});

test("retry dispatch rechecks route, provider, model and credential after Retry-After", async () => {
  for (const gate of ["route", "provider", "model", "key"] as const) {
    await deps.limits.observe(providers.openai!, false, 1);
    const model = await configureModel("openai", { status: 429, failFirst: 1 });
    const credential = await json(`/admin/projects/${projectId}/keys`, { name: `retry-${gate}` });
    const route = (await deps.store.routeDefinitions(model))[0] as { id: string };
    const response = sendChat(model, {}, String(credential.value.key));
    await waitFor(
      async () =>
        Number(
          (
            await deps.store
              .db`SELECT count(*)::int AS n FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.model=${model} AND a.state='UNKNOWN'`
          )[0].n,
        ) === 1,
    );
    try {
      if (gate === "route")
        await json(`/admin/routes/${route.id}`, { enabled: false }, adminKey, "PATCH");
      if (gate === "provider")
        await json(`/admin/providers/${providers.openai}`, { enabled: false }, adminKey, "PATCH");
      if (gate === "model")
        await json(`/admin/models/${model}`, { enabled: false }, adminKey, "PATCH");
      if (gate === "key")
        expect(
          (await json(`/admin/keys/${credential.value.id}`, undefined, adminKey, "DELETE")).response
            .status,
        ).toBe(200);
      const result = await response;
      expect(result.status).toBe(403);
      await result.text();
      const attempts = await deps.store
        .db`SELECT a.state FROM attempts a JOIN gateway_requests q ON q.id=a.request_id WHERE q.model=${model}`;
      expect(attempts).toHaveLength(1);
      expect(attempts[0].state).toBe("UNKNOWN");
    } finally {
      await deps.limits.observe(providers.openai!, false, 1);
      if (gate === "provider")
        await json(`/admin/providers/${providers.openai}`, { enabled: true }, adminKey, "PATCH");
    }
  }
});

test("Gemini video multipart is converted to JSON and unrepresentable reference files are rejected", async () => {
  const model = await configureModel("gemini", {}, ["videos"]);
  const form = new FormData();
  form.set("model", model);
  form.set("prompt", "video fixture");
  form.set("seconds", "8");
  form.set("size", "1280x720");
  const response = await fetch(`${origin}/v1/videos`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(response.status).toBe(200);
  const result = (await response.json()) as { id: string };
  expect(result.id).toStartWith("video_");
  form.set("input_reference", new File(["image"], "image.png", { type: "image/png" }));
  const denied = await fetch(`${origin}/v1/videos`, {
    method: "POST",
    headers: { authorization: `Bearer ${key}` },
    body: form,
  });
  expect(denied.status).toBe(422);
  await denied.text();
});

test("successful JSON without token usage retains exact per-request supplier cost", async () => {
  const model = await configureModel("openai", { omitUsage: true });
  const route = (await deps.store.routeDefinitions(model))[0] as { id: string };
  await json(
    `/admin/routes/${route.id}`,
    { pricing: { currency: "USD", rates: { requests: { numerator: "17", denominator: "1" } } } },
    adminKey,
    "PATCH",
  );
  const response = await sendChat(model);
  expect(response.status).toBe(200);
  const request = response.headers.get("x-gateway-request-id")!;
  await response.text();
  await awaitUsage(request);
  const rows = await deps.store
    .db`SELECT usage,cost_atoms::text,cost_complete FROM usage_events WHERE request_id=${request}`;
  expect(rows).toHaveLength(1);
  expect(rows[0].usage).toEqual({ requests: "1" });
  expect(rows[0].cost_atoms).toBe("17");
  expect(rows[0].cost_complete).toBe(true);
});

test("empty chat JSON and terminal-only streams cannot create successful request charges", async () => {
  for (const scenario of [
    { emptyChoices: true },
    { terminalOnly: true },
    { invalidChoice: "missing_message" as const },
    { invalidChoice: "empty_message" as const },
    { invalidChoice: "unfinished" as const },
  ]) {
    const stream = "terminalOnly" in scenario;
    await deps.limits.observe(providers.openai!, false, 1);
    const model = await configureModel("openai", scenario);
    const response = await sendChat(model, { stream });
    const body = await response.text();
    expect(response.status >= 400 || !body.includes("[DONE]")).toBe(true);
    await waitFor(
      async () =>
        Number(
          (
            await deps.store
              .db`SELECT count(*)::int AS n FROM gateway_requests WHERE model=${model}`
          )[0].n,
        ) > 0,
    );
    const rows = await deps.store
      .db`SELECT q.state,(SELECT count(*)::int FROM billing_charges c WHERE c.request_id=q.id) AS charges FROM gateway_requests q WHERE q.model=${model}`;
    expect(
      rows.every(
        (row: { state: string; charges: number }) => row.state !== "SUCCEEDED" && row.charges === 0,
      ),
    ).toBe(true);
  }
});
