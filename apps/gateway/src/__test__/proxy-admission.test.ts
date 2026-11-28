import { expect, test } from "bun:test";
import { createConnection } from "node:net";
import { createKey } from "@gateway/runtime";
import type { GatewayConfig } from "../config.ts";
import { authenticateRequest } from "../inference/auth.ts";
import { handleInferenceRequest } from "../proxy.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import { createBodyBufferBudget } from "../public-ingress.ts";
import {
  makeBillingAccount,
  makeSnapshot,
  makeApiKey,
  makeConfig,
  makeFakeDeps,
  TEST_PEPPER,
} from "./proxy-fixture.ts";

function sendSlowChunkedUpload(
  port: number,
  apiKey: string,
  initialChunk: string,
): { response: Promise<string>; close: () => void } {
  const socket = createConnection({ host: "127.0.0.1", port });
  const chunks: Buffer[] = [];
  const response = new Promise<string>((resolve) => {
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("error", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  socket.on("connect", () => {
    const hexLen = Buffer.byteLength(initialChunk, "utf8").toString(16);
    socket.write(
      `POST /v1/chat/completions HTTP/1.1\r\n` +
        `Host: 127.0.0.1:${port}\r\n` +
        `Authorization: Bearer ${apiKey}\r\n` +
        `Content-Type: application/json\r\n` +
        `Transfer-Encoding: chunked\r\n` +
        `Connection: close\r\n\r\n` +
        `${hexLen}\r\n${initialChunk}\r\n`,
    );
  });
  return {
    response,
    close: () => socket.destroy(),
  };
}

function sendSlowJsonUpload(
  port: number,
  apiKey: string,
  contentLength: number,
  initialBody: string,
): { response: Promise<string>; close: () => void } {
  const socket = createConnection({ host: "127.0.0.1", port });
  const chunks: Buffer[] = [];
  const response = new Promise<string>((resolve) => {
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.on("error", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  socket.on("connect", () => {
    socket.write(
      `POST /v1/chat/completions HTTP/1.1\r\n` +
        `Host: 127.0.0.1:${port}\r\n` +
        `Authorization: Bearer ${apiKey}\r\n` +
        `Content-Type: application/json\r\n` +
        `Content-Length: ${contentLength}\r\n` +
        `Connection: close\r\n\r\n` +
        initialBody,
    );
  });
  return {
    response,
    close: () => socket.destroy(),
  };
}

test("F1 symptom: slow JSON upload times out with 408 and frees admission slot instead of hanging indefinitely", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 4);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 500,
    ...({ bodyReadTimeoutMs: 120 } as Partial<GatewayConfig>),
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
          json(data, status, headers) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...headers },
            });
          },
        },
        deps,
        "chat",
      );
    },
  });
  const conn = sendSlowJsonUpload(server.port!, rawKey, 200, '{"model":"gpt"');
  try {
    const raw = await Promise.race([
      conn.response,
      Bun.sleep(1000).then(() => "TIMED_OUT_WAITING_FOR_RESPONSE"),
    ]);
    expect(raw).toContain("408");
    expect(raw).toContain("request_timeout");
    await Bun.sleep(30);
    expect(deps.lifecycle.size).toBe(0);
  } finally {
    conn.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F1 symptom: slow chunked JSON upload times out with 408 and frees admission slot", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 4);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 500,
    bodyReadTimeoutMs: 120,
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
          json(data, status, headers) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...headers },
            });
          },
        },
        deps,
        "chat",
      );
    },
  });
  const conn = sendSlowChunkedUpload(server.port!, rawKey, '{"model":"gpt"');
  try {
    const raw = await Promise.race([
      conn.response,
      Bun.sleep(1000).then(() => "TIMED_OUT_WAITING_FOR_RESPONSE"),
    ]);
    expect(raw).toContain("408");
    expect(raw).toContain("request_timeout");
    await Bun.sleep(30);
    expect(deps.lifecycle.size).toBe(0);
  } finally {
    conn.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F1 symptom: single tenant slow uploads are capped at billingAccount concurrency before body read and cannot starve other tenants", async () => {
  const projA = makeBillingAccount("11111111-1111-4111-8111-111111111111", 2);
  const projB = makeBillingAccount("22222222-2222-4222-8222-222222222222", 2);
  const keyA = createKey();
  const keyB = createKey();
  const snap = makeSnapshot(
    1,
    [projA, projB],
    [makeApiKey(keyA, projA.id), makeApiKey(keyB, projB.id)],
  );
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 2_000,
    ...({ bodyReadTimeoutMs: 1_500 } as Partial<GatewayConfig>),
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
          json(data, status, headers) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...headers },
            });
          },
        },
        deps,
        "chat",
      );
    },
  });
  const slow1 = sendSlowJsonUpload(server.port!, keyA, 200, "{");
  const slow2 = sendSlowJsonUpload(server.port!, keyA, 200, "{");
  try {
    const deadline = Date.now() + 1000;
    while (deps.lifecycle.size < 2 && Date.now() < deadline) await Bun.sleep(5);
    expect(deps.lifecycle.size).toBe(2);

    const thirdA = sendSlowJsonUpload(server.port!, keyA, 200, "{");
    const thirdARaw = await Promise.race([
      thirdA.response,
      Bun.sleep(600).then(() => "THIRD_A_BLOCKED_ON_BODY_READ"),
    ]);
    thirdA.close();
    expect(thirdARaw).toContain("429");
    expect(thirdARaw).toContain("rate_limit_concurrency");

    const resB = await fetch(`http://127.0.0.1:${server.port}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${keyB}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "missing-model" }),
    });
    expect(resB.status).toBe(404);
  } finally {
    slow1.close();
    slow2.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("F8 symptom: distinct runtime instances isolate auth caches instead of sharing module globals", () => {
  const projA = makeBillingAccount("11111111-1111-4111-8111-111111111111", 10);
  const projB = makeBillingAccount("22222222-2222-4222-8222-222222222222", 10);
  const rawKey = createKey();
  const snapA = makeSnapshot(1, [projA], [makeApiKey(rawKey, projA.id)]);
  const snapB = makeSnapshot(1, [projB], [makeApiKey(rawKey, projB.id)]);
  const config = makeConfig();
  const makeCtx = () => ({
    req: {
      raw: new Request("http://127.0.0.1/v1/chat/completions", {
        headers: { authorization: `Bearer ${rawKey}` },
      }),
      query: () => undefined,
    },
    json: (d: unknown, s?: number) => Response.json(d, { status: s ?? 200 }),
  });
  const stateA = new ProxyRuntimeState(2);
  const stateB = new ProxyRuntimeState(2);
  try {
    const resA = authenticateRequest(makeCtx(), snapA, config, "chat", undefined, stateA);
    expect(resA.billingAccount.id).toBe(projA.id);
    const resB = authenticateRequest(makeCtx(), snapB, config, "chat", undefined, stateB);
    expect(resB.billingAccount.id).toBe(projB.id);
    const resNoState = authenticateRequest(makeCtx(), snapB, config, "chat");
    expect(resNoState.billingAccount.id).toBe(projB.id);
  } finally {
    stateA.close();
    stateB.close();
  }
});

test("F8: ProxyRuntimeState LRU evicts least-recently-used key instead of wiping entire cache", () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 10);
  const key0 = createKey();
  const key1 = createKey();
  const key2 = createKey();
  const k0 = makeApiKey(key0, proj.id);
  const k1 = makeApiKey(key1, proj.id);
  const k2 = makeApiKey(key2, proj.id);
  const snap = makeSnapshot(1, [proj], [k0, k1, k2]);
  const state = new ProxyRuntimeState(2);
  try {
    expect(state.lookupVerifiedKey(key0, snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey(key1, snap, TEST_PEPPER)?.id).toBe(k1.id);
    expect(state.lookupVerifiedKey(key0, snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey(key2, snap, TEST_PEPPER)?.id).toBe(k2.id);
    snap.keysByVerifier.clear();
    expect(state.lookupVerifiedKey(key0, snap, TEST_PEPPER)?.id).toBe(k0.id);
    expect(state.lookupVerifiedKey(key2, snap, TEST_PEPPER)?.id).toBe(k2.id);
    expect(state.lookupVerifiedKey(key1, snap, TEST_PEPPER)).toBeUndefined();
  } finally {
    state.close();
  }
});

test("symptom: native HTTP mid-body drain leaves stalled keep-alive upload open without connection close", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111111", 4);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const config = makeConfig({
    maxInFlight: 4,
    attemptTimeoutMs: 5_000,
    bodyReadTimeoutMs: 5_000,
    ingressBodyBufferBytes: 4096,
  });
  const deps = makeFakeDeps(snap, config);
  const bodyBudget = createBodyBufferBudget(4096);
  deps.bodyBufferBudget = bodyBudget;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
          json(data, status, headers) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...headers },
            });
          },
        },
        deps,
        "chat",
      );
    },
  });
  const socket = createConnection({ host: "127.0.0.1", port: server.port! });
  const chunks: Buffer[] = [];
  let clientSawClose = false;
  const closed = new Promise<string>((resolve) => {
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => {
      clientSawClose = true;
      resolve(Buffer.concat(chunks).toString("utf8"));
    });
    socket.on("close", () => {
      clientSawClose = true;
      resolve(Buffer.concat(chunks).toString("utf8"));
    });
    socket.on("error", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  socket.on("connect", () => {
    socket.write(
      `POST /v1/chat/completions HTTP/1.1\r\n` +
        `Host: 127.0.0.1:${server.port}\r\n` +
        `Authorization: Bearer ${rawKey}\r\n` +
        `Content-Type: application/json\r\n` +
        `Content-Length: 200\r\n` +
        `Connection: keep-alive\r\n\r\n` +
        '{"model":"gpt"',
    );
  });
  try {
    const deadline = Date.now() + 1000;
    while (deps.lifecycle.size < 1 && Date.now() < deadline) await Bun.sleep(5);
    expect(deps.lifecycle.size).toBe(1);
    void deps.lifecycle.drain(500);
    const raw = await Promise.race([
      closed,
      Bun.sleep(1000).then(() => "STALLED_KEEPALIVE_NOT_CLOSED"),
    ]);
    expect(raw).toContain("503");
    expect(raw).toContain("overloaded");
    expect(raw.toLowerCase()).toContain("connection: close");
    expect(clientSawClose).toBe(true);
    expect(deps.lifecycle.size).toBe(0);
    expect(bodyBudget.metrics()).toContain("gateway_ingress_body_buffer_reserved_bytes 0");
  } finally {
    socket.destroy();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});

test("unauthorized and rate-limited POST requests with partial large bodies reject immediately before body completion", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111177", 1);
  const validKey = createKey();
  const disabledKey = createKey();
  const unknownKey = createKey();
  const disabledRec = { ...makeApiKey(disabledKey, proj.id), enabled: false };
  const snap = makeSnapshot(1, [proj], [makeApiKey(validKey, proj.id), disabledRec]);
  const config = makeConfig({
    maxInFlight: 8,
    maxBodyBytes: 1024 * 1024,
    attemptTimeoutMs: 5_000,
    bodyReadTimeoutMs: 5_000,
  });
  const deps = makeFakeDeps(snap, config);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(req) {
      return handleInferenceRequest(
        {
          req: { raw: req, query: () => undefined },
          json(data, status, headers) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...headers },
            });
          },
        },
        deps,
        "chat",
      );
    },
  });
  const partialPrefix = `{"model":"gpt-4o","messages":[{"role":"user","content":"${"a".repeat(960)}`;
  const declaredLen = 512 * 1024;
  const openConns: Array<{ close: () => void }> = [];
  try {
    for (const badKey of [unknownKey, disabledKey]) {
      const conn = sendSlowJsonUpload(server.port!, badKey, declaredLen, partialPrefix);
      openConns.push(conn);
      const t0 = performance.now();
      const raw = await Promise.race([
        conn.response,
        Bun.sleep(800).then(() => "TIMED_OUT_WAITING_FOR_401"),
      ]);
      const elapsed = performance.now() - t0;
      conn.close();
      expect(raw).toContain("401");
      expect(raw).toContain("unauthorized");
      expect(elapsed).toBeLessThan(800);
    }

    const blocker = sendSlowJsonUpload(server.port!, validKey, declaredLen, partialPrefix);
    openConns.push(blocker);
    const deadline = Date.now() + 1000;
    while (deps.lifecycle.size < 1 && Date.now() < deadline) await Bun.sleep(5);
    expect(deps.lifecycle.size).toBe(1);

    const limited = sendSlowJsonUpload(server.port!, validKey, declaredLen, partialPrefix);
    openConns.push(limited);
    const t0 = performance.now();
    const limitedRaw = await Promise.race([
      limited.response,
      Bun.sleep(800).then(() => "TIMED_OUT_WAITING_FOR_429"),
    ]);
    const elapsed = performance.now() - t0;
    limited.close();
    expect(limitedRaw).toContain("429");
    expect(limitedRaw).toContain("rate_limit_concurrency");
    expect(elapsed).toBeLessThan(800);

    blocker.close();
    const drainDeadline = Date.now() + 1000;
    while (deps.lifecycle.size > 0 && Date.now() < drainDeadline) await Bun.sleep(5);
    expect(deps.lifecycle.size).toBe(0);

    const control = await fetch(`http://127.0.0.1:${server.port}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${validKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "not-configured",
        messages: [{ role: "user", content: "ok" }],
      }),
    });
    expect(control.status).toBe(404);
  } finally {
    for (const conn of openConns) conn.close();
    deps.state?.close();
    deps.transport.close();
    await server.stop(true);
  }
});
