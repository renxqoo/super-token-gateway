import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import type { PluginRecord } from "@gateway/ledger";
import {
  createBunWasmExecutor,
  createFfiWasmtimeExecutor,
  PluginPipelineEngine,
  resolveWasmtimeLibraryPath,
  WebhookPluginExecutor,
  type PluginHook,
  type WasmRuntimeExecutor,
} from "../index.ts";

function lebU(n: number): number[] {
  const out: number[] = [];
  let v = n >>> 0;
  do {
    let b = v & 0x7f;
    v >>>= 7;
    if (v !== 0) b |= 0x80;
    out.push(b);
  } while (v !== 0);
  return out;
}

function lebS64(v: bigint): number[] {
  const out: number[] = [];
  let cur = v;
  let more = true;
  while (more) {
    let b = Number(cur & 0x7fn);
    cur >>= 7n;
    if ((cur === 0n && (b & 0x40) === 0) || (cur === -1n && (b & 0x40) !== 0)) {
      more = false;
    } else {
      b |= 0x80;
    }
    out.push(b);
  }
  return out;
}

function lebS32(v: number): number[] {
  return lebS64(BigInt(v));
}

function wasmSection(id: number, body: number[]): number[] {
  return [id, ...lebU(body.length), ...body];
}

function wasmStr(s: string): number[] {
  const bytes = [...new TextEncoder().encode(s)];
  return [...lebU(bytes.length), ...bytes];
}

function buildSubstringWasm(options: {
  hooks: readonly PluginHook[];
  needle: string;
  matchReply: Record<string, unknown>;
  passReply?: Record<string, unknown>;
}): Uint8Array {
  const enc = new TextEncoder();
  const needleBytes = [...enc.encode(options.needle)];
  const matchBytes = [...enc.encode(JSON.stringify(options.matchReply))];
  const passBytes = [...enc.encode(JSON.stringify(options.passReply ?? { action: "PASS" }))];
  const needleOff = 64;
  const matchOff = 512;
  const passOff = 1536;
  const matchPacked = (BigInt(matchOff) << 32n) | BigInt(matchBytes.length);
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);

  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exportEntries: number[] = [...wasmStr("memory"), 2, 0, ...wasmStr("stg_alloc"), 0, 0];
  for (const h of options.hooks) {
    exportEntries.push(...wasmStr(h), 0, 1);
  }
  const exps = wasmSection(7, [2 + options.hooks.length, ...exportEntries]);

  const allocBody = [0, 0x41, ...lebS32(4096), 0x0b];
  const nLen = needleBytes.length;
  const hookBody = [
    1,
    2,
    0x7f,
    0x41,
    0x00,
    0x21,
    0x02,
    0x03,
    0x40,
    0x20,
    0x02,
    0x41,
    ...lebS32(nLen),
    0x6a,
    0x20,
    0x01,
    0x4b,
    0x04,
    0x40,
    0x42,
    ...lebS64(passPacked),
    0x0f,
    0x0b,
    0x41,
    0x00,
    0x21,
    0x03,
    0x02,
    0x40,
    0x03,
    0x40,
    0x20,
    0x03,
    0x41,
    ...lebS32(nLen),
    0x4f,
    0x04,
    0x40,
    0x42,
    ...lebS64(matchPacked),
    0x0f,
    0x0b,
    0x20,
    0x00,
    0x20,
    0x02,
    0x6a,
    0x20,
    0x03,
    0x6a,
    0x2d,
    0x00,
    0x00,
    0x41,
    ...lebS32(needleOff),
    0x20,
    0x03,
    0x6a,
    0x2d,
    0x00,
    0x00,
    0x47,
    0x0d,
    0x01,
    0x20,
    0x03,
    0x41,
    0x01,
    0x6a,
    0x21,
    0x03,
    0x0c,
    0x00,
    0x0b,
    0x0b,
    0x20,
    0x02,
    0x41,
    0x01,
    0x6a,
    0x21,
    0x02,
    0x0c,
    0x00,
    0x0b,
    0x42,
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [
    2,
    ...lebU(allocBody.length),
    ...allocBody,
    ...lebU(hookBody.length),
    ...hookBody,
  ]);
  const data = wasmSection(11, [
    3,
    0,
    0x41,
    ...lebS32(needleOff),
    0x0b,
    ...lebU(needleBytes.length),
    ...needleBytes,
    0,
    0x41,
    ...lebS32(matchOff),
    0x0b,
    ...lebU(matchBytes.length),
    ...matchBytes,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
    ...passBytes,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
    ...data,
  ]);
}

function buildInfiniteLoopWasm(hook: PluginHook): Uint8Array {
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr(hook),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [0, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x42, 0x00, 0x0b];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
  ]);
}

function buildMemoryBombWasm(initialPages: number, growPages: number): Uint8Array {
  const passBytes = [...new TextEncoder().encode(JSON.stringify({ action: "PASS" }))];
  const passOff = 512;
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, ...lebU(initialPages)]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr("on_response"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [
    0,
    ...(growPages > 0 ? [0x41, ...lebS32(growPages), 0x40, 0x00, 0x1a] : []),
    0x42,
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  const data = wasmSection(11, [
    1,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
    ...passBytes,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
    ...data,
  ]);
}

function buildWasiImportWasm(): Uint8Array {
  const types = wasmSection(
    1,
    [3, 0x60, 0, 0, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e],
  );
  const imports = wasmSection(2, [
    1,
    ...wasmStr("wasi_snapshot_preview1"),
    ...wasmStr("proc_exit"),
    0x00,
    0,
  ]);
  const funcs = wasmSection(3, [2, 1, 2]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    1,
    ...wasmStr("on_response"),
    0,
    2,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [0, 0x42, 0x00, 0x0b];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...imports,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
  ]);
}

function buildCrossTenantProbeWasm(): Uint8Array {
  const enc = new TextEncoder();
  const passBytes = [...enc.encode(JSON.stringify({ action: "PASS" }))];
  const leakBytes = [
    ...enc.encode(JSON.stringify({ action: "BLOCK", rule_code: "MEMORY_LEAKED" })),
  ];
  const passOff = 512;
  const leakOff = 1024;
  const sentinelOff = 2048;
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);
  const leakPacked = (BigInt(leakOff) << 32n) | BigInt(leakBytes.length);
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr("on_response"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(4096), 0x0b];
  const b1 = [
    0,
    0x41,
    ...lebS32(sentinelOff),
    0x2d,
    0x00,
    0x00,
    0x41,
    0x00,
    0x47,
    0x04,
    0x40,
    0x42,
    ...lebS64(leakPacked),
    0x0f,
    0x0b,
    0x41,
    ...lebS32(sentinelOff),
    0x41,
    ...lebS32(0xab),
    0x3a,
    0x00,
    0x00,
    0x42,
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  const data = wasmSection(11, [
    2,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
    ...passBytes,
    0,
    0x41,
    ...lebS32(leakOff),
    0x0b,
    ...lebU(leakBytes.length),
    ...leakBytes,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
    ...data,
  ]);
}

function makeMockWasmExecutor(
  handler: (hook: string, text: string) => Record<string, unknown>,
): WasmRuntimeExecutor {
  const dec = new TextDecoder();
  const enc = new TextEncoder();
  return {
    compile() {},
    async invoke(_id, hook, input) {
      const payload = JSON.parse(dec.decode(input)) as { text: string };
      return enc.encode(JSON.stringify(handler(hook, payload.text)));
    },
    close() {},
  };
}

test("G3.1 WASM sliding window catches cross-chunk PII and redacts cleanly while rejecting invalid signatures", async () => {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const wasmBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const digest = new Bun.CryptoHasher("sha256").update(wasmBytes).digest("hex");
  const sig = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");

  const wasm = makeMockWasmExecutor((_hook, text) => {
    if (text.includes("SECRET123")) {
      return { action: "TRANSFORM", text: text.replaceAll("SECRET123", "[REDACTED]") };
    }
    return { action: "PASS" };
  });

  const engine = new PluginPipelineEngine({
    wasmExecutor: wasm,
    signingPublicKeyPem: pubPem,
  });

  const plugin: PluginRecord = {
    id: "p-redact",
    name: "pii-redact",
    version: "1.0.0",
    kind: "WASM",
    capability: "CONTENT",
    provider_config: null,
    digest,
    wasm_bytes: wasmBytes,
    webhook_config: null,
    signature: sig,
    signing_key_id: "k1",
    manifest: {
      hooks: ["on_stream_chunk", "on_stream_end"],
      window_bytes: 12,
    },
    enabled: true,
  };

  const plugins = new Map([[plugin.id, plugin]]);
  let aborted = false;
  const session = engine.createStreamSession(
    [{ plugin_id: plugin.id, priority: 1 }],
    plugins,
    { request_id: "r1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => {
      aborted = true;
    },
  );

  let emitted = "";
  for (const chunk of ["Hello SEC", "RET123 world!"]) {
    const step = await session.push(chunk);
    expect(step.blocked).toBe(false);
    emitted += step.emit;
  }
  const end = await session.finalize();
  expect(end.blocked).toBe(false);
  expect(aborted).toBe(false);
  emitted += end.emit;
  expect(emitted).toBe("Hello [REDACTED] world!");

  const forged: PluginRecord = { ...plugin, id: "p-forged", signature: "Zm9yZ2Vk" };
  const forgedResult = await engine.runUnary(
    "on_response",
    [{ plugin_id: forged.id, priority: 1 }],
    new Map([[forged.id, forged]]),
    { request_id: "r2", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hello",
  );
  expect(forgedResult).toMatchObject({ blocked: true, rule_code: "plugin_signature_invalid" });
});

test("G3.2 external HTTP Webhook HOLD_UNTIL_APPROVED leaks zero banned bytes and STREAM_AND_CUTOFF aborts upstream", async () => {
  const seen: string[] = [];
  const mockFetch = (async (_url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}")) as { text: string };
    seen.push(body.text);
    if (body.text.includes("BANNED_WORD")) {
      return new Response(
        JSON.stringify({
          action: "BLOCK",
          rule_code: "SAFETY_VIOLATION",
          http_status: 400,
          safe_message: "Blocked by safety webhook",
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    return new Response(JSON.stringify({ action: "PASS" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({ fetchImpl: mockFetch }),
  });

  const holdPlugin: PluginRecord = {
    id: "wh-hold",
    name: "safety-hold",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "a".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://safety.local/check",
      stream_mode: "HOLD_UNTIL_APPROVED",
      window_chars: 16,
      charge_policy: "PLATFORM_ABSORB",
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };

  let holdAborted = false;
  const holdSession = engine.createStreamSession(
    [{ plugin_id: holdPlugin.id, priority: 1 }],
    new Map([[holdPlugin.id, holdPlugin]]),
    { request_id: "r3", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => {
      holdAborted = true;
    },
  );
  let holdEmitted = "";
  const p1 = await holdSession.push("Safe intro ");
  holdEmitted += p1.emit;
  const p2 = await holdSession.push("contains BANNED_WORD!");
  holdEmitted += p2.emit;
  const holdEnd = await holdSession.finalize();
  expect(holdEnd.blocked).toBe(true);
  expect(holdEnd.rule_code).toBe("SAFETY_VIOLATION");
  expect(holdEnd.charge_policy).toBe("PLATFORM_ABSORB");
  expect(holdAborted).toBe(true);
  expect(holdEmitted).not.toContain("BANNED_WORD");

  const cutoffPlugin: PluginRecord = {
    ...holdPlugin,
    id: "wh-cutoff",
    webhook_config: {
      url: "http://safety.local/check",
      stream_mode: "STREAM_AND_CUTOFF",
      window_chars: 10,
      charge_policy: "CHARGE_CUSTOMER",
    },
  };
  let cutoffAborted = false;
  const cutoffSession = engine.createStreamSession(
    [{ plugin_id: cutoffPlugin.id, priority: 1 }],
    new Map([[cutoffPlugin.id, cutoffPlugin]]),
    { request_id: "r4", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => {
      cutoffAborted = true;
    },
  );
  const c1 = await cutoffSession.push("BANNED_WORD here.");
  expect(c1.emit).toBe("BANNED_WORD here.");
  const cutoffEnd = await cutoffSession.finalize();
  expect(cutoffEnd.blocked).toBe(true);
  expect(cutoffEnd.charge_policy).toBe("CHARGE_CUSTOMER");
  expect(cutoffAborted).toBe(true);
  expect(seen.length).toBeGreaterThanOrEqual(2);

  const firstVerdict = Promise.withResolvers<Response>();
  let checks = 0;
  const slowEngine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({
      fetchImpl: (async () => {
        checks++;
        return checks === 1 ? firstVerdict.promise : Response.json({ action: "PASS" });
      }) as unknown as typeof fetch,
    }),
  });
  const slowSession = slowEngine.createStreamSession(
    [{ plugin_id: cutoffPlugin.id, priority: 1 }],
    new Map([[cutoffPlugin.id, cutoffPlugin]]),
    { request_id: "slow-webhook", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => undefined,
  );
  expect((await slowSession.push("Safe first sentence.")).emit).toBe("Safe first sentence.");
  const second = slowSession.push("Safe second sentence.");
  const overlappingChecks = checks;
  firstVerdict.resolve(Response.json({ action: "PASS" }));
  expect((await second).emit).toBe("Safe second sentence.");
  const audit = (await slowSession.finalize()).audit;
  expect(overlappingChecks).toBe(1);
  expect(checks).toBe(2);
  expect(audit).toHaveLength(1);
  expect(audit[0]).toMatchObject({ hook: "on_stream_chunk", action: "PASS", executions: 2 });
});

test("G3.2 webhook FAIL_CLOSED blocks and trips circuit while FAIL_OPEN passes through on outage", async () => {
  let calls = 0;
  const failingFetch = (async () => {
    calls++;
    return new Response("error", { status: 503 });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({ fetchImpl: failingFetch }),
  });

  const closedPlugin: PluginRecord = {
    id: "wh-closed",
    name: "closed",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "b".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://down.local/check",
      failure_policy: "FAIL_CLOSED",
      circuit_failure_threshold: 2,
      circuit_open_ms: 60_000,
    },
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };

  const r1 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r5", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  const r2 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r6", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  const r3 = await engine.runUnary(
    "on_response",
    [{ plugin_id: closedPlugin.id, priority: 1 }],
    new Map([[closedPlugin.id, closedPlugin]]),
    { request_id: "r7", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hi",
  );
  expect(r1.blocked).toBe(true);
  expect(r2.blocked).toBe(true);
  expect(r3).toMatchObject({ blocked: true, rule_code: "webhook_circuit_open" });
  expect(calls).toBe(2);

  const openPlugin: PluginRecord = {
    ...closedPlugin,
    id: "wh-open",
    webhook_config: {
      url: "http://down.local/check",
      failure_policy: "FAIL_OPEN",
    },
  };
  const r4 = await engine.runUnary(
    "on_response",
    [{ plugin_id: openPlugin.id, priority: 1 }],
    new Map([[openPlugin.id, openPlugin]]),
    { request_id: "r8", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hello",
  );
  expect(r4).toMatchObject({ blocked: false, text: "hello" });
});

test("WASM invocations expire while waiting behind a busy worker", async () => {
  const maxMemoryBytes = 4 * 1024 * 1024;
  const executor = createBunWasmExecutor({
    maxMemoryBytes,
    poolSize: 1,
    minTimeoutMs: 25,
    maxTimeoutMs: 150,
  });
  executor.compile("loop", "loop", buildInfiniteLoopWasm("on_response"));
  executor.compile("fast", "fast", buildCrossTenantProbeWasm());
  const input = new Uint8Array();
  try {
    const busy = executor
      .invoke("loop", "on_response", input, { maxMemoryBytes, fuelBudget: 1_500_000 })
      .catch((error: unknown) => error);
    await expect(
      executor.invoke("fast", "on_response", input, { maxMemoryBytes, fuelBudget: 250_000 }),
    ).rejects.toThrow("WASM execution timed out");
    expect(await busy).toBeInstanceOf(Error);
    await expect(
      executor.invoke("fast", "on_response", input, { maxMemoryBytes, fuelBudget: 1_500_000 }),
    ).resolves.toBeInstanceOf(Uint8Array);
  } finally {
    executor.close?.();
  }
});

test("G3.1 Real Bun WebAssembly Worker sandbox executes signed WASM, isolates cross-tenant memory, and kills infinite loops / memory bombs / WASI escapes", async () => {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const signBytes = (bytes: Uint8Array) => {
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");
    return { digest, signature };
  };

  const executor = createBunWasmExecutor({
    maxMemoryBytes: 4 * 1024 * 1024,
    poolSize: 2,
    minTimeoutMs: 40,
    maxTimeoutMs: 100,
  });
  const engine = new PluginPipelineEngine({
    wasmExecutor: executor,
    signingPublicKeyPem: pubPem,
  });

  try {
    const filterBytes = buildSubstringWasm({
      hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
      needle: "SECRET_42",
      matchReply: { action: "TRANSFORM", text: "[CLEANSED_BY_WASM]" },
    });
    const filterSig = signBytes(filterBytes);
    const filterPlugin: PluginRecord = {
      id: "wasm-real-filter",
      name: "real-filter",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: filterSig.digest,
      wasm_bytes: filterBytes,
      webhook_config: null,
      signature: filterSig.signature,
      signing_key_id: "k1",
      manifest: {
        hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
        window_bytes: 12,
      },
      enabled: true,
    };

    const okRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "clean text",
    );
    expect(okRes).toMatchObject({ blocked: false, text: "clean text" });

    const transformRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-2", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "has SECRET_42 inside",
    );
    expect(transformRes).toMatchObject({ blocked: false, text: "[CLEANSED_BY_WASM]" });

    const streamSession = engine.createStreamSession(
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-3", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      () => undefined,
    );
    let streamOut = "";
    streamOut += (await streamSession.push("part SEC")).emit;
    streamOut += (await streamSession.push("RET_42 end")).emit;
    const streamEnd = await streamSession.finalize();
    expect(streamEnd.blocked).toBe(false);
    streamOut += streamEnd.emit;
    expect(streamOut).toContain("[CLEANSED_BY_WASM]");
    expect(streamOut).not.toContain("SECRET_42");

    const probeBytes = buildCrossTenantProbeWasm();
    const probeSig = signBytes(probeBytes);
    const probePlugin: PluginRecord = {
      id: "wasm-tenant-probe",
      name: "tenant-probe",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: probeSig.digest,
      wasm_bytes: probeBytes,
      webhook_config: null,
      signature: probeSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"] },
      enabled: true,
    };
    const probeMap = new Map([[probePlugin.id, probePlugin]]);
    const probe1 = await engine.runUnary(
      "on_response",
      [{ plugin_id: probePlugin.id, priority: 1 }],
      probeMap,
      { request_id: "p1", project_id: "tenant-a", model: "gpt-4o", operation: "chat" },
      "tenant a",
    );
    const probe2 = await engine.runUnary(
      "on_response",
      [{ plugin_id: probePlugin.id, priority: 1 }],
      probeMap,
      { request_id: "p2", project_id: "tenant-b", model: "gpt-4o", operation: "chat" },
      "tenant b",
    );
    expect(probe1.blocked).toBe(false);
    expect(probe2.blocked).toBe(false);

    const loopBytes = buildInfiniteLoopWasm("on_response");
    const loopSig = signBytes(loopBytes);
    const loopPlugin: PluginRecord = {
      id: "wasm-inf-loop",
      name: "inf-loop",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: loopSig.digest,
      wasm_bytes: loopBytes,
      webhook_config: null,
      signature: loopSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"], fuel_budget: 500_000, failure_policy: "FAIL_CLOSED" },
      enabled: true,
    };
    const loopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: loopPlugin.id, priority: 1 }],
      new Map([[loopPlugin.id, loopPlugin]]),
      { request_id: "loop-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hang",
    );
    expect(loopRes).toMatchObject({
      blocked: true,
      rule_code: "wasm_execution_failed",
      http_status: 500,
    });

    const afterLoopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-recover", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "still healthy",
    );
    expect(afterLoopRes).toMatchObject({ blocked: false, text: "still healthy" });

    const bigInitBytes = buildMemoryBombWasm(128, 0);
    const bigInitSig = signBytes(bigInitBytes);
    const bigInitPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-mem-init-bomb",
      digest: bigInitSig.digest,
      wasm_bytes: bigInitBytes,
      signature: bigInitSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 2 * 1024 * 1024 },
    };
    const bigInitRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: bigInitPlugin.id, priority: 1 }],
      new Map([[bigInitPlugin.id, bigInitPlugin]]),
      { request_id: "mem-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "mem",
    );
    expect(bigInitRes).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });

    const growBombBytes = buildMemoryBombWasm(1, 40);
    const growBombSig = signBytes(growBombBytes);
    const growBombPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-mem-grow-bomb",
      digest: growBombSig.digest,
      wasm_bytes: growBombBytes,
      signature: growBombSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 1 * 1024 * 1024 },
    };
    const growBombRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: growBombPlugin.id, priority: 1 }],
      new Map([[growBombPlugin.id, growBombPlugin]]),
      { request_id: "mem-2", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "mem-grow",
    );
    expect(growBombRes).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });

    const wasiBytes = buildWasiImportWasm();
    const wasiSig = signBytes(wasiBytes);
    const wasiPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-wasi-escape",
      digest: wasiSig.digest,
      wasm_bytes: wasiBytes,
      signature: wasiSig.signature,
      manifest: { hooks: ["on_response"] },
    };
    const wasiRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: wasiPlugin.id, priority: 1 }],
      new Map([[wasiPlugin.id, wasiPlugin]]),
      { request_id: "wasi-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "escape",
    );
    expect(wasiRes).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });
  } finally {
    executor.close();
  }
});

test("G3.2 combined HOLD + CUTOFF webhooks keep separate buffers, respect manifest.hooks, and recover after circuit open window", async () => {
  let nowMs = 1000;
  let webhookUp = false;
  const calledHooks: { url: string; hook: string; text: string }[] = [];

  const mockFetch = (async (url: string, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? "{}")) as { hook: string; text: string };
    calledHooks.push({ url, hook: body.hook, text: body.text });
    if (url.includes("/flaky")) {
      if (!webhookUp) return new Response("down", { status: 503 });
      return Response.json({ action: "PASS" });
    }
    return Response.json({ action: "PASS" });
  }) as unknown as typeof fetch;

  const engine = new PluginPipelineEngine({
    webhookExecutor: new WebhookPluginExecutor({
      fetchImpl: mockFetch,
      now: () => nowMs,
    }),
  });

  const holdWh: PluginRecord = {
    id: "wh-combo-hold",
    name: "combo-hold",
    version: "1.0.0",
    kind: "WEBHOOK",
    capability: "CONTENT",
    provider_config: null,
    digest: "c".repeat(64),
    wasm_bytes: null,
    webhook_config: {
      url: "http://mod.local/hold",
      stream_mode: "HOLD_UNTIL_APPROVED",
      window_chars: 100,
    },
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_stream_chunk", "on_stream_end"] },
    enabled: true,
  };
  const cutoffWh: PluginRecord = {
    ...holdWh,
    id: "wh-combo-cutoff",
    name: "combo-cutoff",
    webhook_config: {
      url: "http://mod.local/cutoff",
      stream_mode: "STREAM_AND_CUTOFF",
      window_chars: 100,
    },
  };
  const plugins = new Map([
    [holdWh.id, holdWh],
    [cutoffWh.id, cutoffWh],
  ]);

  const preReq = await engine.runUnary(
    "on_request",
    [
      { plugin_id: holdWh.id, priority: 1 },
      { plugin_id: cutoffWh.id, priority: 2 },
    ],
    plugins,
    { request_id: "hook-filter", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "prompt text",
  );
  expect(preReq.blocked).toBe(false);
  expect(calledHooks.length).toBe(0);

  const session = engine.createStreamSession(
    [
      { plugin_id: cutoffWh.id, priority: 1 },
      { plugin_id: holdWh.id, priority: 2 },
    ],
    plugins,
    { request_id: "combo-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    () => undefined,
  );
  let emitted = "";
  emitted += (await session.push("Hello ")).emit;
  emitted += (await session.push("world")).emit;
  const end = await session.finalize();
  expect(end.blocked).toBe(false);
  emitted += end.emit;
  expect(emitted).toBe("Hello world");
  const holdCalls = calledHooks.filter((c) => c.url.includes("/hold"));
  const cutoffCalls = calledHooks.filter((c) => c.url.includes("/cutoff"));
  expect(holdCalls.map((c) => c.text).join("")).toBe("Hello world");
  expect(cutoffCalls.map((c) => c.text).join("")).toBe("Hello world");

  const flakyWh: PluginRecord = {
    ...holdWh,
    id: "wh-flaky",
    name: "flaky",
    webhook_config: {
      url: "http://mod.local/flaky",
      failure_policy: "FAIL_CLOSED",
      circuit_failure_threshold: 2,
      circuit_open_ms: 5000,
    },
    manifest: {},
  };
  const flakyMap = new Map([[flakyWh.id, flakyWh]]);
  const f1 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  const f2 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-2", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  const f3 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-3", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "x",
  );
  expect(f1.blocked).toBe(true);
  expect(f2.blocked).toBe(true);
  expect(f3).toMatchObject({ blocked: true, rule_code: "webhook_circuit_open" });

  webhookUp = true;
  nowMs += 6000;
  const f4 = await engine.runUnary(
    "on_response",
    [{ plugin_id: flakyWh.id, priority: 1 }],
    flakyMap,
    { request_id: "fl-4", project_id: "pr1", model: "gpt-4o", operation: "chat" },
    "recovered",
  );
  expect(f4).toMatchObject({ blocked: false, text: "recovered" });
});

test("G3.1 Native Rust wasmtime FFI sandbox executes signed WASM, isolates cross-tenant memory, enforces exact fuel & memory limits, and blocks WASI imports", async () => {
  const libPath = resolveWasmtimeLibraryPath();
  if (!libPath) return;

  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const signBytes = (bytes: Uint8Array) => {
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");
    return { digest, signature };
  };

  const executor = createFfiWasmtimeExecutor({
    libraryPath: libPath,
    maxMemoryBytes: 4 * 1024 * 1024,
    defaultFuel: 2_000_000,
  });
  const engine = new PluginPipelineEngine({
    wasmExecutor: executor,
    signingPublicKeyPem: pubPem,
  });

  try {
    const filterBytes = buildSubstringWasm({
      hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
      needle: "SECRET_42",
      matchReply: { action: "TRANSFORM", text: "[CLEANSED_BY_WASMTIME]" },
    });
    const filterSig = signBytes(filterBytes);
    const filterPlugin: PluginRecord = {
      id: "ffi-real-filter",
      name: "ffi-filter",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: filterSig.digest,
      wasm_bytes: filterBytes,
      webhook_config: null,
      signature: filterSig.signature,
      signing_key_id: "k1",
      manifest: {
        hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
        window_bytes: 12,
      },
      enabled: true,
    };

    const passRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "clean text",
    );
    expect(passRes).toMatchObject({ blocked: false, text: "clean text" });

    const transformRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-2", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "payload SECRET_42 here",
    );
    expect(transformRes).toMatchObject({ blocked: false, text: "[CLEANSED_BY_WASMTIME]" });

    const streamSession = engine.createStreamSession(
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-3", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      () => undefined,
    );
    let streamOut = "";
    streamOut += (await streamSession.push("split SEC")).emit;
    streamOut += (await streamSession.push("RET_42 tail")).emit;
    const streamEnd = await streamSession.finalize();
    expect(streamEnd.blocked).toBe(false);
    streamOut += streamEnd.emit;
    expect(streamOut).toContain("[CLEANSED_BY_WASMTIME]");
    expect(streamOut).not.toContain("SECRET_42");

    const probeBytes = buildCrossTenantProbeWasm();
    const probeSig = signBytes(probeBytes);
    const probePlugin: PluginRecord = {
      id: "ffi-tenant-probe",
      name: "ffi-tenant-probe",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: probeSig.digest,
      wasm_bytes: probeBytes,
      webhook_config: null,
      signature: probeSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"] },
      enabled: true,
    };
    const probeMap = new Map([[probePlugin.id, probePlugin]]);
    expect(
      (
        await engine.runUnary(
          "on_response",
          [{ plugin_id: probePlugin.id, priority: 1 }],
          probeMap,
          { request_id: "fp-1", project_id: "t1", model: "gpt-4o", operation: "chat" },
          "t1",
        )
      ).blocked,
    ).toBe(false);
    expect(
      (
        await engine.runUnary(
          "on_response",
          [{ plugin_id: probePlugin.id, priority: 1 }],
          probeMap,
          { request_id: "fp-2", project_id: "t2", model: "gpt-4o", operation: "chat" },
          "t2",
        )
      ).blocked,
    ).toBe(false);

    const loopBytes = buildInfiniteLoopWasm("on_response");
    const loopSig = signBytes(loopBytes);
    const loopPlugin: PluginRecord = {
      id: "ffi-inf-loop",
      name: "ffi-inf-loop",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: loopSig.digest,
      wasm_bytes: loopBytes,
      webhook_config: null,
      signature: loopSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"], fuel_budget: 50_000, failure_policy: "FAIL_CLOSED" },
      enabled: true,
    };
    const t0 = performance.now();
    const loopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: loopPlugin.id, priority: 1 }],
      new Map([[loopPlugin.id, loopPlugin]]),
      { request_id: "floop-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hang",
    );
    expect(loopRes).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });
    expect(performance.now() - t0).toBeLessThan(100);

    const bigInitBytes = buildMemoryBombWasm(128, 0);
    const bigInitSig = signBytes(bigInitBytes);
    const bigInitPlugin: PluginRecord = {
      ...loopPlugin,
      id: "ffi-mem-bomb",
      digest: bigInitSig.digest,
      wasm_bytes: bigInitBytes,
      signature: bigInitSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 2 * 1024 * 1024 },
    };
    expect(
      await engine.runUnary(
        "on_response",
        [{ plugin_id: bigInitPlugin.id, priority: 1 }],
        new Map([[bigInitPlugin.id, bigInitPlugin]]),
        { request_id: "fmem-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
        "mem",
      ),
    ).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });

    const wasiBytes = buildWasiImportWasm();
    const wasiSig = signBytes(wasiBytes);
    const wasiPlugin: PluginRecord = {
      ...loopPlugin,
      id: "ffi-wasi-escape",
      digest: wasiSig.digest,
      wasm_bytes: wasiBytes,
      signature: wasiSig.signature,
      manifest: { hooks: ["on_response"] },
    };
    expect(
      await engine.runUnary(
        "on_response",
        [{ plugin_id: wasiPlugin.id, priority: 1 }],
        new Map([[wasiPlugin.id, wasiPlugin]]),
        { request_id: "fwasi-1", project_id: "pr1", model: "gpt-4o", operation: "chat" },
        "escape",
      ),
    ).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });
  } finally {
    executor.close();
  }
});
