import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import type { PluginRecord } from "@gateway/ledger";
import { PluginPipelineEngine, WebhookPluginExecutor, type WasmRuntimeExecutor } from "../index.ts";

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
