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
    runtimeKind: "mock",
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
    { request_id: "r1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
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
    { request_id: "r2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
    "hello",
  );
  expect(forgedResult).toMatchObject({ blocked: true, rule_code: "plugin_signature_invalid" });
});

test("Invalid plugin http_status is clamped to 400 and WeakMap integrity cache rejects same-length substituted buffers", async () => {
  const whExec = new WebhookPluginExecutor({
    fetchImpl: (async () =>
      Response.json({
        action: "BLOCK",
        rule_code: "BAD_STATUS",
        http_status: 200,
      })) as unknown as typeof fetch,
  });
  const whRes = await whExec.invoke(
    "wh-bad-status",
    { url: "http://mod.local/status" },
    {
      hook: "on_request",
      request_id: "r-st-1",
      billing_account_id: "p1",
      model: "m",
      operation: "chat",
      text: "hi",
    },
  );
  expect(whRes).toMatchObject({
    action: "BLOCK",
    outcome_kind: "content_decision",
    http_status: 400,
  });

  const badStatusWasm: WasmRuntimeExecutor = {
    runtimeKind: "mock",
    compile() {},
    async invoke() {
      return new TextEncoder().encode(
        JSON.stringify({ action: "BLOCK", rule_code: "WASM_BAD_STATUS", http_status: 999 }),
      );
    },
    close() {},
  };
  const goodBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const goodDigest = new Bun.CryptoHasher("sha256").update(goodBytes).digest("hex");
  const plugin: PluginRecord = {
    id: "p-status-integrity",
    name: "status-integrity",
    version: "1.0.0",
    kind: "WASM",
    capability: "CONTENT",
    provider_config: null,
    digest: goodDigest,
    wasm_bytes: goodBytes,
    webhook_config: null,
    signature: null,
    signing_key_id: null,
    manifest: { hooks: ["on_request"] },
    enabled: true,
  };
  const engine = new PluginPipelineEngine({ wasmExecutor: badStatusWasm });
  const first = await engine.runUnary(
    "on_request",
    [{ plugin_id: plugin.id, priority: 1 }],
    new Map([[plugin.id, plugin]]),
    { request_id: "r-st-2", billing_account_id: "p1", model: "m", operation: "chat" },
    "hi",
  );
  expect(first).toMatchObject({
    blocked: true,
    outcome_kind: "content_decision",
    rule_code: "WASM_BAD_STATUS",
    http_status: 400,
  });

  const tamperedSameLen = new Uint8Array(goodBytes);
  tamperedSameLen[7] = 1;
  const tamperedPlugin: PluginRecord = { ...plugin, wasm_bytes: tamperedSameLen };
  const second = await engine.runUnary(
    "on_request",
    [{ plugin_id: tamperedPlugin.id, priority: 1 }],
    new Map([[tamperedPlugin.id, tamperedPlugin]]),
    { request_id: "r-st-3", billing_account_id: "p1", model: "m", operation: "chat" },
    "hi",
  );
  expect(second).toMatchObject({
    blocked: true,
    outcome_kind: "plugin_fault",
    rule_code: "plugin_digest_mismatch",
  });
});
