import { expect, test } from "bun:test";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { createKey } from "@gateway/runtime";
import type { ProxyDependencies } from "../inference/types.ts";
import { handleInferenceRequest } from "../proxy.ts";
import { BedrockEventStreamDecoder } from "../upstream.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

function encodeBedrockEventFrame(
  event: Record<string, unknown>,
  headersBytes = new Uint8Array(0),
): Uint8Array {
  const payload = enc.encode(
    JSON.stringify({ bytes: Buffer.from(JSON.stringify(event), "utf8").toString("base64") }),
  );
  const totalLen = 12 + headersBytes.byteLength + payload.byteLength + 4;
  const out = new Uint8Array(totalLen);
  const view = new DataView(out.buffer);
  view.setUint32(0, totalLen, false);
  view.setUint32(4, headersBytes.byteLength, false);
  out.set(headersBytes, 12);
  out.set(payload, 12 + headersBytes.byteLength);
  return out;
}

test("BedrockEventStreamDecoder handles split frames, incomplete tails, and malformed frames", () => {
  const frames = Buffer.concat([
    encodeBedrockEventFrame({
      type: "message_start",
      message: { id: "m1", usage: { input_tokens: 18, output_tokens: 1 } },
    }),
    encodeBedrockEventFrame({
      type: "content_block_delta",
      index: 0,
      delta: { type: "text_delta", text: "hi" },
    }),
  ]);
  const decoder = new BedrockEventStreamDecoder();
  const decoded = [];
  for (let offset = 0; offset < frames.length; offset += 7) {
    decoded.push(...decoder.feed(frames.subarray(offset, Math.min(offset + 7, frames.length))));
  }
  expect(decoder.incomplete).toBe(false);
  expect(decoded.map((e) => e.event)).toEqual(["message_start", "content_block_delta"]);

  const partial = new BedrockEventStreamDecoder();
  expect(partial.feed(frames.subarray(0, 10))).toEqual([]);
  expect(partial.incomplete).toBe(true);

  const badLen = new Uint8Array(16);
  new DataView(badLen.buffer).setUint32(0, 8, false);
  expect(() => new BedrockEventStreamDecoder().feed(badLen)).toThrow(
    expect.objectContaining({ status: 502, code: "invalid_upstream" }),
  );
});

test("Bedrock EventStream settles final message_delta output_tokens over provisional message_start and stops reading upstream on client cancel", async () => {
  const billingAccount = makeBillingAccount(crypto.randomUUID(), 10);
  const key = createKey();
  const snap = makeSnapshot(1, [billingAccount], [makeApiKey(key, billingAccount.id)]);
  const accountId = crypto.randomUUID();
  const channelId = crypto.randomUUID();
  const providerId = crypto.randomUUID();
  const bedrockPreset = getBuiltinProvider("bedrock")!;
  snap.plugins.set(providerId, {
    id: providerId,
    name: bedrockPreset.name,
    version: bedrockPreset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: bedrockPreset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: bedrockPreset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  });
  snap.accounts.set(accountId, { id: accountId, name: "aws-acct", state: "ACTIVE", revision: "1" });
  snap.channels.set(channelId, {
    id: channelId,
    account_id: accountId,
    name: "ch-bedrock",
    kind: "bedrock",
    provider_plugin_id: providerId,
    base_url: "https://bedrock-runtime.us-east-1.amazonaws.com",
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 1000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  const [custPrice, supPrice] = (["CUSTOMER", "SUPPLIER"] as const).map((side) => {
    const id = crypto.randomUUID();
    snap.prices.set(id, {
      id,
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", output_tokens: "1000" },
      source_digest: "t",
    });
    return id;
  });
  snap.models.set("claude-bedrock", {
    name: "claude-bedrock",
    operations: ["messages"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 500,
    enabled: true,
  });
  snap.routesByModel.set("claude-bedrock", [
    {
      id: crypto.randomUUID(),
      model_name: "claude-bedrock",
      channel_id: channelId,
      upstream_model: "anthropic.claude-3-5-sonnet-20241022-v2:0",
      operations: ["messages"],
      billing_price_id: custPrice!,
      cost_version_id: supPrice!,
      priority: 1,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig({ streamEstimatedSettlement: false }));
  const awsSecret = JSON.stringify({
    accessKeyId: "AKIAEXAMPLE123",
    secretAccessKey: "secretKeyExample123",
    region: "us-east-1",
  });
  deps.snapshot.decryptChannelSecret = () => awsSecret;
  const settledUsages: Array<Record<string, string | undefined>> = [];
  const outcomes: string[] = [];
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "r-br", held_atoms: "1000" };
    },
    async commitTerminal(input: { usage: Record<string, string | undefined> }) {
      settledUsages.push(input.usage);
      outcomes.push("SETTLED");
      return { ok: true, charge_id: "chg-br", supplier_cost_atoms: "5" };
    },
    async markUnknown() {
      outcomes.push("UNKNOWN");
    },
    async releaseHold() {
      outcomes.push("RELEASED");
    },
  } as unknown as ProxyDependencies["ledger"];

  let mode: "complete" | "cancel" = "complete";
  let upstreamCancelled = false;
  let seenAuthHeader = "";
  const fetchFast: typeof deps.transport.fetchFast = async (_url, init) => {
    const hdrs =
      init.headers instanceof Headers
        ? init.headers
        : new Headers(init.headers as Record<string, string>);
    seenAuthHeader = hdrs.get("authorization") ?? "";
    if (mode === "complete") {
      const payload = Buffer.concat([
        encodeBedrockEventFrame({
          type: "message_start",
          message: {
            id: "msg_br_1",
            type: "message",
            role: "assistant",
            model: "anthropic.claude-3-5-sonnet-20241022-v2:0",
            content: [],
            usage: { input_tokens: 18, output_tokens: 1 },
          },
        }),
        encodeBedrockEventFrame({
          type: "content_block_start",
          index: 0,
          content_block: { type: "text", text: "" },
        }),
        encodeBedrockEventFrame({
          type: "content_block_delta",
          index: 0,
          delta: { type: "text_delta", text: "Hello Bedrock" },
        }),
        encodeBedrockEventFrame({ type: "content_block_stop", index: 0 }),
        encodeBedrockEventFrame({
          type: "message_delta",
          delta: { stop_reason: "end_turn" },
          usage: { output_tokens: 423 },
        }),
        encodeBedrockEventFrame({ type: "message_stop" }),
      ]);
      return {
        fast: false,
        response: new Response(payload, {
          headers: { "content-type": "application/vnd.amazon.eventstream" },
        }),
      };
    }

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encodeBedrockEventFrame({
            type: "message_start",
            message: {
              id: "msg_br_2",
              type: "message",
              role: "assistant",
              model: "anthropic.claude-3-5-sonnet-20241022-v2:0",
              content: [],
              usage: { input_tokens: 18, output_tokens: 1 },
            },
          }),
        );
        controller.enqueue(
          encodeBedrockEventFrame({
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: "Partial" },
          }),
        );
      },
      cancel() {
        upstreamCancelled = true;
      },
    });
    return {
      fast: false,
      response: new Response(stream, {
        headers: { "content-type": "application/vnd.amazon.eventstream" },
      }),
    };
  };
  deps.transport.prepareDispatch = async (url) => ({
    fetchFast: (init) => fetchFast(url, init, true),
    release() {},
  });
  deps.transport.fetchFast = fetchFast;

  const invoke = (signal?: AbortSignal) =>
    handleInferenceRequest(
      {
        req: {
          raw: new Request("http://gateway.test/v1/messages", {
            method: "POST",
            ...(signal ? { signal } : {}),
            headers: { "x-api-key": key, "content-type": "application/json" },
            body: JSON.stringify({
              model: "claude-bedrock",
              stream: true,
              max_tokens: 500,
              messages: [{ role: "user", content: "hi" }],
            }),
          }),
          query: () => undefined,
        },
        json: (v, s = 200, h = {}) => Response.json(v, { status: s, headers: h }),
      },
      deps,
      "messages",
    );

  try {
    mode = "complete";
    const okResp = await invoke();
    expect(okResp.status).toBe(200);
    const wire = await okResp.text();
    expect(seenAuthHeader).toStartWith("AWS4-HMAC-SHA256 ");
    expect(wire).toContain("Hello Bedrock");
    expect(wire).toContain('"model":"claude-bedrock"');
    expect(wire).toContain("event: message_stop");
    expect(settledUsages).toEqual([{ input_tokens: "18", output_tokens: "423" }]);
    expect(outcomes).toEqual(["SETTLED"]);

    mode = "cancel";
    const ac = new AbortController();
    const cancelResp = await invoke(ac.signal);
    expect(cancelResp.status).toBe(200);
    const reader = cancelResp.body!.getReader();
    let seen = "";
    while (!seen.includes("Partial")) {
      const next = await reader.read();
      if (next.done) break;
      seen += dec.decode(next.value);
    }
    expect(seen).toContain("Partial");
    expect(seen).not.toContain("message_stop");
    expect(seen).not.toContain("[DONE]");
    ac.abort(new Error("client closed"));
    await reader.cancel();
    reader.releaseLock();

    const deadline = Date.now() + 1000;
    while (!upstreamCancelled && Date.now() < deadline) await Bun.sleep(5);
    expect(upstreamCancelled).toBe(true);
    while (outcomes.length < 2 && Date.now() < deadline) await Bun.sleep(5);
    expect(outcomes).toEqual(["SETTLED", "UNKNOWN"]);
  } finally {
    deps.transport.close();
    deps.state?.close();
  }
});
