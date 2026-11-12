import { expect, test } from "bun:test";
import { createPluginAttempts, createPluginSupervisor } from "@gateway/plugin-host";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

test("16 MiB Chat request and response use the same WASM ABI with exact numeric and UTF-8 slices", async () => {
  const supervisor = createPluginSupervisor({
      executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
      native: { slots: 1 },
    }),
    host = createPluginAttempts(supervisor);
  const signed = await syntheticChatPackage();
  const a = host.create(signed.pkg, {
    tenant: "synthetic-large",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + 30000,
    config: {
      model_map: { logical: "upstream" },
      max_input_tokens: 16 << 20,
      max_output_tokens: 100,
      auth: "bearer",
    },
  });
  const encode = (value: string) => new TextEncoder().encode(value),
    hash = (value: Uint8Array) => new Bun.CryptoHasher("sha256").update(value).digest("hex");
  const prefix = '{"model":"logical","messages":[{"role":"user","content":"合成🌏',
    suffix = '"}],"temperature":0.10000000000000001,"opaque":9007199254740993}';
  const filler = "s".repeat((16 << 20) - encode(prefix + suffix).length),
    request = encode(prefix + filler + suffix);
  const expectedRequest = encode(
    '{"model":"upstream","messages":[{"role":"user","content":"合成🌏' +
      filler +
      '"}],"temperature":0.10000000000000001,"opaque":9007199254740993,"max_completion_tokens":100}',
  );
  const evidence: Record<string, unknown> = {
    requestBytes: request.length,
    guestMemoryPages: 1024,
  };
  try {
    const start = performance.now(),
      prepared = await a.transformJSON(
        "prepare",
        { protocol: "chat", operation: "chat", model: "logical" },
        request,
      );
    evidence.prepareMs = performance.now() - start;
    expect(prepared.result.kind).toBe("prepared");
    expect(prepared.bodyDigest).toBe(hash(expectedRequest));
    expect(hash(prepared.body!)).toBe(hash(expectedRequest));
    expect(prepared.body!.length).toBe(expectedRequest.length);
    expect(a.snapshot().sequence).toBe(2);
    const responsePrefix = '{"model":"upstream","choices":[{"message":{"content":"合成🌏',
      responseSuffix =
        '"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9007199254740993,"completion_tokens":2},"decimal":0.10000000000000001}';
    const responseFiller = "r".repeat((16 << 20) - encode(responsePrefix + responseSuffix).length),
      response = encode(responsePrefix + responseFiller + responseSuffix);
    const expectedResponse = encode(
      '{"model":"logical","choices":[{"message":{"content":"合成🌏' +
        responseFiller +
        responseSuffix,
    );
    const responseStart = performance.now(),
      converted = await a.transformJSON("response", { model: "logical" }, response);
    evidence.responseMs = performance.now() - responseStart;
    evidence.responseBytes = response.length;
    evidence.requestOutputBytes = prepared.body!.length;
    evidence.responseOutputBytes = converted.body!.length;
    evidence.native = await supervisor.stats();
    evidence.bunRssBytes = process.memoryUsage().rss;
    expect(converted.bodyDigest).toBe(hash(expectedResponse));
    expect(hash(converted.body!)).toBe(hash(expectedResponse));
    expect(converted.result).toMatchObject({
      kind: "output",
      chunks: [],
      finality: "FINAL",
      usage: { input_tokens: "9007199254740993", output_tokens: "2" },
      terminal: { kind: "complete", bytes: "" },
    });
    expect(a.snapshot().sequence).toBe(5);
    expect(host.snapshot().json.documents).toBe(0);
    expect(host.snapshot().json.outputs).toBe(2);
    expect(supervisor.snapshot().starts).toBe(1);
  } finally {
    a.dispose();
    evidence.cleanup = host.snapshot();
    expect(host.snapshot().json.bytes).toBe(0);
    await host.close();
    await Bun.write(
      new URL("../../../../.local/json-handle-16m.json", import.meta.url),
      JSON.stringify(evidence, null, 2) + "\n",
    );
  }
}, 30000);
