import { afterAll, beforeAll, expect, test } from "bun:test";
import { createPluginAttempts, createPluginSupervisor } from "@gateway/plugin-host";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";
import type { AbiResult } from "@gateway/plugin-sdk";
const supervisor = createPluginSupervisor({
    executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
    native: { slots: 1 },
  }),
  host = createPluginAttempts(supervisor);
let signed: Awaited<ReturnType<typeof syntheticChatPackage>>;
beforeAll(async () => {
  signed = await syntheticChatPackage();
});
afterAll(async () => {
  await host.close();
});
const encode = (value: string) => new TextEncoder().encode(value),
  wire = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
function create() {
  return host.create(signed.pkg, {
    tenant: "synthetic-sse",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + 30000,
    config: {
      model_map: { logical: "upstream" },
      max_input_tokens: 16777216,
      max_output_tokens: 100,
      auth: "bearer",
    },
  });
}
function output(result: AbiResult & { output_id?: string }) {
  expect(result.kind).toBe("output");
  if (result.kind !== "output") throw new Error("unexpected result");
  return result;
}
async function feed(a: ReturnType<typeof create>, source: Uint8Array) {
  let consumed = 0,
    batches = 0,
    chunks: Buffer[] = [],
    terminal: { kind: string; bytes: string } | undefined;
  let last: ReturnType<typeof output> | undefined;
  while (consumed < source.length) {
    const input = source.subarray(consumed, Math.min(source.length, consumed + 65536)),
      result = output(await a.invoke("stream_feed", input));
    expect(result.consumed).toBeGreaterThan(0);
    consumed += result.consumed;
    last = result;
    chunks.push(...result.chunks.map((s) => Buffer.from(s, "base64")));
    if (result.terminal) terminal = result.terminal;
    if (result.output_id) expect(a.releaseOutput(result.output_id)).toBe(true);
    while (!last.needs_input && !last.terminal && batches++ < 10000) {
      last = output(await a.invoke("stream_drain"));
      expect(last.consumed).toBe(0);
      chunks.push(...last.chunks.map((s) => Buffer.from(s, "base64")));
      if (last.terminal) terminal = last.terminal;
      if (last.output_id) a.releaseOutput(last.output_id);
    }
  }
  return { bytes: Buffer.concat(chunks), terminal, last, consumed };
}
test("the real helper accepts the 70067-byte regression event and a true one MiB event without a native payload increase", async () => {
  const records = [];
  for (const size of [70067, 1048576]) {
    const a = create(),
      prefix = 'data: {"choices":[{"delta":{"content":"',
      suffix = '"},"finish_reason":null}]}\n\n',
      raw = encode(prefix + "s".repeat(size - prefix.length - suffix.length) + suffix);
    try {
      await a.invoke("stream_open", encode('{"model":"logical"}'));
      const start = performance.now(),
        result = await feed(a, raw),
        elapsed = performance.now() - start;
      expect(result.bytes.equals(Buffer.from(raw))).toBe(true);
      expect(result.terminal).toBeUndefined();
      expect(result.consumed).toBe(size);
      expect(a.snapshot().stateBytes).toBeLessThan(4096);
      expect(a.snapshot().stream?.partialBytes).toBe(0);
      expect(a.snapshot().stream?.ready).toBe(0);
      expect(a.snapshot().stream?.outputs).toBe(0);
      const end = await feed(
        a,
        encode(wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) + "data: [DONE]\n\n"),
      );
      expect(end.terminal?.kind).toBe("complete");
      await a.invoke("stream_finish");
      records.push({
        wireBytes: size,
        elapsedMs: elapsed,
        stateBytes: a.snapshot().stateBytes,
        helper: supervisor.snapshot(),
        jsonPeakBytes: host.snapshot().json.peakBytes,
        bunRssBytes: process.memoryUsage().rss,
      });
    } finally {
      a.dispose();
    }
    expect(host.snapshot().json.bytes).toBe(0);
    expect(host.snapshot().attempts).toBe(0);
    expect(supervisor.snapshot().pending).toBe(0);
  }
  await Bun.write(
    new URL("../../../../.local/sse-event-budget-green.json", import.meta.url),
    JSON.stringify({ records, cleanup: host.snapshot() }, null, 2) + "\n",
  );
}, 30000);
test("CRLF multiline JSON and exact numbers remain intact while usage belongs to the guest", async () => {
  const a = create();
  try {
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    const source =
      ': synthetic\r\nid: 7\r\nevent: message\r\ndata: {"choices":\r\ndata: [{"delta":{"content":"你好🙂"},"finish_reason":null}],"usage":{"prompt_tokens":9007199254740993,"completion_tokens":2},"decimal":0.10000000000000001}\r\n\r\n';
    let chunks = "";
    for (const byte of encode(source)) {
      const result = output(await a.invoke("stream_feed", new Uint8Array([byte])));
      chunks += result.chunks.map((s) => Buffer.from(s, "base64").toString()).join("");
      if (result.output_id) a.releaseOutput(result.output_id);
    }
    expect(chunks).toBe(source.replaceAll("\r\n", "\n"));
    expect(chunks).toContain("9007199254740993");
    expect(chunks).toContain("0.10000000000000001");
    const result = await feed(
      a,
      encode(wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) + "data: [DONE]\n\n"),
    );
    expect(result.last?.usage).toEqual({ input_tokens: "9007199254740993", output_tokens: "2" });
    expect(result.last?.finality).toBe("FINAL");
  } finally {
    a.dispose();
  }
  expect(host.snapshot().json.bytes).toBe(0);
});
test("tool JSON accumulation preserves split UTF16 surrogate pairs and exact numeric text", async () => {
  const a = create();
  try {
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    const first = wire({
        choices: [
          {
            delta: {
              tool_calls: [
                {
                  index: 0,
                  id: "call",
                  type: "function",
                  function: { name: "f", arguments: '{"emoji":"\ud83d' },
                },
              ],
            },
          },
        ],
      }),
      second = wire({
        choices: [
          {
            delta: {
              tool_calls: [
                {
                  index: 0,
                  function: { arguments: '\ude42","n":9007199254740993,"d":0.10000000000000001}' },
                },
              ],
            },
          },
        ],
      });
    await feed(a, encode(first));
    await feed(a, encode(second));
    expect(a.snapshot().stream?.bufferMetered).toBe(
      Buffer.byteLength('{"emoji":"\ud83d') +
        Buffer.byteLength('\ude42","n":9007199254740993,"d":0.10000000000000001}'),
    );
    expect(a.snapshot().stateBytes).toBeLessThan(4096);
    const end = await feed(
      a,
      encode(wire({ choices: [{ delta: {}, finish_reason: "tool_calls" }] }) + "data: [DONE]\n\n"),
    );
    expect(end.terminal?.kind).toBe("complete");
    await a.invoke("stream_finish");
    expect(a.snapshot().stream?.buffers).toBe(0);
  } finally {
    a.dispose();
  }
  expect(host.snapshot().json.bytes).toBe(0);
});
test("damaged tool JSON, aggregate overflow, event overflow and bytes after a terminal cannot become successful", async () => {
  const a = create();
  await a.invoke("stream_open", encode('{"model":"logical"}'));
  await feed(
    a,
    encode(
      wire({
        choices: [
          {
            delta: {
              tool_calls: [{ index: 0, id: "call", function: { name: "f", arguments: '{"x":' } }],
            },
          },
        ],
      }),
    ),
  );
  const damaged = await a.invoke(
    "stream_feed",
    encode(wire({ choices: [{ delta: {}, finish_reason: "tool_calls" }] }) + "data: [DONE]\n\n"),
  );
  expect(damaged).toMatchObject({ kind: "failure", code: "incomplete_tool_call" });
  expect(a.snapshot().disposed).toBe(true);
  const b = create();
  await b.invoke("stream_open", encode('{"model":"logical"}'));
  const fragment = "x".repeat(600000);
  await feed(
    b,
    encode(
      wire({
        choices: [
          {
            delta: {
              tool_calls: [{ index: 0, id: "call", function: { name: "f", arguments: fragment } }],
            },
          },
        ],
      }),
    ),
  );
  let overflow: AbiResult | undefined;
  const source = encode(
    wire({
      choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: fragment } }] } }],
    }),
  );
  for (let i = 0; i < source.length; i += 65536) {
    overflow = await b.invoke("stream_feed", source.subarray(i, i + 65536));
    if (overflow.kind === "failure") break;
  }
  expect(overflow).toMatchObject({ kind: "failure", code: "tool_budget" });
  expect(b.snapshot().disposed).toBe(true);
  const c = create();
  await c.invoke("stream_open", encode('{"model":"logical"}'));
  let tooLarge: AbiResult | undefined;
  const oversized = encode("data: " + "x".repeat(1048576));
  for (let i = 0; i < oversized.length; i += 65536) {
    tooLarge = await c.invoke("stream_feed", oversized.subarray(i, i + 65536));
    if (tooLarge.kind === "failure") break;
  }
  expect(tooLarge).toMatchObject({ kind: "failure", code: "stream_frame_budget" });
  for (const tail of [":tail\n\n", ""]) {
    const d = create();
    await d.invoke("stream_open", encode('{"model":"logical"}'));
    await feed(d, encode(wire({ choices: [{ delta: { content: "x" }, finish_reason: "stop" }] })));
    const result = await d.invoke("stream_feed", encode("data: [DONE]\n\n" + tail));
    if (tail) expect(result).toMatchObject({ kind: "failure", code: "data_after_terminal" });
    else {
      expect(output(result).terminal?.kind).toBe("complete");
      expect(await d.invoke("stream_feed", encode(":late\n\n"))).toMatchObject({
        kind: "failure",
        code: "data_after_terminal",
      });
    }
    expect(d.snapshot().disposed).toBe(true);
  }
  expect(host.snapshot().json.bytes).toBe(0);
  expect(host.snapshot().attempts).toBe(0);
}, 30000);
test("cancellation of a partial frame reclaims its owner and an already aborted idle call also closes", async () => {
  for (const partial of ["", "data: " + "x".repeat(70000)]) {
    const a = create();
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    if (partial) await a.invoke("stream_feed", encode(partial));
    const c = new AbortController();
    c.abort();
    await expect(a.invoke("stream_feed", encode("x"), c.signal)).rejects.toMatchObject({
      code: "CALL_CANCELLED",
    });
    expect(a.snapshot().disposed).toBe(true);
    expect(host.snapshot().json.bytes).toBe(0);
    expect(supervisor.snapshot().pending).toBe(0);
  }
});
test("one MiB of tool argument fragments and long opaque tool identifiers never enter guest state", async () => {
  const a = create();
  try {
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    const args = '{"value":"' + "v".repeat(1048576 - 12) + '"}',
      id = "i".repeat(70000),
      name = "f".repeat(70000);
    expect(Buffer.byteLength(args)).toBe(1048576);
    const first = wire({
        choices: [
          {
            delta: {
              tool_calls: [
                {
                  index: 0,
                  id,
                  type: "function",
                  function: { name, arguments: args.slice(0, 600000) },
                },
              ],
            },
          },
        ],
      }),
      second = wire({
        choices: [
          {
            delta: { tool_calls: [{ index: 0, id, function: { arguments: args.slice(600000) } }] },
          },
        ],
      });
    const firstOut = await feed(a, encode(first)),
      secondOut = await feed(a, encode(second));
    expect(firstOut.bytes.toString()).toBe(first);
    expect(secondOut.bytes.toString()).toBe(second);
    expect(a.snapshot().stream?.bufferMetered).toBe(1048576);
    expect(a.snapshot().stateBytes).toBeLessThan(4096);
    const end = await feed(
      a,
      encode(wire({ choices: [{ delta: {}, finish_reason: "tool_calls" }] }) + "data: [DONE]\n\n"),
    );
    expect(end.terminal?.kind).toBe("complete");
    await a.invoke("stream_finish");
  } finally {
    a.dispose();
  }
  expect(host.snapshot().json.bytes).toBe(0);
  expect(host.snapshot().attempts).toBe(0);
}, 30000);
test("finished output leases remain owned only until their finite attempt deadline when a caller omits release", async () => {
  const a = host.create(signed.pkg, {
    tenant: "synthetic-sse",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + 250,
    config: {
      model_map: { logical: "upstream" },
      max_input_tokens: 10000,
      max_output_tokens: 100,
      auth: "bearer",
    },
  });
  try {
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    const result = output(
      await a.invoke(
        "stream_feed",
        encode(
          wire({ choices: [{ delta: { content: "x" }, finish_reason: "stop" }] }) +
            "data: [DONE]\n\n",
        ),
      ),
    );
    expect(result.terminal?.kind).toBe("complete");
    expect(result.output_id).toBeDefined();
    await a.invoke("stream_finish");
    expect(a.snapshot().disposed).toBe(false);
    expect(host.snapshot().json.bytes).toBeGreaterThan(0);
    await Bun.sleep(300);
    expect(a.snapshot().disposed).toBe(true);
    expect(host.snapshot().json.bytes).toBe(0);
    expect(a.releaseOutput(result.output_id!)).toBe(false);
    expect(supervisor.snapshot().pending).toBe(0);
  } finally {
    a.dispose();
  }
});
test("bytes after a terminal cannot promote the last verified PARTIAL usage snapshot to FINAL", async () => {
  const a = create();
  try {
    await a.invoke("stream_open", encode('{"model":"logical"}'));
    await feed(
      a,
      encode(
        wire({
          choices: [{ delta: { content: "x" }, finish_reason: "stop" }],
          usage: { prompt_tokens: 10, completion_tokens: 2 },
        }),
      ),
    );
    expect(await a.invoke("stream_feed", encode("data: [DONE]\n\n: late\n\n"))).toEqual({
      abi_version: 1,
      kind: "failure",
      status: 502,
      code: "data_after_terminal",
      usage: { input_tokens: "10", output_tokens: "2" },
      finality: "PARTIAL",
    });
  } finally {
    a.dispose();
  }
  expect(host.snapshot().json.bytes).toBe(0);
});
