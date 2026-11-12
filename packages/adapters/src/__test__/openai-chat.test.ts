import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  createPluginAttempts,
  createPluginSupervisor,
  runPluginFixtures,
} from "@gateway/plugin-host";
import {
  syntheticChatPackage,
  syntheticSignedPackage,
} from "../../../../tests/support/plugin-package.ts";
import type { AbiResult, OutputResult } from "@gateway/plugin-sdk";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
  native: { slots: 2 },
});
const host = createPluginAttempts(supervisor);
let signed: Awaited<ReturnType<typeof syntheticChatPackage>>;
beforeAll(async () => {
  signed = await syntheticChatPackage();
});
afterAll(async () => {
  await host.close();
});
const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
const wire = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
const config = {
  model_map: { logical: "upstream" },
  max_output_tokens: 100,
  max_input_tokens: 10000,
  auth: "bearer",
};
function attempt() {
  return host.create(signed.pkg, {
    tenant: "synthetic-tenant",
    attempt: crypto.randomUUID(),
    incarnation: crypto.randomUUID(),
    config,
    deadlineAt: Date.now() + 30000,
  });
}
function output(value: AbiResult & { output_id?: string }): OutputResult & { output_id?: string } {
  expect(value.kind).toBe("output");
  if (value.kind !== "output") throw new Error("unexpected result");
  return value;
}
async function responseResult(source: string) {
  const a = attempt();
  try {
    return await a.transformJSON(
      "response",
      { model: "logical" },
      new TextEncoder().encode(source),
    );
  } finally {
    a.dispose();
  }
}
const complete =
  '{"id":"synthetic","model":"upstream","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9007199254740993,"completion_tokens":2},"decimal":0.10000000000000001}';
test("the real WASM describes supported Chat and explicitly refuses resource and balance hooks", async () => {
  const a = attempt();
  expect((await a.invoke("describe")).kind).toBe("description");
  a.dispose();
  for (const hook of ["balance_prepare", "resource_prepare"] as const)
    expect(await attempt().invoke(hook)).toEqual({
      abi_version: 1,
      kind: "failure",
      status: 422,
      code: "unsupported_capability",
    });
});
test("the real WASM prepare maps models, preserves exact numbers and supplies a final ceiling without credentials", async () => {
  const a = attempt();
  try {
    const transformed = await a.transformJSON(
      "prepare",
      { protocol: "chat", operation: "chat", model: "logical" },
      new TextEncoder().encode(
        '{"model":"logical","messages":[{"role":"user","content":"合成"}],"temperature":0.10000000000000001,"opaque":9007199254740993,"stream":true}',
      ),
    );
    const result = transformed.result;
    expect(result.kind).toBe("prepared");
    if (result.kind !== "prepared") throw new Error("unexpected result");
    const body = new TextDecoder().decode(transformed.body);
    expect(body).toContain('"temperature":0.10000000000000001');
    expect(body).toContain('"opaque":9007199254740993');
    expect(body).toContain('"model":"upstream"');
    expect(body).toContain('"max_completion_tokens":100');
    expect(body).toContain('"include_usage":true');
    expect(result.request.auth).toEqual({ type: "bearer" });
    expect(result.request.headers).not.toHaveProperty("authorization");
    expect(result.request.manifest.ceilings.output_tokens).toBe("100");
    expect(transformed.bodyDigest).toBe(
      new Bun.CryptoHasher("sha256").update(transformed.body!).digest("hex"),
    );
  } finally {
    a.dispose();
  }
});
test("unrepresentable protocol, multiple choices and conflicting or excessive output limits are 422", async () => {
  for (const body of [{ n: 2 }, { max_tokens: 1, max_completion_tokens: 2 }, { max_tokens: 101 }]) {
    const a = attempt();
    try {
      const { result } = await a.transformJSON(
        "prepare",
        { protocol: "chat", operation: "chat", model: "logical" },
        encode({ model: "logical", messages: [{ role: "user", content: "x" }], ...body }),
      );
      expect(result.kind).toBe("failure");
      if (result.kind === "failure") expect(result.status).toBe(422);
    } finally {
      a.dispose();
    }
  }
  const a = attempt();
  expect(
    (
      await a.transformJSON(
        "prepare",
        { protocol: "messages", operation: "chat", model: "logical" },
        encode({}),
      )
    ).result,
  ).toEqual({ abi_version: 1, kind: "failure", status: 422, code: "unsupported_semantics" });
});
test("JSON preserves large usage and decimal literals, exposes numeric facts and a withheld terminal candidate", async () => {
  const transformed = await responseResult(complete),
    result = output(transformed.result);
  expect(result.usage).toEqual({ input_tokens: "9007199254740993", output_tokens: "2" });
  expect(result.finality).toBe("FINAL");
  expect(result.terminal).toEqual({ kind: "complete", bytes: "" });
  expect(result.chunks).toEqual([]);
  const body = new TextDecoder().decode(transformed.body);
  expect(body).toContain('"prompt_tokens":9007199254740993');
  expect(body).toContain('"decimal":0.10000000000000001');
  expect(body).toContain('"model":"logical"');
});
test("usage-less valid JSON remains UNKNOWN and length is an incomplete candidate", async () => {
  const result = output(
    (
      await responseResult(
        '{"choices":[{"message":{"content":"partial"},"finish_reason":"length"}]}',
      )
    ).result,
  );
  expect(result.finality).toBe("UNKNOWN");
  expect(result.usage).toBeUndefined();
  expect(result.terminal?.kind).toBe("incomplete");
});
test("empty or malformed JSON, upstream errors and invalid final usage never produce a success candidate", async () => {
  const cases = [
    "{",
    '{"error":{"message":"synthetic"}}',
    '{"choices":[{"message":{"content":""},"finish_reason":"stop"}]}',
    complete.replace("9007199254740993", "-1"),
    complete.replace("9007199254740993", "1e99999"),
    complete.replace("9007199254740993", "1e999999"),
    complete.replace("9007199254740993", "1".repeat(201)),
    complete.replace("9007199254740993", "0.1"),
  ];
  for (const source of cases) {
    const { result } = await responseResult(source);
    expect(result.kind).toBe("failure");
    if (result.kind === "failure") expect(result.status).toBe(502);
  }
});
test("cached and reasoning usage are exact cumulative dimensions and invalid partitions fail", async () => {
  const source =
    '{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":3}}}';
  expect(output((await responseResult(source)).result).usage).toEqual({
    input_tokens: "8",
    output_tokens: "5",
    cached_input_tokens: "4",
    reasoning_tokens: "3",
  });
  expect(
    (await responseResult(source.replace('"cached_tokens":4', '"cached_tokens":13'))).result.kind,
  ).toBe("failure");
  expect(
    (await responseResult(source.replace('"reasoning_tokens":3', '"reasoning_tokens":6'))).result
      .kind,
  ).toBe("failure");
});
test("SSE any single-byte UTF-8 boundary and tool argument increments preserve wire and withhold terminal", async () => {
  const a = attempt();
  output(await a.invoke("stream_open", encode({ model: "logical" })));
  const frames = [
    wire({
      model: "upstream",
      choices: [{ index: 0, delta: { content: "你好🌏" }, finish_reason: null }],
    }),
    wire({
      choices: [
        {
          index: 0,
          delta: {
            tool_calls: [
              {
                index: 0,
                id: "call_1",
                type: "function",
                function: { name: "weather", arguments: '{"city":' },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    }),
    wire({
      choices: [
        {
          index: 0,
          delta: { tool_calls: [{ index: 0, function: { arguments: '"北京"}' } }] },
          finish_reason: null,
        },
      ],
    }),
    wire({ choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] }),
    'data: {"choices":[],"usage":{"prompt_tokens":9007199254740993,"completion_tokens":3}}\n\n',
    "data: [DONE]\n\n",
  ];
  const chunks: string[] = [];
  let candidate: OutputResult["terminal"];
  const bytes = new TextEncoder().encode(frames.join(""));
  for (const byte of bytes) {
    const result = output(await a.invoke("stream_feed", new Uint8Array([byte])));
    chunks.push(...result.chunks.map((x) => Buffer.from(x, "base64").toString()));
    if (result.terminal) {
      candidate = result.terminal;
      expect(result.usage).toEqual({ input_tokens: "9007199254740993", output_tokens: "3" });
    }
  }
  expect(chunks.join("")).toContain("你好🌏");
  expect(chunks.join("")).toContain("北京");
  expect(chunks.join("")).not.toContain("[DONE]");
  expect(chunks.join("")).not.toContain('"finish_reason":"tool_calls"');
  expect(candidate?.kind).toBe("complete");
  expect(Buffer.from(candidate!.bytes, "base64").toString()).toContain("[DONE]");
  output(await a.invoke("stream_finish"));
  expect(a.snapshot().disposed).toBe(false);
  expect(a.snapshot().stream?.buffers).toBe(0);
  a.dispose();
  expect(a.snapshot().disposed).toBe(true);
}, 30000);
test("SSE comments, CRLF and unspaced data lines flush immediately without waiting to fill a batch", async () => {
  const a = attempt();
  await a.invoke("stream_open", encode({ model: "logical" }));
  const start = performance.now(),
    result = output(
      await a.invoke(
        "stream_feed",
        new TextEncoder().encode(
          ': synthetic\r\n\r\ndata:{"choices":[{"delta":{"content":"x"},"finish_reason":null}]}\r\n\r\n',
        ),
      ),
    );
  expect(result.chunks.length).toBe(2);
  expect(performance.now() - start).toBeLessThan(1000);
  expect(result.terminal).toBeUndefined();
  a.dispose();
});
test("truncated, empty, error, malformed UTF-8 and incomplete tools cannot turn EOF into success", async () => {
  for (const source of [
    "",
    wire({ choices: [{ delta: { content: "partial" } }] }),
    wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) + "data: [DONE]\n\n",
    "event: error\ndata: {}\n\n",
    "data: broken\n\n",
    wire({
      choices: [
        {
          delta: { tool_calls: [{ index: 0, id: "c", function: { name: "f", arguments: "{" } }] },
          finish_reason: "tool_calls",
        },
      ],
    }) + "data: [DONE]\n\n",
  ]) {
    const a = attempt();
    await a.invoke("stream_open", encode({ model: "logical" }));
    const result = await a.invoke("stream_feed", new TextEncoder().encode(source));
    if (result.kind === "output") {
      expect(result.terminal).toBeUndefined();
      expect((await a.invoke("stream_finish")).kind).toBe("failure");
    } else expect(result.kind).toBe("failure");
    expect(a.snapshot().disposed).toBe(true);
  }
  const a = attempt();
  await a.invoke("stream_open", encode({ model: "logical" }));
  expect((await a.invoke("stream_feed", new Uint8Array([255]))).kind).toBe("failure");
});
test("attempts interleave explicit state and final usage snapshots without sharing text or totals", async () => {
  const a = attempt(),
    b = attempt();
  await Promise.all([
    a.invoke("stream_open", encode({ model: "A" })),
    b.invoke("stream_open", encode({ model: "B" })),
  ]);
  const first = await Promise.all([
    a.invoke(
      "stream_feed",
      new TextEncoder().encode(
        wire({
          model: "up",
          choices: [{ delta: { content: "canary-A" } }],
          usage: { prompt_tokens: 10 },
        }),
      ),
    ),
    b.invoke(
      "stream_feed",
      new TextEncoder().encode(
        wire({
          model: "up",
          choices: [{ delta: { content: "canary-B" } }],
          usage: { prompt_tokens: 20 },
        }),
      ),
    ),
  ]);
  expect(Buffer.from(output(first[0]!).chunks[0]!, "base64").toString()).not.toContain("canary-B");
  expect(Buffer.from(output(first[1]!).chunks[0]!, "base64").toString()).not.toContain("canary-A");
  const last =
    wire({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { completion_tokens: 2 } }) +
    "data: [DONE]\n\n";
  expect(output(await a.invoke("stream_feed", new TextEncoder().encode(last))).usage).toEqual({
    input_tokens: "10",
    output_tokens: "2",
  });
  expect(output(await b.invoke("stream_feed", new TextEncoder().encode(last))).usage).toEqual({
    input_tokens: "20",
    output_tokens: "2",
  });
  a.dispose();
  b.dispose();
  expect(host.snapshot().attempts).toBe(0);
  expect(host.snapshot().stateBytes).toBe(0);
});
test("cumulative usage is replaced rather than summed and regressing snapshots fail", async () => {
  const a = attempt();
  await a.invoke("stream_open", encode({ model: "logical" }));
  output(
    await a.invoke(
      "stream_feed",
      new TextEncoder().encode(
        wire({
          choices: [{ delta: { content: "x" } }],
          usage: { prompt_tokens: 10, completion_tokens: 1 },
        }),
      ),
    ),
  );
  expect(
    output(
      await a.invoke(
        "stream_feed",
        new TextEncoder().encode(
          wire({ choices: [], usage: { prompt_tokens: 10, completion_tokens: 3 } }),
        ),
      ),
    ).usage,
  ).toEqual({ input_tokens: "10", output_tokens: "3" });
  expect(
    await a.invoke(
      "stream_feed",
      new TextEncoder().encode(wire({ choices: [], usage: { completion_tokens: 2 } })),
    ),
  ).toEqual({
    abi_version: 1,
    kind: "failure",
    status: 502,
    code: "usage_regressed",
    usage: { input_tokens: "10", output_tokens: "3" },
    finality: "PARTIAL",
  });
});
test("package synthetic fixtures execute through the production WASM ABI", async () => {
  const observers = supervisor.snapshot().observers;
  const result = await runPluginFixtures(supervisor, signed.pkg);
  expect(supervisor.snapshot().observers).toBe(observers);
  expect(result.cases).toBe(15);
  expect(result.steps).toBe(24);
  expect(result.state.attempts).toBe(0);
  expect(result.state.json.bytes).toBe(0);
  const rejected = syntheticSignedPackage(
    signed.pkg.files.map((file) => {
      if (file.path !== "fixtures/cases.json") return file;
      const suite = JSON.parse(new TextDecoder().decode(file.bytes));
      suite.cases = [
        {
          name: "synthetic-failing-expectation",
          steps: [
            {
              action: "invoke",
              hook: "describe",
              input: "",
              expected: { kind: "description", sdk_version: "wrong" },
            },
          ],
        },
      ];
      return { path: file.path, bytes: encode(suite) };
    }),
  );
  await expect(runPluginFixtures(supervisor, rejected.pkg)).rejects.toMatchObject({
    code: "FIXTURE_EXPECTATION",
  });
  expect(supervisor.snapshot().observers).toBe(observers);
  expect(supervisor.snapshot().pending).toBe(0);
});
test("already ready SSE frames drain in bounded batches and partial usage never claims FINAL", async () => {
  const a = attempt();
  await a.invoke("stream_open", encode({ model: "logical" }));
  const frame = wire({ choices: [{ delta: { content: "x" } }] }),
    source =
      frame.repeat(140) +
      wire({ choices: [], usage: { prompt_tokens: 10, completion_tokens: 2 } }) +
      wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) +
      "data: [DONE]\n\n";
  const first = output(await a.invoke("stream_feed", new TextEncoder().encode(source)));
  expect(first.chunks.length).toBeGreaterThan(0);
  expect(first.chunks.length).toBeLessThanOrEqual(128);
  expect(first.needs_input).toBe(false);
  expect(first.terminal).toBeUndefined();
  const chunks = [...first.chunks];
  let drained = first,
    batches = 0;
  while (!drained.terminal && batches++ < 100) {
    if (drained.output_id) a.releaseOutput(drained.output_id);
    drained = output(await a.invoke("stream_drain"));
    chunks.push(...drained.chunks);
    expect(drained.consumed).toBe(0);
  }
  expect(batches).toBeLessThan(100);
  expect(chunks.length).toBe(141);
  expect(drained.finality).toBe("FINAL");
  expect(drained.terminal?.kind).toBe("complete");
  expect(drained.needs_input).toBe(false);
  await a.invoke("stream_finish");
  a.dispose();
  const b = attempt();
  await b.invoke("stream_open", encode({ model: "logical" }));
  expect(
    output(
      await b.invoke(
        "stream_feed",
        new TextEncoder().encode(
          wire({
            choices: [{ delta: { content: "x" } }],
            usage: { prompt_tokens: 10, completion_tokens: 1 },
          }),
        ),
      ),
    ).finality,
  ).toBe("PARTIAL");
  b.dispose();
});
