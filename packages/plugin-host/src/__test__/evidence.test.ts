import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  createPluginAttempts,
  createPluginSupervisor,
  observeAttempt,
  type AbiResult,
  type ObservedEvidence,
} from "../index.ts";
import { anthropicMessagesPackageFiles } from "@gateway/adapters";
import {
  syntheticChatPackage,
  syntheticSignedPackage,
} from "../../../../tests/support/plugin-package.ts";

const supervisor = createPluginSupervisor({
  executable: new URL("../../../../.local/plugin-host", import.meta.url).pathname,
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

function observed(
  attemptId = crypto.randomUUID(),
  pkg = signed.pkg,
  beforeRecord?: (e: ObservedEvidence[]) => Promise<void>,
) {
  const recorded: ObservedEvidence[] = [];
  const attempt = host.create(pkg, {
    tenant: "synthetic",
    attempt: attemptId,
    incarnation: crypto.randomUUID(),
    config: {
      model_map: { logical: "upstream" },
      max_output_tokens: 100,
      max_input_tokens: 10000,
      auth: "bearer",
    },
    deadlineAt: Date.now() + 30000,
  });
  const bridge = observeAttempt(attempt, {
    attemptId,
    permitEpoch: "1",
    delivery: "progressive",
    seal: (evidence) => evidence,
    record: async (evidence) => {
      await beforeRecord?.(evidence);
      recorded.push(...evidence);
    },
  });
  return { bridge, recorded, attemptId };
}

test("a shared upstream read preserves every distinct cumulative checkpoint before success, errors or cancellation", async () => {
  const delta = (output: number) =>
    wire({
      choices: [{ delta: { content: String(output) } }],
      usage: { prompt_tokens: 7, completion_tokens: output },
    });
  const hold = (output: number) =>
    wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: output } });
  const stop = wire({ choices: [{ delta: {}, finish_reason: "stop" }] });
  const error = wire({ error: { message: "synthetic" }, usage: { completion_tokens: 4 } });
  for (const outcome of ["success", "error", "cancel"]) {
    const { bridge, recorded } = observed();
    try {
      await bridge.invoke("stream_open", encode({ model: "logical" }));
      const suffix = outcome === "error" ? error : "data: [DONE]\n\n";
      const input = new TextEncoder().encode(delta(1) + stop + hold(2) + hold(3) + suffix);
      let reply = await bridge.invoke("stream_feed", input);
      expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([["PARTIAL", "1"]]);
      if (outcome === "cancel") {
        await expect(
          bridge.invoke("stream_drain", undefined, AbortSignal.abort()),
        ).rejects.toMatchObject({
          code: "CALL_CANCELLED",
        });
        expect(recorded).toHaveLength(1);
        continue;
      }
      while (reply.kind === "output" && !reply.terminal && !reply.needs_input)
        reply = await bridge.invoke("stream_drain");
      if (outcome === "error") {
        expect(reply).toMatchObject({ kind: "failure", code: "upstream_error" });
        expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([
          ["PARTIAL", "1"],
          ["PARTIAL", "2"],
          ["PARTIAL", "3"],
          ["UNKNOWN", "4"],
        ]);
      } else {
        expect(reply).toMatchObject({ kind: "output", terminal: { kind: "complete" } });
        expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([
          ["PARTIAL", "1"],
          ["PARTIAL", "2"],
          ["FINAL", "3"],
        ]);
      }
    } finally {
      bridge.dispose();
    }
  }
});

test("a failed publication between held usage updates blocks terminal delivery with its original error", async () => {
  const unavailable = new TypeError("synthetic external settlement unavailable");
  const { bridge, recorded } = observed(crypto.randomUUID(), signed.pkg, async () => {
    throw unavailable;
  });
  try {
    await bridge.invoke("stream_open", encode({ model: "logical" }));
    const input =
      wire({ choices: [{ delta: { content: "x" } }] }) +
      wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) +
      wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 1 } }) +
      wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 3 } }) +
      "data: [DONE]\n\n";
    await expect(bridge.invoke("stream_feed", new TextEncoder().encode(input))).rejects.toBe(
      unavailable,
    );
    expect(recorded).toHaveLength(0);
  } finally {
    bridge.dispose();
  }
});

test("cancellation during a held usage publication keeps the subsequent terminal private", async () => {
  const controller = new AbortController();
  const { bridge, recorded } = observed(crypto.randomUUID(), signed.pkg, async () => {
    controller.abort();
  });
  try {
    await bridge.invoke("stream_open", encode({ model: "logical" }));
    const input =
      wire({ choices: [{ delta: { content: "x" } }] }) +
      wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) +
      wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 1 } }) +
      wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 3 } }) +
      "data: [DONE]\n\n";
    await expect(
      bridge.invoke("stream_feed", new TextEncoder().encode(input), controller.signal),
    ).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    expect(recorded.map((event) => [event.finality, event.usage.output_tokens])).toEqual([
      ["PARTIAL", "1"],
    ]);
  } finally {
    bridge.dispose();
  }
});

test("a malformed frame after valid held usage preserves the original provider error and observed quantities", async () => {
  const { bridge, recorded } = observed();
  try {
    await bridge.invoke("stream_open", encode({ model: "logical" }));
    const source =
      wire({ choices: [{ delta: { content: "x" } }] }) +
      wire({ choices: [{ delta: {}, finish_reason: "stop" }] }) +
      wire({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 1 } }) +
      "data: {broken}\n\n";
    let reply = await bridge.invoke("stream_feed", new TextEncoder().encode(source));
    while (reply.kind === "output" && !reply.terminal && !reply.needs_input)
      reply = await bridge.invoke("stream_drain");
    expect(reply).toMatchObject({
      kind: "failure",
      status: 502,
      code: "invalid_json",
      usage: { input_tokens: "7", output_tokens: "1" },
    });
    expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([
      ["PARTIAL", "1"],
      ["UNKNOWN", "1"],
    ]);
  } finally {
    bridge.dispose();
  }
});
test("an Anthropic shared read observes initial usage before exposing the terminal usage", async () => {
  const pkg = syntheticSignedPackage(await anthropicMessagesPackageFiles()).pkg;
  const { bridge, recorded } = observed(crypto.randomUUID(), pkg);
  const event = (type: string, fields: object) =>
    `event: ${type}\ndata: ${JSON.stringify({ type, ...fields })}\n\n`;
  try {
    await bridge.invoke("stream_open", encode({ model: "logical", protocol: "messages" }));
    const input = new TextEncoder().encode(
      [
        event("message_start", {
          message: {
            id: "message",
            type: "message",
            role: "assistant",
            model: "upstream",
            content: [],
            usage: { input_tokens: 7, output_tokens: 1 },
          },
        }),
        event("content_block_start", { index: 0, content_block: { type: "text", text: "" } }),
        event("content_block_delta", { index: 0, delta: { type: "text_delta", text: "ok" } }),
        event("content_block_stop", { index: 0 }),
        event("message_delta", {
          delta: { stop_reason: "end_turn", stop_sequence: null },
          usage: { output_tokens: 2 },
        }),
        event("message_stop", {}),
      ].join(""),
    );
    let reply = await bridge.invoke("stream_feed", input);
    expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([["PARTIAL", "1"]]);
    while (reply.kind === "output" && !reply.terminal && !reply.needs_input)
      reply = await bridge.invoke("stream_drain");
    expect(reply).toMatchObject({ kind: "output", terminal: { kind: "complete" } });
    expect(recorded.map((e) => [e.finality, e.usage.output_tokens])).toEqual([
      ["PARTIAL", "1"],
      ["FINAL", "2"],
    ]);
  } finally {
    bridge.dispose();
  }
});
test("a final JSON response becomes one FINAL observation over exactly the reported dimensions", async () => {
  const { bridge, recorded, attemptId } = observed();
  try {
    await bridge.transformJSON(
      "response",
      { model: "logical" },
      new TextEncoder().encode(
        '{"id":"x","model":"upstream","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":15}}',
      ),
    );
    await bridge.finish({ attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" });
  } finally {
    bridge.dispose();
  }
  expect(recorded.map((e) => ({ ...e, event_id: "id" }))).toEqual([
    {
      event_id: "id",
      attempt_id: attemptId,
      permit_epoch: "1",
      source: "execution",
      source_revision: "1",
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
    },
    {
      event_id: "id",
      attempt_id: attemptId,
      permit_epoch: "1",
      source: "execution",
      source_revision: "2",
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    },
  ]);
  expect(new Set(recorded.map((e) => e.event_id)).size).toBe(2);
});

test("streamed usage is recorded only when the cumulative snapshot changes and stays non-final until the end", async () => {
  const { bridge, recorded } = observed();
  try {
    await bridge.invoke("stream_open", encode({ model: "logical" }));
    await bridge.invoke(
      "stream_feed",
      new TextEncoder().encode(
        wire({
          model: "upstream",
          choices: [{ delta: { content: "a" } }],
          usage: { prompt_tokens: 10 },
        }),
      ),
    );
    await bridge.invoke(
      "stream_feed",
      new TextEncoder().encode(wire({ model: "upstream", choices: [{ delta: { content: "b" } }] })),
    );
    await bridge.invoke(
      "stream_feed",
      new TextEncoder().encode(
        wire({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { completion_tokens: 2 } }) +
          "data: [DONE]\n\n",
      ),
    );
  } finally {
    bridge.dispose();
  }
  expect(recorded.map((e) => [e.source_revision, e.finality, e.usage, e.final_dimensions])).toEqual(
    [
      ["1", "PARTIAL", { input_tokens: "10" }, []],
      ["2", "FINAL", { input_tokens: "10", output_tokens: "2" }, ["input_tokens", "output_tokens"]],
    ],
  );
});

test("an outcome without any usage is recorded as UNKNOWN and the bridge refuses a second finish", async () => {
  const { bridge, recorded } = observed();
  await bridge.finish({
    attempt_state: "UNKNOWN",
    request_state: "FAILED",
    error_code: "upstream_reset",
  });
  expect(recorded.map((e) => ({ ...e, event_id: "id", attempt_id: "attempt" }))).toEqual([
    {
      event_id: "id",
      attempt_id: "attempt",
      permit_epoch: "1",
      source: "execution",
      source_revision: "1",
      finality: "UNKNOWN",
      usage: {},
      final_dimensions: [],
      attempt_state: "UNKNOWN",
      request_state: "FAILED",
      error_code: "upstream_reset",
    },
  ]);
  await expect(bridge.finish({ request_state: "FAILED" })).rejects.toThrow("already finished");
  bridge.dispose();
});

test("a failed record write is surfaced before the plugin output is returned", async () => {
  const attemptId = crypto.randomUUID();
  const attempt = host.create(signed.pkg, {
    tenant: "synthetic",
    attempt: attemptId,
    incarnation: crypto.randomUUID(),
    config: {
      model_map: { logical: "upstream" },
      max_output_tokens: 100,
      max_input_tokens: 10000,
      auth: "bearer",
    },
    deadlineAt: Date.now() + 30000,
  });
  const bridge = observeAttempt(attempt, {
    attemptId,
    permitEpoch: "1",
    delivery: "progressive",
    seal: (evidence) => evidence,
    record: async () => {
      throw new Error("queue unavailable");
    },
  });
  try {
    await expect(
      bridge.transformJSON(
        "response",
        { model: "logical" },
        new TextEncoder().encode(
          '{"id":"x","model":"upstream","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}',
        ),
      ),
    ).rejects.toThrow("queue unavailable");
  } finally {
    bridge.dispose();
  }
});

function scripted(results: AbiResult[]) {
  return {
    invoke: async () => results.shift()!,
    transformJSON: async () => ({ result: results.shift()! }),
    dispose: () => undefined,
  } as unknown as Parameters<typeof observeAttempt>[0];
}
const output = (usage: Record<string, string>): AbiResult => ({
  abi_version: 1,
  kind: "output",
  consumed: 0,
  chunks: [],
  usage,
  finality: "FINAL",
  needs_input: false,
});

test("FINAL reasoning tokens are recorded but never become a final pricing dimension", async () => {
  const { bridge, recorded } = observed();
  try {
    await bridge.transformJSON(
      "response",
      { model: "logical" },
      new TextEncoder().encode(
        '{"id":"x","model":"upstream","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":15,"completion_tokens_details":{"reasoning_tokens":5}}}',
      ),
    );
  } finally {
    bridge.dispose();
  }
  expect(recorded.map((e) => [e.usage, e.final_dimensions])).toEqual([
    [
      { input_tokens: "30", output_tokens: "15", reasoning_tokens: "5" },
      ["input_tokens", "output_tokens"],
    ],
  ]);
});

test("a reported aggregate cache write is final while unreported TTL partitions stay unknown", async () => {
  const recorded: ObservedEvidence[] = [];
  const bridge = observeAttempt(
    scripted([output({ input_tokens: "4", cache_write_tokens: "9" })]),
    {
      attemptId: crypto.randomUUID(),
      permitEpoch: "1",
      delivery: "progressive",
      seal: (evidence) => evidence,
      record: async (e) => void recorded.push(...e),
    },
  );
  await bridge.invoke("stream_feed");
  expect(recorded.map((e) => e.final_dimensions)).toEqual([["cache_write_tokens", "input_tokens"]]);
});

test("an observation whose record failed is recorded again when the same usage is observed next", async () => {
  const recorded: ObservedEvidence[] = [];
  let fail = true;
  const bridge = observeAttempt(
    scripted([output({ input_tokens: "7" }), output({ input_tokens: "7" })]),
    {
      attemptId: crypto.randomUUID(),
      permitEpoch: "1",
      delivery: "progressive",
      seal: (evidence) => evidence,
      record: async (e) => {
        if (fail) {
          fail = false;
          throw new Error("queue unavailable");
        }
        recorded.push(...e);
      },
    },
  );
  await expect(bridge.invoke("stream_feed")).rejects.toThrow("queue unavailable");
  await bridge.invoke("stream_feed");
  expect(recorded.map((e) => [e.usage, e.finality])).toEqual([[{ input_tokens: "7" }, "FINAL"]]);
});

test("a send that never left the host is recorded once as NOT_EXECUTED without quantities", async () => {
  const { bridge, recorded, attemptId } = observed();
  await bridge.notExecuted({ request_state: "FAILED", error_code: "upstream_not_dispatched" });
  expect(recorded.map((e) => ({ ...e, event_id: "id" }))).toEqual([
    {
      event_id: "id",
      attempt_id: attemptId,
      permit_epoch: "1",
      source: "execution",
      source_revision: "1",
      finality: "NOT_EXECUTED",
      usage: {},
      final_dimensions: [],
      attempt_state: "NOT_EXECUTED",
      request_state: "FAILED",
      error_code: "upstream_not_dispatched",
    },
  ]);
  await expect(bridge.finish({ request_state: "FAILED" })).rejects.toThrow("already finished");
  bridge.dispose();
});

test("an attempt with observed usage can no longer claim it was never executed", async () => {
  const recorded: ObservedEvidence[] = [];
  const bridge = observeAttempt(scripted([output({ input_tokens: "3" })]), {
    attemptId: crypto.randomUUID(),
    permitEpoch: "1",
    delivery: "progressive",
    seal: (evidence) => evidence,
    record: async (e) => void recorded.push(...e),
  });
  await bridge.invoke("stream_feed");
  await expect(bridge.notExecuted({ request_state: "FAILED" })).rejects.toThrow("already observed");
  expect(recorded.map((e) => e.finality)).toEqual(["FINAL"]);
});
