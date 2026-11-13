import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  createPluginAttempts,
  createPluginSupervisor,
  observeAttempt,
  type ObservedEvidence,
} from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

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

function observed(attemptId = crypto.randomUUID()) {
  const recorded: ObservedEvidence[] = [];
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
    record: async (evidence) => {
      recorded.push(evidence);
    },
  });
  return { bridge, recorded, attemptId };
}

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
