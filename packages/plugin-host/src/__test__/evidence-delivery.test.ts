import { expect, test } from "bun:test";
import { observeAttempt, type ObservedEvidence } from "../evidence.ts";
import type { AbiResult } from "@gateway/plugin-sdk";

type Sealed = { body: ObservedEvidence; signer: string };

function result(input: string): AbiResult {
  return {
    abi_version: 1,
    kind: "output",
    consumed: 0,
    chunks: [],
    usage: { input_tokens: input, output_tokens: "5" },
    finality: "FINAL",
    needs_input: false,
  };
}

function fixture(results = [result("7")]) {
  const groups: Sealed[][] = [],
    sealed: Sealed[] = [];
  let signer = "original";
  const attempt = {
    invoke: async () => results.shift()!,
    transformJSON: async () => ({ result: results.shift()! }),
    dispose() {},
  } as unknown as Parameters<typeof observeAttempt>[0];
  const binding = {
    attemptId: "owned-attempt",
    permitEpoch: "1",
    delivery: "terminal-group" as const,
    seal(body: ObservedEvidence) {
      const event = { body, signer };
      sealed.push(event);
      return event;
    },
    async record(events: Sealed[]) {
      groups.push(events);
    },
  };
  return { attempt, binding, groups, sealed, rotate: () => (signer = "replacement") };
}

test("a complete JSON result and its outcome share one durable group without replacing the already sealed identity", async () => {
  const f = fixture(),
    accepted = Promise.withResolvers<void>();
  f.binding.record = async (events) => {
    f.groups.push(events);
    await accepted.promise;
  };
  const bridge = observeAttempt(f.attempt, f.binding);
  try {
    await bridge.transformJSON("response", {}, new Uint8Array());
    expect(f.groups).toHaveLength(0);
    expect(f.sealed).toHaveLength(1);
    f.rotate();
    let complete = false;
    const finishing = bridge
      .finish({ attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" })
      .then(() => {
        complete = true;
      });
    await Promise.resolve();
    expect(complete).toBe(false);
    accepted.resolve();
    await finishing;
    expect(f.groups).toHaveLength(1);
    expect(f.groups[0]).toHaveLength(2);
    expect(f.groups[0]![0]).toBe(f.sealed[0]);
    expect(
      f.groups[0]!.map((event) => [event.signer, event.body.source_revision, event.body.usage]),
    ).toEqual([
      ["original", "1", { input_tokens: "7", output_tokens: "5" }],
      ["replacement", "2", { input_tokens: "7", output_tokens: "5" }],
    ]);
    expect(f.groups[0]![1]!.body.request_state).toBe("SUCCEEDED");
    expect(new Set(f.groups[0]!.map((event) => event.body.event_id)).size).toBe(2);
  } finally {
    accepted.resolve();
    bridge.dispose();
  }
});

test("a sealed short JSON group frees its plugin slot while durable acceptance is pending", async () => {
  const f = fixture(),
    recorded = Promise.withResolvers<void>(),
    accepted = Promise.withResolvers<void>();
  let closed = false,
    finished = false;
  f.attempt.dispose = () => {
    closed = true;
  };
  f.binding.record = async (events) => {
    f.groups.push(events);
    recorded.resolve();
    await accepted.promise;
  };
  const bridge = observeAttempt(f.attempt, f.binding);
  try {
    await bridge.transformJSON("response", {}, new Uint8Array());
    const finishing = bridge
      .finish({ attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" }, true)
      .then(() => {
        finished = true;
      });
    await recorded.promise;
    try {
      expect(closed).toBe(true);
      expect(finished).toBe(false);
    } finally {
      accepted.resolve();
      await finishing;
    }
    expect(f.groups[0]?.length).toBe(2);
  } finally {
    accepted.resolve();
    bridge.dispose();
  }
});

test("stream FINAL remains immediately durable even when JSON terminal grouping is selected", async () => {
  const f = fixture(),
    bridge = observeAttempt(f.attempt, f.binding);
  try {
    await bridge.invoke("stream_feed");
    expect(f.groups.map((group) => group.map((event) => event.body.source_revision))).toEqual([
      ["1"],
    ]);
    expect(f.groups[0]![0]!.body.final_dimensions).toEqual(["input_tokens", "output_tokens"]);
  } finally {
    bridge.dispose();
  }
});

test("a later JSON observation flushes its predecessor instead of overwriting a buffered final fact", async () => {
  const f = fixture([result("7"), result("9")]),
    bridge = observeAttempt(f.attempt, f.binding);
  try {
    await bridge.transformJSON("response", {}, new Uint8Array());
    await bridge.transformJSON("response", {}, new Uint8Array());
    expect(f.groups.map((group) => group.map((event) => event.body.usage.input_tokens))).toEqual([
      ["7"],
    ]);
    await bridge.finish({ attempt_state: "FAILED", request_state: "FAILED" });
    expect(
      f.groups.map((group) =>
        group.map((event) => [event.body.source_revision, event.body.usage.input_tokens]),
      ),
    ).toEqual([
      [["1", "7"]],
      [
        ["2", "9"],
        ["3", "9"],
      ],
    ]);
    expect(f.groups[1]![1]!.body.request_state).toBe("FAILED");
  } finally {
    bridge.dispose();
  }
});

test("retrying a failed flush reuses the original fact identity and an unknown terminal write cannot create an opposite outcome", async () => {
  const f = fixture(),
    attempts: Sealed[][] = [];
  let fail = true;
  f.binding.record = async (events) => {
    attempts.push(events);
    if (fail) throw new Error("durability result unknown");
    f.groups.push(events);
  };
  const bridge = observeAttempt(f.attempt, f.binding);
  try {
    await bridge.transformJSON("response", {}, new Uint8Array());
    await expect(bridge.flush()).rejects.toThrow("durability result unknown");
    f.rotate();
    fail = false;
    await bridge.flush();
    expect(attempts[0]![0]).toBe(attempts[1]![0]);
    expect(f.sealed).toHaveLength(1);
    expect(f.groups[0]![0]!.signer).toBe("original");
    fail = true;
    await expect(bridge.finish({ request_state: "SUCCEEDED" })).rejects.toThrow(
      "durability result unknown",
    );
    await expect(bridge.finish({ request_state: "CANCELLED" })).rejects.toThrow("already finished");
    expect(f.sealed).toHaveLength(2);
    expect(attempts.at(-1)![0]!.body.request_state).toBe("SUCCEEDED");
  } finally {
    bridge.dispose();
  }
});

test("disposing while a plugin result is pending prevents late evidence from being sealed or published", async () => {
  const f = fixture(),
    returned = Promise.withResolvers<AbiResult>();
  f.attempt.invoke = () => returned.promise;
  const bridge = observeAttempt(f.attempt, f.binding),
    operation = bridge.invoke("response");
  bridge.dispose();
  returned.resolve(result("7"));
  await expect(operation).rejects.toThrow("disposed");
  await expect(bridge.flush()).rejects.toThrow("disposed");
  expect(f.sealed).toHaveLength(0);
  expect(f.groups).toHaveLength(0);
});
