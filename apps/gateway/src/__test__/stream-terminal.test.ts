import { generateKeyPairSync, sign, verify } from "node:crypto";
import {
  type SignedCoreEvidenceEventV1,
  coreEvidenceEventV1Schema,
  evidenceProofFields,
  signedCoreEvidenceEventV1Schema,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime";
import { afterAll, beforeAll, expect, test } from "bun:test";
import {
  createBuiltinWorkerExecutor,
  createPluginAttempts,
  observeAttempt,
  type ObservedEvidence,
} from "@gateway/plugin-host";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";
import { createTerminalBudget } from "../terminal-budget.ts";
import { CoreRpcError } from "@gateway/core-client";
import { publishTerminalEvidence, relaySSE } from "../chat.ts";

type Relay = Parameters<typeof relaySSE>;
let host: ReturnType<typeof createPluginAttempts>;
let signed: Awaited<ReturnType<typeof syntheticChatPackage>>;
beforeAll(async () => {
  signed = await syntheticChatPackage();
  const registry = await Bun.file(".local/builtin-adapters/registry.json").json();
  host = createPluginAttempts(
    createBuiltinWorkerExecutor({
      code: await Bun.file(".local/builtin-adapters/worker.js").bytes(),
      digest: registry.worker_digest,
      modules: registry.records.map((r: { module_hash: string }) => r.module_hash),
      slots: 1,
    }),
  );
});
afterAll(async () => {
  await host?.close();
});
const line = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
const content = line({ choices: [{ delta: { content: "visible" } }] });
const done =
  line({ choices: [{ delta: {}, finish_reason: "stop" }] }) +
  line({ choices: [], usage: { prompt_tokens: 7, completion_tokens: 3 } }) +
  "data: [DONE]\n\n";
function run(
  parts: string[],
  accepted: Promise<void>,
  started: () => void,
  controller = new AbortController(),
  enabled = true,
  onSeal?: (e: ObservedEvidence) => void,
  lifetime = 10000,
  budget = createTerminalBudget(1),
  fixed?: { length: number; tail: Promise<string | undefined> },
) {
  const id = crypto.randomUUID();
  const one = host.create(signed.pkg, {
    tenant: "local",
    attempt: id,
    incarnation: crypto.randomUUID(),
    deadlineAt: Date.now() + lifetime,
    config: {
      model_map: { logical: "upstream" },
      max_output_tokens: 100,
      max_input_tokens: 4096,
      auth: "bearer",
    },
  });
  const failures: string[] = [];
  const groups: SignedCoreEvidenceEventV1[][] = [];
  const keys = generateKeyPairSync("ed25519");
  const identity = {
    origin_identity: "gateway:local",
    origin_session_id: crypto.randomUUID(),
    origin_incarnation: crypto.randomUUID(),
  };
  const proof = (body: ReturnType<typeof coreEvidenceEventV1Schema.parse>) =>
    Buffer.from(JSON.stringify(evidenceProofFields(body, sha256(canonical(body)))));
  const bridge = observeAttempt(one, {
    attemptId: id,
    permitEpoch: "1",
    delivery: "progressive",
    seal(event) {
      onSeal?.(event);
      const body = coreEvidenceEventV1Schema.parse({ ...event, ...identity });
      return signedCoreEvidenceEventV1Schema.parse({
        ...body,
        signature: sign(null, proof(body), keys.privateKey).toString("base64url"),
      });
    },
    record: async (events) => {
      for (const { signature, ...body } of events)
        expect(verify(null, proof(body), keys.publicKey, Buffer.from(signature, "base64url"))).toBe(
          true,
        );
      groups.push(events);
      if (events.at(-1)?.request_state === "SUCCEEDED") {
        started();
        await accepted;
      }
    },
  });
  const deps = {
    terminalBudget: budget,
    config: { commit_short_chat_sse: enabled, downstream_stall_timeout_ms: 10000 },
  } as Relay[0];
  const facts = {
    operation: "chat",
    protocol: "chat",
    model: "logical",
    requestId: id,
    background: false,
    references: [],
  } as unknown as Relay[1];
  const admitted = {
    attempt: one,
    outbound: new Uint8Array(),
    committedGrant: { reply: { stream_terminal_v1: true } },
    streamTerminalDimensions: ["input_tokens", "output_tokens"],
  } as Relay[2];
  const signal = controller.signal;
  const request = { signal, drain: signal, body: new Uint8Array() } as Relay[5];
  const control = {
    signal,
    end: async (v: { outcome: Parameters<typeof bridge.finish>[0] }) => {
      try {
        await bridge.finish(v.outcome);
      } finally {
        bridge.dispose();
      }
    },
    cause: () => undefined,
    timeoutError: (e: Error) => e,
    fail: async (o: { attempt_state: "PARTIAL" | "UNKNOWN" }, e: Error) => {
      failures.push("fail");
      await bridge.finish({ ...o, request_state: signal.aborted ? "CANCELLED" : "FAILED" });
      throw e;
    },
    abandon: async (o: { attempt_state: "PARTIAL" | "UNKNOWN" }, e: Error) => {
      failures.push("abandon");
      await bridge.finish({ ...o, request_state: signal.aborted ? "CANCELLED" : "FAILED" });
      throw e;
    },
  } as unknown as Relay[6];
  const upstream = new ReadableStream<Uint8Array>({
    async start(s) {
      for (const part of parts) s.enqueue(new TextEncoder().encode(part));
      if (fixed) {
        const tail = await fixed.tail;
        if (controller.signal.aborted) return;
        if (tail) s.enqueue(new TextEncoder().encode(tail));
      }
      s.close();
    },
  });
  return {
    response: relaySSE(
      deps,
      facts,
      admitted,
      bridge,
      upstream,
      request,
      control,
      fixed?.length ?? "chunked",
    ),
    groups,
    failures,
    budget,
    snapshot: () => one.snapshot(),
    close: () => bridge.dispose(),
  };
}
test("ordinary chunks beside the first complete metered terminal await the same replicated group", async () => {
  const accept = Promise.withResolvers<void>(),
    started = Promise.withResolvers<void>();
  const f = run([content + done], accept.promise, started.resolve);
  let returned = false;
  const response = f.response.then((r) => {
    returned = true;
    return r;
  });
  try {
    await Promise.race([started.promise, response]);
    expect(returned).toBe(false);
    expect(f.groups.map((g) => g.map((e) => [e.finality, e.request_state ?? null]))).toEqual([
      [
        ["FINAL", null],
        ["FINAL", "SUCCEEDED"],
      ],
    ]);
    accept.resolve();
    const result = await response;
    if (!("response" in result)) throw new Error("missing SSE");
    expect(await result.response.text()).toBe(content + done);
  } finally {
    accept.resolve();
    f.close();
  }
});

test("earlier unmetered chunks remain progressive and a published partial is never overtaken by the direct terminal", async () => {
  for (const withProgress of [false, true]) {
    const accept = Promise.withResolvers<void>(),
      started = Promise.withResolvers<void>();
    const first = withProgress
      ? line({
          choices: [{ delta: { content: "visible" } }],
          usage: { prompt_tokens: 7, completion_tokens: 1 },
        })
      : content;
    const f = run([first, done], accept.promise, started.resolve);
    try {
      const result = await f.response;
      if (!("response" in result)) throw new Error("missing SSE");
      const reader = result.response.body!.getReader();
      expect(new TextDecoder().decode((await reader.read()).value)).toBe(first);
      const rest = new Response(
        new ReadableStream({
          async start(controller) {
            for (;;) {
              const r = await reader.read();
              if (r.done) break;
              controller.enqueue(r.value);
            }
            controller.close();
          },
        }),
      ).text();
      await started.promise;
      expect(f.groups.map((g) => g.map((e) => e.finality))).toEqual(
        withProgress ? [["PARTIAL"], ["FINAL"], ["FINAL"]] : [["FINAL", "FINAL"]],
      );
      accept.resolve();
      expect(await rest).toBe(done);
    } finally {
      accept.resolve();
      f.close();
    }
  }
});
test("unknown terminal commit keeps the entire metered batch private and never signs an opposite result", async () => {
  const accept = Promise.withResolvers<void>(),
    started = Promise.withResolvers<void>();
  const f = run([content + done], accept.promise, started.resolve);
  try {
    await started.promise;
    accept.reject(new Error("unconfirmed"));
    await expect(f.response).rejects.toBeInstanceOf(Error);
    expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([null, "SUCCEEDED"]);
  } finally {
    f.close();
  }
});
test("stream without opt-in keeps its original two publications and earlier ordinary bytes", async () => {
  const f = run([content + done], Promise.resolve(), () => {}, new AbortController(), false);
  try {
    const result = await f.response;
    if (!("response" in result)) throw new Error("missing SSE");
    expect(await result.response.text()).toBe(content + done);
    expect(f.groups.map((g) => g.length)).toEqual([1, 1]);
  } finally {
    f.close();
  }
});
test("post-terminal data in the same read is rejected and withheld known usage cannot become a successful group", async () => {
  const f = run([content + done + content], Promise.resolve(), () => {});
  try {
    await expect(f.response).rejects.toBeInstanceOf(Error);
    expect(f.groups.flat().map((e) => e.request_state ?? null)).not.toContain("SUCCEEDED");
    expect(f.groups.flat().at(-1)?.request_state).toBe("FAILED");
  } finally {
    f.close();
  }
});

test("same upstream read with metered terminal followed by malformed bytes cannot dispatch another provider", async () => {
  const empty = line({ choices: [{ delta: { content: "" } }] });
  const padding = "x".repeat((64 << 10) - Buffer.byteLength(empty + done));
  const extended = line({ choices: [{ delta: { content: padding } }] });
  for (const input of [content + done + content, extended + done + content]) {
    const f = run([input], Promise.resolve(), () => {});
    try {
      const error = await f.response.catch((e: unknown) => e);
      expect(error).toBeInstanceOf(Error);
      expect(f.failures).toEqual(["fail"]);
      expect(f.groups.flat().at(-1)?.request_state).toBe("FAILED");
      expect(f.groups.flat().some((e) => e.usage.input_tokens === "7")).toBe(true);
    } finally {
      f.close();
    }
  }
});

test("slow reader retains bounded terminal bytes after the plugin deadline and discards buffered bytes at request deadline", async () => {
  const controller = new AbortController();
  const f = run(
    [content + content + done],
    Promise.resolve(),
    () => {},
    controller,
    true,
    undefined,
    150,
  );
  try {
    const result = await f.response;
    if (!("response" in result)) throw new Error("missing SSE");
    const reader = result.response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe(content);
    await Bun.sleep(180);
    expect(f.snapshot().disposed).toBe(true);
    expect(() => f.budget.reserve()).toThrow();
    const error = new Error("request_deadline");
    controller.abort(error);
    expect(await reader.read().catch((failure: unknown) => failure)).toBe(error);
    await Bun.sleep(0);
    const released = f.budget.reserve();
    released.release();
    expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([null, "SUCCEEDED"]);
  } finally {
    f.close();
  }
});

test("exhausted terminal byte budget publishes the original signed final before streaming a metered batch", async () => {
  const budget = createTerminalBudget(1, 16 << 10);
  const f = run(
    [content + done],
    Promise.resolve(),
    () => {},
    undefined,
    true,
    undefined,
    10000,
    budget,
  );
  try {
    const result = await f.response;
    if (!("response" in result)) throw new Error("missing SSE");
    expect(await result.response.text()).toBe(content + done);
    expect(f.groups.map((g) => g.map((e) => e.finality))).toEqual([["FINAL"], ["FINAL"]]);
    const released = budget.reserve();
    released.release();
  } finally {
    f.close();
  }
});

test("cancellation after sealing the first final keeps its original identity and withholds all metered bytes", async () => {
  const controller = new AbortController();
  let first: string | undefined;
  const f = run(
    [content + done],
    Promise.resolve(),
    () => {},
    controller,
    true,
    (event) => {
      if (!first) {
        first = event.event_id;
        controller.abort();
      }
    },
  );
  try {
    await expect(f.response).rejects.toBeInstanceOf(Error);
    expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([null, "CANCELLED"]);
    expect(f.groups.flat()[0]?.event_id).toBe(first);
  } finally {
    f.close();
  }
});
test("cancellation during an unknown success commit cannot release a terminal or replace its signed outcome", async () => {
  const controller = new AbortController(),
    accept = Promise.withResolvers<void>(),
    started = Promise.withResolvers<void>();
  const f = run([content + done], accept.promise, started.resolve, controller);
  try {
    await started.promise;
    controller.abort();
    accept.resolve();
    await expect(f.response).rejects.toBeInstanceOf(Error);
    expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([null, "SUCCEEDED"]);
  } finally {
    accept.resolve();
    f.close();
  }
});

test("slow readers retain the whole terminal output lease until the last pull or cancellation", async () => {
  const f = run([content + done], Promise.resolve(), () => {});
  try {
    const result = await f.response;
    if (!("response" in result)) throw new Error("missing SSE");
    const reader = result.response.body!.getReader();
    expect((await reader.read()).done).toBe(false);
    expect(() => f.budget.reserve()).toThrow();
    await reader.cancel();
    const released = f.budget.reserve();
    released.release();
    expect(f.snapshot().disposed).toBe(true);
    expect(host.snapshot().json.bytes).toBe(0);
  } finally {
    f.close();
  }
});
test("only a positively identified unattempted disabled Core falls back to ordinary durable delivery", async () => {
  const fixture = run([content + done], Promise.resolve(), () => {});
  try {
    const r = await fixture.response;
    if (!("response" in r)) throw new Error("missing SSE");
    await r.response.text();
    const evidence = fixture.groups[0]!;
    for (const [code, status, command_state] of [
      ["internal_stream_terminal_disabled", 409, "NOT_ATTEMPTED"],
      ["internal_owner_mismatch", 403, "NOT_ATTEMPTED"],
      ["internal_request_invalid", 400, "NOT_ATTEMPTED"],
      ["identity_store_unavailable", 503, "UNKNOWN"],
    ] as const) {
      const failure = new CoreRpcError(
        { code, category: "DEPENDENCY", retryability: "NONE", command_state, message: "synthetic" },
        status,
      );
      const writes: string[] = [];
      const deps = {
        core: {
          settleTerminal: async () => {
            throw failure;
          },
        },
        publish: async (group: typeof evidence) => {
          expect(group).toBe(evidence);
          writes.push("ordinary");
        },
        recoverStreamTerminal: async (group: typeof evidence) => {
          expect(group).toBe(evidence);
          writes.push("recovery");
        },
      } as unknown as Parameters<typeof publishTerminalEvidence>[0];
      const publish = publishTerminalEvidence(
        deps,
        evidence,
        crypto.randomUUID(),
        new Date(Date.now() + 60000).toISOString(),
        true,
      );
      if (code === "internal_stream_terminal_disabled") await publish;
      else await expect(publish).rejects.toBe(failure);
      expect(writes).toEqual([command_state === "UNKNOWN" ? "recovery" : "ordinary"]);
    }
  } finally {
    fixture.close();
  }
});

test("a fixed-length stream waits for clean EOF before publishing the first final pair or releasing its metered batch", async () => {
  for (const ending of ["clean", "extra", "short", "cancel"] as const) {
    const controller = new AbortController(),
      eof = Promise.withResolvers<string | undefined>(),
      sealed = Promise.withResolvers<void>();
    const accept = Promise.withResolvers<void>(),
      started = Promise.withResolvers<void>();
    const bytes = content + done;
    const f = run(
      [bytes],
      accept.promise,
      started.resolve,
      controller,
      true,
      () => sealed.resolve(),
      10000,
      createTerminalBudget(1),
      {
        length: Buffer.byteLength(bytes) + (ending === "short" ? 1 : 0),
        tail: eof.promise,
      },
    );
    let visible = false;
    const result = f.response.then(
      (r) => {
        visible = true;
        return r;
      },
      (e: unknown) => e,
    );
    try {
      await sealed.promise;
      await Bun.sleep(0);
      expect(visible).toBe(false);
      expect(f.groups).toEqual([]);
      if (ending === "cancel") controller.abort();
      eof.resolve(ending === "extra" ? content : undefined);
      if (ending === "clean") {
        await started.promise;
        expect(visible).toBe(false);
        accept.resolve();
        const r = (await result) as Awaited<typeof f.response>;
        if (!("response" in r)) throw new Error("missing SSE");
        expect(await r.response.text()).toBe(bytes);
        expect(f.groups.map((g) => g.map((e) => e.request_state ?? null))).toEqual([
          [null, "SUCCEEDED"],
        ]);
      } else {
        expect(await result).toBeInstanceOf(Error);
        expect(f.failures).toEqual(["fail"]);
        expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([
          null,
          ending === "cancel" ? "CANCELLED" : "FAILED",
        ]);
        expect(f.groups.flat()[0]?.usage.input_tokens).toBe("7");
      }
    } finally {
      eof.resolve(undefined);
      accept.resolve();
      f.close();
    }
  }
});

test("fixed streams outside the synchronous group preserve earlier progress and last DONE when an ignored trailing comment arrives on another feed", async () => {
  const comment = ": keepalive\n\n";
  const prior = line({
    choices: [{ delta: { content: "progress" } }],
    usage: { prompt_tokens: 7, completion_tokens: 1 },
  });
  const empty = line({ choices: [{ delta: { content: "" } }] });
  const padding = "x".repeat((64 << 10) - Buffer.byteLength(empty + done));
  const extended = line({ choices: [{ delta: { content: padding } }] });
  for (const eligibility of ["published", "no-budget"] as const) {
    for (const position of ["new-read", "same-read-new-feed", "same-feed"] as const) {
      const first = eligibility === "published" ? prior : "";
      const before = position === "same-read-new-feed" ? extended : content;
      const terminalWire = before + done;
      const source = [first, terminalWire + (position === "new-read" ? "" : comment)].filter(
        Boolean,
      );
      for (const enabled of [false, true]) {
        const f = run(
          source,
          Promise.resolve(),
          () => {},
          undefined,
          enabled,
          undefined,
          10000,
          createTerminalBudget(1, eligibility === "no-budget" ? 16 << 10 : 64 << 20),
          {
            length: Buffer.byteLength(first + terminalWire + comment),
            tail: Promise.resolve(position === "new-read" ? comment : undefined),
          },
        );
        try {
          const r = await f.response.catch((e: unknown) => e);
          const failed = position === "same-feed";
          if (r instanceof Error) {
            expect(failed).toBe(true);
          } else {
            const outcome = r as Awaited<typeof f.response>;
            if (!("response" in outcome)) throw new Error("missing SSE");
            const text = await outcome.response.text();
            if (failed) {
              expect(text).toContain("gateway_stream_error");
              expect(text).not.toContain("data: [DONE]");
            } else {
              expect(text).toBe(first + terminalWire);
            }
          }
          expect(f.groups.map((g) => g.length).every((n) => n === 1)).toBe(true);
          if (eligibility === "published") expect(f.groups[0]?.[0]?.finality).toBe("PARTIAL");
          expect(f.groups.flat().at(-1)?.request_state).toBe(failed ? "FAILED" : "SUCCEEDED");
        } finally {
          f.close();
        }
      }
    }
  }
});

test("a selected synchronous terminal group rejects even a keepalive on a later fixed-length read", async () => {
  const comment = ": keepalive\n\n";
  const source = content + done;
  const f = run(
    [source],
    Promise.resolve(),
    () => {},
    undefined,
    true,
    undefined,
    10000,
    createTerminalBudget(1),
    {
      length: Buffer.byteLength(source + comment),
      tail: Promise.resolve(comment),
    },
  );
  try {
    expect(await f.response.catch((e: unknown) => e)).toBeInstanceOf(Error);
    expect(f.groups.flat().map((e) => e.request_state ?? null)).toEqual([null, "FAILED"]);
    expect(f.failures).toEqual(["fail"]);
  } finally {
    f.close();
  }
});
