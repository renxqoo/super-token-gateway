import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { WebhookPluginExecutor } from "@gateway/plugin-engine";
import { fakeUpstream, makeConfig, db, setup } from "./smart-routing-fixture.ts";
import { registerRoutingFixture } from "./smart-routing-fixture.ts";
import { registerDiagnosticsCases } from "./stream-diagnostics-cases.ts";
import { registerFailoverCases } from "./channel-failover-cases.ts";
import { registerAnthropicStreamCases } from "./anthropic-stream-cases.ts";

registerRoutingFixture();
registerAnthropicStreamCases();

test("a failed SSE releases the only billing account slot while its hold awaits reconciliation", async () => {
  const source = Promise.withResolvers<ReadableStreamDefaultController<Uint8Array>>();
  const encoder = new TextEncoder();
  const upstream = fakeUpstream("single-stream-slot", {
    streamFrames: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode(
            'data: {"id":"slot","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}\n\n',
          ),
        );
        source.resolve(controller);
      },
    }),
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    await runtime.start();
    const fixture = await setup(runtime, [upstream], { billingConcurrency: 1 });
    try {
      const active = await fixture.chat(true);
      expect(active.status).toBe(200);
      reader = active.body!.getReader();
      const decoder = new TextDecoder();
      expect(decoder.decode((await reader.read()).value)).toContain('"content":"partial"');

      const rejected = await fixture.chat();
      expect(rejected.status).toBe(429);
      expect(await rejected.json()).toMatchObject({ error: { code: "rate_limit_concurrency" } });
      expect(upstream.calls()).toBe(1);
      expect(await fixture.holdsByState()).toEqual({ HELD: 1 });

      const controller = await source.promise;
      controller.enqueue(
        encoder.encode('event: error\ndata: {"error":{"message":"interrupted"}}\n\n'),
      );
      controller.close();
      let terminal = "";
      for (let result = await reader.read(); !result.done; result = await reader.read()) {
        terminal += decoder.decode(result.value, { stream: true });
      }
      expect(terminal).toContain("provider_stream_error");
      expect(terminal).not.toContain("data: [DONE]");
      await fixture.settle();
      expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });

      upstream.set({});
      const retry = await fixture.chat();
      expect(retry.status).toBe(200);
      expect(await retry.json()).toMatchObject({
        choices: [{ message: { content: "single-stream-slot" } }],
      });
      expect(upstream.calls()).toBe(2);
      expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "15" });
      expect(await fixture.holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 1 });
    } finally {
      await reader?.cancel().catch(() => undefined);
      reader?.releaseLock();
      await fixture.resolveUnknownHolds();
    }
  } finally {
    upstream.server.stop(true);
    await runtime.close();
  }
});

test("media SSE previously exposed success on incomplete or unmetered streams", async () => {
  const up = fakeUpstream("media-terminal");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { speech, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up], {
      operation: "speech",
    });
    const delta = 'data: {"type":"speech.audio.delta","audio":"YWJj"}\n\n';
    const usage = 'data: {"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\n';
    up.set({
      streamFrames:
        delta + usage + 'event: speech.audio.done\ndata: {"type":"speech.audio.done"}\n\n',
    });
    const complete = await speech();
    expect(complete.status).toBe(200);
    const completedWire = await complete.text();
    expect(completedWire).toContain("event: speech.audio.done");
    expect(completedWire).not.toContain("event: error");
    up.set({ streamFrames: delta + usage + "data: [DONE]\n\n" });
    const done = await speech();
    expect((await done.text()).trimEnd()).toEndWith("data: [DONE]");
    for (const [frames, code] of [
      [delta + "data: [DONE]\n\n", "invalid_upstream_usage"],
      [delta + usage + 'data: {"type":"speech.segment.done"}\n\n', "stream_interrupted"],
      [delta + usage + "event: error\n\ndata: [DONE]\n\n", "upstream_stream_error"],
      [delta + usage + "event: error\ndata: [DONE]\n\n", "upstream_stream_error"],
    ] as const) {
      up.set({ streamFrames: frames });
      const response = await speech();
      expect(response.status).toBe(200);
      const wire = await response.text();
      expect(wire).toContain("speech.audio.delta");
      expect(wire).toContain("event: error");
      expect(wire).toContain(code);
      expect(wire).not.toContain("[DONE]");
    }
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "30" });
    expect(await holdsByState()).toEqual({ SETTLED: 2, UNKNOWN: 4 });
    expect(up.calls()).toBe(6);
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("image settlement previously lost delivered image units when upstream also reported tokens", async () => {
  const up = fakeUpstream("image-metering");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { images, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up], {
      operation: "images",
      adapterOverrides: { usage_aliases: [{ target: "images", source_path: "meter.images" }] },
    });
    up.set({
      unaryBody: {
        data: [{ b64_json: "YWJj" }, { url: "https://provider.test/output.png" }],
        usage: { input_tokens: 10, output_tokens: 5 },
      },
    });
    const unary = await images(3, false);
    expect(unary.status).toBe(200);
    await unary.json();
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "10" });

    up.set({
      streamFrames:
        'data: {"type":"image_generation.partial_image","b64_json":"YWJj"}\n\ndata: {"type":"image_generation.completed","b64_json":"ZGVm","usage":{"input_tokens":10,"output_tokens":5}}\n\n',
    });
    const streamed = await images(1, true);
    expect(streamed.status).toBe(200);
    expect(await streamed.text()).toContain('"type":"image_generation.completed"');
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "15" });

    up.set({ unaryBody: { data: [{ b64_json: "YWJj" }, { b64_json: "ZGVm" }] } });
    const excess = await images(1, false);
    expect(excess.status).toBe(502);
    expect(await excess.json()).toMatchObject({ error: { code: "invalid_upstream_usage" } });

    up.set({ unaryBody: { data: [{ b64_json: "  " }], meter: { images: 1 } } });
    const conflicting = await images(1, false);
    expect(conflicting.status).toBe(502);
    expect(await conflicting.json()).toMatchObject({ error: { code: "usage_alias_conflict" } });

    const repeatedFrame = 'data: {"type":"image_generation.completed","b64_json":"ZGVm"}\n\n';
    up.set({ streamFrames: repeatedFrame.repeat(2) });
    const repeated = await images(2, true);
    const repeatedWire = await repeated.text();
    expect(repeatedWire).toContain("invalid_upstream_usage");
    expect(repeatedWire).not.toContain("image_generation.completed");

    up.set({
      streamFrames:
        'data: {"type":"image_generation.completed","usage":{"input_tokens":10,"output_tokens":5}}\n\n',
    });
    const unmetered = await images(1, true);
    const wire = await unmetered.text();
    expect(wire).toContain("invalid_upstream_usage");
    expect(wire).not.toContain("image_generation.completed");
    expect(await charges()).toEqual({ n: 2, u: 2, atoms: "15" });
    expect(await holdsByState()).toEqual({ SETTLED: 2, UNKNOWN: 4 });
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("a new media model uses configurable terminal and metering rules without a Gateway restart", async () => {
  const up = fakeUpstream("media-declared");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { speech, charges, holdsByState, resolveUnknownHolds, channels, health } = await setup(
      runtime,
      [up],
      {
        operation: "speech",
        adapterOverrides: {
          media_stream_terminal: { path: "payload.state", equals: "ready" },
          usage_aliases: [
            { target: "input_tokens", source_path: "counts.read" },
            { target: "output_tokens", source_path: "counts.write" },
          ],
          error_rules: [
            { path: "payload.rejected", equals: "unsupported_input", retry_kind: "client_error" },
          ],
        },
      },
    );
    const media =
      'data: {"type":"synthesis.segment.done","audio":"YWJj","counts":{"read":7,"write":3}}\n\n';
    const final = 'data: {"payload":{"state":"ready"}}\n\n';
    up.set({ streamFrames: media + final });
    const good = await speech();
    expect(good.status).toBe(200);
    expect(await good.text()).toBe(media + final);
    const before = (await health())[String(channels[0]!.id)]!;

    up.set({
      streamFrames:
        media + 'event: error\ndata: {"payload":{"rejected":"unsupported_input"}}\n\n' + final,
    });
    const rejected = await speech();
    const rejectedWire = await rejected.text();
    expect(rejectedWire).toContain("upstream_stream_error");
    expect(rejectedWire).not.toContain(final);
    expect((await health())[String(channels[0]!.id)]!.consecutive_failures).toBe(
      before.consecutive_failures,
    );

    up.set({ streamFrames: media });
    const truncated = await speech();
    expect(await truncated.text()).toContain("stream_interrupted");
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "10" });
    expect(await holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 2 });
    await resolveUnknownHolds();

    const named = await setup(runtime, [up], {
      operation: "speech",
      adapterOverrides: {
        media_stream_terminal: { event: "vendor_finished" },
        usage_aliases: [
          { target: "input_tokens", source_path: "counts.read" },
          { target: "output_tokens", source_path: "counts.write" },
        ],
      },
    });
    for (const marker of [
      "event: vendor_finished\n\n",
      "event: vendor_finished\ndata: complete\n\n",
    ]) {
      up.set({ streamFrames: media + marker });
      const finished = await named.speech();
      expect(await finished.text()).toBe(media + marker);
    }
    expect(await named.charges()).toEqual({ n: 2, u: 2, atoms: "20" });
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("chat finish and cache usage sharing the final frame survive through response and ledger", async () => {
  const up = fakeUpstream("finish-with-usage", {
    streamFrames: [
      'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}\n\n',
      'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4}}}\n\n',
      "data: [DONE]\n\n",
    ].join(""),
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { chat, charges } = await setup(runtime, [up], { cachedInputPrice: true });
    const response = await chat(true);
    expect(response.status).toBe(200);
    const wire = await response.text();
    expect(wire).toContain('"finish_reason":"stop"');
    expect(wire.trimEnd()).toEndWith("data: [DONE]");
    const [charge] =
      await db`SELECT usage FROM public.billing_charges WHERE request_id = ${response.headers.get("x-request-id")}`;
    expect(charge?.usage).toEqual({
      input_tokens: "6",
      output_tokens: "5",
      cached_input_tokens: "4",
    });
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "19" });
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("chat bills the usage-only trailer after the finish frame and preserves it on the wire", async () => {
  const requestedUsage: unknown[] = [];
  const upstream = fakeUpstream("usage-trailer", {
    async respond(request) {
      requestedUsage.push((await request.json()).stream_options);
      return new Response(
        [
          'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}\n\n',
          'data: {"id":"x","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
          'data: {"id":"x","model":"m","choices":[],"usage":{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20,"prompt_tokens_details":{"cached_tokens":5}}}\n\n',
          "data: [DONE]\n\n",
        ].join(""),
        { headers: { "content-type": "text/event-stream" } },
      );
    },
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { chat, charges } = await setup(runtime, [upstream], { cachedInputPrice: true });
    const response = await chat(true);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toStartWith("text/event-stream");
    const frames = (await response.text()).trimEnd().split("\n\n");
    expect(requestedUsage).toEqual([{ include_usage: true }]);
    expect(frames).toHaveLength(4);
    expect(frames[0]).toContain('"content":"answer"');
    expect(frames[1]).toContain('"finish_reason":"stop"');
    expect(JSON.parse(frames[2]!.slice("data: ".length))).toMatchObject({
      choices: [],
      usage: {
        prompt_tokens: 13,
        completion_tokens: 7,
        prompt_tokens_details: { cached_tokens: 5 },
      },
    });
    expect(frames[3]).toBe("data: [DONE]");
    const [charge] =
      await db`SELECT usage FROM public.billing_charges WHERE request_id = ${response.headers.get("x-request-id")}`;
    expect(charge?.usage).toEqual({
      input_tokens: "8",
      output_tokens: "7",
      cached_input_tokens: "5",
    });
    expect(await charges()).toEqual({ n: 1, u: 1, atoms: "25" });
  } finally {
    upstream.server.stop(true);
    await runtime.close();
  }
});

test.each(["commits", "fails"] as const)(
  "chat with settlement pending sends text before the ledger %s and sends the terminal outcome afterward",
  async (settlement) => {
    const source = Promise.withResolvers<ReadableStreamDefaultController<Uint8Array>>();
    const encoder = new TextEncoder();
    const upstream = fakeUpstream("settlement-gate", {
      streamFrames: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(
            encoder.encode(
              'data: {"id":"x","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}\n\n',
            ),
          );
          source.resolve(controller);
        },
      }),
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    const commitStarted = Promise.withResolvers<void>();
    const allowCommit = Promise.withResolvers<void>();
    const commit = runtime.ledger.commitTerminal;
    runtime.ledger.commitTerminal = async (input) => {
      commitStarted.resolve();
      await allowCommit.promise;
      if (settlement === "fails")
        throw new Error("Injected ledger failure before writing the charge");
      return commit(input);
    };
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      await runtime.start();
      const fixture = await setup(runtime, [upstream]);
      const response = await fixture.chat(true, AbortSignal.timeout(5_000));
      expect(response.status).toBe(200);
      reader = response.body!.getReader();
      const decoder = new TextDecoder();
      const first = await reader.read();
      expect(first.done).toBe(false);
      expect(decoder.decode(first.value, { stream: true })).toContain('"content":"answer"');
      const controller = await source.promise;
      controller.enqueue(
        encoder.encode(
          'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7}}\n\ndata: [DONE]\n\n',
        ),
      );
      controller.close();
      const next = reader.read();
      await Promise.race([
        commitStarted.promise,
        next.then(() => {
          throw new Error("The stream advanced before settlement was attempted");
        }),
      ]);
      expect(await fixture.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
      const pending = Symbol("settlement pending");
      expect(await Promise.race([next, pending])).toBe(pending);

      allowCommit.resolve();
      let terminal = "";
      for (let result = await next; !result.done; result = await reader.read()) {
        terminal += decoder.decode(result.value, { stream: true });
      }
      terminal += decoder.decode();
      if (settlement === "commits") {
        expect(terminal).toContain('"finish_reason":"stop"');
        expect(terminal.match(/data: \[DONE\]/g)).toHaveLength(1);
        expect(terminal.trimEnd()).toEndWith("data: [DONE]");
        expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "18" });
        expect(await fixture.holdsByState()).toEqual({ SETTLED: 1 });
      } else {
        expect(terminal).toContain("event: error");
        expect(terminal).toContain('"code":"ledger_commit_failed"');
        expect(terminal).not.toContain('"finish_reason":"stop"');
        expect(terminal).not.toContain("data: [DONE]");
        expect(await fixture.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
        expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
        await fixture.resolveUnknownHolds();
      }
    } finally {
      allowCommit.resolve();
      await reader?.cancel().catch(() => undefined);
      reader?.releaseLock();
      upstream.server.stop(true);
      await runtime.close();
    }
  },
);

test("symptom: chat SSE bills and emits DONE when EOF leaves a partial trailing event", async () => {
  const up = fakeUpstream("partial-trailer");
  const runtime = createGatewayRuntime({
    config: makeConfig({ streamEstimatedSettlement: false }),
    db,
    wasmExecutor: null,
  });
  try {
    await runtime.start();
    const { chat, charges, audit, holdsByState, resolveUnknownHolds } = await setup(runtime, [up]);
    try {
      const complete =
        'data: {"id":"x","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\ndata: [DONE]\n\n';
      up.set({ streamFrames: complete });
      const control = await chat(true);
      expect((await control.text()).trimEnd()).toEndWith("data: [DONE]");
      expect(await charges()).toMatchObject({ n: 1, u: 1 });

      up.set({ streamFrames: complete + 'event: error\ndata: {"error":' });
      const response = await chat(true);
      expect(response.status).toBe(200);
      const wire = await response.text();
      expect(wire).toContain("stream_interrupted");
      expect(wire).not.toContain("data: [DONE]");
      expect(await charges()).toMatchObject({ n: 1, u: 1 });
      expect(await holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 1 });
      expect(await audit()).toMatchObject({ ok: true, open_holds: 0 });
    } finally {
      await resolveUnknownHolds();
    }
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

test("symptom: a tail heartbeat turns a completed chat SSE stream into UNKNOWN", async () => {
  const up = fakeUpstream("terminal-comment", {
    streamFrames:
      'data: {"id":"x","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\ndata: [DONE]\n\n: keep-alive\n',
  });
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { chat, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up]);
    try {
      const response = await chat(true);
      expect(response.status).toBe(200);
      const wire = await response.text();
      expect(wire.trimEnd()).toEndWith("data: [DONE]");
      expect(await charges()).toEqual({ n: 1, u: 1, atoms: "15" });
      expect(await holdsByState()).toEqual({ SETTLED: 1 });
    } finally {
      await resolveUnknownHolds();
    }
  } finally {
    await runtime.close();
    await up.server.stop(true);
  }
});

for (const termination of [
  "partial frame",
  "missing terminal event",
  "client cancelled",
] as const) {
  test(`symptom: an ended chat SSE response leaves moderation running (${termination})`, async () => {
    const clientCancels = termination === "client cancelled";
    const encoder = new TextEncoder();
    const source = Promise.withResolvers<ReadableStreamDefaultController<Uint8Array>>();
    const up = fakeUpstream("ended-with-cutoff", {
      streamFrames: new ReadableStream<Uint8Array>({
        start(controller) {
          source.resolve(controller);
          const content = clientCancels ? "answer" : "answer.";
          controller.enqueue(
            encoder.encode(
              `data: {"id":"x","choices":[{"index":0,"delta":{"content":"${content}"}}]}\n\n`,
            ),
          );
        },
      }),
    });
    const finishWithUsage =
      'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}\n\n';
    const hooks: string[] = [];
    const started = Promise.withResolvers<AbortSignal>();
    const webhookReply = Promise.withResolvers<Response>();
    const webhook = new WebhookPluginExecutor({
      async fetchImpl(_url, init) {
        const { hook } = JSON.parse(String(init.body)) as { hook: string };
        hooks.push(hook);
        const signal = init.signal!;
        const aborted = Promise.withResolvers<never>();
        const onAbort = () => aborted.reject(signal.reason);
        signal.addEventListener("abort", onAbort, { once: true });
        started.resolve(signal);
        try {
          return await Promise.race([webhookReply.promise, aborted.promise]);
        } finally {
          signal.removeEventListener("abort", onAbort);
        }
      },
    });
    const runtime = createGatewayRuntime({
      config: makeConfig(),
      db,
      wasmExecutor: null,
      webhookExecutor: webhook,
    });
    try {
      const plugin = await runtime.catalog.upsertPlugin({
        name: `eof-cutoff-${termination.replaceAll(" ", "-")}`,
        version: "1.0.0",
        kind: "WEBHOOK",
        webhook_config: {
          url: "http://moderation.test/check",
          stream_mode: "STREAM_AND_CUTOFF",
          window_chars: 16,
          timeout_ms: 2_000,
        },
        manifest: { hooks: ["on_stream_chunk", "on_stream_end"] },
      });
      await db`UPDATE public.plugins SET enabled = true WHERE id = ${plugin.id}`;
      await runtime.start();
      const { chat, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [up], {
        contentPluginId: plugin.id,
      });
      try {
        const response = await chat(true);
        expect(response.status).toBe(200);
        const upstream = await source.promise;
        let signal: AbortSignal;
        if (clientCancels) {
          upstream.enqueue(encoder.encode(finishWithUsage + "data: [DONE]\n\n"));
          upstream.close();
          signal = await started.promise;
          expect(webhook.stats()[plugin.id]?.inFlight).toBe(1);
          const cancelled = response.body!.cancel();
          webhookReply.resolve(Response.json({ action: "PASS" }));
          await cancelled;
        } else {
          const reading = response.text();
          signal = await started.promise;
          expect(webhook.stats()[plugin.id]?.inFlight).toBe(1);
          const end =
            termination === "partial frame" ? "data: [DONE]\n\nevent: error\ndata: {" : "";
          upstream.enqueue(encoder.encode(finishWithUsage + end));
          upstream.close();
          const wire = await reading;
          expect(wire).toContain(
            termination === "partial frame" ? "stream_interrupted" : "stream_truncated",
          );
          expect(wire).not.toContain("data: [DONE]");
        }
        expect(await charges()).toMatchObject({ n: 0 });
        expect(await holdsByState()).toEqual({ UNKNOWN: 1 });
        expect(signal.aborted).toBe(true);
        expect(signal.reason).not.toHaveProperty("message", "webhook_timeout");
        expect(webhook.stats()[plugin.id]?.inFlight).toBe(0);
        expect(hooks).toEqual([clientCancels ? "on_stream_end" : "on_stream_chunk"]);
      } finally {
        webhookReply.resolve(Response.json({ action: "PASS" }));
        await resolveUnknownHolds();
      }
    } finally {
      await up.server.stop(true);
      await runtime.close();
    }
  });
}

for (const stream of [false, true]) {
  test(`symptom: an accepted Unicode channel name breaks chat response headers with stream=${stream}`, async () => {
    const up = fakeUpstream("中文渠道");
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    try {
      await runtime.start();
      const { chat, channels, charges, holdsByState, resolveUnknownHolds } = await setup(runtime, [
        up,
      ]);
      try {
        const response = await chat(stream);
        const body = await response.text();
        expect(response.status, body).toBe(200);
        expect(response.headers.get("x-gateway-channel")).toBe(String(channels[0]!.id));
        expect(body).toContain("中文渠道");
        if (stream) expect(body.trimEnd()).toEndWith("data: [DONE]");
        expect(up.calls()).toBe(1);
        expect(await charges()).toEqual({ n: 1, u: 1, atoms: "15" });
        expect(await holdsByState()).toEqual({ SETTLED: 1 });
      } finally {
        await resolveUnknownHolds();
      }
    } finally {
      await up.server.stop(true);
      await runtime.close();
    }
  });
}

test("symptom: cancelled or saturated inference waits create reservations before the dial is admitted", async () => {
  const upstream = fakeUpstream("dial", { headerDelayMs: 250 });
  const runtime = createGatewayRuntime({
    config: makeConfig({
      upstreamDial: { concurrency: 1, startsPerSecond: 1, maxQueueWaitMs: 65 },
    }),
    db,
  });
  const pending: Promise<Response>[] = [];
  try {
    await runtime.start();
    const fixture = await setup(runtime, [upstream]);
    const first = Promise.resolve(fixture.chat());
    pending.push(first);
    for (let i = 0; upstream.calls() === 0 && i < 100; i++) await Bun.sleep(5);
    expect(upstream.calls()).toBe(1);

    const cancellation = new AbortController();
    const cancelled = Promise.resolve(fixture.chat(false, cancellation.signal));
    const excess = Promise.resolve(fixture.chat());
    pending.push(cancelled, excess);
    await Bun.sleep(15);
    expect(await fixture.holdsByState()).toEqual({ HELD: 1 });
    cancellation.abort();
    const [aborted, saturated] = await Promise.all([cancelled, excess]);
    expect(aborted.status).toBe(499);
    expect(saturated.status).toBe(503);
    expect(saturated.headers.get("retry-after")).toBe("1");
    expect(await saturated.json()).toMatchObject({ error: { code: "upstream_capacity" } });
    expect((await first).status).toBe(200);
    expect(upstream.calls()).toBe(1);
    expect(await fixture.holdsByState()).toEqual({ SETTLED: 1 });
    expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "15" });
    expect(await fixture.audit()).toMatchObject({ ok: true, open_holds: 0, unknown_holds: 0 });
  } finally {
    await Promise.allSettled(pending);
    await runtime.close();
    upstream.server.stop(true);
  }
});

test("cascade: primary 503 -> secondary 429 -> tertiary healthy; one request succeeds through two failures with a single exact charge", async () => {
  const A = fakeUpstream("A", { status: 503 });
  const B = fakeUpstream("B", { status: 429 });
  const C = fakeUpstream("C");
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const ctx = await setup(runtime, [A, B, C], { priority: [20, 10, 0], strategy: "WEIGHTED" });
    const before = await ctx.wallet();
    const t0 = performance.now();
    const res = await ctx.chat();
    const elapsed = performance.now() - t0;
    expect(res.status).toBe(200);
    expect(res.headers.get("x-gateway-channel")?.startsWith("C-")).toBe(true);
    expect(
      ((await res.json()) as { choices: { message: { content: string } }[] }).choices[0]!.message
        .content,
    ).toBe("C");
    expect(elapsed).toBeLessThan(1500);
    expect(A.calls()).toBe(1);
    expect(B.calls()).toBe(1);
    expect(C.calls()).toBe(1);
    await ctx.settle();
    const after = await ctx.wallet();
    expect(BigInt(after.spent_atoms) - BigInt(before.spent_atoms)).toBe(15n);
    expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
    const c = await ctx.charges();
    expect(c.n).toBe(1);
    expect(c.u).toBe(1);
    expect(BigInt(c.atoms)).toBe(15n);
    expect(await ctx.holdsByState()).toEqual({ SETTLED: 1 });
    expect((await ctx.audit()).ok).toBe(true);
    const h = await ctx.health();
    expect(h[ctx.channels[0]!.id as string]!.consecutive_failures).toBe(1);
    expect(h[ctx.channels[1]!.id as string]!.consecutive_failures).toBe(0);
    expect(h[ctx.channels[2]!.id as string]!.state).toBe("CLOSED");
  } finally {
    await runtime.close();
    for (const u of [A, B, C]) u.server.stop(true);
  }
});

registerDiagnosticsCases();
registerFailoverCases();
