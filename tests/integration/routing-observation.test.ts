import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createLedgerQueries } from "@gateway/ledger";
import {
  db,
  fakeUpstream,
  makeConfig,
  registerRoutingFixture,
  setup,
} from "./smart-routing-fixture.ts";

registerRoutingFixture();

test("routing observation records first tool output and counts a 200 SSE interruption as failed delivery", async () => {
  const models = new Set<string>();
  const sent = Promise.withResolvers<ReadableStreamDefaultController<Uint8Array>>();
  const encoder = new TextEncoder();
  const upstream = fakeUpstream("observed-stream", {
    streamFrames: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode('data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}\n\n'),
        );
        sent.resolve(controller);
      },
    }),
  });
  const runtime = createGatewayRuntime({
    config: makeConfig({ routingObservationModels: models }),
    db,
    wasmExecutor: null,
  });
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    await runtime.start();
    const { model, chat, resolveUnknownHolds } = await setup(runtime, [upstream]);
    models.add(model);
    const response = await chat(true);
    expect(response.status).toBe(200);
    reader = response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('"role":"assistant"');
    const scrape = async () => (await runtime.metricsResponse()).text();
    expect(await scrape()).not.toMatch(/^gateway_routing_upstream_first_content_seconds_count\{/m);
    const controller = await sent.promise;
    controller.enqueue(
      encoder.encode(
        'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}\n\n',
      ),
    );
    expect(new TextDecoder().decode((await reader.read()).value)).toContain('"name":"lookup"');
    expect(await scrape()).toMatch(
      /^gateway_routing_upstream_first_content_seconds_count\{[^\n]+\} 1$/m,
    );
    controller.close();
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("stream_truncated");
    expect((await reader.read()).done).toBe(true);
    const metrics = await scrape();
    expect(metrics).toContain(
      `gateway_routing_deliveries_total{model="${model}",mode="stream",outcome="failed"} 1`,
    );
    expect(metrics).toMatch(
      /^gateway_routing_attempts_total\{[^\n]*dispatch="sent",outcome="failed"\} 1$/m,
    );
    await resolveUnknownHolds();
  } finally {
    await reader?.cancel().catch(() => undefined);
    reader?.releaseLock();
    await runtime.close();
    await upstream.server.stop(true);
  }
});

test("routing cost reports supplier expense in the original request window and unknown exposure separately", async () => {
  const upstream = fakeUpstream("cost-observation");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const { model, chat, resolveUnknownHolds } = await setup(runtime, [upstream]);
    const from = new Date(Date.now() - 1000);
    const success = await chat();
    expect(success.status).toBe(200);
    await success.json();
    upstream.set({ truncate: true });
    const failure = await chat(true);
    expect(await failure.text()).toContain("stream_truncated");
    const requestId = failure.headers.get("x-request-id");
    const [attempt] = await db<{ id: string; held_atoms: string; cutoff: Date }[]>`
      SELECT supplier_holds->0->>'attempt_id' AS id, supplier_holds->0->>'held_atoms' AS held_atoms,
             clock_timestamp() AS cutoff
      FROM public.billing_holds WHERE request_id = ${requestId}
    `;
    const to = attempt!.cutoff;
    const getCost = () => createLedgerQueries(db).getRoutingCostReport({ model, from, to });
    const before = await getCost();
    expect(before.cohort_requests).toBe(2);
    expect(before.currencies).toEqual([
      {
        currency: "USD",
        supplier_cost_atoms: "15",
        unknown_reserved_atoms: attempt!.held_atoms,
        unknown_attempts: 1,
      },
    ]);
    expect(
      await runtime.ledger.resolveSupplierAttempt({
        request_id: requestId!,
        attempt_id: attempt!.id,
        action: "SETTLE",
        supplier_cost_atoms: "7",
        evidence_ref: "observation-test-receipt",
        actor: "fixture",
        reason: "late supplier charge",
      }),
    ).toMatchObject({ ok: true, state: "SETTLED" });
    expect((await getCost()).currencies).toEqual([
      {
        currency: "USD",
        supplier_cost_atoms: "22",
        unknown_reserved_atoms: "0",
        unknown_attempts: 0,
      },
    ]);
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await upstream.server.stop(true);
  }
});

test("routing observation counts a normal SSE terminator with missing usage as a failed upstream attempt", async () => {
  const models = new Set<string>();
  const upstream = fakeUpstream("observation-missing-usage", {
    streamFrames:
      'data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
  });
  const runtime = createGatewayRuntime({
    config: makeConfig({ routingObservationModels: models }),
    db,
    wasmExecutor: null,
  });
  try {
    await runtime.start();
    const { model, chat, resolveUnknownHolds } = await setup(runtime, [upstream]);
    models.add(model);
    const response = await chat(true);
    expect(await response.text()).toContain("invalid_upstream_usage");
    const metrics = await (await runtime.metricsResponse()).text();
    expect(metrics).toMatch(
      /^gateway_routing_attempts_total\{[^\n]*dispatch="sent",outcome="failed"\} 1$/m,
    );
    await resolveUnknownHolds();
  } finally {
    await runtime.close();
    await upstream.server.stop(true);
  }
});

test("same-priority weighted routes both receive first attempts and a 429 is counted separately from successful delivery", async () => {
  const models = new Set<string>();
  const upstreams = [
    fakeUpstream("observation-weighted-a"),
    fakeUpstream("observation-weighted-b"),
  ];
  const draws = [0, 0, 0.999, 0];
  const runtime = createGatewayRuntime({
    config: makeConfig({ routingObservationModels: models }),
    db,
    wasmExecutor: null,
    routerOptions: { random: () => draws.shift() ?? 0 },
  });
  try {
    await runtime.start();
    const { model, chat } = await setup(runtime, upstreams, {
      strategy: "WEIGHTED",
      adaptiveRouting: false,
      priority: [100, 100],
    });
    const hidden = await chat();
    await hidden.json();
    expect(await (await runtime.metricsResponse()).text()).not.toMatch(
      /^gateway_routing_deliveries_total\{/m,
    );
    const first = upstreams.find((upstream) => upstream.calls() === 1)!;
    draws.unshift(0, 0, 0.999, 0);
    models.add(model);
    for (let i = 0; i < 2; i++) {
      const response = await chat();
      expect(response.status).toBe(200);
      await response.json();
    }
    expect(upstreams.map((upstream) => upstream.calls()).sort()).toEqual([1, 2]);
    draws.length = 0;
    first.set({ status: 429 });
    const recovered = await chat();
    expect(recovered.status).toBe(200);
    await recovered.json();
    expect(upstreams.map((upstream) => upstream.calls()).sort()).toEqual([2, 3]);
    const metrics = await (await runtime.metricsResponse()).text();
    expect(metrics).toContain(
      `gateway_routing_deliveries_total{model="${model}",mode="unary",outcome="complete"} 3`,
    );
    expect(metrics).toContain(
      `gateway_routing_completed_delivery_duration_seconds_count{model="${model}",mode="unary"} 3`,
    );
    expect(metrics).toMatch(
      /^gateway_routing_attempts_total\{[^\n]*dispatch="sent",outcome="rate_limited"\} 1$/m,
    );
    expect(
      metrics.match(
        /^gateway_routing_attempts_total\{[^\n]*dispatch="sent",outcome="complete"\} [12]$/gm,
      ),
    ).toHaveLength(2);
  } finally {
    await runtime.close();
    await Promise.all(upstreams.map((upstream) => upstream.server.stop(true)));
  }
});
