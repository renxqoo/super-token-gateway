import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import type { AdaptiveRoutingPolicy } from "@gateway/kernel";
import {
  db,
  fakeUpstream,
  internalCall,
  makeConfig,
  registerRoutingFixture,
  setup,
} from "./smart-routing-fixture.ts";

registerRoutingFixture();

const observe: AdaptiveRoutingPolicy = {
  mode: "observe",
  operations: ["chat"],
  first_content_target_ms: 300_000,
};

const notExecuted = {
  path: "error.code",
  equals: "rejected_before_generation",
  execution: "NOT_EXECUTED",
  retry_kind: "retryable",
  when_http_status: [429, 503],
} as const;

const fee = (atoms: string) => ({
  basis: "REQUEST_COMPLETION" as const,
  rates: { requests: { numerator: atoms, denominator: "1" } },
  ceilings: { requests: "1" },
});

const refusalFixture = (status: number, evidence: boolean) => ({
  status,
  unaryBody: { error: { code: evidence ? "rejected_before_generation" : "unknown_execution" } },
});

test("weighted chat applies adaptive ranking by default and supports durable opt-out and reset", async () => {
  const cheap = fakeUpstream("default-adaptive-cheap");
  const expensive = fakeUpstream("default-adaptive-expensive");
  let draw = 0;
  const runtime = createGatewayRuntime({
    config: makeConfig(),
    db,
    wasmExecutor: null,
    routerOptions: { random: () => draw },
  });
  try {
    await runtime.start();
    const f = await setup(runtime, [cheap, expensive], {
      strategy: "WEIGHTED",
      supplierPrices: [fee("5"), fee("20")],
    });
    const chat = async () => {
      const response = await f.chat();
      expect(response.status).toBe(200);
      return (await response.json()).choices[0].message.content as string;
    };
    const first = await chat();
    for (let i = 1; i < 12; i++) expect(await chat()).toBe(first);
    draw = 0.999;
    for (let i = 0; i < 12; i++) expect(await chat()).not.toBe(first);
    expect([cheap.calls(), expensive.calls()]).toEqual([12, 12]);

    draw = first === cheap.label ? 0.7 : 0.3;
    expect(await chat()).toBe(cheap.label);
    const scrape = () => runtime.metricsResponse().then((res) => res.text());
    expect(await scrape()).toContain(`{model="${f.model}",mode="active",decision="applied"} 1`);
    expect((await f.putModel(false)).adaptive_routing).toBe(false);
    expect((await f.putModel()).adaptive_routing).toBe(false);
    expect(await chat()).toBe(expensive.label);
    expect(await scrape()).toContain(`{model="${f.model}",mode="active",decision="applied"} 1`);

    expect((await f.putModel(null)).adaptive_routing).toBeNull();
    expect(await chat()).toBe(cheap.label);
    expect(await scrape()).toContain(`{model="${f.model}",mode="active",decision="applied"} 2`);
    const changedRetry = await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      id: f.routes[0]!.id,
      model_name: f.model,
      channel_id: f.channels[0]!.id,
      upstream_model: "m",
      operations: ["chat"],
      billing_price_id: f.customer.id,
      cost_version_id: f.supplierPrices[0]!.id,
      retry_policy: { max_attempts: 1 },
    });
    expect(changedRetry.status).toBe(200);
    expect(await chat()).toBe(expensive.label);
    expect(await scrape()).toContain(
      `{model="${f.model}",mode="active",decision="incompatible"} 1`,
    );
  } finally {
    await runtime.close();
    await Promise.all([cheap.server.stop(true), expensive.server.stop(true)]);
  }
});

test("default weighted chat retains the three-attempt route policy and charges once on a legacy cascade", async () => {
  const first = fakeUpstream("default-adaptive-503", refusalFixture(503, false));
  const second = fakeUpstream("default-adaptive-429", refusalFixture(429, false));
  const third = fakeUpstream("default-adaptive-success");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const f = await setup(runtime, [first, second, third], {
      strategy: "WEIGHTED",
      priority: [20, 10, 0],
      maxAttempts: 3,
    });
    const response = await f.chat();
    expect(response.status).toBe(200);
    expect((await response.json()).choices[0].message.content).toBe(third.label);
    expect([first.calls(), second.calls(), third.calls()]).toEqual([1, 1, 1]);
    expect(await f.charges()).toEqual({ n: 1, u: 1, atoms: "15" });
  } finally {
    await runtime.close();
    await Promise.all([first, second, third].map((upstream) => upstream.server.stop(true)));
  }
});

test("observe keeps live first choice, active uses learned cost, and customer price stays the same", async () => {
  const cheap = fakeUpstream("adaptive-cheap");
  const expensive = fakeUpstream("adaptive-expensive");
  let draw = 0;
  const runtime = createGatewayRuntime({
    config: makeConfig(),
    db,
    wasmExecutor: null,
    routerOptions: { random: () => draw },
  });
  try {
    await runtime.start();
    const f = await setup(runtime, [cheap, expensive], {
      strategy: "WEIGHTED",
      adaptiveRouting: observe,
      supplierPrices: [
        {
          ...fee("5"),
          rates: { ...fee("5").rates, input_tokens: { numerator: "100", denominator: "1" } },
          ceilings: { ...fee("5").ceilings, input_tokens: "100" },
        },
        fee("20"),
      ],
    });
    const override = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/pricing-overrides",
      {
        billing_account_id: f.billingAccountId,
        model_name: f.model,
        operation: "chat",
        override_kind: "MULTIPLIER",
        multiplier_numerator: "2",
        multiplier_denominator: "1",
      },
    );
    expect(override.status).toBe(200);
    const chat = async () => {
      const res = await f.chat();
      expect(res.status).toBe(200);
      return (await res.json()).choices[0].message.content as string;
    };
    const first = await chat();
    expect(await (await runtime.metricsResponse()).text()).toContain('decision="insufficient"} 1');
    for (let i = 1; i < 12; i++) expect(await chat()).toBe(first);
    draw = 0.999;
    for (let i = 0; i < 12; i++) expect(await chat()).not.toBe(first);
    expect([cheap.calls(), expensive.calls()]).toEqual([12, 12]);

    draw = first === cheap.label ? 0.7 : 0.3;
    expect(await chat()).toBe(expensive.label);
    expect(await (await runtime.metricsResponse()).text()).toMatch(
      /mode="observe",decision="(?:same|different)"\} 1/,
    );
    await f.putModel({ ...observe, mode: "active" });
    expect(await chat()).toBe(cheap.label);
    expect(await (await runtime.metricsResponse()).text()).toContain(
      'mode="active",decision="applied"} 1',
    );
    expect(await f.charges()).toEqual({ n: 26, u: 26, atoms: "780" });

    await f.putModel(observe);
    expect(await chat()).toBe(expensive.label);
    expect([cheap.calls(), expensive.calls()]).toEqual([13, 14]);
    await f.putModel({ ...observe, mode: "active" });
    const route = {
      id: f.routes[0]!.id,
      model_name: f.model,
      channel_id: f.channels[0]!.id,
      upstream_model: "m",
      operations: ["chat"],
      billing_price_id: f.customer.id,
      cost_version_id: f.supplierPrices[0]!.id,
    };
    const altered = await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      ...route,
      adapter_overrides: { path_override: "/changed-endpoint" },
    });
    expect(altered.status).toBe(200);
    expect(await chat()).toBe(expensive.label);
    expect(await (await runtime.metricsResponse()).text()).toContain(
      'mode="active",decision="insufficient"} 1',
    );
    const incompatible = await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
      ...route,
      adapter_overrides: { extra_headers: { "x-api-feature": "alternate-mode" } },
    });
    expect(incompatible.status).toBe(200);
    expect(await chat()).toBe(expensive.label);
    expect(await (await runtime.metricsResponse()).text()).toContain(
      'mode="active",decision="incompatible"} 1',
    );
  } finally {
    await runtime.close();
    await Promise.all([cheap.server.stop(true), expensive.server.stop(true)]);
  }
});

test.each([
  [503, true],
  [503, false],
  [429, true],
  [429, false],
] as const)(
  "HTTP %d with execution proof=%p only retries on an exact match",
  async (status, proof) => {
    const primary = fakeUpstream("adaptive-evidence-first", refusalFixture(status, proof));
    const backup = fakeUpstream("adaptive-evidence-backup");
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    try {
      await runtime.start();
      const f = await setup(runtime, [primary, backup], {
        strategy: "WEIGHTED",
        priority: [10, 0],
        adaptiveRouting: observe,
        retryOnAmbiguous: true,
        adapterOverrides: {
          error_rules: [{ ...notExecuted, when_http_status: [...notExecuted.when_http_status] }],
        },
      });
      const result = await f.chat();
      expect(result.status).toBe(proof ? 200 : status === 429 ? 429 : 502);
      await result.json();
      expect([primary.calls(), backup.calls()]).toEqual([1, proof ? 1 : 0]);
      const [actual] = await db<{ attempts: string[] }[]>`
      SELECT ARRAY(SELECT attempt->>'state' FROM jsonb_array_elements(supplier_holds) attempt) AS attempts
      FROM public.billing_holds WHERE billing_account_id = ${f.billingAccountId}
    `;
      expect(actual?.attempts).toEqual(proof ? ["RELEASED", "SETTLED"] : ["UNKNOWN"]);
      expect(await f.charges()).toEqual(
        proof ? { n: 1, u: 1, atoms: "15" } : { n: 0, u: 0, atoms: "0" },
      );
      if (!proof) await f.resolveUnknownHolds();
    } finally {
      await runtime.close();
      await Promise.all([primary.server.stop(true), backup.server.stop(true)]);
    }
  },
);

test("the exact reservation cap skips an expensive first choice and preserves a prior idempotent result when tightened", async () => {
  const primary = fakeUpstream("adaptive-cap-first");
  const backup = fakeUpstream("adaptive-cap-backup");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const f = await setup(runtime, [primary, backup], {
      strategy: "WEIGHTED",
      priority: [10, 0],
      adaptiveRouting: { ...observe, supplier_reserve_limit: { currency: "USD", atoms: "5" } },
      supplierPrices: [fee("20"), fee("5")],
    });
    const key = "cap-replay";
    const result = await f.chat(false, undefined, { key });
    expect(result.status).toBe(200);
    await result.json();
    await f.putModel({
      ...observe,
      mode: "active",
      supplier_reserve_limit: { currency: "USD", atoms: "4" },
    });
    const replay = await f.chat(false, undefined, { key });
    expect(replay.status).toBe(409);
    expect(replay.headers.get("x-request-id")).toBe(result.headers.get("x-request-id"));
    expect(await replay.text()).toContain("idempotency_replayed");
    const conflict = await f.chat(false, undefined, { key, message: "A different request" });
    expect(conflict.status).toBe(409);
    expect(await conflict.text()).toContain("idempotency_conflict");
    const blocked = await f.chat();
    expect(blocked.status).toBe(503);
    expect(await blocked.text()).toContain("supplier_reserve_limit");
    expect([primary.calls(), backup.calls()]).toEqual([0, 1]);
    expect(await f.holdsByState()).toEqual({ SETTLED: 1 });
  } finally {
    await runtime.close();
    await Promise.all([primary.server.stop(true), backup.server.stop(true)]);
  }
});

test("a known failed first send cannot authorize an over-cap fallback reservation", async () => {
  const primary = fakeUpstream("adaptive-cap-failure", refusalFixture(503, true));
  const backup = fakeUpstream("adaptive-cap-skipped");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const f = await setup(runtime, [primary, backup], {
      strategy: "WEIGHTED",
      priority: [10, 0],
      adaptiveRouting: { ...observe, supplier_reserve_limit: { currency: "USD", atoms: "5" } },
      supplierPrices: [fee("5"), fee("20")],
      adapterOverrides: { error_rules: [{ ...notExecuted, when_http_status: [503] }] },
    });
    const result = await f.chat();
    expect(result.status).toBe(503);
    expect(await result.text()).toContain("supplier_reserve_limit");
    expect([primary.calls(), backup.calls()]).toEqual([1, 0]);
    const [row] = await db<{ attempts: number; state: string }[]>`
      SELECT jsonb_array_length(supplier_holds) AS attempts, state
      FROM public.billing_holds WHERE billing_account_id = ${f.billingAccountId}
    `;
    expect(row).toEqual({ attempts: 1, state: "RELEASED" });
  } finally {
    await runtime.close();
    await Promise.all([primary.server.stop(true), backup.server.stop(true)]);
  }
});

test("confirmed failures never send a third request under the adaptive attempt ceiling", async () => {
  const first = fakeUpstream("adaptive-attempt-1", refusalFixture(503, true));
  const second = fakeUpstream("adaptive-attempt-2", refusalFixture(503, true));
  const third = fakeUpstream("adaptive-attempt-3");
  const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
  try {
    await runtime.start();
    const f = await setup(runtime, [first, second, third], {
      strategy: "WEIGHTED",
      priority: [20, 10, 0],
      adaptiveRouting: observe,
      maxAttempts: 3,
      adapterOverrides: { error_rules: [{ ...notExecuted, when_http_status: [503] }] },
    });
    const result = await f.chat();
    expect(result.status).toBe(502);
    await result.json();
    expect([first.calls(), second.calls(), third.calls()]).toEqual([1, 1, 0]);
    expect(await f.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
    expect(await f.holdsByState()).toEqual({ RELEASED: 1 });
  } finally {
    await runtime.close();
    await Promise.all([first, second, third].map((upstream) => upstream.server.stop(true)));
  }
});

test.each([true, false])(
  "streamed execution proof before public output=%p only switches before delivery",
  async (early) => {
    const content = 'data: {"choices":[{"index":0,"delta":{"content":"already visible"}}]}\n\n';
    const error = 'event: error\ndata: {"error":{"code":"rejected_before_generation"}}\n\n';
    const first = fakeUpstream("adaptive-stream-first", {
      streamFrames: early ? error : content + error,
    });
    const backup = fakeUpstream("adaptive-stream-backup");
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    try {
      await runtime.start();
      const f = await setup(runtime, [first, backup], {
        strategy: "WEIGHTED",
        priority: [10, 0],
        adaptiveRouting: observe,
        adapterOverrides: { error_rules: [{ ...notExecuted, when_http_status: [200] }] },
      });
      const result = await f.chat(true);
      expect(result.status).toBe(200);
      const body = await result.text();
      expect(body).toContain(early ? backup.label : "already visible");
      expect([first.calls(), backup.calls()]).toEqual([1, early ? 1 : 0]);
      if (!early) {
        expect(body).not.toContain("[DONE]");
        expect(await f.holdsByState()).toEqual({ UNKNOWN: 1 });
        await f.resolveUnknownHolds();
      }
    } finally {
      await runtime.close();
      await Promise.all([first.server.stop(true), backup.server.stop(true)]);
    }
  },
);

test("symptom: an upstream that waits forever to finish is excluded from failures after the stream deadline", async () => {
  const encoder = new TextEncoder();
  let keepAlive: ReturnType<typeof setInterval> | undefined;
  const first = fakeUpstream("adaptive-stream-deadline", {
    streamFrames: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          encoder.encode('data: {"choices":[{"index":0,"delta":{"content":"partial text"}}]}\n\n'),
        );
        keepAlive = setInterval(() => controller.enqueue(encoder.encode(": heartbeat\n\n")), 20);
      },
      cancel() {
        clearInterval(keepAlive);
      },
    }),
  });
  const runtime = createGatewayRuntime({
    config: makeConfig({ streamMaxDurationMs: 120, streamIdleTimeoutMs: 1000 }),
    db,
    wasmExecutor: null,
  });
  try {
    await runtime.start();
    const f = await setup(runtime, [first], { strategy: "WEIGHTED", adaptiveRouting: observe });
    const result = await f.chat(true);
    expect(result.status).toBe(200);
    const body = await result.text();
    expect(body).toContain("partial text");
    expect(body).not.toContain("[DONE]");
    await f.settle();
    expect(await (await runtime.metricsResponse()).text()).toMatch(
      /^gateway_routing_attempts_total\{[^\n]*mode="stream",dispatch="sent",outcome="failed"\} 1$/m,
    );
    expect(await f.holdsByState()).toEqual({ UNKNOWN: 1 });
    await f.resolveUnknownHolds();
  } finally {
    clearInterval(keepAlive);
    await runtime.close();
    await first.server.stop(true);
  }
});

test.each([200, 503])(
  "local interruption while reading upstream HTTP %i keeps the attempt uncertain without blaming the provider",
  async (upstreamStatus) => {
    const first = fakeUpstream("adaptive-interrupted-first", {
      respond: () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("{"));
            },
          }),
          {
            status: upstreamStatus,
            headers: {
              "content-type": "application/json",
              "x-provider-request-id": "incomplete-response",
            },
          },
        ),
    });
    const backup = fakeUpstream("adaptive-interrupted-backup");
    const runtime = createGatewayRuntime({ config: makeConfig(), db, wasmExecutor: null });
    const readingResponse = Promise.withResolvers<void>();
    const channelPermit = Promise.withResolvers<() => void>();
    const remember = runtime.ledger.rememberProviderRequestId;
    runtime.ledger.rememberProviderRequestId = async (...args) => {
      const result = await remember(...args);
      setImmediate(readingResponse.resolve);
      return result;
    };
    const acquire = runtime.cluster.acquireRateLimit.bind(runtime.cluster);
    runtime.cluster.acquireRateLimit = (scope, ...args) => {
      if (scope.startsWith("channel:") && args[3]) channelPermit.resolve(args[3]);
      return acquire(scope, ...args);
    };
    const client = new AbortController();
    try {
      await runtime.start();
      const f = await setup(runtime, [first, backup], {
        strategy: "WEIGHTED",
        priority: [10, 0],
        adaptiveRouting: observe,
        adapterOverrides: {
          reconciliation_response_id_header: "x-provider-request-id",
          error_rules: [{ ...notExecuted, when_http_status: [503] }],
        },
      });
      const pending = f.chat(false, client.signal);
      await readingResponse.promise;
      if (upstreamStatus === 503) client.abort();
      else (await channelPermit.promise)();
      const response = await pending;
      if (upstreamStatus === 503) expect(response.status).toBe(499);
      await response.text();
      const metrics = await (await runtime.metricsResponse()).text();
      expect(metrics).toMatch(
        new RegExp(
          `^gateway_routing_attempts_total\\{[^\\n]*dispatch="sent",outcome="${upstreamStatus === 503 ? "cancelled" : "local"}"\\} 1$`,
          "m",
        ),
      );
      expect([first.calls(), backup.calls()]).toEqual([1, 0]);
      expect(await f.holdsByState()).toEqual({ UNKNOWN: 1 });
      await f.resolveUnknownHolds();
    } finally {
      client.abort();
      await runtime.close();
      await Promise.all([first.server.stop(true), backup.server.stop(true)]);
    }
  },
);
