import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createLedgerQueries } from "@gateway/ledger";
import type { AdaptiveRoutingPolicy } from "@gateway/kernel";
import { SSEParser } from "@gateway/protocol";
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
  execution_policy: "route",
  operations: ["chat"],
  first_content_target_ms: 1000,
};

function perRequest(atoms: string) {
  return {
    basis: "REQUEST_COMPLETION" as const,
    rates: { requests: { numerator: atoms, denominator: "1" } },
    ceilings: { requests: "1" },
  };
}

async function customerMultiplier(
  runtime: ReturnType<typeof createGatewayRuntime>,
  model: string,
  channelId: unknown,
) {
  const price = await internalCall(runtime.internalApp, "PUT", "/internal/v1/pricing-modifiers", {
    layer: "COMMERCIAL",
    kind: "MULTIPLIER",
    match: { side: "CUSTOMER", model, operation: "chat", channel_id: channelId },
    multiplier_numerator: "2",
    multiplier_denominator: "1",
  });
  expect(price.status, JSON.stringify(price.data)).toBe(200);
}

async function readStream(
  fixture: Awaited<ReturnType<typeof setup>>,
  terminal: "complete" | "truncated" = "complete",
) {
  const startedAt = performance.now();
  const response = await fixture.chat(true);
  expect(response.status).toBe(200);
  const parser = new SSEParser();
  const reader = response.body!.getReader();
  let firstContentMs: number | undefined;
  let content: string | undefined;
  let complete = false;
  let errorCode: string | undefined;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      for (const { data } of parser.feed(chunk.value)) {
        if (data === "[DONE]") {
          complete = true;
          continue;
        }
        const event = JSON.parse(data) as {
          choices?: { delta?: { content?: string } }[];
          error?: { code?: string };
        };
        errorCode ??= event.error?.code;
        const delta = event.choices?.[0]?.delta?.content;
        if (delta && firstContentMs === undefined) {
          content = delta;
          firstContentMs = performance.now() - startedAt;
        }
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  expect(complete).toBe(terminal === "complete");
  expect(errorCode).toBe(terminal === "truncated" ? "stream_truncated" : undefined);
  expect(firstContentMs).toBeGreaterThan(0);
  return { id: response.headers.get("x-request-id")!, content, firstContentMs: firstContentMs! };
}

function requestMillis(id: string) {
  return Number.parseInt(id.slice(0, 8) + id.slice(9, 13), 16);
}

test("a single streaming model keeps its live choices, deliveries and charges during group-scoped observation and rollback", async () => {
  const lowCost = fakeUpstream("pilot-low-cost", { headerDelayMs: 5 });
  const highCost = fakeUpstream("pilot-high-cost", { headerDelayMs: 12 });
  const standby = fakeUpstream("pilot-standby");
  const upstreams = [lowCost, highCost, standby];
  let draw = 0;
  const models = new Set<string>();
  const runtime = createGatewayRuntime({
    config: makeConfig({ routingObservationModels: models, streamEstimatedSettlement: false }),
    db,
    wasmExecutor: null,
    routerOptions: { random: () => draw },
  });
  try {
    await runtime.start();
    const fixture = await setup(runtime, upstreams, {
      strategy: "WEIGHTED",
      priority: [10, 10, 0],
      adaptiveRouting: observe,
      supplierPrices: [perRequest("5"), perRequest("20"), perRequest("50")],
    });
    models.add(fixture.model);
    await customerMultiplier(runtime, fixture.model, fixture.channels[2]!.id);

    let first: string | undefined;
    for (let i = 0; i < 24; i++) {
      draw = i < 12 ? 0 : 0.999;
      const sample = await readStream(fixture);
      if (i === 0) first = sample.content;
    }
    expect(upstreams.map((upstream) => upstream.calls())).toEqual([12, 12, 0]);
    const differentDraw = first === lowCost.label ? 0.7 : 0.3;
    const interruptedUpstream = first === lowCost.label ? lowCost : highCost;
    const interruptedDelay = first === lowCost.label ? 5 : 12;
    const sequence = Array.from({ length: 8 }, () => [0, differentDraw, 0.999]).flat();

    async function cohort(name: string) {
      const before = upstreams.map((upstream) => upstream.calls());
      const delivered = [];
      for (const sample of sequence) {
        draw = sample;
        delivered.push(await readStream(fixture));
      }
      const completedCalls = upstreams.map((upstream, index) => upstream.calls() - before[index]!);
      interruptedUpstream.set({ truncate: true, headerDelayMs: interruptedDelay });
      draw = 0;
      const interrupted = await readStream(fixture, "truncated");
      interruptedUpstream.set({ headerDelayMs: interruptedDelay });
      expect(interrupted.content).toBe(interruptedUpstream.label);
      const all = [...delivered, interrupted];
      const calls = upstreams.map((upstream, index) => upstream.calls() - before[index]!);
      expect(calls.reduce((sum, count) => sum + count, 0)).toBe(all.length);
      expect(calls[2]).toBe(0);
      const [charge] = await db<
        { customer: string; supplier: string; n: number; distinct: number }[]
      >`
        SELECT sum(customer_atoms)::text AS customer, sum(supplier_cost_atoms)::text AS supplier,
               count(*)::int AS n, count(DISTINCT request_id)::int AS distinct
        FROM public.billing_charges
        WHERE request_id = ANY(${db.array(
          all.map(({ id }) => id),
          "UUID",
        )})
      `;
      expect(charge).toMatchObject({ customer: "360", n: 24, distinct: 24 });
      expect(Number(charge!.supplier)).toBe(completedCalls[0]! * 5 + completedCalls[1]! * 20);

      const range = {
        model: fixture.model,
        from: new Date(requestMillis(all[0]!.id)),
        to: new Date(requestMillis(interrupted.id) + 1),
      };
      const queries = createLedgerQueries(db);
      const pending = await queries.getRoutingCostReport(range);
      const [attempt] = await db<{ id: string }[]>`
        SELECT supplier_holds->0->>'attempt_id' AS id
        FROM public.billing_holds WHERE request_id = ${interrupted.id}::uuid AND state = 'UNKNOWN'
      `;
      expect(pending).toMatchObject({
        cohort_requests: all.length,
        currencies: [
          {
            supplier_cost_atoms: charge!.supplier,
            unknown_reserved_atoms: interruptedUpstream === lowCost ? "5" : "20",
            unknown_attempts: 1,
            unknown_attempts_without_reconciliation_start: 0,
          },
        ],
      });
      const age = pending.currencies[0]!.oldest_unknown_reconciliation_age_ms;
      expect(age).toBeGreaterThanOrEqual(0);
      expect(
        await runtime.ledger.resolveSupplierAttempt({
          request_id: interrupted.id,
          attempt_id: attempt!.id,
          action: "SETTLE",
          supplier_cost_atoms: "3",
          evidence_ref: "pilot-receipt-" + name,
          actor: "fixture",
          reason: "Controlled interrupted stream receipt",
        }),
      ).toMatchObject({ ok: true, state: "SETTLED" });

      const reconciled = await queries.getRoutingCostReport(range);
      expect(reconciled.currencies).toMatchObject([
        {
          supplier_cost_atoms: String(Number(charge!.supplier) + 3),
          unknown_reserved_atoms: "0",
          unknown_attempts: 0,
          oldest_unknown_reconciliation_age_ms: null,
        },
      ]);
      await fixture.resolveUnknownHolds();
      const timings = all.map(({ firstContentMs }) => firstContentMs).sort((a, b) => a - b);
      const report = {
        name,
        requests: all.length,
        completed: delivered.length,
        first_content_p95_ms: Number(timings[Math.ceil(timings.length * 0.95) - 1]!.toFixed(2)),
        supplier_atoms_per_1000_completions: Number(
          (
            (Number(reconciled.currencies[0]!.supplier_cost_atoms) * 1000) /
            delivered.length
          ).toFixed(2),
        ),
        observed_reconciliation_age_ms: age,
        outbound_calls: calls,
      };
      return { report, choices: all.map(({ content }) => content) };
    }

    await fixture.putModel(false);
    const baseline = await cohort("baseline");
    const scrape = async () => (await runtime.metricsResponse()).text();
    const observations = (text: string, reason: string) =>
      Number(text.match(new RegExp(`mode="observe",decision="${reason}"\\} (\\d+)`))?.[1] ?? 0);
    const before = observations(await scrape(), "different");

    await fixture.putModel(observe);
    const shadow = await cohort("observe");
    expect(shadow.choices).toEqual(baseline.choices);
    expect(shadow.report.outbound_calls).toEqual(baseline.report.outbound_calls);
    expect(shadow.report.supplier_atoms_per_1000_completions).toBe(
      baseline.report.supplier_atoms_per_1000_completions,
    );
    expect(observations(await scrape(), "different") - before).toBe(8);

    await fixture.putModel(false);
    const rollback = await cohort("rollback");
    expect(rollback.choices).toEqual(baseline.choices);
    expect(rollback.report.supplier_atoms_per_1000_completions).toBe(
      baseline.report.supplier_atoms_per_1000_completions,
    );
    await fixture.putModel({ ...observe, mode: "active" });
    draw = differentDraw;
    expect((await readStream(fixture)).content).toBe(lowCost.label);
    expect(await scrape()).toContain('mode="active",decision="applied"} 1');

    await fixture.putModel(observe);
    await customerMultiplier(runtime, fixture.model, fixture.channels[0]!.id);
    const beforeScoped = await scrape();
    draw = differentDraw;
    expect((await readStream(fixture)).content).toBe(highCost.label);
    const afterScoped = await scrape();
    expect(
      observations(afterScoped, "incompatible") - observations(beforeScoped, "incompatible"),
    ).toBe(1);
    expect(observations(afterScoped, "different")).toBe(observations(beforeScoped, "different"));
    expect(await fixture.audit()).toMatchObject({ ok: true, open_holds: 0, unknown_holds: 0 });
    console.log(
      JSON.stringify({ routing_policy_pilot: [baseline.report, shadow.report, rollback.report] }),
    );
  } finally {
    await runtime.close();
    await Promise.all(upstreams.map((upstream) => upstream.server.stop(true)));
  }
});

test("pure observation preserves a three-attempt fallback, unknown supplier exposure and channel-specific customer charges", async () => {
  const primary = fakeUpstream("pilot-ambiguous");
  const middle = fakeUpstream("pilot-rejected", { status: 503 });
  const standby = fakeUpstream("pilot-repriced");
  const upstreams = [primary, middle, standby];
  const runtime = createGatewayRuntime({
    config: makeConfig(),
    db,
    wasmExecutor: null,
    routerOptions: { failureThreshold: 10 },
  });
  try {
    await runtime.start();
    const fixture = await setup(runtime, upstreams, {
      strategy: "WEIGHTED",
      priority: [20, 10, 0],
      adaptiveRouting: false,
      retryOnAmbiguous: true,
      supplierPrices: [perRequest("5"), perRequest("5"), perRequest("10")],
      adapterOverrides: (index) => (index === 0 ? { reconciliation_evidence_required: true } : {}),
    });
    await customerMultiplier(runtime, fixture.model, fixture.channels[2]!.id);
    const from = new Date(Date.now() - 1000);
    const control = await fixture.chat();
    expect(control.status).toBe(200);
    expect((await control.json()).choices[0].message.content).toBe(primary.label);
    expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "15" });

    primary.set({ status: 503 });
    for (const policy of [false, observe] as const) {
      await fixture.putModel(policy);
      const result = await fixture.chat();
      expect(result.status).toBe(200);
      expect((await result.json()).choices[0].message.content).toBe(standby.label);
      const [row] = await db<{ states: string[]; customer: string; price: string }[]>`
        SELECT jsonb_path_query_array(h.supplier_holds, '$[*].state') AS states,
               c.customer_atoms::text AS customer, c.billing_price_id AS price
        FROM public.billing_holds h JOIN public.billing_charges c USING (request_id)
        WHERE h.request_id = ${result.headers.get("x-request-id")}::uuid
      `;
      expect(row).toEqual({
        states: ["UNKNOWN", "RELEASED", "SETTLED"],
        customer: "30",
        price: String(fixture.customer.id),
      });
    }
    expect(upstreams.map((upstream) => upstream.calls())).toEqual([3, 2, 2]);
    expect(await fixture.charges()).toEqual({ n: 3, u: 3, atoms: "75" });
    const observed = await createLedgerQueries(db).getRoutingCostReport({
      model: fixture.model,
      from,
      to: new Date(Date.now() + 1000),
    });
    expect(observed).toEqual({
      cohort_requests: 3,
      currencies: [
        {
          currency: "USD",
          supplier_cost_atoms: "25",
          unknown_attempts: 2,
          unknown_reserved_atoms: "10",
          unknown_attempts_without_reconciliation_start: 2,
          oldest_unknown_reconciliation_age_ms: null,
        },
      ],
    });
    expect(await fixture.audit()).toMatchObject({ ok: true, open_holds: 0 });
  } finally {
    await runtime.close();
    await Promise.all(upstreams.map((upstream) => upstream.server.stop(true)));
  }
});
