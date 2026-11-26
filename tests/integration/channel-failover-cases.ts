import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { fakeUpstream, makeConfig, db, setup, internalCall } from "./smart-routing-fixture.ts";

export function registerFailoverCases() {
  test("latency: a slow-failing primary near the attempt timeout adds at most one timeout of latency before switching; the client never waits for a second slow channel", async () => {
    const A = fakeUpstream("A", { status: 503, headerDelayMs: 2500 });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const res = await ctx.chat();
      const elapsedLabel = res.headers.get("x-gateway-channel");
      expect(res.status).toBe(200);
      expect(elapsedLabel?.startsWith("B-")).toBe(true);
      await res.text();
      await ctx.settle();
      const slow = await ctx.health();
      expect(slow[ctx.channels[0]!.id as string]!.consecutive_failures).toBeGreaterThanOrEqual(1);

      A.set({ status: 503, headerDelayMs: 4000 });
      B.set({ status: 503, headerDelayMs: 4000 });
      const before2 = await ctx.wallet();
      const t0 = performance.now();
      const failing = await ctx.chat();
      const waited = performance.now() - t0;
      expect(failing.status).toBe(504);
      expect(((await failing.json()) as { error: { code: string } }).error.code).toBe(
        "upstream_timeout",
      );
      expect(waited).toBeGreaterThan(2800);
      expect(waited).toBeLessThan(7000);
      await ctx.settle();
      const during = await ctx.wallet();
      expect(BigInt(during.spent_atoms)).toBe(BigInt(before2.spent_atoms));
      const unk =
        await db`SELECT held_atoms::text AS h FROM public.billing_holds WHERE billing_account_id = ${ctx.billingAccountId} AND state = 'UNKNOWN'`;
      expect(unk.length).toBe(1);
      expect(BigInt(during.reserved_atoms) - BigInt(before2.reserved_atoms)).toBe(
        BigInt(unk[0]!.h as string),
      );
      expect(await ctx.charges()).toMatchObject({ n: 1, u: 1 });
      expect(await ctx.holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 1 });
      const a = await ctx.audit();
      expect(a.ok).toBe(true);
      expect(a.open_holds).toBe(0);
      expect(a.unknown_holds).toBe(1);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("exhaustion: every channel down returns the last upstream status with no stranded hold, no charge, and no retry storm (each channel hit once)", async () => {
    const A = fakeUpstream("A", { status: 503 });
    const B = fakeUpstream("B", { status: 429 });
    const C = fakeUpstream("C", { status: 500 });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B, C], { priority: [20, 10, 0] });
      const before = await ctx.wallet();
      const res = await ctx.chat();
      expect(res.status).toBe(502);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe("upstream_error");
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(1);
      expect(C.calls()).toBe(1);
      await ctx.settle();
      for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++)
        await Bun.sleep(10);
      const after = await ctx.wallet();
      expect(after).toEqual(before);
      expect(await ctx.charges()).toMatchObject({ n: 0 });
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
      const a = await ctx.audit();
      expect(a.ok).toBe(true);
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
    } finally {
      await runtime.close();
      for (const u of [A, B, C]) u.server.stop(true);
    }
  });

  test("exhaustion with only 429s surfaces 429 upstream_rate_limited to the client", async () => {
    const A = fakeUpstream("A", { status: 429 });
    const B = fakeUpstream("B", { status: 429 });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B]);
      const res = await ctx.chat();
      expect(res.status).toBe(429);
      expect(((await res.json()) as { error: { code: string } }).error.code).toBe(
        "upstream_rate_limited",
      );
      await ctx.settle();
      for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++)
        await Bun.sleep(10);
      expect(await ctx.charges()).toMatchObject({ n: 0 });
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("dead channel: ECONNREFUSED on primary switches before dispatch, the customer is never charged for the dead attempt, and a later success closes the circuit via probe", async () => {
    const dead = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("x") });
    const deadPort = dead.port!;
    dead.stop(true);
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const tag = crypto.randomUUID().slice(0, 6);
      const call = async (method: string, path: string, body?: unknown) => {
        const r = await internalCall(runtime.internalApp, method, path, body);
        expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
        return r.data as Record<string, unknown>;
      };
      const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
        name: `sr-dead-${tag}`,
        currency: "USD",
        rpm: 10_000_000,
        tpm: 100_000_000,
        concurrency: 5000,
      });
      await call("POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
        currency: "USD",
        external_ref: `d-${tag}`,
        amount_atoms: "1000000000",
        kind: "CASH",
      });
      const key = await call("PUT", "/internal/v1/api-keys", {
        billing_account_id: billingAccount.id,
        name: "k",
      });
      const account = await call("PUT", "/internal/v1/upstream-accounts", { name: `d-${tag}` });
      await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
        currency: "USD",
        external_ref: `ds-${tag}`,
        amount_atoms: "1000000000",
      });
      const deadCh = await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: "openai",
        name: `dead-${tag}`,
        base_url: `http://127.0.0.1:${deadPort}/openai`,
        api_key: "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      });
      const liveCh = await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: "openai",
        name: `live-${tag}`,
        base_url: `${B.url}/openai`,
        api_key: "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      });
      const customer = await call("POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "1", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "100" },
      });
      const supplier = await call("POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "1", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "100" },
      });
      const model = `sr-dead-${tag}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: deadCh.id,
        upstream_model: "m",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        priority: 10,
        retry_policy: { max_attempts: 2 },
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: liveCh.id,
        upstream_model: "m",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        priority: 0,
        retry_policy: { max_attempts: 2 },
      });
      const chat = () =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model,
              stream: false,
              messages: [{ role: "user", content: "x" }],
              max_tokens: 20,
            }),
          }),
        );
      const t0 = performance.now();
      const res = await chat();
      const sw = performance.now() - t0;
      expect(res.status).toBe(200);
      expect(res.headers.get("x-gateway-channel")).toBe(String(liveCh.name));
      expect(sw).toBeLessThan(1500);
      await res.text();
      for (let i = 0; i < 100 && runtime.lifecycle.size > 0; i++) await Bun.sleep(10);
      const charges =
        await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id = ${billingAccount.id}`;
      expect(charges[0]!.n).toBe(1);
      const hold =
        await db`SELECT supplier_holds FROM public.billing_holds WHERE billing_account_id = ${billingAccount.id}`;
      const sup = hold[0]!.supplier_holds as { channel_id: string; state: string }[];
      expect(sup.some((x) => x.channel_id === String(deadCh.id) && x.state === "RELEASED")).toBe(
        true,
      );
      expect(sup.some((x) => x.channel_id === String(liveCh.id) && x.state === "SETTLED")).toBe(
        true,
      );
      const w = (
        await internalCall(runtime.internalApp, "GET", `/internal/v1/wallets/${billingAccount.id}`)
      ).data as { spent_atoms: string; reserved_atoms: string };
      expect(BigInt(w.spent_atoms)).toBe(15n);
      expect(BigInt(w.reserved_atoms)).toBe(0n);
    } finally {
      await runtime.close();
      B.server.stop(true);
    }
  });

  test("post-dispatch: upstream dies after SSE first byte -> no failover, hold UNKNOWN, no charge, zero duplicate upstream calls", async () => {
    const A = fakeUpstream("A", { truncate: true });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const res = await ctx.chat(true);
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).not.toContain("[DONE]");
      expect(text).toContain("event: error");
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(0);
      const requestId = res.headers.get("x-request-id")!;
      for (let i = 0; i < 100; i++) {
        const rows =
          await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
        if (rows[0]?.state === "UNKNOWN") break;
        await Bun.sleep(10);
      }
      expect(runtime.lifecycle.size).toBe(0);
      expect(await ctx.charges()).toMatchObject({ n: 0 });
      expect(await ctx.holdsByState()).toEqual({ UNKNOWN: 1 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("LEAST_LATENCY: after at most two samples the slow channel is deprioritised and all remaining traffic goes to the fast peer", async () => {
    const slow = fakeUpstream("slow", { headerDelayMs: 400 });
    const fast = fakeUpstream("fast");
    const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [slow, fast], {
        strategy: "LEAST_LATENCY",
        priority: [0, 0],
      });
      const picked = { slow: 0, fast: 0 };
      for (let i = 0; i < 12; i++) {
        const r = await ctx.chat();
        expect(r.status).toBe(200);
        picked[r.headers.get("x-gateway-channel")?.startsWith("fast-") ? "fast" : "slow"]++;
        await r.text();
      }
      await ctx.settle();
      expect(picked.slow).toBeLessThanOrEqual(2);
      expect(picked.fast).toBeGreaterThanOrEqual(10);
      const h = await ctx.health();
      const fastRow = h[String(ctx.channels[1]!.id)]!;
      const slowRow = h[String(ctx.channels[0]!.id)];
      expect(fastRow.state).toBe("CLOSED");
      expect(fastRow.ewma_latency_ms).toBeLessThan(slowRow?.ewma_latency_ms ?? Infinity);
      expect(await ctx.charges()).toMatchObject({ n: 12, u: 12 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      slow.server.stop(true);
      fast.server.stop(true);
    }
  });

  test("429 cooldown: a rate-limited channel is skipped for the retry-after window and receives no further calls until the cooldown passes", async () => {
    const A = fakeUpstream("A", { status: 429 });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const first = await ctx.chat();
      expect(first.status).toBe(200);
      expect(first.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
      await first.text();
      for (let i = 0; i < 10; i++) {
        const r = await ctx.chat();
        expect(r.status).toBe(200);
        expect(r.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
        await r.text();
      }
      await ctx.settle();
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(11);
      const h = await ctx.health();
      const aRow = h[String(ctx.channels[0]!.id)]!;
      expect(aRow.cooldown_until_ms!).toBeGreaterThan(performance.now());
      expect(await ctx.charges()).toMatchObject({ n: 11, u: 11 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });
}
