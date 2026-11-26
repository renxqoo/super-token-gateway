import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import {
  makeConfig,
  db,
  internalCall,
  seed,
  chatRequest,
  wallet,
  waitFor,
  realServer,
} from "./adversarial-billing-fixture.ts";

export function registerCancellationCases() {
  test("symptom: a freshly issued Key is rejected as expired when Gateway runs outside UTC", async () => {
    const originalTimeZone = process.env.TZ;
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      process.env.TZ = "Asia/Singapore";
      await runtime.start();
      const billingAccount = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "key-expiration-timezone",
        },
      );
      expect(billingAccount.status).toBe(200);
      const now = Date.now();
      const issue = (expiresAt: string) =>
        internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
          billing_account_id: billingAccount.data.id,
          expires_at: expiresAt,
        });
      const listModels = (key: unknown) =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/models", {
            headers: { authorization: `Bearer ${String(key)}` },
          }),
        );

      const currentExpiry = new Date(now + 15 * 60_000).toISOString();
      const current = await issue(currentExpiry);
      expect(current.status).toBe(200);
      expect((await listModels(current.data.key)).status).toBe(200);
      expect(current.data.expires_at).toBe(currentExpiry);

      process.env.TZ = "America/Los_Angeles";
      const pastExpiry = new Date(now - 15 * 60_000).toISOString();
      const expired = await issue(pastExpiry);
      expect(expired.status).toBe(200);
      expect(expired.data.expires_at).toBe(pastExpiry);
      const rejected = await listModels(expired.data.key);
      expect(rejected.status).toBe(401);
      expect(await rejected.json()).toMatchObject({ error: { code: "key_expired" } });
    } finally {
      if (originalTimeZone === undefined) delete process.env.TZ;
      else process.env.TZ = originalTimeZone;
      await runtime.close();
    }
  });

  test("symptom: fixed input ceilings reject short text on a Key that can pay for the request", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: mock.app.fetch,
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, channel, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = "adv-dynamic-reservation";
      const customer = await call("POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          cached_input_tokens: { numerator: "1", denominator: "2" },
        },
        ceilings: { input_tokens: "100000", cached_input_tokens: "100000" },
      });
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: model,
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      const createKey = (name: string, limit: string) =>
        call("PUT", "/internal/v1/api-keys", {
          billing_account_id: billingAccountId,
          name,
          spend_limit_atoms: limit,
        });
      const budget = await createKey("adv-small-budget", "1000");
      const tinyBudget = await createKey("adv-tiny-budget", "1");
      const request = (key: unknown, content: unknown) =>
        runtime.publicApp.fetch(
          chatRequest(String(key), model, {
            stream: false,
            messages: [{ role: "user", content }],
          }),
        );
      const short = await request(budget.key, "Hello");
      const output = await short.json();
      expect(short.status, JSON.stringify(output)).toBe(200);
      const shortId = short.headers.get("x-request-id")!;
      const [charge] = await db`
      SELECT h.held_atoms::text, c.customer_atoms::text
      FROM public.billing_holds h JOIN public.billing_charges c USING (request_id)
      WHERE h.request_id = ${shortId}
    `;
      expect(charge?.customer_atoms).toBe("9");
      expect(BigInt(charge?.held_atoms)).toBeLessThan(1000n);

      for (const [key, content] of [
        [budget.key, "a".repeat(2000)],
        [tinyBudget.key, "Hello"],
        [budget.key, [{ type: "image_url", image_url: { url: "https://example.invalid/image" } }]],
      ]) {
        const denied = await request(key, content);
        expect(denied.status).toBe(402);
        expect(await denied.json()).toMatchObject({ error: { code: "key_spend_limit_exceeded" } });
      }
    } finally {
      await runtime.close(1000);
      mockServer.stop(true);
    }
  });

  test("symptom: client cancels SSE mid-stream after consuming output -> hold is UNKNOWN, nothing spent, nothing refunded, funds fully reserved", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: mock.app.fetch,
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = `adv-cancel-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-slow",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      mock.scenarios.set("adv-slow", { chunkDelayMs: 40, repeat: 20 });

      const before = await wallet(runtime, billingAccountId);
      const ac = new AbortController();
      const res = await runtime.publicApp.fetch(chatRequest(key, model), {
        signal: ac.signal,
      } as never);
      expect(res.status).toBe(200);
      const requestId = res.headers.get("x-request-id")!;
      const reader = res.body!.getReader();
      let received = "";
      for (let i = 0; i < 5; i++) {
        const step = await reader.read();
        if (step.done) break;
        received += new TextDecoder().decode(step.value);
      }
      expect(received).toContain("Hello");
      ac.abort();
      await reader.cancel().catch(() => undefined);

      await waitFor(async () => {
        const rows =
          await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
        return rows[0]?.state === "UNKNOWN";
      });
      expect(runtime.lifecycle.size).toBe(0);

      const during = await wallet(runtime, billingAccountId);
      const holdRow = (
        await db`SELECT held_atoms::text AS h FROM public.billing_holds WHERE request_id = ${requestId}`
      )[0]!;
      const held = BigInt(holdRow.h as string);
      expect(BigInt(during.spent_atoms) - BigInt(before.spent_atoms)).toBe(0n);
      expect(BigInt(during.available_atoms)).toBe(BigInt(before.available_atoms) - held);
      expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(held);

      const audit = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.status).toBe(200);
      expect(audit.data.ok).toBe(true);
      expect(audit.data.unknown_holds).toBe(1);

      const resolve = await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${requestId}/resolve`,
        {
          action: "SETTLE",
          billing_price_id: customer.id,
          usage: { input_tokens: "10", output_tokens: "5" },
          supplier_cost_atoms: "15",
          reason: "ops-measured-actual-usage",
        },
      );
      expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
      const after = await wallet(runtime, billingAccountId);
      expect(BigInt(after.available_atoms)).toBe(BigInt(before.available_atoms) - 10n);
      expect(BigInt(after.spent_atoms) - BigInt(before.spent_atoms)).toBe(10n);
      expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
      const audit2 = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit2.data.ok).toBe(true);
      expect(audit2.data.unknown_holds).toBe(0);
      expect(audit2.data.open_holds).toBe(0);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test("symptom: RELEASE on a cancelled request refunds exactly the hold and never more", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = `adv-rel-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-slow",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      mock.scenarios.set("adv-slow", { chunkDelayMs: 150, repeat: 20 });
      const before = await wallet(runtime, billingAccountId);
      const res = await runtime.publicApp.fetch(chatRequest(key, model));
      const requestId = res.headers.get("x-request-id")!;
      const reader = res.body!.getReader();
      await reader.read();
      await reader.cancel();
      await waitFor(async () => {
        const rows =
          await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
        return rows[0]?.state === "UNKNOWN";
      });
      const resolve = await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${requestId}/resolve`,
        {
          action: "RELEASE",
          reason: "no-output-confirmed",
        },
      );
      expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
      const after = await wallet(runtime, billingAccountId);
      expect(after).toEqual(before);
      const again = await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${requestId}/resolve`,
        {
          action: "SETTLE",
          usage: { input_tokens: "10", output_tokens: "5" },
          reason: "double-resolve-attack",
        },
      );
      expect(again.data).toMatchObject({ replayed: true, state: "RELEASE" });
      const after2 = await wallet(runtime, billingAccountId);
      expect(after2).toEqual(before);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test("symptom: a cancel storm of 100 concurrent SSE requests leaves funds exactly conserved with no negative balances and no duplicates", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig({ maxInFlight: 2000 }), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = `adv-storm-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-storm",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      mock.scenarios.set("adv-storm", { streamDurationMs: 6000, repeat: 30 });
      const before = await wallet(runtime, billingAccountId);
      const publicServer = realServer(runtime);
      const N = 100;
      const results = await Promise.all(
        Array.from({ length: N }, async () => {
          const ac = new AbortController();
          const res = await fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
            method: "POST",
            headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model,
              stream: true,
              messages: [{ role: "user", content: "attack" }],
              max_tokens: 50,
            }),
            signal: ac.signal,
          }).catch(() => null);
          if (!res || res.status !== 200)
            return { status: res?.status ?? 0, id: null as string | null, firstChunkBytes: 0 };
          const id = res.headers.get("x-request-id");
          const reader = res.body!.getReader();
          const first = await reader.read();
          await Bun.sleep(5 + Math.random() * 30);
          ac.abort();
          await reader.cancel().catch(() => undefined);
          return { status: 200, id, firstChunkBytes: first.value?.byteLength ?? 0 };
        }),
      );
      publicServer.stop(true);
      expect(results.every((r) => r.status === 200 && r.firstChunkBytes > 0)).toBe(true);
      await waitFor(async () => mock.stats.active === 0);
      expect(mock.stats.cancellations).toBe(N);
      const ids = results.map((r) => r.id!);
      expect(new Set(ids).size).toBe(N);
      await waitFor(
        async () =>
          (await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'HELD'`)[0]!
            .n === 0,
      );
      expect(runtime.lifecycle.size).toBe(0);
      const holds = await db<
        { state: string; h: string }[]
      >`SELECT state, held_atoms::text AS h FROM public.billing_holds WHERE request_id = ANY(${ids})`;
      const totalHeld = holds.reduce((acc: bigint, h) => acc + BigInt(h.h), 0n);
      await waitFor(async () => {
        const w = await wallet(runtime, billingAccountId);
        return BigInt(w.reserved_atoms) - BigInt(before.reserved_atoms) === totalHeld;
      }, 8000);
      expect(holds.length).toBe(N);
      expect(holds.every((h) => h.state === "UNKNOWN")).toBe(true);
      const routeHealth = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/health/channels",
      );
      expect(routeHealth.status).toBe(200);
      const channelHealth = (
        routeHealth.data.channels as {
          channel_id: string;
          state: string;
          consecutive_failures: number;
        }[]
      ).find((health) => health.channel_id === channel.id);
      expect(channelHealth?.state ?? "CLOSED").toBe("CLOSED");
      expect(channelHealth?.consecutive_failures ?? 0).toBe(0);
      const during = await wallet(runtime, billingAccountId);
      expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(totalHeld);
      expect(BigInt(during.spent_atoms)).toBe(BigInt(before.spent_atoms));
      expect(BigInt(during.available_atoms) >= 0n).toBe(true);
      const audit = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.data.ok).toBe(true);
      expect(audit.data.unknown_holds).toBe(N);
      for (const id of ids)
        await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${id}/resolve`, {
          action: "RELEASE",
          reason: "storm-cleanup",
        });
      const after = await wallet(runtime, billingAccountId);
      expect(after).toEqual(before);
      const audit2 = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit2.data.ok).toBe(true);
      expect(audit2.data.unknown_holds).toBe(0);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });
}
