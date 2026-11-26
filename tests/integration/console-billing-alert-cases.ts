import { expect, test } from "bun:test";
import { createConsoleRuntime } from "@gateway/console-api";
import {
  createTeamBillingAccount,
  h,
  adminAccess,
  customerAccess,
  eurBillingAccountId,
} from "./console-billing-fixture.ts";

export function registerAlertCases() {
  test("a temporary wallet read error keeps an ingested alert eligible for retry without another event", async () => {
    const retryAccountId = await createTeamBillingAccount("Alert Retry", "alert-retry", "EUR");
    const funded = await h.adminApi(
      "POST",
      "/admin/v1/finance/billing-accounts/" + retryAccountId + "/topups",
      adminAccess,
      { external_ref: "alert-retry-seed", amount_atoms: "1" },
    );
    expect(funded.status).toBe(200);
    const rule = await h.clientApi<{ id: string }>(
      "POST",
      `/api/v1/billing-accounts/${retryAccountId}/alerts`,
      customerAccess,
      { kind: "LOW_BALANCE", threshold_atoms: "99999999999999999999999999999999999999" },
    );
    expect(rule.status).toBe(201);
    const event = {
      event_id: crypto.randomUUID(),
      kind: "billing.settled",
      billing_account_id: retryAccountId,
      payload: { currency: "EUR" },
    };
    let walletUnavailable = false;
    const obsolete = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
      events: [
        {
          event_id: crypto.randomUUID(),
          kind: "billing.settled",
          project_id: retryAccountId,
        },
      ],
    });
    expect(obsolete.status).toBe(400);
    h.setGatewayInterceptor(async (url, init, forward) => {
      if (
        init?.method === "GET" &&
        new URL(url).pathname === `/internal/v1/wallets/${retryAccountId}`
      ) {
        walletUnavailable = true;
        return new Response("temporarily unavailable", { status: 503 });
      }
      return forward();
    });
    try {
      const push = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
        events: [event],
      });
      expect(push.data).toMatchObject({ ingested: 1 });
      expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
      expect(walletUnavailable).toBe(true);
    } finally {
      h.setGatewayInterceptor(null);
    }
    await h.conDb`UPDATE public.alert_eval_queue SET next_attempt_at = now() - interval '1 second' WHERE billing_account_id = ${retryAccountId}`;
    expect(await h.consoleRuntime.flushAlertsOnce()).toBe(1);
    expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
  });

  test("a late failed webhook attempt cannot overwrite a newer successful delivery", async () => {
    const rule = await h.clientApi<{ id: string }>(
      "POST",
      `/api/v1/billing-accounts/${eurBillingAccountId}/alerts`,
      customerAccess,
      { kind: "DAILY_SPEND", threshold_atoms: "1", webhook_url: h.webhookUrl },
    );
    expect(rule.status).toBe(201);
    const push = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
      events: [
        {
          event_id: crypto.randomUUID(),
          kind: "billing.settled",
          billing_account_id: eurBillingAccountId,
          payload: { currency: "EUR", customer_atoms: "1", model: "bill-gpt", operation: "chat" },
        },
      ],
    });
    expect(push.data).toMatchObject({ ingested: 1 });
    expect(await h.consoleRuntime.flushAlertsOnce()).toBe(1);

    const started = Promise.withResolvers<void>();
    const firstResponse = Promise.withResolvers<Response>();
    const runtime = createConsoleRuntime({
      config: h.consoleConfig,
      db: h.conDb,
      alertFetch: async () => {
        started.resolve();
        return firstResponse.promise;
      },
    });
    const first = runtime.deliverAlertWebhooksOnce();
    try {
      await Promise.race([started.promise, first]);
      await h.conDb`UPDATE public.alert_deliveries SET next_attempt_at = now() - interval '1 second' WHERE rule_id = ${rule.data.id}`;
      const second = await h.consoleRuntime.deliverAlertWebhooksOnce();
      expect(second.delivered).toBe(1);
    } finally {
      firstResponse.resolve(new Response(null, { status: 503 }));
      await first;
    }
    const [delivery] =
      await h.conDb`SELECT status, attempts, last_http_status FROM public.alert_deliveries WHERE rule_id = ${rule.data.id}`;
    expect(delivery).toEqual({ status: "DELIVERED", attempts: 2, last_http_status: 200 });
  });

  test("low-balance alerts use the current wallet instead of a delayed settlement snapshot", async () => {
    const id = await createTeamBillingAccount(
      "Wallet Snapshot Recovery",
      "wallet-snapshot-recovery",
    );
    expect(
      (
        await h.adminApi("POST", `/admin/v1/finance/billing-accounts/${id}/topups`, adminAccess, {
          external_ref: "alert-wallet-current",
          amount_atoms: "10000",
        })
      ).status,
    ).toBe(200);
    expect(
      (
        await h.clientApi("POST", `/api/v1/billing-accounts/${id}/alerts`, customerAccess, {
          kind: "LOW_BALANCE",
          threshold_atoms: "5",
        })
      ).status,
    ).toBe(201);
    const event = await h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
      events: [
        {
          event_id: crypto.randomUUID(),
          kind: "billing.settled",
          billing_account_id: id,
          payload: { currency: "USD", wallet_available_atoms: "0" },
        },
      ],
    });
    expect(event.data).toMatchObject({ ingested: 1 });
    expect(await h.consoleRuntime.flushAlertsOnce()).toBe(0);
  });

  test("Outbox push acknowledges persisted billing before any slow alert wallet read", async () => {
    const id = await createTeamBillingAccount(
      "Deferred Alert Evaluation",
      "deferred-alert-evaluation",
    );
    expect(
      (
        await h.clientApi("POST", `/api/v1/billing-accounts/${id}/alerts`, customerAccess, {
          kind: "LOW_BALANCE",
          threshold_atoms: "5",
        })
      ).status,
    ).toBe(201);
    const release = Promise.withResolvers<Response>();
    h.setGatewayInterceptor(async (url, init, forward) => {
      if (init?.method === "GET" && new URL(url).pathname === `/internal/v1/wallets/${id}`)
        return release.promise;
      return forward();
    });
    const push = h.adminApi("POST", "/internal/outbox/events", h.outboxToken, {
      events: [
        {
          event_id: crypto.randomUUID(),
          kind: "billing.settled",
          billing_account_id: id,
          payload: { currency: "USD" },
        },
      ],
    });
    try {
      const ack = await Promise.race([push, Bun.sleep(500).then(() => null)]);
      expect(ack).toMatchObject({ status: 200, data: { ingested: 1 } });
      const evaluation = h.consoleRuntime.flushAlertsOnce();
      release.resolve(Response.json({ available_atoms: "0" }));
      expect(await evaluation).toBe(1);
    } finally {
      release.resolve(Response.json({ available_atoms: "0" }));
      await push;
      h.setGatewayInterceptor(null);
    }
  });

  test("shutdown during the last available webhook attempt leaves the alert eligible for delivery", async () => {
    const rule = await h.clientApi<{ id: string }>(
      "POST",
      `/api/v1/billing-accounts/${eurBillingAccountId}/alerts`,
      customerAccess,
      { kind: "DAILY_SPEND", threshold_atoms: "1", webhook_url: h.webhookUrl },
    );
    expect(rule.status).toBe(201);
    const deliveryId = crypto.randomUUID();
    await h.conDb`
    INSERT INTO public.alert_deliveries (id, rule_id, billing_account_id, status, webhook_url, attempts, payload)
    VALUES (${deliveryId}, ${rule.data.id}, ${eurBillingAccountId}, 'PENDING', ${h.webhookUrl}, 4,
      jsonb_build_object('billing_account_id', ${eurBillingAccountId}::uuid, 'currency', 'EUR'))
  `;
    const started = Promise.withResolvers<void>();
    const runtime = createConsoleRuntime({
      config: { ...h.consoleConfig, alertEvaluationIntervalMs: 0, alertDeliveryIntervalMs: 10 },
      db: h.conDb,
      alertFetch: async (_input, init) => {
        const request = Promise.withResolvers<Response>();
        const signal = init!.signal!;
        signal.addEventListener("abort", () => request.reject(signal.reason), { once: true });
        started.resolve();
        return request.promise;
      },
    });
    try {
      await runtime.start();
      expect(
        await Promise.race([started.promise.then(() => true), Bun.sleep(1000).then(() => false)]),
      ).toBe(true);
    } finally {
      await runtime.close();
    }
    expect((await h.consoleRuntime.deliverAlertWebhooksOnce()).delivered).toBe(1);
    const [delivery] =
      await h.conDb`SELECT status, attempts FROM public.alert_deliveries WHERE id = ${deliveryId}`;
    expect(delivery).toEqual({ status: "DELIVERED", attempts: 5 });
    const sent = h.webhookDeliveries.find((item) => item.deliveryId === deliveryId);
    expect(sent?.body).toMatchObject({ billing_account_id: eurBillingAccountId, currency: "EUR" });
    expect(sent?.body).not.toHaveProperty("project_id");
  });
}
