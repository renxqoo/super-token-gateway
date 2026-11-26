import { expect, test } from "bun:test";
import { createWorkerRunner } from "@gateway/worker";
import { db } from "./gateway-fixture.ts";

export function registerGatewayWorkerCases() {
  test("Worker advisory lease leader election, timed-out hold sweep, and Outbox webhook push delivery", async () => {
    const deliveredBatches: { consumer: string; events: { seq: string; kind: string }[] }[] = [];
    const bizWebhook = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        if (req.headers.get("authorization") !== "Bearer biz-secret") {
          return new Response("unauthorized", { status: 401 });
        }
        const body = (await req.json()) as {
          consumer: string;
          events: { seq: string; kind: string }[];
        };
        deliveredBatches.push(body);
        return Response.json({ ok: true });
      },
    });

    const workerA = createWorkerRunner({
      db,
      leaseKey: 81442601,
      businessWebhookUrl: `http://127.0.0.1:${bizWebhook.port}/events`,
      businessWebhookToken: "biz-secret",
      outboxConsumerName: "biz_sync_test",
    });
    const workerB = createWorkerRunner({
      db,
      leaseKey: 81442601,
      businessWebhookUrl: `http://127.0.0.1:${bizWebhook.port}/events`,
      businessWebhookToken: "biz-secret",
      outboxConsumerName: "biz_sync_test",
    });

    try {
      const tickA = await workerA.runOnce(true);
      expect(tickA.is_leader).toBe(true);
      expect(tickA.prepared_months.length).toBe(2);
      expect(tickA.delivered_outbox_events).toBeGreaterThan(0);
      expect(deliveredBatches.length).toBe(1);

      const tickB = await workerB.runOnce(false);
      expect(tickB.is_leader).toBe(false);
      expect(tickB.delivered_outbox_events).toBe(0);

      await workerA.stop();
      const tickBAfterFailover = await workerB.runOnce(false);
      expect(tickBAfterFailover.is_leader).toBe(true);
      expect(tickBAfterFailover.delivered_outbox_events).toBe(0);
    } finally {
      await workerA.stop();
      await workerB.stop();
      await bizWebhook.stop(true);
    }
  });
}
