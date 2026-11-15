import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { UsageConsumer, UsageProducer, type QueueChannel } from "@gateway/runtime";
import { verifier } from "@gateway/runtime/crypto";
import { createCoreManagementApplication } from "../../apps/core-api/src/platform/management.ts";
import { syntheticPepper } from "../support/catalog.ts";
import { withFinancial } from "../support/financial-execution.ts";
import { clearQueueNamespace, eventually } from "../support/gateway-process.ts";

async function credential(db: SQL, permissions: string[]) {
  const administrator = crypto.randomUUID(),
    token = `synthetic-operations-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${administrator},'Synthetic operations administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${administrator},${verifier(token, syntheticPepper)},'synthetic')`;
  return { administrator, token };
}

const channel: QueueChannel<{ n: number }> = {
  name: "synthetic-settlement",
  scope: "synthetic",
  parse: (item) => item as { n: number },
  encode: (items) => ({ items }),
  decode: (payload) => (payload as { items: { n: number }[] }).items,
};

test("a failed settlement job is visible to operators and is re-run once through an audited retry without new generation", async () => {
  const namespace = `operations_${crypto.randomUUID().replaceAll("-", "")}`;
  const options = {
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 2000,
    channel,
  };
  const queue = new UsageProducer(options);
  let runs = 0;
  const consumer = new UsageConsumer({
    ...options,
    process: async () => {
      runs++;
      if (runs === 1) {
        const error = new Error("synthetic settlement failure");
        error.name = "UnrecoverableError";
        throw error;
      }
    },
  });
  try {
    await consumer.ready();
    const failed = new Promise<string>((resolve) =>
      consumer.worker.once("failed", (job) => resolve(String(job?.id))),
    );
    await queue.publish({ n: 1 });
    const job = await failed;
    await withFinancial(
      async (f, db) => {
        const reader = await credential(db, ["operations:read"]),
          operator = await credential(db, ["operations:manage"]),
          unrelated = await credential(db, ["events:read"]);
        const stats = await f.request("/queue", undefined, reader.token);
        expect(stats.status).toBe(200);
        expect(stats.value).toMatchObject({ failed: 1 });
        const metrics = await f.request("/metrics", undefined, reader.token);
        expect(metrics.status).toBe(200);
        expect(metrics.value).toMatchObject({
          open_requests: 0,
          open_holds: 0,
          expired_permits: 0,
          resources_awaiting_final: 0,
          usage_conflicts: 0,
          settlement_queue: { failed: 1 },
        });
        expect((await f.request("/metrics", undefined, unrelated.token)).status).toBe(403);
        expect((await f.request("/queue", undefined, unrelated.token)).status).toBe(403);

        expect((await f.request(`/queue/jobs/${job}/retry`, {}, reader.token)).status).toBe(403);
        expect((await queue.stats()).failed).toBe(1);
        expect(runs).toBe(1);

        const retried = await f.request(`/queue/jobs/${job}/retry`, {}, operator.token);
        expect(retried).toEqual({ status: 200, value: { scheduled: true } });
        await eventually(async () => runs === 2, "retried settlement job");
        await eventually(async () => (await queue.stats()).failed === 0, "failed job drained");
        expect(await db`SELECT action,actor FROM audit_events WHERE resource_id=${job}`).toEqual([
          { action: "settlement_job.retry", actor: `technical-admin:${operator.administrator}` },
        ]);
        expect((await f.request(`/queue/jobs/${job}/retry`, {}, operator.token)).status).toBe(404);
        expect((await f.request(`/queue/jobs/not-a-job/retry`, {}, operator.token)).status).toBe(
          400,
        );
        expect(runs).toBe(2);
      },
      {
        managementFactory: (input) => createCoreManagementApplication({ ...input, queue }),
      },
    );
  } finally {
    queue.close();
    await consumer.close();
    await clearQueueNamespace(namespace);
  }
}, 120000);

test(
  "an event consumer backlog is reported in operations metrics",
  () =>
    withFinancial(async (f, db) => {
      const reader = await credential(db, ["operations:read"]);
      const created = await f.request("/consumers", { name: "synthetic-metrics", replay: true });
      expect(created.status).toBe(201);
      const metrics = await f.request("/metrics", undefined, reader.token);
      expect(metrics.value.settlement_queue).toBeNull();
      expect(metrics.value.event_consumers).toEqual([
        { id: created.value.id, name: "synthetic-metrics", pending: 0 },
      ]);
      expect(
        (
          await f.management.request("http://management.test/queue", {
            headers: { authorization: `Bearer ${reader.token}` },
          })
        ).status,
      ).toBe(404);
    }),
  60000,
);
