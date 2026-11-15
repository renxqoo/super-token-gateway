import type { TransactionSQL } from "bun";
import type { Hono } from "hono";
import type { PlatformPermission } from "./authentication.ts";

export type SettlementQueue = {
  stats(): Promise<Record<string, number>>;
  retry(jobId: string): Promise<void>;
};

export function registerOperations(
  app: Hono,
  tools: {
    run<T>(
      header: string | undefined,
      permission: PlatformPermission,
      action: (tx: TransactionSQL, actor: string) => Promise<T>,
    ): Promise<T>;
    audit(tx: TransactionSQL, actor: string, action: string, resource: string): Promise<void>;
    queue: SettlementQueue | undefined;
  },
) {
  const { run, audit, queue } = tools;
  app.get("/metrics", async (c) =>
    c.json(
      await run(c.req.header("authorization"), "operations:read", async (tx) => {
        const [gauges] = await tx`SELECT
          (SELECT count(*) FROM gateway_requests WHERE state IN ('ADMITTED','RUNNING','UNKNOWN'))::integer AS open_requests,
          (SELECT count(*) FROM billing_holds WHERE state='HELD')::integer AS open_holds,
          (SELECT count(*) FROM execution_permits WHERE state='RESERVED' AND granted_at IS NULL AND expires_at<=now())::integer AS expired_permits,
          (SELECT count(*) FROM resources x JOIN attempts a ON a.id=x.attempt_id WHERE x.binding_id IS NOT NULL AND a.state='PARTIAL')::integer AS resources_awaiting_final,
          (SELECT count(*) FROM usage_conflicts)::integer AS usage_conflicts`;
        const consumers =
          await tx`SELECT c.id,c.name,count(d.event_id) FILTER (WHERE d.state<>'ACKED')::integer AS pending
          FROM event_consumers c LEFT JOIN event_deliveries d ON d.consumer_id=c.id GROUP BY c.id,c.name ORDER BY c.name,c.id LIMIT 1000`;
        return {
          ...gauges,
          event_consumers: consumers,
          settlement_queue: queue ? await queue.stats() : null,
        };
      }),
    ),
  );
  if (!queue) return;
  app.get("/queue", async (c) =>
    c.json(await run(c.req.header("authorization"), "operations:read", () => queue.stats())),
  );
  app.post("/queue/jobs/:id/retry", async (c) => {
    const job = c.req.param("id");
    await run(c.req.header("authorization"), "operations:manage", async (tx, actor) => {
      await queue.retry(job);
      await audit(tx, actor, "settlement_job.retry", job);
    });
    return c.json({ scheduled: true });
  });
}
