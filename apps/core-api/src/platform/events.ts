import type { TransactionSQL } from "bun";
import type { Hono } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { createEventStore, recordEventConsumer } from "@gateway/storage";
import type { PlatformPermission } from "./authentication.ts";

const consumerSchema = z
  .object({ name: z.string().min(1).max(100), replay: z.boolean().default(true) })
  .strict();
const claimSchema = z.object({ limit: z.number().int().min(1).max(500).default(100) }).strict();
const ackSchema = z
  .object({ lease_token: z.uuid(), event_ids: z.array(z.uuid()).min(1).max(500) })
  .strict();

export function registerEvents(
  app: Hono,
  tools: {
    run<T>(
      header: string | undefined,
      permission: PlatformPermission,
      action: (tx: TransactionSQL, actor: string) => Promise<T>,
    ): Promise<T>;
    audit(tx: TransactionSQL, actor: string, action: string, resource: string): Promise<void>;
    body(request: Request): Promise<unknown>;
  },
) {
  const { run, audit, body } = tools;
  const id = (value: string | undefined) => z.uuid().parse(value);
  const consumer = async (tx: TransactionSQL, value: string | undefined) => {
    const consumerId = id(value);
    if (!(await tx`SELECT 1 FROM event_consumers WHERE id=${consumerId}`).length)
      throw new GatewayError(404, "not_found", "Event consumer not found");
    return consumerId;
  };
  app.get("/usage", async (c) => {
    const project = c.req.query("project_id");
    const projectId = project === undefined ? null : id(project);
    return c.json({
      data: await run(c.req.header("authorization"), "events:read", (tx) =>
        createEventStore(tx).usage({ projectId, limit: 100, before: c.req.query("before") }),
      ),
    });
  });
  app.post("/consumers", async (c) => {
    const input = consumerSchema.parse(await body(c.req.raw));
    return c.json(
      {
        id: await run(c.req.header("authorization"), "events:consume", async (tx, actor) => {
          const consumerId = await recordEventConsumer(tx, input.name, input.replay);
          await audit(tx, actor, "event_consumer.create", consumerId);
          return consumerId;
        }),
      },
      201,
    );
  });
  app.post("/consumers/:id/claim", async (c) => {
    const input = claimSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "events:consume", async (tx) =>
        createEventStore(tx).claim(await consumer(tx, c.req.param("id")), input.limit),
      ),
    );
  });
  app.post("/consumers/:id/ack", async (c) => {
    const input = ackSchema.parse(await body(c.req.raw));
    return c.json({
      acknowledged: await run(c.req.header("authorization"), "events:consume", async (tx) =>
        createEventStore(tx).ack(
          await consumer(tx, c.req.param("id")),
          input.lease_token,
          input.event_ids,
        ),
      ),
    });
  });
}
