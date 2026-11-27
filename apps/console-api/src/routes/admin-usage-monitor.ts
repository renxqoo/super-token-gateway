import { GatewayError } from "@gateway/contracts";
import { Hono } from "hono";
import { z } from "zod";
import { recordAuditLog } from "../auth.ts";
import { createConsoleDrizzle } from "../db/schema.ts";
import { parseUsageMonitorPolicy } from "../usage-monitor/policy.ts";
import { lockUsageMonitorWrites, suspendUsageTrends } from "../usage-monitor/store.ts";
import { parseJsonBody, parseUuid, type ConsoleDeps, type ConsoleEnv } from "./shared.ts";

const disposition = z.enum(["OPEN", "ACKNOWLEDGED", "RESOLVED"]);
const listingQuery = z
  .strictObject({
    limit: z.coerce.number().int().min(1).max(100).default(50),
    disposition: disposition.optional(),
    channel_id: z.uuid().optional(),
    before_window: z.iso.datetime({ offset: true }).optional(),
    before_id: z.uuid().optional(),
  })
  .refine((query) => (query.before_window === undefined) === (query.before_id === undefined));

const dispositionChange = z.strictObject({
  revision: z.int().min(1).max(2_147_483_647),
  disposition: z.enum(["ACKNOWLEDGED", "RESOLVED"]),
  reason: z.string().trim().min(1).max(512),
});

const replayRequest = z
  .strictObject({
    policy_version: z.string().regex(/^[0-9a-f]{64}$/),
    from: z.iso.datetime({ offset: true }),
    to: z.iso.datetime({ offset: true }),
  })
  .refine(
    ({ from, to }) =>
      Date.parse(to) > Date.parse(from) && Date.parse(to) - Date.parse(from) <= 86_400_000,
  );

type FindingRow = {
  id: string;
  window_start: Date;
  revision: number;
  disposition: z.infer<typeof disposition>;
};

const replayLimit = 288;

export function createAdminUsageMonitorRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

  app.get("/usage-monitor/findings", async (c) => {
    const valid = listingQuery.safeParse(c.req.query());
    if (!valid.success) throw new GatewayError(400, "validation_error", "Invalid findings query");
    const { limit, before_window, before_id, disposition: state, channel_id } = valid.data;
    const rows = await deps.db<FindingRow[]>`
      SELECT id, policy_version, window_start, rule_key, channel_id, model, operation,
        currency, evidence, matched, disposition, revision, created_at, observed_at
      FROM public.usage_monitor_findings
      WHERE (${state ?? null}::text IS NULL OR disposition = ${state ?? null})
        AND (${channel_id ?? null}::uuid IS NULL OR channel_id = ${channel_id ?? null}::uuid)
        AND (${before_window ?? null}::timestamptz IS NULL
          OR (window_start, id) < (${before_window ?? null}::timestamptz, ${before_id ?? null}::uuid))
      ORDER BY window_start DESC, id DESC LIMIT ${limit + 1}
    `;
    const data = rows.slice(0, limit);
    const last = rows.length > limit ? data.at(-1) : undefined;
    return c.json(
      {
        data,
        next_cursor: last
          ? { before_window: last.window_start.toISOString(), before_id: last.id }
          : null,
      },
      200,
      { "cache-control": "no-store" },
    );
  });

  app.patch("/usage-monitor/findings/:id", async (c) => {
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, dispositionChange);
    const actor = c.get("user");
    const result = await deps.db.begin(async (tx) => {
      const [row] = await tx<FindingRow[]>`
        SELECT id, disposition, revision FROM public.usage_monitor_findings
        WHERE id = ${id} FOR UPDATE
      `;
      if (!row) throw new GatewayError(404, "not_found", "Usage finding not found");
      const previous = body.disposition === "ACKNOWLEDGED" ? "OPEN" : "ACKNOWLEDGED";
      if (row.revision !== body.revision || row.disposition !== previous) {
        throw new GatewayError(
          409,
          "conflict",
          "Usage finding changed or disposition is not allowed",
        );
      }
      const [updated] = await tx<FindingRow[]>`
        UPDATE public.usage_monitor_findings
        SET disposition = ${body.disposition}, revision = revision + 1
        WHERE id = ${id} RETURNING id, disposition, revision
      `;
      await recordAuditLog(createConsoleDrizzle(tx), {
        actor,
        action: "admin.usage_monitor.disposition",
        targetType: "usage_monitor_finding",
        targetId: id,
        metadata: {
          from: previous,
          to: body.disposition,
          reviewed_revision: body.revision,
          reason: body.reason,
        },
      });
      return updated!;
    });
    return c.json(result);
  });

  app.post("/usage-monitor/replay", async (c) => {
    if (!deps.config.usageMonitor) {
      throw new GatewayError(409, "conflict", "Usage monitoring is disabled");
    }
    const body = await parseJsonBody(c, replayRequest);
    const actor = c.get("user");
    const scheduled = await deps.db.begin(async (tx) => {
      await lockUsageMonitorWrites(tx);
      const windows = await tx<{ window_start: string; policy: unknown }[]>`
        SELECT window_start::text, policy FROM public.usage_monitor_windows
        WHERE policy_version = ${body.policy_version} AND window_start >= ${body.from}::timestamptz
          AND window_start < ${body.to}::timestamptz AND finalized_at IS NOT NULL
          AND (NOT replay_requested OR (lease_token IS NOT NULL AND NOT lease_superseded))
        ORDER BY window_start LIMIT ${replayLimit + 1} FOR UPDATE
      `;
      if (windows.length > replayLimit)
        throw new GatewayError(422, "replay_limit", "Reduce replay range");
      const first = windows[0];
      if (!first) return 0;
      const starts = windows.map((row) => row.window_start);
      await tx`
        UPDATE public.usage_monitor_windows
        SET replay_requested = true, trend_refresh = false, next_attempt_at = statement_timestamp(),
          lease_superseded = lease_token IS NOT NULL
        WHERE policy_version = ${body.policy_version}
          AND window_start = ANY(${starts}::timestamptz[])
      `;
      await suspendUsageTrends(
        tx,
        body.policy_version,
        starts,
        parseUsageMonitorPolicy(first.policy),
      );
      await recordAuditLog(createConsoleDrizzle(tx), {
        actor,
        action: "admin.usage_monitor.replay",
        targetType: "usage_monitor_policy",
        targetId: body.policy_version,
        metadata: { from: body.from, to: body.to, scheduled: windows.length },
      });
      return windows.length;
    });
    return c.json({ scheduled }, 202);
  });

  return app;
}
