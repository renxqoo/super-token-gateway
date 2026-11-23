import { Hono } from "hono";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { recordAuditLog } from "../auth.ts";
import {
  channels,
  consoleAuditLogs,
  organizations,
  projects,
  sessions,
  users,
} from "../db/schema.ts";
import {
  nonNegativeAtomsSchema,
  parseJsonBody,
  parseLimitOffset,
  parseOptionalJsonObject,
  parseUuid,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const jsonObjectSchema = z.record(z.string(), z.unknown());

const updateUserSchema = z
  .object({
    status: z.enum(["ACTIVE", "SUSPENDED"]).optional(),
    is_platform_admin: z.boolean().optional(),
  })
  .strict();

const updateOrgStatusSchema = z
  .object({
    status: z.enum(["ACTIVE", "SUSPENDED"]),
  })
  .strict();

const adminUpdateProjectSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    rpm: z.number().int().min(1).max(10_000_000).optional(),
    tpm: z.number().int().min(1).max(9_007_199_254_740_991).optional(),
    concurrency: z.number().int().min(1).max(1_000_000).optional(),
    developer_key_spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    developer_max_active_keys: z.number().int().min(1).max(1000).nullable().optional(),
    enabled: z.boolean().optional(),
  })
  .strict();

async function syncReleasedChannelBindings(
  deps: ConsoleDeps,
  releaseResult: Record<string, unknown>,
): Promise<void> {
  const changes = releaseResult.changes;
  if (!Array.isArray(changes)) return;
  for (const change of changes) {
    if (!change || typeof change !== "object") continue;
    const after = (change as { after?: Record<string, unknown> }).after;
    if (!after || typeof after !== "object") continue;
    if (typeof after.channel_id === "string" && typeof after.provider_plugin_id === "string") {
      await deps.orm
        .update(channels)
        .set({ providerPluginId: after.provider_plugin_id, updatedAt: sql`now()` })
        .where(eq(channels.id, after.channel_id));
    }
  }
}

export function createAdminOpsRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

  app.get("/plugins", async (c) => {
    return c.json(await deps.gateway.listPlugins(c.req.query("capability")));
  });

  app.get("/plugins/builtins", async (c) => {
    return c.json(await deps.gateway.listBuiltinPlugins(c.req.query("capability")));
  });

  app.post("/plugins/builtins/:name/install", async (c) => {
    const actor = c.get("user");
    const name = c.req.param("name");
    const body = await parseOptionalJsonObject(c);
    const res = await deps.gateway.installBuiltinPlugin(name, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.install_builtin",
      targetType: "plugin",
      targetId: name,
    });
    return c.json(res);
  });

  app.put("/plugins", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.upsertPlugin(body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.upsert",
      targetType: "plugin",
      targetId: String(res.id ?? "unknown"),
    });
    return c.json(res);
  });

  app.post("/plugin-releases/validate", async (c) => {
    const body = await parseJsonBody(c, jsonObjectSchema);
    return c.json(await deps.gateway.validatePluginRelease(body));
  });

  app.post("/plugin-releases", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.publishPluginRelease(body);
    await syncReleasedChannelBindings(deps, res);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin_release.publish",
      targetType: "plugin_release",
      targetId: String(body.id ?? "unknown"),
    });
    return c.json(res);
  });

  app.post("/plugin-releases/:releaseId/rollback", async (c) => {
    const actor = c.get("user");
    const releaseId = parseUuid(c.req.param("releaseId"), "releaseId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.rollbackPluginRelease(releaseId, body);
    await syncReleasedChannelBindings(deps, res);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin_release.rollback",
      targetType: "plugin_release",
      targetId: releaseId,
    });
    return c.json(res);
  });

  app.post("/plugins/:pluginId/revoke", async (c) => {
    const actor = c.get("user");
    const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
    const body = await parseJsonBody(
      c,
      z.object({ reason: z.string().trim().min(1).max(1024) }).strict(),
    );
    const res = await deps.gateway.revokePlugin(pluginId, body.reason);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.plugin.revoke",
      targetType: "plugin",
      targetId: pluginId,
      metadata: { reason: body.reason },
    });
    return c.json(res);
  });

  app.post("/providers/preview", async (c) => {
    const body = await parseJsonBody(c, jsonObjectSchema);
    return c.json(await deps.gateway.previewProvider(body));
  });

  app.get("/audit-logs", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const items = await deps.orm
      .select()
      .from(consoleAuditLogs)
      .orderBy(desc(consoleAuditLogs.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
  });

  app.get("/users", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const items = await deps.orm
      .select({
        id: users.id,
        email: users.email,
        display_name: users.displayName,
        is_platform_admin: users.isPlatformAdmin,
        status: users.status,
        created_at: users.createdAt,
      })
      .from(users)
      .orderBy(desc(users.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
  });

  app.patch("/users/:userId", async (c) => {
    const actor = c.get("user");
    const userId = parseUuid(c.req.param("userId"), "userId");
    const body = await parseJsonBody(c, updateUserSchema);
    const updated = await deps.orm.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(88421001)`);
      const [target] = await tx.select().from(users).where(eq(users.id, userId)).for("update");
      if (!target) throw new GatewayError(404, "not_found", "User not found");
      const removingAdminPower =
        target.isPlatformAdmin &&
        target.status === "ACTIVE" &&
        (body.is_platform_admin === false || body.status === "SUSPENDED");
      if (removingAdminPower) {
        const activeAdmins = await tx
          .select({ id: users.id })
          .from(users)
          .where(and(eq(users.isPlatformAdmin, true), eq(users.status, "ACTIVE")));
        if (activeAdmins.length <= 1) {
          throw new GatewayError(
            400,
            "last_platform_admin",
            "Platform must retain at least one active administrator",
          );
        }
      }
      const [row] = await tx
        .update(users)
        .set({
          ...(body.status !== undefined ? { status: body.status } : {}),
          ...(body.is_platform_admin !== undefined
            ? { isPlatformAdmin: body.is_platform_admin }
            : {}),
          updatedAt: sql`now()`,
        })
        .where(eq(users.id, userId))
        .returning({
          id: users.id,
          email: users.email,
          display_name: users.displayName,
          is_platform_admin: users.isPlatformAdmin,
          status: users.status,
        });
      if (
        body.status === "SUSPENDED" ||
        body.is_platform_admin === false ||
        body.is_platform_admin === true
      ) {
        await tx
          .update(sessions)
          .set({ revokedAt: sql`now()` })
          .where(and(eq(sessions.userId, userId), isNull(sessions.revokedAt)));
      }
      return row!;
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.user.update",
      targetType: "user",
      targetId: userId,
      metadata: body,
    });
    return c.json(updated);
  });

  app.patch("/orgs/:orgId", async (c) => {
    const actor = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const body = await parseJsonBody(c, updateOrgStatusSchema);
    const [updated] = await deps.orm
      .update(organizations)
      .set({ status: body.status, updatedAt: sql`now()` })
      .where(eq(organizations.id, orgId))
      .returning();
    if (!updated) throw new GatewayError(404, "not_found", "Organization not found");
    await recordAuditLog(deps.orm, {
      actor,
      orgId,
      action: "admin.org.update",
      targetType: "organization",
      targetId: orgId,
      metadata: body,
    });
    return c.json(updated);
  });

  app.get("/projects", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const items = await deps.orm
      .select()
      .from(projects)
      .orderBy(desc(projects.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items, limit, offset });
  });

  app.patch("/projects/:projectId", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, adminUpdateProjectSchema);
    const [existing] = await deps.orm
      .select()
      .from(projects)
      .where(eq(projects.id, projectId))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "Project not found");
    const touchesGateway =
      body.name !== undefined ||
      body.rpm !== undefined ||
      body.tpm !== undefined ||
      body.concurrency !== undefined ||
      body.enabled !== undefined;
    const [updated] = await deps.orm
      .update(projects)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.rpm !== undefined ? { rpm: body.rpm } : {}),
        ...(body.tpm !== undefined ? { tpm: body.tpm } : {}),
        ...(body.concurrency !== undefined ? { concurrency: body.concurrency } : {}),
        ...(body.developer_key_spend_limit_atoms !== undefined
          ? { developerKeySpendLimitAtoms: body.developer_key_spend_limit_atoms }
          : {}),
        ...(body.developer_max_active_keys !== undefined
          ? { developerMaxActiveKeys: body.developer_max_active_keys }
          : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        ...(touchesGateway
          ? { desiredRevision: sql`${projects.desiredRevision} + 1`, syncStatus: "PENDING" }
          : {}),
        updatedAt: sql`now()`,
      })
      .where(eq(projects.id, projectId))
      .returning();
    const synced =
      touchesGateway && updated!.gatewaySynced
        ? await deps.sync.ensureProjectSynced(projectId)
        : updated!;
    await recordAuditLog(deps.orm, {
      actor,
      orgId: existing.orgId,
      action: "admin.project.update",
      targetType: "project",
      targetId: projectId,
      metadata: body,
    });
    return c.json(synced);
  });

  return app;
}
