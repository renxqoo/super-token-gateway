import { Hono } from "hono";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { isValidCidr, recordAuditLog, requireProjectAccess } from "../auth.ts";
import { apiKeyRecords, type SpendLimitWindow } from "../db/schema.ts";
import {
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseUuid,
  requireAuth,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const cidrSchema = z
  .string()
  .min(2)
  .max(64)
  .refine((val) => isValidCidr(val), { message: "Invalid IP or CIDR notation" });

const createApiKeySchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    role: z.enum(["ADMIN", "BILLING", "INVOKER", "VIEWER"]).default("INVOKER"),
    allowed_cidrs: z.array(cidrSchema).max(100).default([]),
    models: z.array(z.string().min(1).max(150)).max(1000).default([]),
    operations: z.array(operationEnum).max(32).default([]),
    expires_at: z.iso.datetime().nullable().optional(),
    spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    spend_limit_window: z.enum(["LIFETIME", "DAILY", "MONTHLY"]).default("LIFETIME"),
  })
  .strict();

const updateApiKeySchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    allowed_cidrs: z.array(cidrSchema).max(100).optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).optional(),
    operations: z.array(operationEnum).max(32).optional(),
    enabled: z.boolean().optional(),
    expires_at: z.iso.datetime().nullable().optional(),
    spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    spend_limit_window: z.enum(["LIFETIME", "DAILY", "MONTHLY"]).optional(),
  })
  .strict();

export function createApiKeyRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireAuth(deps);
  app.use("/projects/:projectId/api-keys", auth);
  app.use("/projects/:projectId/api-keys/*", auth);

  app.get("/projects/:projectId/api-keys", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    await requireProjectAccess(deps.orm, user.id, projectId);
    const items = await deps.orm
      .select()
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.projectId, projectId))
      .orderBy(desc(apiKeyRecords.createdAt));
    return c.json({ items });
  });

  app.post("/projects/:projectId/api-keys", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "DEVELOPER",
    ]);
    const body = await parseJsonBody(c, createApiKeySchema);
    const keyId = crypto.randomUUID();
    await deps.orm.insert(apiKeyRecords).values({
      id: keyId,
      projectId,
      createdBy: user.id,
      name: body.name,
      keyPrefix: "pending",
      role: body.role,
      allowedCidrs: body.allowed_cidrs,
      models: body.models,
      operations: body.operations,
      spendLimitAtoms: body.spend_limit_atoms ?? null,
      spendLimitWindow: body.spend_limit_window as SpendLimitWindow,
      enabled: true,
      expiresAt: body.expires_at ?? null,
      syncStatus: "PENDING",
    });

    try {
      const synced = await deps.sync.syncApiKey(keyId);
      await recordAuditLog(deps.orm, {
        actor: user,
        orgId: project.orgId,
        action: "api_key.create",
        targetType: "api_key",
        targetId: keyId,
        metadata: {
          project_id: projectId,
          name: body.name,
          spend_limit_atoms: body.spend_limit_atoms ?? null,
          spend_limit_window: body.spend_limit_window,
        },
      });
      return c.json({ ...synced.record, key: synced.key }, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(apiKeyRecords).where(eq(apiKeyRecords.id, keyId));
      }
      throw err;
    }
  });

  app.patch("/projects/:projectId/api-keys/:keyId", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { project, role } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "FINANCE",
      "DEVELOPER",
    ]);
    const [existing] = await deps.orm
      .select()
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.projectId, projectId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    if (existing.revokedAt) {
      throw new GatewayError(409, "key_revoked", "Revoked API keys cannot be modified");
    }

    const body = await parseJsonBody(c, updateApiKeySchema);
    const touchesSpend =
      body.spend_limit_atoms !== undefined || body.spend_limit_window !== undefined;
    const touchesNonSpend =
      body.name !== undefined ||
      body.allowed_cidrs !== undefined ||
      body.models !== undefined ||
      body.operations !== undefined ||
      body.enabled !== undefined ||
      body.expires_at !== undefined;

    if (role === "FINANCE" && touchesNonSpend) {
      throw new GatewayError(403, "forbidden", "FINANCE role can only manage API key spend limits");
    }
    if (role === "DEVELOPER") {
      if (existing.createdBy !== user.id) {
        throw new GatewayError(403, "forbidden", "DEVELOPER role can only modify own API keys");
      }
      if (touchesSpend) {
        throw new GatewayError(
          403,
          "forbidden",
          "Only OWNER, ADMIN, or FINANCE can modify API key spend limits",
        );
      }
    }

    await deps.orm
      .update(apiKeyRecords)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.allowed_cidrs !== undefined ? { allowedCidrs: body.allowed_cidrs } : {}),
        ...(body.models !== undefined ? { models: body.models } : {}),
        ...(body.operations !== undefined ? { operations: body.operations } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        ...(body.expires_at !== undefined ? { expiresAt: body.expires_at } : {}),
        ...(body.spend_limit_atoms !== undefined
          ? { spendLimitAtoms: body.spend_limit_atoms }
          : {}),
        ...(body.spend_limit_window !== undefined
          ? { spendLimitWindow: body.spend_limit_window as SpendLimitWindow }
          : {}),
        desiredRevision: sql`${apiKeyRecords.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(apiKeyRecords.id, keyId));

    const synced = await deps.sync.syncApiKey(keyId);
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "api_key.update",
      targetType: "api_key",
      targetId: keyId,
    });
    return c.json(synced.record);
  });

  app.get("/projects/:projectId/api-keys/:keyId/spend", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    await requireProjectAccess(deps.orm, user.id, projectId);
    const [existing] = await deps.orm
      .select({ id: apiKeyRecords.id })
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.projectId, projectId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    const spend = await deps.gateway.getApiKeySpend(keyId);
    return c.json(spend);
  });

  app.post("/projects/:projectId/api-keys/:keyId/reset-spend", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "FINANCE",
    ]);
    const [existing] = await deps.orm
      .select({ id: apiKeyRecords.id })
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.projectId, projectId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    const spend = await deps.gateway.resetApiKeySpend(keyId, `reset by ${user.email}`);
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "api_key.reset_spend",
      targetType: "api_key",
      targetId: keyId,
    });
    return c.json(spend);
  });

  app.post("/projects/:projectId/api-keys/:keyId/rotate", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { project, role } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "DEVELOPER",
    ]);

    const newKeyId = crypto.randomUUID();
    await deps.orm.transaction(async (tx) => {
      const [existing] = await tx
        .select()
        .from(apiKeyRecords)
        .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.projectId, projectId)))
        .for("update");
      if (!existing) throw new GatewayError(404, "not_found", "API key not found");
      if (existing.revokedAt) {
        throw new GatewayError(409, "key_revoked", "Revoked API key cannot be rotated");
      }
      if (role === "DEVELOPER") {
        if (existing.createdBy !== user.id) {
          throw new GatewayError(403, "forbidden", "DEVELOPER role can only rotate own API keys");
        }
        if (existing.spendLimitAtoms !== null) {
          throw new GatewayError(
            403,
            "forbidden",
            "Only OWNER or ADMIN can rotate a spend-limited API key",
          );
        }
      }
      await tx
        .update(apiKeyRecords)
        .set({
          enabled: false,
          revokedAt: sql`now()`,
          desiredRevision: sql`${apiKeyRecords.desiredRevision} + 1`,
          syncStatus: "PENDING",
          updatedAt: sql`now()`,
        })
        .where(and(eq(apiKeyRecords.id, keyId), isNull(apiKeyRecords.revokedAt)));
      await tx.insert(apiKeyRecords).values({
        id: newKeyId,
        projectId,
        createdBy: user.id,
        name: existing.name,
        keyPrefix: "pending",
        role: existing.role,
        allowedCidrs: existing.allowedCidrs,
        models: existing.models,
        operations: existing.operations,
        spendLimitAtoms: existing.spendLimitAtoms,
        spendLimitWindow: existing.spendLimitWindow,
        enabled: true,
        expiresAt: existing.expiresAt,
        syncStatus: "PENDING",
      });
    });

    let syncedNew: Awaited<ReturnType<typeof deps.sync.syncApiKey>>;
    try {
      syncedNew = await deps.sync.syncApiKey(newKeyId);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(apiKeyRecords).where(eq(apiKeyRecords.id, newKeyId));
      }
      throw err;
    }

    let oldKeyRevokePending = false;
    try {
      await deps.sync.syncApiKey(keyId);
    } catch {
      oldKeyRevokePending = true;
    }

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "api_key.rotate",
      targetType: "api_key",
      targetId: newKeyId,
      metadata: { rotated_from: keyId, old_key_revoke_pending: oldKeyRevokePending },
    });
    return c.json(
      {
        ...syncedNew.record,
        key: syncedNew.key,
        rotated_from: keyId,
        old_key_revoke_pending: oldKeyRevokePending,
      },
      201,
    );
  });

  app.delete("/projects/:projectId/api-keys/:keyId", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { project, role } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
      "DEVELOPER",
    ]);
    const [existing] = await deps.orm
      .select()
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.projectId, projectId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    if (role === "DEVELOPER" && existing.createdBy !== user.id) {
      throw new GatewayError(403, "forbidden", "DEVELOPER role can only revoke own API keys");
    }

    await deps.orm
      .update(apiKeyRecords)
      .set({
        enabled: false,
        allowedCidrs: [],
        revokedAt: sql`COALESCE(${apiKeyRecords.revokedAt}, now())`,
        desiredRevision: sql`${apiKeyRecords.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(apiKeyRecords.id, keyId));
    await deps.sync.syncApiKey(keyId);

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "api_key.revoke",
      targetType: "api_key",
      targetId: keyId,
    });
    return c.json({ ok: true, id: keyId });
  });

  return app;
}
