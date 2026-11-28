import { Hono } from "hono";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { recordAuditLog, requireBillingAccountAccess } from "../auth.ts";
import { apiKeyRecords, type ConsoleDrizzle, type SpendLimitWindow } from "../db/schema.ts";
import {
  cidrSchema,
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  requireClientAuth,
  sanitizeApiKeyForClient,
  toBillingAccountScope,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const createApiKeySchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    role: z.enum(["ADMIN", "BILLING", "INVOKER", "VIEWER"]).default("INVOKER"),
    allowed_cidrs: z.array(cidrSchema).max(100).default([]),
    models: z.array(z.string().min(1).max(150)).max(1000).default([]),
    operations: z.array(operationEnum).max(32).default([]),
    expires_at: z.iso.datetime({ offset: true }).nullable().optional(),
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
    expires_at: z.iso.datetime({ offset: true }).nullable().optional(),
    spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    spend_limit_window: z.enum(["LIFETIME", "DAILY", "MONTHLY"]).optional(),
  })
  .strict();

function requireDeveloperKeyRole(role: typeof apiKeyRecords.$inferSelect.role): void {
  if (role !== "INVOKER" && role !== "VIEWER") {
    throw new GatewayError(
      403,
      "forbidden",
      "DEVELOPER role can only manage INVOKER or VIEWER API keys",
    );
  }
}

function revokeKeyFields() {
  return {
    enabled: false,
    allowedCidrs: [],
    revokedAt: sql`COALESCE(${apiKeyRecords.revokedAt}, now())`,
    desiredRevision: sql`${apiKeyRecords.desiredRevision} + 1`,
    syncStatus: "PENDING" as const,
    updatedAt: sql`now()`,
  };
}

async function abandonKey(orm: ConsoleDrizzle, keyId: string): Promise<void> {
  await orm.update(apiKeyRecords).set(revokeKeyFields()).where(eq(apiKeyRecords.id, keyId));
}

export function createApiKeyRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireClientAuth(deps);
  app.use("/billing-accounts/:billingAccountId/api-keys", auth);
  app.use("/billing-accounts/:billingAccountId/api-keys/*", auth);

  app.get("/billing-accounts/:billingAccountId/api-keys", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    await requireBillingAccountAccess(deps.orm, user.id, billingAccountId);
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const items = await deps.orm
      .select()
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.billingAccountId, billingAccountId))
      .orderBy(desc(apiKeyRecords.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items: items.map(sanitizeApiKeyForClient), limit, offset });
  });

  app.post("/billing-accounts/:billingAccountId/api-keys", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const body = await parseJsonBody(c, createApiKeySchema);

    const keyId = crypto.randomUUID();
    const { billingAccount, effectiveSpendLimit } = await deps.orm.transaction(async (tx) => {
      const { billingAccount, role } = await requireBillingAccountAccess(
        tx,
        user.id,
        billingAccountId,
        ["OWNER", "ADMIN", "DEVELOPER"],
        true,
      );
      let effectiveSpendLimit = body.spend_limit_atoms ?? null;
      if (role === "DEVELOPER") {
        requireDeveloperKeyRole(body.role);
        if (billingAccount.developerKeySpendLimitAtoms !== null) {
          if (body.spend_limit_atoms === undefined) {
            effectiveSpendLimit = billingAccount.developerKeySpendLimitAtoms;
          } else if (
            body.spend_limit_atoms === null ||
            BigInt(body.spend_limit_atoms) > BigInt(billingAccount.developerKeySpendLimitAtoms)
          ) {
            throw new GatewayError(
              403,
              "forbidden",
              "API key spend limit exceeds the billing account policy for DEVELOPER role",
            );
          }
        }
      }

      if (role === "DEVELOPER" && billingAccount.developerMaxActiveKeys !== null) {
        await tx.execute(
          sql`SELECT pg_advisory_xact_lock(88421201, hashtext(${`${billingAccountId}:${user.id}`}))`,
        );
        const [cnt] = await tx
          .select({ total: sql<number>`count(*)::int` })
          .from(apiKeyRecords)
          .where(
            and(
              eq(apiKeyRecords.billingAccountId, billingAccountId),
              eq(apiKeyRecords.createdBy, user.id),
              isNull(apiKeyRecords.revokedAt),
            ),
          );
        if (Number(cnt?.total ?? 0) >= billingAccount.developerMaxActiveKeys) {
          throw new GatewayError(
            403,
            "key_limit_exceeded",
            "Active API key limit reached for DEVELOPER role in this billing account",
          );
        }
      }
      await tx.insert(apiKeyRecords).values({
        id: keyId,
        billingAccountId,
        createdBy: user.id,
        name: body.name,
        keyPrefix: "pending",
        role: body.role,
        allowedCidrs: body.allowed_cidrs,
        models: body.models,
        operations: body.operations,
        spendLimitAtoms: effectiveSpendLimit,
        spendLimitWindow: body.spend_limit_window as SpendLimitWindow,
        enabled: true,
        expiresAt: body.expires_at ? new Date(body.expires_at) : null,
        syncStatus: "PENDING",
      });
      return { billingAccount, effectiveSpendLimit };
    });

    try {
      const synced = await deps.sync.admission.syncApiKey(keyId);
      if (!synced.key)
        throw new GatewayError(
          502,
          "gateway_invalid_response",
          "Gateway did not return the new key",
        );
      await recordAuditLog(deps.orm, {
        actor: user,
        orgId: billingAccount.orgId,
        action: "api_key.create",
        targetType: "api_key",
        targetId: keyId,
        metadata: {
          billing_account_id: billingAccountId,
          name: body.name,
          allowed_cidrs: body.allowed_cidrs,
          spend_limit_atoms: effectiveSpendLimit,
          spend_limit_window: body.spend_limit_window,
        },
      });
      return c.json({ ...sanitizeApiKeyForClient(synced.record), key: synced.key }, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(apiKeyRecords).where(eq(apiKeyRecords.id, keyId));
      } else {
        await abandonKey(deps.orm, keyId);
      }
      throw err;
    }
  });

  app.patch("/billing-accounts/:billingAccountId/api-keys/:keyId", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
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

    const { billingAccount, existing, updated } = await deps.orm.transaction(async (tx) => {
      const { billingAccount, role } = await requireBillingAccountAccess(
        tx,
        user.id,
        billingAccountId,
        ["OWNER", "ADMIN", "FINANCE", "DEVELOPER"],
        true,
      );
      const [existing] = await tx
        .select()
        .from(apiKeyRecords)
        .where(
          and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.billingAccountId, billingAccountId)),
        )
        .for("update")
        .limit(1);
      if (!existing) throw new GatewayError(404, "not_found", "API key not found");
      if (existing.revokedAt) {
        throw new GatewayError(409, "key_revoked", "Revoked API keys cannot be modified");
      }

      if (role === "FINANCE" && touchesNonSpend) {
        throw new GatewayError(
          403,
          "forbidden",
          "FINANCE role can only manage API key spend limits",
        );
      }
      if (role === "DEVELOPER") {
        requireDeveloperKeyRole(existing.role);
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

      const [updated] = await tx
        .update(apiKeyRecords)
        .set({
          ...(body.name !== undefined ? { name: body.name } : {}),
          ...(body.allowed_cidrs !== undefined ? { allowedCidrs: body.allowed_cidrs } : {}),
          ...(body.models !== undefined ? { models: body.models } : {}),
          ...(body.operations !== undefined ? { operations: body.operations } : {}),
          ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
          ...(body.expires_at !== undefined
            ? { expiresAt: body.expires_at ? new Date(body.expires_at) : null }
            : {}),
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
        .where(
          and(
            eq(apiKeyRecords.id, keyId),
            eq(apiKeyRecords.desiredRevision, existing.desiredRevision),
            isNull(apiKeyRecords.revokedAt),
          ),
        )
        .returning({ revision: apiKeyRecords.desiredRevision });
      if (!updated) throw new GatewayError(409, "key_changed", "API key changed during update");
      return { billingAccount, existing, updated };
    });

    try {
      const synced = await deps.sync.admission.syncApiKey(keyId);
      await recordAuditLog(deps.orm, {
        actor: user,
        orgId: billingAccount.orgId,
        action: "api_key.update",
        targetType: "api_key",
        targetId: keyId,
      });
      return c.json(sanitizeApiKeyForClient(synced.record));
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm
          .update(apiKeyRecords)
          .set({
            name: existing.name,
            allowedCidrs: existing.allowedCidrs,
            models: existing.models,
            operations: existing.operations,
            enabled: existing.enabled,
            expiresAt: existing.expiresAt,
            spendLimitAtoms: existing.spendLimitAtoms,
            spendLimitWindow: existing.spendLimitWindow,
            desiredRevision: existing.desiredRevision,
            syncStatus: existing.syncStatus,
            lastSyncError: existing.lastSyncError,
            updatedAt: sql`now()`,
          })
          .where(
            and(
              eq(apiKeyRecords.id, keyId),
              eq(apiKeyRecords.desiredRevision, updated.revision),
              isNull(apiKeyRecords.revokedAt),
            ),
          );
      }
      throw err;
    }
  });

  app.get("/billing-accounts/:billingAccountId/api-keys/:keyId/spend", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    await requireBillingAccountAccess(deps.orm, user.id, billingAccountId);
    const [existing] = await deps.orm
      .select({ id: apiKeyRecords.id })
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.billingAccountId, billingAccountId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    const spend = await deps.gateway.getApiKeySpend(keyId);
    return c.json(toBillingAccountScope(spend, billingAccountId));
  });

  app.post("/billing-accounts/:billingAccountId/api-keys/:keyId/reset-spend", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { billingAccount } = await requireBillingAccountAccess(
      deps.orm,
      user.id,
      billingAccountId,
      ["OWNER", "ADMIN", "FINANCE"],
    );
    const [existing] = await deps.orm
      .select({ id: apiKeyRecords.id })
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.billingAccountId, billingAccountId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    const spend = await deps.gateway.resetApiKeySpend(keyId, `reset by ${user.email}`);
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: billingAccount.orgId,
      action: "api_key.reset_spend",
      targetType: "api_key",
      targetId: keyId,
    });
    return c.json(toBillingAccountScope(spend, billingAccountId));
  });

  app.post("/billing-accounts/:billingAccountId/api-keys/:keyId/rotate", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const newKeyId = crypto.randomUUID();
    const { billingAccount, previousRevision } = await deps.orm.transaction(async (tx) => {
      const { billingAccount, role } = await requireBillingAccountAccess(
        tx,
        user.id,
        billingAccountId,
        ["OWNER", "ADMIN", "DEVELOPER"],
        true,
      );

      const [existing] = await tx
        .select()
        .from(apiKeyRecords)
        .where(
          and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.billingAccountId, billingAccountId)),
        )
        .for("update");
      if (!existing) throw new GatewayError(404, "not_found", "API key not found");
      if (existing.revokedAt) {
        throw new GatewayError(409, "key_revoked", "Revoked API key cannot be rotated");
      }
      if (role === "DEVELOPER") {
        requireDeveloperKeyRole(existing.role);
        if (existing.createdBy !== user.id) {
          throw new GatewayError(403, "forbidden", "DEVELOPER role can only rotate own API keys");
        }
        if (
          existing.spendLimitAtoms !== null ||
          billingAccount.developerKeySpendLimitAtoms !== null
        ) {
          throw new GatewayError(
            403,
            "forbidden",
            "Only OWNER or ADMIN can rotate a spend-limited API key",
          );
        }
      }
      await tx.insert(apiKeyRecords).values({
        id: newKeyId,
        billingAccountId,
        createdBy: user.id,
        rotatedFromKeyId: keyId,
        name: existing.name,
        keyPrefix: "pending",
        role: existing.role,
        allowedCidrs: existing.allowedCidrs,
        models: existing.models,
        operations: existing.operations,
        spendLimitAtoms: existing.spendLimitAtoms,
        spendLimitWindow: existing.spendLimitWindow,
        enabled: existing.enabled,
        expiresAt: existing.expiresAt,
        syncStatus: "PENDING",
      });
      return { billingAccount, previousRevision: existing.desiredRevision };
    });

    let syncedNew: Awaited<ReturnType<typeof deps.sync.admission.syncApiKey>>;
    try {
      syncedNew = await deps.sync.admission.syncApiKey(newKeyId);
      if (!syncedNew.key)
        throw new GatewayError(
          502,
          "gateway_invalid_response",
          "Gateway did not return the new key",
        );
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(apiKeyRecords).where(eq(apiKeyRecords.id, newKeyId));
      } else {
        await abandonKey(deps.orm, newKeyId);
      }
      throw err;
    }

    const [revoked] = await deps.orm
      .update(apiKeyRecords)
      .set(revokeKeyFields())
      .where(
        and(
          eq(apiKeyRecords.id, keyId),
          isNull(apiKeyRecords.revokedAt),
          eq(apiKeyRecords.desiredRevision, previousRevision),
        ),
      )
      .returning({ id: apiKeyRecords.id });
    if (!revoked) {
      await abandonKey(deps.orm, newKeyId);
      throw new GatewayError(409, "key_changed", "API key changed during rotation");
    }

    let oldKeyRevokePending = false;
    try {
      await deps.sync.admission.syncApiKey(keyId);
    } catch {
      oldKeyRevokePending = true;
    }

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: billingAccount.orgId,
      action: "api_key.rotate",
      targetType: "api_key",
      targetId: newKeyId,
      metadata: { rotated_from: keyId, old_key_revoke_pending: oldKeyRevokePending },
    });
    return c.json(
      {
        ...sanitizeApiKeyForClient(syncedNew.record),
        key: syncedNew.key,
        rotated_from: keyId,
        old_key_revoke_pending: oldKeyRevokePending,
      },
      201,
    );
  });

  app.delete("/billing-accounts/:billingAccountId/api-keys/:keyId", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const { billingAccount, role } = await requireBillingAccountAccess(
      deps.orm,
      user.id,
      billingAccountId,
      ["OWNER", "ADMIN", "DEVELOPER"],
    );
    const [existing] = await deps.orm
      .select()
      .from(apiKeyRecords)
      .where(and(eq(apiKeyRecords.id, keyId), eq(apiKeyRecords.billingAccountId, billingAccountId)))
      .limit(1);
    if (!existing) throw new GatewayError(404, "not_found", "API key not found");
    if (role === "DEVELOPER") {
      if (existing.createdBy !== user.id) {
        throw new GatewayError(403, "forbidden", "DEVELOPER role can only revoke own API keys");
      }
      if (existing.spendLimitAtoms !== null) {
        throw new GatewayError(
          403,
          "forbidden",
          "Only OWNER or ADMIN can revoke a spend-limited API key; disable it instead",
        );
      }
    }

    await abandonKey(deps.orm, keyId);
    await deps.sync.admission.syncApiKey(keyId);

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: billingAccount.orgId,
      action: "api_key.revoke",
      targetType: "api_key",
      targetId: keyId,
    });
    return c.json({ ok: true, id: keyId });
  });

  return app;
}
