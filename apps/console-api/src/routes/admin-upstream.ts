import { z } from "zod";
import {
  currencySchema,
  positiveAtomsSchema,
  positiveDecimalSchema,
  signedAtomsSchema,
  signedDecimalSchema,
  parseLimitOffset,
  parseJsonBody,
  parseUuid,
  resolveAtomsAmount,
  safeFormatAtoms,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";
import { upstreamAccounts, channels } from "../db/schema.ts";
import { desc, eq, sql } from "drizzle-orm";
import { recordAuditLog, maskSecret } from "../auth.ts";
import { GatewayError } from "@gateway/contracts";
import { AMOUNT_DECIMALS } from "@gateway/kernel";
import { Hono } from "hono";

const createUpstreamAccountSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).default("ACTIVE"),
  })
  .strict();

const updateUpstreamAccountSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
    expected_state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
  })
  .strict();

const rechargeSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema.optional(),
    amount_decimal: positiveDecimalSchema.optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

const adjustSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema.optional(),
    delta_decimal: signedDecimalSchema.optional(),
    allow_negative: z.boolean().optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const createChannelSchema = z
  .object({
    account_id: z.uuid(),
    name: z.string().trim().min(1).max(120),
    kind: z.string().regex(/^[a-z0-9][a-z0-9_-]{1,63}$/),
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000),
    api_key: z.string().max(16384).default(""),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    enabled: z.boolean().default(true),
    routing_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_config: z.record(z.string(), z.unknown()).default({}),
  })
  .strict();

const updateChannelSchema = z
  .object({
    account_id: z.uuid().optional(),
    name: z.string().trim().min(1).max(120).optional(),
    kind: z
      .string()
      .regex(/^[a-z0-9][a-z0-9_-]{1,63}$/)
      .optional(),
    base_url: z.url().max(1000).optional(),
    api_key: z.string().max(16384).optional(),
    rpm: z.number().int().min(1).max(10_000_000).optional(),
    concurrency: z.number().int().min(1).max(100_000).optional(),
    enabled: z.boolean().optional(),
    routing_policy: z.record(z.string(), z.unknown()).optional(),
    adapter_config: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

export function registerAdminUpstreamRoutes({
  app,
  deps,
}: {
  app: Hono<ConsoleEnv>;
  deps: ConsoleDeps;
}) {
  app.get("/upstream-accounts", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const localItems = await deps.orm
      .select()
      .from(upstreamAccounts)
      .orderBy(desc(upstreamAccounts.createdAt))
      .limit(limit)
      .offset(offset);
    const gwRes = await deps.gateway.listUpstreamAccounts().catch(() => null);
    const gwMap = new Map((gwRes?.items ?? []).map((a) => [a.id, a]));
    const items = localItems.map(
      ({ pendingState: _pendingState, expectedState: _expectedState, ...item }) => {
        const gw = gwMap.get(item.id);
        return gw ? { ...item, state: gw.state, gateway_state: gw.state } : item;
      },
    );
    return c.json({ items, limit, offset, gateway_state_available: gwRes !== null });
  });

  app.post("/upstream-accounts", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createUpstreamAccountSchema);
    const id = crypto.randomUUID();
    await deps.orm.insert(upstreamAccounts).values({
      id,
      name: body.name,
      state: body.state,
      syncStatus: "PENDING",
    });
    try {
      const {
        pendingState: _pendingState,
        expectedState: _expectedState,
        ...synced
      } = await deps.sync.catalog.syncUpstreamAccount(id);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.upstream_account.create",
        targetType: "upstream_account",
        targetId: id,
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(upstreamAccounts).where(eq(upstreamAccounts.id, id));
      }
      throw err;
    }
  });

  app.patch("/upstream-accounts/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateUpstreamAccountSchema);
    const {
      pendingState: _pendingState,
      expectedState: _expectedState,
      ...account
    } = await deps.sync.catalog.updateUpstreamAccount(id, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.update",
      targetType: "upstream_account",
      targetId: id,
      metadata: {
        ...(body.state ? { state: body.state } : {}),
        ...(body.expected_state ? { expected_state: body.expected_state } : {}),
      },
    });
    return c.json(account);
  });

  app.post("/upstream-accounts/:id/recharges", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, rechargeSupplierSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    await deps.sync.catalog.syncUpstreamAccount(id);
    const res = await deps.gateway.rechargeSupplier(id, {
      currency: body.currency,
      external_ref: body.external_ref,
      amount_atoms: amountAtoms,
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.recharge",
      targetType: "upstream_account",
      targetId: id,
      metadata: {
        currency: body.currency,
        amount_atoms: amountAtoms,
        external_ref: body.external_ref,
      },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      total_balance_decimal: safeFormatAtoms(res.total_balance_atoms),
    });
  });

  app.post("/upstream-accounts/:id/adjustments", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, adjustSupplierSchema);
    const deltaAtoms = resolveAtomsAmount(body.delta_atoms, body.delta_decimal, {
      field: "delta",
      allowNegative: true,
      allowZero: false,
    });
    await deps.sync.catalog.syncUpstreamAccount(id);
    const res = await deps.gateway.adjustSupplierBalance(id, {
      currency: body.currency,
      external_ref: body.external_ref,
      delta_atoms: deltaAtoms,
      ...(body.allow_negative !== undefined ? { allow_negative: body.allow_negative } : {}),
      reason: body.reason,
    });
    await deps.orm
      .update(upstreamAccounts)
      .set({ state: res.account_state, updatedAt: sql`now()` })
      .where(eq(upstreamAccounts.id, id));
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.upstream_account.adjustment",
      targetType: "upstream_account",
      targetId: id,
      metadata: {
        currency: body.currency,
        delta_atoms: deltaAtoms,
        allow_negative: Boolean(body.allow_negative),
        account_state: res.account_state,
        external_ref: body.external_ref,
        reason: body.reason,
      },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      delta_decimal: safeFormatAtoms(res.delta_atoms),
      total_balance_decimal: safeFormatAtoms(res.total_balance_atoms),
      reserved_decimal: safeFormatAtoms(res.reserved_atoms),
    });
  });

  app.get("/upstream-accounts/:id/balance", async (c) => {
    const id = parseUuid(c.req.param("id"), "id");
    const currency = c.req.query("currency") ?? "USD";
    const bal = await deps.gateway.getSupplierBalance(id, currency);
    return c.json({
      ...bal,
      currency,
      scale: AMOUNT_DECIMALS,
      balance_decimal: safeFormatAtoms(bal.balance_atoms),
      reserved_decimal: safeFormatAtoms(bal.reserved_atoms),
      spent_decimal: safeFormatAtoms(bal.spent_atoms),
    });
  });

  app.get("/channels", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 100, 200);
    const localItems = await deps.orm
      .select()
      .from(channels)
      .orderBy(desc(channels.createdAt))
      .limit(limit)
      .offset(offset);
    const gwRes = await deps.gateway.listChannels().catch(() => null);
    const gwMap = new Map((gwRes?.items ?? []).map((ch) => [ch.id, ch]));
    const items = localItems.map((item) => {
      const gw = gwMap.get(item.id);
      return gw ? { ...item, providerPluginId: gw.provider_plugin_id } : item;
    });
    return c.json({ items, limit, offset });
  });

  app.get("/channels/health", async (c) => {
    return c.json(await deps.gateway.getChannelHealth());
  });

  app.post("/channels", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, createChannelSchema);
    const id = crypto.randomUUID();
    const keyHint = maskSecret(body.api_key);
    await deps.orm.insert(channels).values({
      id,
      accountId: body.account_id,
      name: body.name,
      kind: body.kind,
      providerPluginId: body.provider_plugin_id ?? null,
      baseUrl: body.base_url,
      secretSyncPending: true,
      rpm: body.rpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      routingPolicy: body.routing_policy,
      adapterConfig: body.adapter_config,
      syncStatus: "PENDING",
    });
    try {
      const synced = await deps.sync.catalog.syncChannel(id, body.api_key);
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.channel.create",
        targetType: "channel",
        targetId: id,
        metadata: { name: body.name, kind: body.kind, key_hint: keyHint },
      });
      return c.json(synced, 201);
    } catch (err) {
      if (err instanceof GatewayError && err.status >= 400 && err.status < 500) {
        await deps.orm.delete(channels).where(eq(channels.id, id));
      }
      throw err;
    }
  });

  app.patch("/channels/:id", async (c) => {
    const actor = c.get("user");
    const id = parseUuid(c.req.param("id"), "id");
    const body = await parseJsonBody(c, updateChannelSchema);
    const synced = await deps.sync.catalog.updateChannel(
      id,
      {
        ...(body.account_id !== undefined ? { accountId: body.account_id } : {}),
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.kind !== undefined ? { kind: body.kind } : {}),
        ...(body.base_url !== undefined ? { baseUrl: body.base_url } : {}),
        ...(body.rpm !== undefined ? { rpm: body.rpm } : {}),
        ...(body.concurrency !== undefined ? { concurrency: body.concurrency } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
        ...(body.routing_policy !== undefined ? { routingPolicy: body.routing_policy } : {}),
        ...(body.adapter_config !== undefined ? { adapterConfig: body.adapter_config } : {}),
      },
      body.api_key,
    );
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.channel.update",
      targetType: "channel",
      targetId: id,
      metadata: { rotated_secret: body.api_key !== undefined },
    });
    return c.json(synced);
  });
}
