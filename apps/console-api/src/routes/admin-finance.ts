import { Hono } from "hono";
import { asc, eq, inArray, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { AMOUNT_DECIMALS } from "@gateway/kernel";
import { recordAuditLog } from "../auth.ts";
import { organizations, platformCreditDefaults, projects } from "../db/schema.ts";
import {
  currencySchema,
  enrichAdminChargeDecimals,
  enrichFinanceSummaryDecimals,
  enrichFundingOperationDecimals,
  enrichLedgerEntryDecimals,
  enrichWalletDecimals,
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  positiveAtomsSchema,
  positiveDecimalSchema,
  resolveAtomsAmount,
  safeFormatAtoms,
  signedAtomsSchema,
  signedDecimalSchema,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const walletPolicyEnum = z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]);

const topupSchema = z
  .object({
    currency: currencySchema.optional(),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema.optional(),
    amount_decimal: positiveDecimalSchema.optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

const grantSchema = z
  .object({
    currency: currencySchema.optional(),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema.optional(),
    amount_decimal: positiveDecimalSchema.optional(),
    priority: z.number().int().min(0).max(10_000).optional(),
    expires_at: z.iso.datetime({ offset: true }).nullable().optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).optional(),
    operations: z.array(operationEnum).max(32).optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

const walletAdjustmentSchema = z
  .object({
    currency: currencySchema.optional(),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema.optional(),
    delta_decimal: signedDecimalSchema.optional(),
    lot_kind: z.enum(["CASH", "GRANT"]).optional(),
    force_debt: z.boolean().optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const walletFreezeSchema = z
  .object({
    currency: currencySchema.optional(),
    frozen: z.boolean(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const creditPolicySchema = z
  .object({
    currency: currencySchema.optional(),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    credit_limit_decimal: positiveDecimalSchema.optional(),
    policy: walletPolicyEnum.optional(),
  })
  .strict();

const defaultCreditPolicySchema = z
  .object({
    default_credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    default_credit_limit_decimal: positiveDecimalSchema.optional(),
    default_wallet_policy: walletPolicyEnum.optional(),
  })
  .strict();

const batchCreditPolicySchema = z
  .object({
    scope: z.enum(["ALL_PROJECTS", "ORGANIZATION", "USER", "PROJECTS"]),
    org_id: z.uuid().optional(),
    user_id: z.uuid().optional(),
    project_ids: z.array(z.uuid()).min(1).max(500).optional(),
    only_default_project: z.boolean().optional(),
    credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    credit_limit_decimal: positiveDecimalSchema.optional(),
    policy: walletPolicyEnum.optional(),
    dry_run: z.boolean().default(false),
    reason: z.string().min(1).max(500),
  })
  .strict();

const refundSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema.optional(),
    amount_decimal: positiveDecimalSchema.optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const adjustmentSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema.optional(),
    delta_decimal: signedDecimalSchema.optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const jsonObjectSchema = z.record(z.string(), z.unknown());

function resolveProjectCurrency(requested: string | undefined, projectCurrency: string): string {
  const effective = requested ?? projectCurrency;
  if (effective !== projectCurrency) {
    throw new GatewayError(
      400,
      "currency_mismatch",
      `Project currency is ${projectCurrency}; received ${effective}`,
    );
  }
  return effective;
}

export function createAdminFinanceRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

  app.get("/finance/projects/:projectId/wallet", async (c) => {
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const [proj] = await deps.orm
      .select()
      .from(projects)
      .where(eq(projects.id, projectId))
      .limit(1);
    if (!proj) throw new GatewayError(404, "not_found", "Project not found");
    const includeLots = c.req.query("include_lots") !== "false";
    if (!proj.gatewaySynced) {
      return c.json(
        enrichWalletDecimals({
          project_id: proj.id,
          currency: proj.currency,
          synced: false,
          policy: "GIFT_FIRST",
          available_atoms: "0",
          reserved_atoms: "0",
          spent_atoms: "0",
          expired_atoms: "0",
          debt_atoms: "0",
          credit_limit_atoms: "0",
          credit_reserved_atoms: "0",
          credit_policy_version: "0",
          manual_frozen: false,
          evidence_disputed: false,
          freeze_reason: null,
          ...(includeLots ? { lots: [] } : {}),
        }),
      );
    }
    const wallet = await deps.gateway.getWallet(proj.gatewayProjectId, proj.currency, includeLots);
    return c.json(enrichWalletDecimals({ ...wallet, synced: true }));
  });

  app.post("/finance/projects/:projectId/topups", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, topupSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency,
      external_ref: body.external_ref,
      amount_atoms: amountAtoms,
      kind: "CASH",
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.topup",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, amount_atoms: amountAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      available_decimal: safeFormatAtoms(res.available_atoms),
    });
  });

  app.post("/finance/projects/:projectId/grants", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, grantSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency,
      external_ref: body.external_ref,
      amount_atoms: amountAtoms,
      kind: "GRANT",
      ...(body.priority !== undefined ? { priority: body.priority } : {}),
      ...(body.expires_at !== undefined
        ? { expires_at: body.expires_at ? new Date(body.expires_at).toISOString() : null }
        : {}),
      ...(body.models ? { models: body.models } : {}),
      ...(body.operations ? { operations: body.operations } : {}),
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.grant",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, amount_atoms: amountAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      available_decimal: safeFormatAtoms(res.available_atoms),
    });
  });

  app.post("/finance/projects/:projectId/adjustments", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, walletAdjustmentSchema);
    const deltaAtoms = resolveAtomsAmount(body.delta_atoms, body.delta_decimal, {
      field: "delta",
      allowNegative: true,
      allowZero: false,
    });
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.adjustWallet(proj.gatewayProjectId, {
      currency,
      external_ref: body.external_ref,
      delta_atoms: deltaAtoms,
      ...(body.lot_kind ? { lot_kind: body.lot_kind } : {}),
      ...(body.force_debt !== undefined ? { force_debt: body.force_debt } : {}),
      reason: body.reason,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.wallet_adjustment",
      targetType: "project",
      targetId: projectId,
      metadata: {
        currency,
        delta_atoms: deltaAtoms,
        force_debt: Boolean(body.force_debt),
        external_ref: body.external_ref,
        reason: body.reason,
      },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      delta_decimal: safeFormatAtoms(res.delta_atoms),
      funded_decimal: safeFormatAtoms(res.funded_atoms),
      debt_delta_decimal: safeFormatAtoms(res.debt_delta_atoms),
      available_decimal: safeFormatAtoms(res.available_atoms),
      debt_decimal: safeFormatAtoms(res.debt_atoms),
    });
  });

  app.post("/finance/projects/:projectId/freeze", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, walletFreezeSchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.setWalletFreeze(proj.gatewayProjectId, {
      currency,
      frozen: body.frozen,
      reason: body.reason,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: body.frozen ? "admin.finance.wallet_freeze" : "admin.finance.wallet_unfreeze",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, frozen: body.frozen, reason: body.reason },
    });
    return c.json(enrichWalletDecimals(res));
  });

  app.post("/finance/projects/:projectId/credit-policy", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, creditPolicySchema);
    const creditLimitAtoms = resolveAtomsAmount(
      body.credit_limit_atoms,
      body.credit_limit_decimal,
      {
        field: "credit_limit",
        allowNegative: false,
        allowZero: true,
      },
    );
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.configureCreditPolicy(proj.gatewayProjectId, {
      currency,
      expected_version: body.expected_version,
      credit_limit_atoms: creditLimitAtoms,
      ...(body.policy ? { policy: body.policy } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.credit_policy",
      targetType: "project",
      targetId: projectId,
      metadata: {
        currency,
        credit_limit_atoms: creditLimitAtoms,
        ...(body.policy ? { policy: body.policy } : {}),
      },
    });
    return c.json(enrichWalletDecimals(res));
  });

  app.get("/finance/credit-policy/default", async (c) => {
    const [row] = await deps.orm
      .select()
      .from(platformCreditDefaults)
      .where(eq(platformCreditDefaults.id, 1))
      .limit(1);
    const limitAtoms = row?.defaultCreditLimitAtoms ?? "0";
    const policy = row?.defaultWalletPolicy ?? "GIFT_FIRST";
    return c.json({
      scale: AMOUNT_DECIMALS,
      default_credit_limit_atoms: limitAtoms,
      default_credit_limit_decimal: safeFormatAtoms(limitAtoms),
      default_wallet_policy: policy,
      updated_by: row?.updatedBy ?? null,
      updated_at: row?.updatedAt ? row.updatedAt.toISOString() : null,
    });
  });

  app.put("/finance/credit-policy/default", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, defaultCreditPolicySchema);
    const [existing] = await deps.orm
      .select()
      .from(platformCreditDefaults)
      .where(eq(platformCreditDefaults.id, 1))
      .limit(1);
    const hasLimit =
      body.default_credit_limit_atoms !== undefined ||
      body.default_credit_limit_decimal !== undefined;
    const nextLimitAtoms = hasLimit
      ? resolveAtomsAmount(body.default_credit_limit_atoms, body.default_credit_limit_decimal, {
          field: "default_credit_limit",
          allowNegative: false,
          allowZero: true,
        })
      : (existing?.defaultCreditLimitAtoms ?? "0");
    const nextPolicy = body.default_wallet_policy ?? existing?.defaultWalletPolicy ?? "GIFT_FIRST";

    const [saved] = await deps.orm
      .insert(platformCreditDefaults)
      .values({
        id: 1,
        defaultCreditLimitAtoms: nextLimitAtoms,
        defaultWalletPolicy: nextPolicy,
        updatedBy: actor.id,
      })
      .onConflictDoUpdate({
        target: platformCreditDefaults.id,
        set: {
          defaultCreditLimitAtoms: nextLimitAtoms,
          defaultWalletPolicy: nextPolicy,
          updatedBy: actor.id,
          updatedAt: sql`now()`,
        },
      })
      .returning();

    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.default_credit_policy",
      targetType: "platform_credit_defaults",
      targetId: "1",
      metadata: {
        default_credit_limit_atoms: nextLimitAtoms,
        default_wallet_policy: nextPolicy,
      },
    });
    return c.json({
      scale: AMOUNT_DECIMALS,
      default_credit_limit_atoms: saved!.defaultCreditLimitAtoms,
      default_credit_limit_decimal: safeFormatAtoms(saved!.defaultCreditLimitAtoms),
      default_wallet_policy: saved!.defaultWalletPolicy,
      updated_by: saved!.updatedBy,
      updated_at: saved!.updatedAt.toISOString(),
    });
  });

  app.post("/finance/credit-policy/batch", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, batchCreditPolicySchema);
    const creditLimitAtoms = resolveAtomsAmount(
      body.credit_limit_atoms,
      body.credit_limit_decimal,
      {
        field: "credit_limit",
        allowNegative: false,
        allowZero: true,
      },
    );
    const nextLimitBig = BigInt(creditLimitAtoms);

    let targets: (typeof projects.$inferSelect)[] = [];
    if (body.scope === "ALL_PROJECTS") {
      targets = await deps.orm
        .select()
        .from(projects)
        .orderBy(asc(projects.createdAt), asc(projects.id))
        .limit(500);
    } else if (body.scope === "ORGANIZATION") {
      if (!body.org_id) {
        throw new GatewayError(
          400,
          "validation_error",
          "org_id is required for ORGANIZATION scope",
        );
      }
      targets = await deps.orm
        .select()
        .from(projects)
        .where(eq(projects.orgId, body.org_id))
        .orderBy(asc(projects.createdAt), asc(projects.id))
        .limit(500);
    } else if (body.scope === "USER") {
      if (!body.user_id) {
        throw new GatewayError(400, "validation_error", "user_id is required for USER scope");
      }
      const ownedOrgs = await deps.orm
        .select({ id: organizations.id, kind: organizations.kind })
        .from(organizations)
        .where(eq(organizations.ownerUserId, body.user_id));
      const orgIds = ownedOrgs.map((o) => o.id);
      if (orgIds.length > 0) {
        const allUserProjects = await deps.orm
          .select()
          .from(projects)
          .where(inArray(projects.orgId, orgIds))
          .orderBy(asc(projects.createdAt), asc(projects.id))
          .limit(500);
        if (body.only_default_project) {
          const personalOrg = ownedOrgs.find((o) => o.kind === "PERSONAL");
          const defaultProj =
            (personalOrg && allUserProjects.find((p) => p.orgId === personalOrg.id)) ??
            allUserProjects[0];
          targets = defaultProj ? [defaultProj] : [];
        } else {
          targets = allUserProjects;
        }
      }
    } else if (body.scope === "PROJECTS") {
      if (!body.project_ids || body.project_ids.length === 0) {
        throw new GatewayError(
          400,
          "validation_error",
          "project_ids is required for PROJECTS scope",
        );
      }
      targets = await deps.orm
        .select()
        .from(projects)
        .where(inArray(projects.id, body.project_ids))
        .orderBy(asc(projects.createdAt), asc(projects.id));
    }

    const items: Record<string, unknown>[] = [];
    let updatedCount = 0;
    let failedCount = 0;

    for (const target of targets) {
      try {
        if (body.dry_run) {
          let currentLimit = "0";
          let currentDebt = "0";
          let currentPolicy = "GIFT_FIRST";
          if (target.gatewaySynced) {
            const w = await deps.gateway
              .getWallet(target.gatewayProjectId, target.currency)
              .catch(() => null);
            if (w) {
              currentLimit = String(w.credit_limit_atoms ?? "0");
              currentDebt = String(w.debt_atoms ?? "0");
              currentPolicy = String(w.policy ?? "GIFT_FIRST");
            }
          }
          items.push({
            project_id: target.id,
            org_id: target.orgId,
            name: target.name,
            currency: target.currency,
            current_credit_limit_atoms: currentLimit,
            current_credit_limit_decimal: safeFormatAtoms(currentLimit),
            next_credit_limit_atoms: creditLimitAtoms,
            next_credit_limit_decimal: safeFormatAtoms(creditLimitAtoms),
            current_debt_atoms: currentDebt,
            current_debt_decimal: safeFormatAtoms(currentDebt),
            current_policy: currentPolicy,
            next_policy: body.policy ?? currentPolicy,
            will_freeze_due_to_debt: BigInt(currentDebt) > nextLimitBig,
          });
          continue;
        }

        const syncedProj = await deps.sync.ensureProjectSynced(target.id);
        let applied: Record<string, unknown> | null = null;
        for (let attempt = 0; attempt < 2; attempt++) {
          const w = await deps.gateway.getWallet(syncedProj.gatewayProjectId, syncedProj.currency);
          try {
            applied = await deps.gateway.configureCreditPolicy(syncedProj.gatewayProjectId, {
              currency: syncedProj.currency,
              expected_version: String(w.credit_policy_version ?? "0"),
              credit_limit_atoms: creditLimitAtoms,
              ...(body.policy ? { policy: body.policy } : {}),
            });
            break;
          } catch (err) {
            if (attempt === 0 && err instanceof GatewayError && err.status === 400) {
              continue;
            }
            throw err;
          }
        }
        updatedCount++;
        items.push({
          project_id: syncedProj.id,
          org_id: syncedProj.orgId,
          name: syncedProj.name,
          currency: syncedProj.currency,
          ok: true,
          credit_limit_atoms: String(applied?.credit_limit_atoms ?? creditLimitAtoms),
          credit_limit_decimal: safeFormatAtoms(applied?.credit_limit_atoms ?? creditLimitAtoms),
          debt_atoms: String(applied?.debt_atoms ?? "0"),
          debt_decimal: safeFormatAtoms(applied?.debt_atoms ?? "0"),
          freeze_reason: applied?.freeze_reason ?? null,
        });
      } catch (err) {
        failedCount++;
        items.push({
          project_id: target.id,
          org_id: target.orgId,
          name: target.name,
          currency: target.currency,
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    if (!body.dry_run) {
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.finance.credit_policy_batch",
        targetType: body.scope.toLowerCase(),
        targetId: body.org_id ?? body.user_id ?? "batch",
        metadata: {
          scope: body.scope,
          matched_count: targets.length,
          updated_count: updatedCount,
          failed_count: failedCount,
          credit_limit_atoms: creditLimitAtoms,
          policy: body.policy ?? null,
          reason: body.reason,
        },
      });
    }

    return c.json({
      dry_run: body.dry_run,
      scale: AMOUNT_DECIMALS,
      scope: body.scope,
      matched_count: targets.length,
      updated_count: updatedCount,
      failed_count: failedCount,
      items,
    });
  });

  app.post("/finance/charges/:chargeId/refunds", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, refundSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    const res = await deps.gateway.refundCharge(chargeId, {
      external_ref: body.external_ref,
      amount_atoms: amountAtoms,
      reason: body.reason,
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.refund",
      targetType: "charge",
      targetId: chargeId,
      metadata: { amount_atoms: amountAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      refunded_decimal: safeFormatAtoms(res.refunded_atoms),
      waived_debt_decimal: safeFormatAtoms(res.waived_debt_atoms),
      expired_decimal: safeFormatAtoms(res.expired_atoms),
    });
  });

  app.post("/finance/charges/:chargeId/adjustments", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, adjustmentSchema);
    const deltaAtoms = resolveAtomsAmount(body.delta_atoms, body.delta_decimal, {
      field: "delta",
      allowNegative: true,
      allowZero: false,
    });
    const res = await deps.gateway.adjustCharge(chargeId, {
      external_ref: body.external_ref,
      delta_atoms: deltaAtoms,
      reason: body.reason,
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.adjustment",
      targetType: "charge",
      targetId: chargeId,
      metadata: { delta_atoms: deltaAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...res,
      scale: AMOUNT_DECIMALS,
      delta_decimal: safeFormatAtoms(res.delta_atoms),
    });
  });

  app.get("/finance/charges", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const raw = (await deps.gateway.listCharges({
      project_id: c.req.query("project_id"),
      api_key_id: c.req.query("api_key_id"),
      outcome: c.req.query("outcome"),
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      scale: AMOUNT_DECIMALS,
      items: (raw.items ?? []).map(enrichAdminChargeDecimals),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/finance/transactions", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const raw = (await deps.gateway.listFundingOperations({
      side: c.req.query("side"),
      entity_id: c.req.query("entity_id"),
      project_id: c.req.query("project_id"),
      account_id: c.req.query("account_id"),
      kind: c.req.query("kind"),
      currency: c.req.query("currency"),
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      scale: AMOUNT_DECIMALS,
      items: (raw.items ?? []).map(enrichFundingOperationDecimals),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/finance/ledger-entries", async (c) => {
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const raw = (await deps.gateway.listLedgerEntries({
      side: c.req.query("side"),
      entity_id: c.req.query("entity_id"),
      request_id: c.req.query("request_id"),
      kind: c.req.query("kind"),
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      scale: AMOUNT_DECIMALS,
      items: (raw.items ?? []).map(enrichLedgerEntryDecimals),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/finance/requests/:requestId", async (c) => {
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const raw = (await deps.gateway.getRequestBilling(requestId)) as {
      hold?: Record<string, unknown> | null;
      charge?: Record<string, unknown> | null;
      entries?: Record<string, unknown>[];
    };
    return c.json({
      scale: AMOUNT_DECIMALS,
      hold: raw.hold ?? null,
      charge: raw.charge ? enrichAdminChargeDecimals(raw.charge) : null,
      entries: (raw.entries ?? []).map(enrichLedgerEntryDecimals),
    });
  });

  app.get("/finance/summary", async (c) => {
    const report = await deps.gateway.getFinanceSummary({
      from: c.req.query("from"),
      to: c.req.query("to"),
      project_id: c.req.query("project_id"),
      currency: c.req.query("currency"),
    });
    return c.json(enrichFinanceSummaryDecimals(report));
  });

  app.get("/finance/holds", async (c) => {
    return c.json(
      await deps.gateway.listHolds({
        state: c.req.query("state"),
        project_id: c.req.query("project_id"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.post("/finance/requests/:requestId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.resolveRequest(requestId, {
      ...body,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_request",
      targetType: "request",
      targetId: requestId,
    });
    return c.json(res);
  });

  app.post("/finance/requests/:requestId/attempts/:attemptId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const attemptId = parseUuid(c.req.param("attemptId"), "attemptId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.resolveSupplierAttempt(requestId, attemptId, {
      ...body,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_supplier_attempt",
      targetType: "supplier_attempt",
      targetId: `${requestId}:${attemptId}`,
    });
    return c.json(res);
  });

  app.get("/finance/async-tasks", async (c) => {
    return c.json(
      await deps.gateway.listAsyncTasks({
        project_id: c.req.query("project_id"),
        status: c.req.query("status"),
        refund_eligibility: c.req.query("refund_eligibility"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.get("/finance/conservation", async (c) => {
    const audit = await deps.gateway.getConservationAudit();
    return c.json(audit.data, audit.status as 200 | 500);
  });

  return app;
}
