import { Hono, type Context } from "hono";
import { eq, inArray } from "drizzle-orm";
import { z } from "zod";
import { assertKnownQueryParameters, GatewayError } from "@gateway/contracts";
import { AMOUNT_DECIMALS } from "@gateway/kernel";
import { recordAuditLog } from "../auth.ts";
import { billingAccounts } from "../db/schema.ts";
import {
  currencySchema,
  enrichAdminChargeDecimals,
  enrichFinanceSummaryDecimals,
  enrichFundingOperationDecimals,
  enrichLedgerEntryDecimals,
  enrichWalletDecimals,
  operationEnum,
  parseBillingAccountFilter,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  positiveAtomsSchema,
  positiveDecimalSchema,
  resolveAtomsAmount,
  safeFormatAtoms,
  toBillingAccountScope,
  signedAtomsSchema,
  signedDecimalSchema,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";
import { registerAdminCreditPolicyRoutes } from "./admin-credit-policy.ts";
import { creditPolicySchema } from "./admin-wallet-policy-schemas.ts";

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

function resolveBillingAccountCurrency(
  requested: string | undefined,
  billingAccountCurrency: string,
): string {
  const effective = requested ?? billingAccountCurrency;
  if (effective !== billingAccountCurrency) {
    throw new GatewayError(
      400,
      "currency_mismatch",
      `Billing account currency is ${billingAccountCurrency}; received ${effective}`,
    );
  }
  return effective;
}

export function createAdminFinanceRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

  async function gatewayBillingAccountFilter(c: Context, ...otherFilters: string[]) {
    const id = parseBillingAccountFilter(c, ...otherFilters);
    if (id === undefined) return undefined;
    const [account] = await deps.orm
      .select({ id: billingAccounts.gatewayBillingAccountId })
      .from(billingAccounts)
      .where(eq(billingAccounts.id, id))
      .limit(1);
    return account?.id ?? id;
  }

  async function billingAccountIdsFor(gatewayIds: unknown[]) {
    const ids = Array.from(
      new Set(gatewayIds.filter((id): id is string => typeof id === "string")),
    );
    if (ids.length === 0) return new Map<string, string>();
    const rows = await deps.orm
      .select({ id: billingAccounts.id, gatewayId: billingAccounts.gatewayBillingAccountId })
      .from(billingAccounts)
      .where(inArray(billingAccounts.gatewayBillingAccountId, ids));
    return new Map(rows.map((row) => [row.gatewayId, row.id]));
  }

  async function mapBillingAccounts(items: Record<string, unknown>[]) {
    const accountIds = await billingAccountIdsFor(
      items.map((item) => item.billing_account_id ?? item.billingAccountId),
    );
    return items.map((item) => {
      const id = item.billing_account_id ?? item.billingAccountId;
      return toBillingAccountScope(item, typeof id === "string" ? accountIds.get(id) : undefined);
    });
  }

  app.get("/finance/billing-accounts/:billingAccountId/wallet", async (c) => {
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const [billingAccount] = await deps.orm
      .select()
      .from(billingAccounts)
      .where(eq(billingAccounts.id, billingAccountId))
      .limit(1);
    if (!billingAccount) throw new GatewayError(404, "not_found", "Billing account not found");
    const includeLots = c.req.query("include_lots") !== "false";
    if (!billingAccount.gatewaySynced) {
      return c.json(
        enrichWalletDecimals({
          billing_account_id: billingAccount.id,
          currency: billingAccount.currency,
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
    const wallet = await deps.gateway.getWallet(
      billingAccount.gatewayBillingAccountId,
      billingAccount.currency,
      includeLots,
    );
    return c.json(
      enrichWalletDecimals({ ...toBillingAccountScope(wallet, billingAccountId), synced: true }),
    );
  });

  app.post("/finance/billing-accounts/:billingAccountId/topups", async (c) => {
    const actor = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const body = await parseJsonBody(c, topupSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    const billingAccount = await deps.sync.admission.ensureBillingAccountSynced(billingAccountId);
    const currency = resolveBillingAccountCurrency(body.currency, billingAccount.currency);
    const res = await deps.gateway.creditWallet(billingAccount.gatewayBillingAccountId, {
      currency,
      external_ref: body.external_ref,
      amount_atoms: amountAtoms,
      kind: "CASH",
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: billingAccount.orgId,
      action: "admin.finance.topup",
      targetType: "billing_account",
      targetId: billingAccountId,
      metadata: { currency, amount_atoms: amountAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...toBillingAccountScope(res, billingAccountId),
      scale: AMOUNT_DECIMALS,
      available_decimal: safeFormatAtoms(res.available_atoms),
    });
  });

  app.post("/finance/billing-accounts/:billingAccountId/grants", async (c) => {
    const actor = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const body = await parseJsonBody(c, grantSchema);
    const amountAtoms = resolveAtomsAmount(body.amount_atoms, body.amount_decimal, {
      field: "amount",
      allowNegative: false,
      allowZero: false,
    });
    const billingAccount = await deps.sync.admission.ensureBillingAccountSynced(billingAccountId);
    const currency = resolveBillingAccountCurrency(body.currency, billingAccount.currency);
    const res = await deps.gateway.creditWallet(billingAccount.gatewayBillingAccountId, {
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
      orgId: billingAccount.orgId,
      action: "admin.finance.grant",
      targetType: "billing_account",
      targetId: billingAccountId,
      metadata: { currency, amount_atoms: amountAtoms, external_ref: body.external_ref },
    });
    return c.json({
      ...toBillingAccountScope(res, billingAccountId),
      scale: AMOUNT_DECIMALS,
      available_decimal: safeFormatAtoms(res.available_atoms),
    });
  });

  app.post("/finance/billing-accounts/:billingAccountId/adjustments", async (c) => {
    const actor = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const body = await parseJsonBody(c, walletAdjustmentSchema);
    const deltaAtoms = resolveAtomsAmount(body.delta_atoms, body.delta_decimal, {
      field: "delta",
      allowNegative: true,
      allowZero: false,
    });
    const billingAccount = await deps.sync.admission.ensureBillingAccountSynced(billingAccountId);
    const currency = resolveBillingAccountCurrency(body.currency, billingAccount.currency);
    const res = await deps.gateway.adjustWallet(billingAccount.gatewayBillingAccountId, {
      currency,
      external_ref: body.external_ref,
      delta_atoms: deltaAtoms,
      ...(body.lot_kind ? { lot_kind: body.lot_kind } : {}),
      ...(body.force_debt !== undefined ? { force_debt: body.force_debt } : {}),
      reason: body.reason,
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: billingAccount.orgId,
      action: "admin.finance.wallet_adjustment",
      targetType: "billing_account",
      targetId: billingAccountId,
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

  app.post("/finance/billing-accounts/:billingAccountId/freeze", async (c) => {
    const actor = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const body = await parseJsonBody(c, walletFreezeSchema);
    const billingAccount = await deps.sync.admission.ensureBillingAccountSynced(billingAccountId);
    const currency = resolveBillingAccountCurrency(body.currency, billingAccount.currency);
    const res = await deps.gateway.setWalletFreeze(billingAccount.gatewayBillingAccountId, {
      currency,
      frozen: body.frozen,
      reason: body.reason,
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: billingAccount.orgId,
      action: body.frozen ? "admin.finance.wallet_freeze" : "admin.finance.wallet_unfreeze",
      targetType: "billing_account",
      targetId: billingAccountId,
      metadata: { currency, frozen: body.frozen, reason: body.reason },
    });
    return c.json(enrichWalletDecimals(toBillingAccountScope(res, billingAccountId)));
  });

  app.post("/finance/billing-accounts/:billingAccountId/credit-policy", async (c) => {
    const actor = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
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
    const billingAccount = await deps.sync.admission.ensureBillingAccountSynced(billingAccountId);
    const currency = resolveBillingAccountCurrency(body.currency, billingAccount.currency);
    const res = await deps.gateway.configureCreditPolicy(billingAccount.gatewayBillingAccountId, {
      currency,
      expected_version: body.expected_version,
      credit_limit_atoms: creditLimitAtoms,
      ...(body.policy ? { policy: body.policy } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: billingAccount.orgId,
      action: "admin.finance.credit_policy",
      targetType: "billing_account",
      targetId: billingAccountId,
      metadata: {
        currency,
        credit_limit_atoms: creditLimitAtoms,
        ...(body.policy ? { policy: body.policy } : {}),
      },
    });
    return c.json(enrichWalletDecimals(toBillingAccountScope(res, billingAccountId)));
  });

  registerAdminCreditPolicyRoutes({ app, deps });

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
      billing_account_id: await gatewayBillingAccountFilter(
        c,
        "api_key_id",
        "outcome",
        "from",
        "to",
        "limit",
        "offset",
      ),
      api_key_id: c.req.query("api_key_id"),
      outcome: c.req.query("outcome"),
      from: c.req.query("from"),
      to: c.req.query("to"),
      limit: String(limit),
      offset: String(offset),
    })) as { items?: Record<string, unknown>[]; total?: number; limit?: number; offset?: number };
    return c.json({
      scale: AMOUNT_DECIMALS,
      items: (await mapBillingAccounts(raw.items ?? [])).map(enrichAdminChargeDecimals),
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
      billing_account_id: await gatewayBillingAccountFilter(
        c,
        "side",
        "entity_id",
        "account_id",
        "kind",
        "currency",
        "from",
        "to",
        "limit",
        "offset",
      ),
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
      items: (await mapBillingAccounts(raw.items ?? [])).map(enrichFundingOperationDecimals),
      total: raw.total ?? 0,
      limit: raw.limit ?? limit,
      offset: raw.offset ?? offset,
    });
  });

  app.get("/finance/ledger-entries", async (c) => {
    assertKnownQueryParameters(c.req.query(), [
      "side",
      "entity_id",
      "request_id",
      "kind",
      "from",
      "to",
      "limit",
      "offset",
    ]);
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
    const [hold, charge] = await mapBillingAccounts([raw.hold ?? {}, raw.charge ?? {}]);
    return c.json({
      scale: AMOUNT_DECIMALS,
      hold: raw.hold ? hold : null,
      charge: raw.charge ? enrichAdminChargeDecimals(charge!) : null,
      entries: (raw.entries ?? []).map(enrichLedgerEntryDecimals),
    });
  });

  app.get("/finance/summary", async (c) => {
    const report = await deps.gateway.getFinanceSummary({
      from: c.req.query("from"),
      to: c.req.query("to"),
      billing_account_id: await gatewayBillingAccountFilter(c, "from", "to", "currency"),
      currency: c.req.query("currency"),
    });
    const byAccount = Array.isArray(report.by_billing_account) ? report.by_billing_account : [];
    const ids = await billingAccountIdsFor([
      report.billing_account_id,
      ...byAccount.map((row) => row.key),
    ]);
    return c.json(enrichFinanceSummaryDecimals(report, ids));
  });

  app.get("/finance/holds", async (c) => {
    const raw = (await deps.gateway.listHolds({
      state: c.req.query("state"),
      billing_account_id: await gatewayBillingAccountFilter(c, "state", "limit", "offset"),
      limit: c.req.query("limit"),
      offset: c.req.query("offset"),
    })) as { items: Record<string, unknown>[] };
    return c.json({ ...raw, items: await mapBillingAccounts(raw.items) });
  });

  app.post("/finance/requests/:requestId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.resolveRequest(requestId, body);
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
    const res = await deps.gateway.resolveSupplierAttempt(requestId, attemptId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_supplier_attempt",
      targetType: "supplier_attempt",
      targetId: `${requestId}:${attemptId}`,
    });
    return c.json(res);
  });

  app.get("/finance/async-tasks", async (c) => {
    const raw = (await deps.gateway.listAsyncTasks({
      billing_account_id: await gatewayBillingAccountFilter(
        c,
        "status",
        "refund_eligibility",
        "limit",
        "offset",
      ),
      status: c.req.query("status"),
      refund_eligibility: c.req.query("refund_eligibility"),
      limit: c.req.query("limit"),
      offset: c.req.query("offset"),
    })) as { items: Record<string, unknown>[] };
    return c.json({ ...raw, items: await mapBillingAccounts(raw.items) });
  });

  app.get("/finance/conservation", async (c) => {
    const audit = await deps.gateway.getConservationAudit();
    return c.json(audit.data, audit.status as 200 | 500);
  });

  return app;
}
