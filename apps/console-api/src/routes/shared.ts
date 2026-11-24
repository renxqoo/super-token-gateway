import type { SQL } from "bun";
import type { Context, MiddlewareHandler } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import {
  AMOUNT_DECIMALS,
  formatAtomsToDecimal,
  operations,
  parseDecimalToAtoms,
} from "@gateway/kernel";
import {
  authenticateSession,
  normalizeAndValidateCidr,
  resolveClientIp,
  type AuthUser,
  type LoginRateLimiter,
} from "../auth.ts";
import type { ConsoleConfig } from "../config.ts";
import type { apiKeyRecords, ConsoleDrizzle, projects } from "../db/schema.ts";
import type { GatewayClient } from "../gateway-client.ts";
import type { ConsoleSyncEngine } from "../sync.ts";

export type ConsoleDeps = {
  config: ConsoleConfig;
  db: SQL;
  orm: ConsoleDrizzle;
  gateway: GatewayClient;
  sync: ConsoleSyncEngine;
  loginLimiter: LoginRateLimiter;
};

export type ConsoleEnv = {
  Variables: {
    user: AuthUser;
    peerIp?: string;
  };
};

export type ConsoleContext = Context<ConsoleEnv>;

export const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
export const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
export const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
export const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
export const positiveDecimalSchema = z.string().regex(/^(0|[1-9][0-9]{0,19})(\.[0-9]{1,18})?$/);
export const signedDecimalSchema = z.string().regex(/^-?(0|[1-9][0-9]{0,19})(\.[0-9]{1,18})?$/);
export const operationEnum = z.enum(operations);

export function resolveAtomsAmount(
  atoms: string | undefined,
  decimal: string | undefined,
  options: { field: string; allowNegative?: boolean; allowZero?: boolean },
): string {
  if (atoms === undefined && decimal === undefined) {
    throw new GatewayError(
      400,
      "validation_error",
      `${options.field}_atoms or ${options.field}_decimal is required`,
    );
  }
  let fromDecimal: string | undefined;
  if (decimal !== undefined) {
    try {
      fromDecimal = parseDecimalToAtoms(decimal, {
        ...(options.allowNegative !== undefined ? { allowNegative: options.allowNegative } : {}),
        ...(options.allowZero !== undefined ? { allowZero: options.allowZero } : {}),
      });
    } catch (err) {
      throw new GatewayError(
        400,
        "validation_error",
        err instanceof Error ? err.message : `Invalid ${options.field}_decimal`,
      );
    }
  }
  if (atoms !== undefined && fromDecimal !== undefined && atoms !== fromDecimal) {
    throw new GatewayError(
      400,
      "validation_error",
      `${options.field}_atoms and ${options.field}_decimal disagree`,
    );
  }
  return atoms ?? fromDecimal!;
}

export function safeFormatAtoms(value: unknown): string {
  if (typeof value === "bigint") return formatAtomsToDecimal(value);
  if (typeof value === "string" && /^-?(0|[1-9][0-9]{0,37})$/.test(value)) {
    return formatAtomsToDecimal(value);
  }
  return formatAtomsToDecimal("0");
}

export function enrichWalletLotDecimals(lot: Record<string, unknown>) {
  return {
    ...lot,
    scale: AMOUNT_DECIMALS,
    issued_decimal: safeFormatAtoms(lot.issued_atoms),
    available_decimal: safeFormatAtoms(lot.available_atoms),
    reserved_decimal: safeFormatAtoms(lot.reserved_atoms),
    spent_decimal: safeFormatAtoms(lot.spent_atoms),
    expired_decimal: safeFormatAtoms(lot.expired_atoms),
  };
}

export function enrichWalletDecimals(wallet: Record<string, unknown>) {
  const lots = Array.isArray(wallet.lots)
    ? (wallet.lots as Record<string, unknown>[]).map(enrichWalletLotDecimals)
    : undefined;
  return {
    ...wallet,
    scale: AMOUNT_DECIMALS,
    available_decimal: safeFormatAtoms(wallet.available_atoms),
    reserved_decimal: safeFormatAtoms(wallet.reserved_atoms),
    spent_decimal: safeFormatAtoms(wallet.spent_atoms),
    expired_decimal: safeFormatAtoms(wallet.expired_atoms),
    debt_decimal: safeFormatAtoms(wallet.debt_atoms),
    credit_limit_decimal: safeFormatAtoms(wallet.credit_limit_atoms),
    credit_reserved_decimal: safeFormatAtoms(wallet.credit_reserved_atoms),
    ...(lots !== undefined ? { lots } : {}),
  };
}

export function enrichFundingOperationDecimals(op: Record<string, unknown>) {
  return {
    ...op,
    scale: AMOUNT_DECIMALS,
    amount_decimal: safeFormatAtoms(op.amount_atoms),
  };
}

export function enrichAdminChargeDecimals(raw: Record<string, unknown>) {
  const customerAtoms = String(raw.customerAtoms ?? raw.customer_atoms ?? "0");
  const fundedAtoms = String(raw.fundedAtoms ?? raw.funded_atoms ?? "0");
  const debtAtoms = String(raw.debtAtoms ?? raw.debt_atoms ?? "0");
  const refundedAtoms = String(raw.refundedAtoms ?? raw.refunded_atoms ?? "0");
  const supplierCostAtoms = String(raw.supplierCostAtoms ?? raw.supplier_cost_atoms ?? "0");
  const netRevenue = BigInt(customerAtoms) - BigInt(refundedAtoms);
  const supplierCurrency = raw.supplierCurrency ?? raw.supplier_currency ?? null;
  const grossMargin =
    supplierCurrency === null || supplierCurrency === raw.currency
      ? netRevenue - BigInt(supplierCostAtoms)
      : null;
  return {
    id: raw.id,
    request_id: raw.requestId ?? raw.request_id,
    project_id: raw.projectId ?? raw.project_id,
    wallet_id: raw.walletId ?? raw.wallet_id,
    api_key_id: raw.apiKeyId ?? raw.api_key_id ?? null,
    channel_id: raw.channelId ?? raw.channel_id ?? null,
    model: raw.model,
    operation: raw.operation,
    currency: raw.currency,
    supplier_currency: supplierCurrency,
    billing_price_id: raw.billingPriceId ?? raw.billing_price_id,
    cost_version_id: raw.costVersionId ?? raw.cost_version_id ?? null,
    pricing_override_id: raw.pricingOverrideId ?? raw.pricing_override_id ?? null,
    usage: raw.usage,
    scale: AMOUNT_DECIMALS,
    customer_atoms: customerAtoms,
    customer_decimal: safeFormatAtoms(customerAtoms),
    funded_atoms: fundedAtoms,
    funded_decimal: safeFormatAtoms(fundedAtoms),
    debt_atoms: debtAtoms,
    debt_decimal: safeFormatAtoms(debtAtoms),
    refunded_atoms: refundedAtoms,
    refunded_decimal: safeFormatAtoms(refundedAtoms),
    net_revenue_atoms: netRevenue.toString(),
    net_revenue_decimal: safeFormatAtoms(netRevenue),
    supplier_cost_atoms: supplierCostAtoms,
    supplier_cost_decimal: safeFormatAtoms(supplierCostAtoms),
    gross_margin_atoms: grossMargin?.toString() ?? null,
    gross_margin_decimal: grossMargin === null ? null : safeFormatAtoms(grossMargin),
    outcome: raw.outcome,
    customer_journal_id: raw.customerJournalId ?? raw.customer_journal_id ?? null,
    supplier_journal_id: raw.supplierJournalId ?? raw.supplier_journal_id ?? null,
    created_at: raw.createdAt ?? raw.created_at,
  };
}

export function enrichLedgerEntryDecimals(raw: Record<string, unknown>) {
  const atoms = String(raw.atoms ?? "0");
  return {
    journal_id: raw.journalId ?? raw.journal_id,
    ordinal: raw.ordinal,
    created_at: raw.createdAt ?? raw.created_at,
    side: raw.side,
    entity_id: raw.entityId ?? raw.entity_id,
    request_id: raw.requestId ?? raw.request_id ?? null,
    reference: raw.reference,
    kind: raw.kind,
    currency: raw.currency,
    account: raw.account,
    scale: AMOUNT_DECIMALS,
    atoms,
    atoms_decimal: safeFormatAtoms(atoms),
  };
}

export function enrichFinanceSummaryDecimals(report: Record<string, unknown>) {
  const byCurrency = Array.isArray(report.by_currency)
    ? (report.by_currency as Record<string, unknown>[]).map((r) => ({
        ...r,
        scale: AMOUNT_DECIMALS,
        customer_billed_decimal: safeFormatAtoms(r.customer_billed_atoms),
        customer_funded_decimal: safeFormatAtoms(r.customer_funded_atoms),
        customer_debt_decimal: safeFormatAtoms(r.customer_debt_atoms),
        customer_refunded_decimal: safeFormatAtoms(r.customer_refunded_atoms),
        net_customer_revenue_decimal: safeFormatAtoms(r.net_customer_revenue_atoms),
        supplier_cost_decimal: safeFormatAtoms(r.supplier_cost_atoms),
        gross_margin_decimal: safeFormatAtoms(r.gross_margin_atoms),
        cash_topup_decimal: safeFormatAtoms(r.cash_topup_atoms),
        grant_issued_decimal: safeFormatAtoms(r.grant_issued_atoms),
        customer_adjustment_net_decimal: safeFormatAtoms(r.customer_adjustment_net_atoms),
        supplier_recharge_decimal: safeFormatAtoms(r.supplier_recharge_atoms),
        supplier_adjustment_net_decimal: safeFormatAtoms(r.supplier_adjustment_net_atoms),
      }))
    : [];
  const enrichDim = (list: unknown) =>
    Array.isArray(list)
      ? (list as Record<string, unknown>[]).map((r) => ({
          ...r,
          scale: AMOUNT_DECIMALS,
          customer_billed_decimal: safeFormatAtoms(r.customer_billed_atoms),
          customer_refunded_decimal: safeFormatAtoms(r.customer_refunded_atoms),
          net_customer_revenue_decimal: safeFormatAtoms(r.net_customer_revenue_atoms),
          supplier_cost_decimal: safeFormatAtoms(r.supplier_cost_atoms),
          gross_margin_decimal: safeFormatAtoms(r.gross_margin_atoms),
        }))
      : [];
  return {
    ...report,
    scale: AMOUNT_DECIMALS,
    by_currency: byCurrency,
    by_model: enrichDim(report.by_model),
    by_project: enrichDim(report.by_project),
    by_channel: enrichDim(report.by_channel),
  };
}
const uuidSchema = z.uuid();

export const cidrSchema = z
  .string()
  .trim()
  .min(2)
  .max(64)
  .transform((val, ctx) => {
    const normalized = normalizeAndValidateCidr(val);
    if (!normalized) {
      ctx.addIssue({ code: "custom", message: "Invalid IP or CIDR notation" });
      return z.NEVER;
    }
    return normalized;
  });

export function extractPgErrno(err: unknown): string | null {
  let cur: unknown = err;
  for (let depth = 0; depth < 6 && cur && typeof cur === "object"; depth++) {
    if ("errno" in cur && typeof cur.errno === "string" && /^[0-9A-Z]{5}$/.test(cur.errno)) {
      return cur.errno;
    }
    if ("code" in cur && typeof cur.code === "string" && /^[0-9A-Z]{5}$/.test(cur.code)) {
      return cur.code;
    }
    cur = "cause" in cur ? cur.cause : null;
  }
  return null;
}

export function parseUuid(value: string | undefined, label: string): string {
  const res = uuidSchema.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

export function parseLimitOffset(
  c: Context,
  defaultLimit = 50,
  maxLimit = 200,
): { limit: number; offset: number } {
  const rawLimit = c.req.query("limit");
  const rawOffset = c.req.query("offset");
  let limit = defaultLimit;
  let offset = 0;
  if (rawLimit !== undefined && rawLimit !== "") {
    if (!/^\d+$/.test(rawLimit)) {
      throw new GatewayError(400, "validation_error", "limit must be a positive integer");
    }
    const parsed = Number(rawLimit);
    if (!Number.isSafeInteger(parsed) || parsed < 1) {
      throw new GatewayError(400, "validation_error", "limit must be a positive integer");
    }
    limit = Math.min(maxLimit, parsed);
  }
  if (rawOffset !== undefined && rawOffset !== "") {
    if (!/^\d+$/.test(rawOffset)) {
      throw new GatewayError(400, "validation_error", "offset must be a non-negative integer");
    }
    const parsed = Number(rawOffset);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
      throw new GatewayError(400, "validation_error", "offset must be a non-negative integer");
    }
    offset = parsed;
  }
  return { limit, offset };
}

export function parseIsoDateParam(raw: string | undefined, label: string): string | undefined {
  if (raw === undefined || raw === "") return undefined;
  const trimmed = raw.trim();
  const datePart = /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
    ? trimmed
    : /^\d{4}-\d{2}-\d{2}T/.test(trimmed) && !Number.isNaN(Date.parse(trimmed))
      ? new Date(trimmed).toISOString().slice(0, 10)
      : null;
  if (!datePart) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid YYYY-MM-DD date`);
  }
  const parsed = new Date(`${datePart}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== datePart) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid calendar date`);
  }
  return datePart;
}

export function parseIsoDatetimeParam(raw: string | undefined, label: string): Date | undefined {
  if (raw === undefined || raw === "") return undefined;
  const trimmed = raw.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const parsed = new Date(`${trimmed}T00:00:00.000Z`);
    if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== trimmed) {
      throw new GatewayError(400, "validation_error", `${label} must be a valid date`);
    }
    return parsed;
  }
  const res = z.iso.datetime({ offset: true }).safeParse(trimmed);
  if (!res.success) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid ISO-8601 datetime`);
  }
  return new Date(res.data);
}

export async function parseJsonBody<T>(c: Context, schema: z.ZodType<T>): Promise<T> {
  let raw: unknown;
  try {
    raw = await c.req.json();
  } catch {
    throw new GatewayError(400, "invalid_json", "Request body must be valid JSON");
  }
  const res = schema.safeParse(raw);
  if (!res.success) {
    const first = res.error.issues[0];
    throw new GatewayError(
      400,
      "validation_error",
      first ? `${first.path.join(".") || "body"}: ${first.message}` : "Validation failed",
    );
  }
  return res.data;
}

export async function parseOptionalJsonObject(c: Context): Promise<Record<string, unknown>> {
  const text = await c.req.text();
  if (!text.trim()) return {};
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    throw new GatewayError(400, "invalid_json", "Request body must be valid JSON");
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new GatewayError(400, "validation_error", "Request body must be a JSON object");
  }
  return raw as Record<string, unknown>;
}

export function getClientIp(deps: ConsoleDeps, c: ConsoleContext): string | null {
  return resolveClientIp(
    c.get("peerIp"),
    c.req.header("x-forwarded-for"),
    deps.config.trustedProxyCidrs,
  );
}

export function requireClientAuth(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"), "CLIENT");
    c.set("user", user);
    await next();
  };
}

export function requirePlatformAdmin(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"), "ADMIN");
    c.set("user", user);
    await next();
  };
}

export function sanitizeProjectForClient(row: typeof projects.$inferSelect) {
  return {
    id: row.id,
    orgId: row.orgId,
    name: row.name,
    currency: row.currency,
    rpm: row.rpm,
    tpm: row.tpm,
    concurrency: row.concurrency,
    developerKeySpendLimitAtoms: row.developerKeySpendLimitAtoms,
    developerMaxActiveKeys: row.developerMaxActiveKeys,
    enabled: row.enabled,
    gatewaySynced: row.gatewaySynced,
    syncStatus: row.syncStatus,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

export function sanitizeApiKeyForClient(row: typeof apiKeyRecords.$inferSelect) {
  return {
    id: row.id,
    projectId: row.projectId,
    createdBy: row.createdBy,
    rotatedFromKeyId: row.rotatedFromKeyId,
    name: row.name,
    keyPrefix: row.keyPrefix,
    role: row.role,
    allowedCidrs: row.allowedCidrs,
    models: row.models,
    operations: row.operations,
    spendLimitAtoms: row.spendLimitAtoms,
    spendLimitWindow: row.spendLimitWindow,
    enabled: row.enabled,
    expiresAt: row.expiresAt ? row.expiresAt.toISOString() : null,
    revokedAt: row.revokedAt ? row.revokedAt.toISOString() : null,
    syncStatus: row.syncStatus,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}
