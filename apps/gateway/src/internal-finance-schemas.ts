import type { Context } from "hono";
import { z } from "zod";
import { assertKnownQueryParameters, GatewayError, usageSchema } from "@gateway/contracts";
import {
  currencySchema,
  positiveAtomsSchema,
  operationEnum,
  nonNegativeAtomsSchema,
  signedAtomsSchema,
  type AdminContext,
  parseUuid,
} from "./internal-request.ts";

export const creditWalletSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    kind: z.enum(["CASH", "GRANT"]).default("CASH"),
    priority: z.number().int().min(0).max(10000).optional(),
    expires_at: z.iso.datetime().nullable().optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).optional(),
    operations: z.array(operationEnum).max(32).optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

export const creditPolicySchema = z
  .object({
    currency: currencySchema.default("USD"),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema,
    policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
  })
  .strict();

export const refundChargeSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

export const adjustChargeSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

export const adjustWalletSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    lot_kind: z.enum(["CASH", "GRANT"]).optional(),
    force_debt: z.boolean().optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

export const setWalletFreezeSchema = z
  .object({
    currency: currencySchema.default("USD"),
    frozen: z.boolean(),
    reason: z.string().min(1).max(500),
  })
  .strict();

export const rechargeSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

export const adjustSupplierBalanceSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    allow_negative: z.boolean().optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

export const resolveRequestSchema = z
  .object({
    action: z.enum(["SETTLE", "RELEASE"]),
    billing_price_id: z.uuid().optional(),
    usage: usageSchema.optional(),
    supplier_cost_atoms: nonNegativeAtomsSchema.optional(),
    supplier_cost_version_id: z.uuid().optional(),
    reason: z.string().min(1).max(500),
  })
  .strict();

const supplierResolutionFields = {
  reason: resolveRequestSchema.shape.reason,
  evidence_ref: z.string().trim().min(1).max(512),
};

export const resolveSupplierAttemptSchema = z.discriminatedUnion("action", [
  z.object({ ...supplierResolutionFields, action: z.literal("RELEASE") }).strict(),
  z
    .object({
      ...supplierResolutionFields,
      action: z.literal("SETTLE"),
      supplier_cost_atoms: nonNegativeAtomsSchema,
    })
    .strict(),
]);

const reconciliationReceiptFields = {
  request_id: z.uuid(),
  attempt_id: z.uuid(),
  account_id: z.uuid(),
  channel_id: z.uuid(),
  currency: currencySchema,
  provider_request_id: z.string().regex(/^[A-Za-z0-9._:/-]{1,256}$/),
  evidence_ref: z.string().trim().min(1).max(512),
  match_reason: z.string().trim().min(1).max(500).optional(),
};

export const reconciliationReceiptSchema = z.discriminatedUnion("verdict", [
  z.object({ ...reconciliationReceiptFields, verdict: z.literal("NOT_EXECUTED") }).strict(),
  z
    .object({
      ...reconciliationReceiptFields,
      verdict: z.literal("FINAL"),
      supplier_cost_atoms: nonNegativeAtomsSchema,
      customer_usage: usageSchema.optional(),
    })
    .strict(),
]);

export const reconciliationPolicySchema = z
  .object({
    billing_account_id: z.uuid(),
    channel_id: z.uuid(),
    supplier_currency: currencySchema,
    enabled: z.boolean().default(false),
    include_historical: z.boolean().default(false),
    age_seconds: z.number().int().positive().max(2_147_483_647),
    max_customer_atoms: positiveAtomsSchema,
    limit_atoms: positiveAtomsSchema,
  })
  .strict();

export const claimEventsSchema = z
  .object({
    consumer: z.string().min(1).max(120).default("default"),
    limit: z.number().int().min(1).max(500).default(100),
  })
  .strict();

export const ackEventsSchema = z
  .object({
    consumer: z.string().min(1).max(120).default("default"),
    lease_token: z.uuid(),
    through_seq: nonNegativeAtomsSchema,
  })
  .strict();

export function requireConsumer(c: AdminContext, consumer: string): void {
  const principal = c.get("principal");
  if (!principal.consumers.includes(consumer)) {
    throw new GatewayError(403, "forbidden", "Consumer is outside the credential scope");
  }
}

export function billingAccountFilter(c: Context, ...otherFilters: string[]) {
  assertKnownQueryParameters(c.req.query(), ["billing_account_id", ...otherFilters]);
  const id = c.req.query("billing_account_id");
  return id === undefined ? undefined : parseUuid(id, "billing_account_id");
}
