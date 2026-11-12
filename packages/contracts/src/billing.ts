import { operations } from "@gateway/kernel";
import { z } from "zod";
import { pricingSchema, usageSchema } from "./public.ts";
export const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
export const atomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
export const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
export const creditPolicySchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    expected_version: atomsSchema,
    credit_limit_atoms: atomsSchema,
  })
  .strict();
export type CreditPolicyInput = z.infer<typeof creditPolicySchema>;
export const creditSchema = z
  .object({
    currency: currencySchema,
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    kind: z.enum(["CASH", "GIFT"]),
    priority: z.number().int().min(0).max(100000).default(100),
    expires_at: z.iso.datetime().optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).default([]),
    operations: z.array(z.enum(operations)).default([]),
  })
  .strict()
  .superRefine((value, context) => {
    if (
      value.kind === "CASH" &&
      (value.expires_at || value.models.length || value.operations.length)
    )
      context.addIssue({
        code: "custom",
        message: "Cash cannot expire or have model/operation scopes",
      });
  });
export const tariffSchema = z
  .object({
    model: z.string().min(1).max(150),
    operation: z.enum(operations),
    provider_id: z.uuid().optional(),
    pricing: pricingSchema,
    ceilings: usageSchema,
    basis: z
      .enum(["PROVIDER_USAGE", "INPUT_CHARACTERS", "REQUEST_COMPLETION"])
      .default("PROVIDER_USAGE"),
  })
  .strict()
  .superRefine((value, context) => {
    for (const dimension of Object.keys(value.pricing.rates))
      if (!Object.hasOwn(value.ceilings, dimension))
        context.addIssue({
          code: "custom",
          message: "Every rate requires an explicit reservation ceiling",
          path: ["ceilings", dimension],
        });
    if (!Object.keys(value.pricing.rates).length)
      context.addIssue({
        code: "custom",
        message: "A tariff must explicitly declare at least one rate",
      });
    if (
      value.basis === "INPUT_CHARACTERS" &&
      Object.keys(value.pricing.rates).some((d) => !["characters", "requests"].includes(d))
    )
      context.addIssue({
        code: "custom",
        message: "Input character pricing only supports characters and requests",
      });
    if (
      value.basis === "REQUEST_COMPLETION" &&
      Object.keys(value.pricing.rates).some((d) => d !== "requests")
    )
      context.addIssue({
        code: "custom",
        message: "Request completion pricing only supports requests",
      });
  });
export const refundSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z
      .enum(["CUSTOMER_REFUND", "ADMIN_CORRECTION", "PROVIDER_RECONCILIATION"])
      .default("CUSTOMER_REFUND"),
  })
  .strict();
export const adjustmentSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    target_atoms: atomsSchema,
    reason: z.enum(["ADMIN_CORRECTION", "PROVIDER_RECONCILIATION"]),
  })
  .strict();
export const resolutionSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    action: z.enum(["RELEASE", "SETTLE"]),
    usage: usageSchema.default({}),
    pricing_id: z.uuid().optional(),
    reason: z.enum(["PROVIDER_INVOICE", "ADMIN_WRITE_OFF", "EVIDENCE_RECONCILIATION"]),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.action === "SETTLE" && !value.pricing_id)
      context.addIssue({
        code: "custom",
        message: "Settlement resolution requires the captured pricing version",
      });
  });
export type CreditInput = z.infer<typeof creditSchema>;
export type TariffInput = z.infer<typeof tariffSchema>;
export type RefundInput = z.infer<typeof refundSchema>;
export type AdjustmentInput = z.infer<typeof adjustmentSchema>;
export type ResolutionInput = z.infer<typeof resolutionSchema>;

export const usageEvidenceSchema = z
  .object({
    attemptId: z.uuid(),
    source: z
      .string()
      .regex(/^[a-z0-9_]{1,80}$/)
      .default("execution"),
    state: z.enum(["DISPATCHING", "SUCCEEDED", "NOT_EXECUTED", "UNKNOWN", "PARTIAL", "FAILED"]),
    finality: z.enum(["FINAL", "PARTIAL", "UNKNOWN", "ESTIMATED"]),
    usage: usageSchema.default({}),
    code: z
      .string()
      .regex(/^[a-z0-9_]{1,80}$/)
      .optional(),
    providerRequestId: z
      .string()
      .regex(/^[\x21-\x7e]{1,200}$/)
      .optional(),
    requestState: z.enum(["RUNNING", "SUCCEEDED", "FAILED", "UNKNOWN", "CANCELLED"]).optional(),
  })
  .strict();
export type UsageEvidence = z.infer<typeof usageEvidenceSchema>;
export const usageBatchSchema = z
  .object({ version: z.literal(1), events: z.array(usageEvidenceSchema).min(1).max(128) })
  .strict();
