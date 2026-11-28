import { z } from "zod";
import { pricingSchema } from "@gateway/contracts";
import { dimensions, type Dimension } from "@gateway/kernel";
import {
  currencySchema,
  nonNegativeAtomsSchema,
  operationEnum,
  positiveAtomsSchema,
} from "./internal-request.ts";

const dimensionEnum = z.enum(dimensions);
const rationalSchema = z
  .object({
    numerator: nonNegativeAtomsSchema,
    denominator: positiveAtomsSchema,
  })
  .strict();

const positiveRationalSchema = z
  .object({
    numerator: positiveAtomsSchema,
    denominator: positiveAtomsSchema,
  })
  .strict();

const quantityStringSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$/);

const quantizerSchema = z
  .object({
    min_units: quantityStringSchema.optional(),
    step: quantityStringSchema.optional(),
  })
  .strict();

const quantizersMapSchema = z.partialRecord(dimensionEnum, quantizerSchema);

const cliffTierSchema = z
  .object({
    min_driver: nonNegativeAtomsSchema,
    rates: pricingSchema.shape.rates,
    reasoning_rate: rationalSchema.optional(),
  })
  .strict();

const cliffRuleSchema = z
  .object({
    driver: z.array(dimensionEnum).min(1).max(24),
    tiers: z.array(cliffTierSchema).min(1).max(32),
  })
  .strict();

const progressiveBandSchema = z
  .object({
    up_to: quantityStringSchema.nullable(),
    rate: rationalSchema,
  })
  .strict();

const progressiveMapSchema = z.partialRecord(
  dimensionEnum,
  z.array(progressiveBandSchema).min(1).max(32),
);

const attrValueSchema = z.union([z.string().max(120), z.number().finite(), z.boolean()]);
const attrRecordSchema = z
  .record(z.string().min(1).max(64), attrValueSchema)
  .refine((obj) => Object.keys(obj).length <= 32, { message: "At most 32 attributes allowed" });

const matrixEntrySchema = z
  .object({
    match: attrRecordSchema.refine((obj) => Object.keys(obj).length > 0, {
      message: "Matrix entry match must not be empty",
    }),
    rates: pricingSchema.shape.rates,
    quantizers: quantizersMapSchema.optional(),
    reasoning_rate: rationalSchema.optional(),
    min_charge_atoms: nonNegativeAtomsSchema.optional(),
    max_charge_atoms: nonNegativeAtomsSchema.optional(),
  })
  .strict();

export const pricingSpecSchema = z
  .object({
    quantizers: quantizersMapSchema.optional(),
    cliff: cliffRuleSchema.optional(),
    progressive: progressiveMapSchema.optional(),
    matrix: z.array(matrixEntrySchema).min(1).max(256).optional(),
    reasoning_rate: rationalSchema.optional(),
    min_charge_atoms: nonNegativeAtomsSchema.optional(),
    max_charge_atoms: nonNegativeAtomsSchema.optional(),
  })
  .strict();

const timeWindowSchema = z
  .object({
    timezone: z.string().min(1).max(64),
    days_of_week: z.array(z.number().int().min(0).max(6)).min(1).max(7).optional(),
    start_minute: z.number().int().min(0).max(1439),
    end_minute: z.number().int().min(0).max(1439),
  })
  .strict();

const modifierMatchSchema = z
  .object({
    side: z.enum(["CUSTOMER", "SUPPLIER"]).optional(),
    model: z
      .union([z.literal("*"), z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/)])
      .optional(),
    operation: z.union([z.literal("*"), operationEnum]).optional(),
    execution_mode: z.enum(["SYNC", "BATCH"]).optional(),
    service_tier: z.string().min(1).max(64).optional(),
    region: z.string().min(1).max(64).optional(),
    billing_account_id: z.uuid().optional(),
    customer_group_id: z.string().min(1).max(120).optional(),
    channel_id: z.uuid().optional(),
    upstream_account_id: z.uuid().optional(),
    attrs: attrRecordSchema.optional(),
  })
  .strict();

const sourceRevisionSchema = z.number().int().min(1).max(2_147_483_647).optional();

export const upsertPricingModifierSchema = z
  .object({
    id: z.uuid().optional(),
    layer: z.enum(["PRODUCT", "COMMERCIAL"]),
    kind: z.enum(["MULTIPLIER", "EXACT_RATE_CARD"]),
    stacking: z.enum(["MULTIPLY", "BEST_FOR_CUSTOMER", "EXCLUSIVE"]).default("MULTIPLY"),
    priority: z.number().int().min(-1_000_000).max(1_000_000).default(0),
    enabled: z.boolean().default(true),
    effective_from: z.iso.datetime().nullable().optional(),
    effective_to: z.iso.datetime().nullable().optional(),
    time_windows: z.array(timeWindowSchema).max(32).nullable().optional(),
    match: modifierMatchSchema.default({}),
    multiplier_numerator: positiveAtomsSchema.nullable().optional(),
    multiplier_denominator: positiveAtomsSchema.nullable().optional(),
    rate_card_price_version_id: z.uuid().nullable().optional(),
    source_revision: sourceRevisionSchema,
  })
  .strict()
  .superRefine((v, ctx) => {
    if (v.kind === "MULTIPLIER") {
      if (!v.multiplier_numerator || !v.multiplier_denominator) {
        ctx.addIssue({
          code: "custom",
          path: ["multiplier_numerator"],
          message: "multiplier_numerator and multiplier_denominator are required for MULTIPLIER",
        });
      }
      if (v.rate_card_price_version_id) {
        ctx.addIssue({
          code: "custom",
          path: ["rate_card_price_version_id"],
          message: "rate_card_price_version_id is not allowed for MULTIPLIER",
        });
      }
    } else {
      if (v.stacking === "BEST_FOR_CUSTOMER") {
        ctx.addIssue({
          code: "custom",
          path: ["stacking"],
          message: "EXACT_RATE_CARD modifier cannot use BEST_FOR_CUSTOMER stacking",
        });
      }
      if (!v.rate_card_price_version_id) {
        ctx.addIssue({
          code: "custom",
          path: ["rate_card_price_version_id"],
          message: "rate_card_price_version_id is required for EXACT_RATE_CARD",
        });
      }
      if (v.multiplier_numerator || v.multiplier_denominator) {
        ctx.addIssue({
          code: "custom",
          path: ["multiplier_numerator"],
          message: "multiplier fields are not allowed for EXACT_RATE_CARD",
        });
      }
    }
  });

export const createFxRateSchema = z
  .object({
    id: z.uuid().optional(),
    base_currency: currencySchema,
    quote_currency: currencySchema,
    rate_numerator: positiveAtomsSchema,
    rate_denominator: positiveAtomsSchema,
    effective_from: z.iso.datetime(),
    effective_to: z.iso.datetime().nullable().optional(),
  })
  .strict();

const usageKeys = [...dimensions, "reasoning_tokens"] as [
  Dimension | "reasoning_tokens",
  ...(Dimension | "reasoning_tokens")[],
];
const usageMapSchema = z.partialRecord(z.enum(usageKeys), quantityStringSchema);

const marginPolicySchema = z
  .object({
    allow_negative_margin: z.boolean().optional(),
    min_margin_bps: z.number().int().min(-100_000_000).max(10_000).optional(),
    max_loss_atoms: nonNegativeAtomsSchema.optional(),
  })
  .strict();

export const simulatePricingSchema = z
  .object({
    model: z.string().min(1).max(150),
    operation: operationEnum,
    currency: currencySchema,
    route_id: z.uuid().optional(),
    billing_account_id: z.uuid().optional(),
    customer_group_id: z.string().min(1).max(120).optional(),
    channel_id: z.uuid().optional(),
    upstream_account_id: z.uuid().optional(),
    rated_at: z.iso.datetime().optional(),
    execution_mode: z.enum(["SYNC", "BATCH"]).default("SYNC"),
    service_tier: z.string().min(1).max(64).optional(),
    region: z.string().min(1).max(64).optional(),
    attrs: attrRecordSchema.optional(),
    usage: usageMapSchema.default({}),
    input_characters: nonNegativeAtomsSchema.optional(),
    output_bound: z.number().int().min(0).max(10_000_000).optional(),
    estimated_input_tokens: z.number().int().min(1).max(100_000_000).optional(),
    images: z.number().int().min(1).max(100).optional(),
    margin_policy: marginPolicySchema.optional(),
  })
  .strict();

export { positiveRationalSchema, quantityStringSchema };
