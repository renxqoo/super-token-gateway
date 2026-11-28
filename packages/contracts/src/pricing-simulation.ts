import { dimensions, operations, type Dimension } from "@gateway/kernel";
import { z } from "zod";
import { atomsSchema, currencySchema } from "./billing.ts";

export const quantityStringSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$/);

const attrValueSchema = z.union([z.string().max(120), z.number().finite(), z.boolean()]);
export const pricingAttributesSchema = z
  .record(z.string().min(1).max(64), attrValueSchema)
  .refine((obj) => Object.keys(obj).length <= 32, { message: "At most 32 attributes allowed" });

const usageKeys = [...dimensions, "reasoning_tokens"] as [
  Dimension | "reasoning_tokens",
  ...(Dimension | "reasoning_tokens")[],
];
const usageMapSchema = z.partialRecord(z.enum(usageKeys), quantityStringSchema);

const marginPolicySchema = z
  .object({
    allow_negative_margin: z.boolean().optional(),
    min_margin_bps: z.number().int().min(-100_000_000).max(10_000).optional(),
    max_loss_atoms: atomsSchema.optional(),
  })
  .strict();

export const simulatePricingSchema = z
  .object({
    model: z.string().min(1).max(150),
    operation: z.enum(operations),
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
    attrs: pricingAttributesSchema.optional(),
    usage: usageMapSchema.default({}),
    input_characters: atomsSchema.optional(),
    output_bound: z.number().int().min(0).max(10_000_000).optional(),
    estimated_input_tokens: z.number().int().min(1).max(100_000_000).optional(),
    images: z.number().int().min(1).max(100).optional(),
    margin_policy: marginPolicySchema.optional(),
  })
  .strict();
