import { z } from "zod";
import { pricingSchema } from "@gateway/contracts";
import { BILLING_ACCOUNT_API_KEY_LENGTH, BILLING_ACCOUNT_API_KEY_PATTERN } from "@gateway/runtime";
import {
  currencySchema,
  operationEnum,
  nonNegativeAtomsSchema,
  multiplierIntSchema,
} from "./internal-request.ts";
import { providerNameSchema, adapterOverridesSchema } from "./internal-provider-schemas.ts";

const sourceRevisionSchema = z.number().int().min(1).max(2_147_483_647).optional();

export const consoleOrganizationAdmissionSchema = z
  .object({
    suspended: z.boolean(),
    source_revision: z.number().int().min(1).max(2_147_483_647),
  })
  .strict();

export const upsertBillingAccountSchema = z
  .object({
    id: z.uuid().optional(),
    external_ref: z.string().max(200).nullable().optional(),
    name: z.string().min(1).max(120),
    currency: currencySchema.default("USD"),
    rpm: z.number().int().min(1).max(10_000_000).default(6000),
    tpm: z.number().int().min(1).max(9_007_199_254_740_991).default(10_000_000),
    concurrency: z.number().int().min(1).max(1_000_000).default(1000),
    enabled: z.boolean().default(true),
    source_revision: sourceRevisionSchema,
  })
  .strict();

export const upsertApiKeySchema = z
  .object({
    id: z.uuid().optional(),
    billing_account_id: z.uuid(),
    rotated_from_key_id: z.uuid().optional(),
    name: z.string().min(1).max(120).default("default"),
    raw_key: z
      .string()
      .length(BILLING_ACCOUNT_API_KEY_LENGTH)
      .regex(BILLING_ACCOUNT_API_KEY_PATTERN)
      .optional(),
    role: z.enum(["ADMIN", "BILLING", "INVOKER", "VIEWER"]).default("INVOKER"),
    allowed_cidrs: z.array(z.string().min(2).max(64)).max(100).default([]),
    models: z.array(z.string().min(1).max(150)).max(1000).default([]),
    operations: z.array(operationEnum).max(32).default([]),
    enabled: z.boolean().default(true),
    expires_at: z.iso.datetime().nullable().optional(),
    spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    spend_limit_window: z.enum(["LIFETIME", "DAILY", "MONTHLY"]).default("LIFETIME"),
    source_revision: sourceRevisionSchema,
  })
  .strict();

export const resetApiKeySpendSchema = z
  .object({
    reason: z.string().max(500).optional(),
  })
  .strict();

export const upsertPricingOverrideSchema = z
  .object({
    id: z.uuid().optional(),
    billing_account_id: z.uuid(),
    model_name: z.union([z.literal("*"), z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/)]),
    operation: z.union([z.literal("*"), operationEnum]).default("*"),
    override_kind: z.enum(["EXACT_PRICE", "MULTIPLIER"]),
    billing_price_id: z.uuid().nullable().optional(),
    multiplier_numerator: multiplierIntSchema.nullable().optional(),
    multiplier_denominator: multiplierIntSchema.nullable().optional(),
    enabled: z.boolean().default(true),
    source_revision: sourceRevisionSchema,
  })
  .strict()
  .superRefine((v, ctx) => {
    if (v.override_kind === "EXACT_PRICE") {
      if (v.model_name === "*") {
        ctx.addIssue({
          code: "custom",
          path: ["model_name"],
          message: "EXACT_PRICE override requires a concrete model_name",
        });
      }
      if (!v.billing_price_id) {
        ctx.addIssue({
          code: "custom",
          path: ["billing_price_id"],
          message: "billing_price_id is required for EXACT_PRICE",
        });
      }
    } else if (!v.multiplier_numerator || !v.multiplier_denominator) {
      ctx.addIssue({
        code: "custom",
        path: ["multiplier_numerator"],
        message: "multiplier_numerator and multiplier_denominator are required for MULTIPLIER",
      });
    }
  });

export const upsertAccountSchema = z
  .object({
    id: z.uuid().optional(),
    name: z.string().min(1).max(120),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
    expected_state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
    source_revision: sourceRevisionSchema,
  })
  .strict();

export const upsertChannelSchema = z
  .object({
    id: z.uuid().optional(),
    account_id: z.uuid(),
    name: z.string().min(1).max(120),
    kind: providerNameSchema,
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000),
    api_key: z.string().max(16384).optional(),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    enabled: z.boolean().default(true),
    routing_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_config: adapterOverridesSchema.default({}),
    source_revision: sourceRevisionSchema,
  })
  .strict();

export const upsertModelSchema = z
  .object({
    name: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/),
    operations: z.array(operationEnum).min(1),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32768),
    enabled: z.boolean().default(true),
    source_revision: sourceRevisionSchema,
  })
  .strict();

export const createPriceSchema = z
  .object({
    id: z.uuid().optional(),
    side: z.enum(["CUSTOMER", "SUPPLIER"]),
    currency: currencySchema,
    basis: z
      .enum(["PROVIDER_USAGE", "REQUEST_COMPLETION", "INPUT_CHARACTERS"])
      .default("PROVIDER_USAGE"),
    rates: pricingSchema.shape.rates,
    ceilings: z.record(z.string(), nonNegativeAtomsSchema).default({}),
  })
  .strict();

export const pluginStepSchema = z
  .object({
    plugin_id: z.uuid(),
    priority: z.number().int().min(0).max(1000),
    enabled: z.boolean().optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
  })
  .strict();

const retryPolicySchema = z
  .object({
    max_attempts: z.number().int().min(1).max(10).optional(),
    retry_on_ambiguous: z.boolean().optional(),
    attempt_timeout_ms: z.number().int().min(100).max(300_000).optional(),
  })
  .strict();

export const upsertRouteSchema = z
  .object({
    id: z.uuid().optional(),
    model_name: z.string().min(1).max(150),
    channel_id: z.uuid(),
    upstream_model: z.string().min(1).max(200),
    operations: z.array(operationEnum).min(1),
    priority: z.number().int().min(0).max(1000).default(0),
    weight: z.number().positive().max(1000).default(1),
    billing_price_id: z.uuid(),
    cost_version_id: z.uuid(),
    request_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_overrides: adapterOverridesSchema.default({}),
    plugin_pipeline: z.array(pluginStepSchema).max(20).optional(),
    retry_policy: retryPolicySchema.default({}),
    enabled: z.boolean().default(true),
    source_revision: sourceRevisionSchema,
  })
  .strict();
