import { z } from "zod";
import { providerKinds, operations, dimensions } from "@gateway/kernel";
import type { Operation, ProviderKind, Pricing, Usage, Finality } from "@gateway/kernel";
const positiveInteger = z.string().regex(/^[1-9][0-9]{0,37}$/);
const integer = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
export const rateSchema = z.object({ numerator: integer, denominator: positiveInteger }).strict();
export const pricingSchema = z
  .object({
    currency: z.string().regex(/^[A-Z]{3,8}$/),
    rates: z.partialRecord(z.enum(dimensions), rateSchema),
  })
  .strict()
  .superRefine((value, context) => {
    if (
      value.rates.cache_write_tokens &&
      (value.rates.cache_write_5m_tokens || value.rates.cache_write_1h_tokens)
    )
      context.addIssue({
        code: "custom",
        message: "Choose aggregate or TTL-specific cache write rates",
      });
    for (const prefix of ["audio", "video"] as const) {
      if (value.rates[`${prefix}_ms`] && value.rates[`${prefix}_seconds`])
        context.addIssue({ code: "custom", message: "Choose one duration unit per media type" });
    }
  });
const parameterPath = z
  .string()
  .regex(/^[A-Za-z_][A-Za-z0-9_]*(?:\.(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+|\*))*$/)
  .max(200);
const parameterRange = z
  .object({ minimum: z.number().finite().optional(), maximum: z.number().finite().optional() })
  .strict()
  .refine(
    (v) =>
      (v.minimum !== undefined || v.maximum !== undefined) &&
      !(v.minimum !== undefined && v.maximum !== undefined && v.minimum > v.maximum),
    "A parameter range requires ordered bounds",
  );
export const requestPolicySchema = z
  .object({
    allowed_parameters: z
      .array(
        z
          .string()
          .regex(/^[A-Za-z_][A-Za-z0-9_]*$/)
          .max(100),
      )
      .max(100)
      .optional(),
    blocked_parameters: z.array(parameterPath).max(100).optional(),
    parameter_ranges: z
      .record(parameterPath, parameterRange)
      .refine((v) => Object.keys(v).length <= 100, "Too many parameter ranges")
      .optional(),
  })
  .strict();
export type RequestPolicy = z.infer<typeof requestPolicySchema>;
export const projectSchema = z
  .object({
    name: z.string().min(1).max(100),
    external_ref: z.string().max(200).optional(),
    organization_id: z.uuid().optional(),
    billing_currency: z
      .string()
      .regex(/^[A-Z]{3,8}$/)
      .default("USD"),
    rpm: z.number().int().min(1).max(10_000_000).default(6000),
    tpm: z.number().int().min(1).max(1_000_000_000).default(10_000_000),
    concurrency: z.number().int().min(1).max(1_000_000).default(1000),
  })
  .strict();
export const keySchema = z
  .object({
    name: z.string().min(1).max(100),
    models: z.array(z.string().min(1).max(150)).max(1000).default([]),
    operations: z
      .array(z.enum(operations))
      .min(1)
      .default([...operations]),
    principal_id: z.uuid().optional(),
    role: z.enum(["ADMIN", "INVOKER", "BILLING", "VIEWER"]).default("INVOKER"),
    allowed_cidrs: z.array(z.string().min(2).max(60)).max(100).default([]),
    rpm: z.number().int().min(1).max(10000000).optional(),
    tpm: z.number().int().min(1).max(1000000000).optional(),
    concurrency: z.number().int().min(1).max(1000000).optional(),
    expires_at: z.iso.datetime().optional(),
  })
  .strict();
export const providerSchema = z
  .object({
    name: z.string().min(1).max(100),
    kind: z.enum(providerKinds),
    base_url: z.url().max(1000),
    api_key: z.string().min(1).max(16384),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
  })
  .strict();
export const modelSchema = z
  .object({
    name: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/),
    operations: z.array(z.enum(operations)).min(1),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32768),
  })
  .strict();
export const usageSchema = z
  .partialRecord(
    z.enum([...dimensions, "reasoning_tokens"]),
    z.string().regex(/^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$/),
  )
  .superRefine((value, context) => {
    for (const [key, quantity] of Object.entries(value))
      if (quantity.includes(".") && key !== "audio_seconds" && key !== "video_seconds")
        context.addIssue({ code: "custom", message: "Counters must be integers", path: [key] });
    if (
      value.cache_write_tokens !== undefined &&
      value.cache_write_5m_tokens !== undefined &&
      value.cache_write_1h_tokens !== undefined &&
      [value.cache_write_tokens, value.cache_write_5m_tokens, value.cache_write_1h_tokens].every(
        (q) => /^(0|[1-9][0-9]{0,37})$/.test(q),
      ) &&
      BigInt(value.cache_write_tokens) !==
        BigInt(value.cache_write_5m_tokens) + BigInt(value.cache_write_1h_tokens)
    )
      context.addIssue({ code: "custom", message: "Provider cache write partitions disagree" });
  });

export const upstreamCostConfigurationSchema = z
  .object({ pricing: pricingSchema, ceilings: usageSchema })
  .strict()
  .superRefine((value, context) => {
    for (const dimension of Object.keys(value.pricing.rates))
      if (!Object.hasOwn(value.ceilings, dimension))
        context.addIssue({
          code: "custom",
          message: "Every upstream rate requires an explicit reservation ceiling",
          path: ["ceilings", dimension],
        });
    if (!Object.keys(value.pricing.rates).length)
      context.addIssue({
        code: "custom",
        message: "Upstream pricing must explicitly declare at least one rate",
      });
  });
export type UpstreamCostConfiguration = z.infer<typeof upstreamCostConfigurationSchema>;

export const routeSchema = z
  .object({
    provider_id: z.uuid(),
    upstream_model: z.string().min(1).max(200),
    operations: z.array(z.enum(operations)).min(1),
    priority: z.number().int().min(0).max(1000).default(0),
    weight: z.number().min(0.001).max(1000).default(1),
    pricing: pricingSchema,
    ceilings: usageSchema,
    request_policy: requestPolicySchema.optional(),
  })
  .strict()
  .superRefine((value, context) => {
    const result = upstreamCostConfigurationSchema.safeParse({
      pricing: value.pricing,
      ceilings: value.ceilings,
    });
    if (!result.success) for (const issue of result.error.issues) context.addIssue({ ...issue });
  });
export const routePatchSchema = z
  .object({
    enabled: z.boolean().optional(),
    priority: z.number().int().min(0).max(1000).optional(),
    weight: z.number().min(0.001).max(1000).optional(),
    pricing: pricingSchema.optional(),
    ceilings: usageSchema.optional(),
    request_policy: requestPolicySchema.optional(),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.pricing !== undefined || value.ceilings !== undefined) {
      const result = upstreamCostConfigurationSchema.safeParse({
        pricing: value.pricing,
        ceilings: value.ceilings,
      });
      if (!result.success) for (const issue of result.error.issues) context.addIssue({ ...issue });
    }
  });

export interface Project {
  id: string;
  name: string;
  enabled: boolean;
  billing_currency: string;
  organization_id?: string | null;
  organization_rpm?: number;
  organization_tpm?: number;
  organization_concurrency?: number;
  rpm: number;
  tpm: number;
  concurrency: number;
}
export interface Credential {
  id: string;
  project_id: string;
  principal_id?: string | null;
  role?: string;
  allowed_cidrs?: string[];
  rpm?: number | null;
  tpm?: number | null;
  concurrency?: number | null;
  principal_rpm?: number;
  principal_tpm?: number;
  principal_concurrency?: number;
  models: string[];
  operations: Operation[];
}
export interface Route {
  id: string;
  model_name: string;
  provider_id: string;
  kind: ProviderKind;
  base_url: string;
  secret: string;
  upstream_model: string;
  operations: Operation[];
  priority: number;
  weight: number;
  pricing_id: string;
  pricing: Pricing;
  ceilings: Usage;
  request_policy?: RequestPolicy;
  concurrency: number;
  rpm: number;
}
export type { BatchManifest, BatchResourceMetadata } from "./enterprise.ts";
import type { BatchManifest } from "./enterprise.ts";
export interface Resource {
  id: string;
  project_id: string;
  provider_id: string;
  remote_id: string;
  kind: string;
  state: string;
  model: string | null;
  created_at: Date;
  attempt_id: string | null;
  batch_endpoint: string | null;
  batch_upstream_model: string | null;
  batch_manifest: BatchManifest | null;
}
export interface MeteredUsage {
  usage: Usage;
  finality: Finality;
}
