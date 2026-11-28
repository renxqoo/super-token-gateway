import { operations } from "@gateway/kernel";
import { z } from "zod";
import { simulatePricingSchema } from "./pricing-simulation.ts";
import type { ConfigReleaseNode } from "./route-releases.ts";

const uuid = z.uuid().transform((id) => id.toLowerCase());
const modelName = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/);

const onboardingModelSchema = z
  .object({
    name: modelName,
    display_name: z.string().trim().min(1).max(150),
    provider_brand: z.string().trim().min(1).max(80).default("gateway"),
    description: z.string().max(2000).default(""),
    category: z.string().trim().min(1).max(64).default("chat"),
    context_length: z.number().int().min(1).max(100_000_000).default(128_000),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32_768),
    operations: z.array(z.enum(operations)).min(1).max(operations.length),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    tags: z.array(z.string().min(1).max(40)).max(32).default([]),
    is_public: z.boolean().default(true),
  })
  .strict()
  .refine(({ operations: ops }) => new Set(ops).size === ops.length, {
    path: ["operations"],
    message: "Operations must be distinct",
  });

export const modelOnboardingSchema = z
  .object({
    id: uuid,
    expected_epoch: z.number().int().nonnegative(),
    model: onboardingModelSchema,
    billing_price_id: uuid,
    deployments: z
      .array(
        z
          .object({
            id: uuid,
            channel_id: uuid,
            upstream_model: z.string().min(1).max(200),
            cost_version_id: uuid,
            priority: z.number().int().min(0).max(1000).default(0),
            weight: z.number().positive().max(1000).default(1),
          })
          .strict(),
      )
      .min(1)
      .max(32),
  })
  .strict()
  .refine(
    ({ deployments }) => new Set(deployments.map(({ id }) => id)).size === deployments.length,
    {
      path: ["deployments"],
      message: "Deployment identifiers must be distinct",
    },
  );

const modelOnboardingQuoteSchema = simulatePricingSchema
  .omit({ model: true, channel_id: true, upstream_account_id: true, customer_group_id: true })
  .extend({
    route_id: uuid,
    billing_account_id: uuid.optional(),
  });

export const modelOnboardingPreviewSchema = z
  .object({ release: modelOnboardingSchema, quote: modelOnboardingQuoteSchema })
  .strict();

export type ModelOnboardingInput = z.output<typeof modelOnboardingSchema>;
export type ModelOnboardingPreviewInput = z.output<typeof modelOnboardingPreviewSchema>;
export type ModelOnboardingResult = {
  id: string;
  model_name: string;
  config_epoch: number;
  expected_nodes: string[];
  local_applied_epoch: number;
  nodes: ConfigReleaseNode[];
  customer_price_id: string;
  observed_revisions: { model: number | null; routes: Record<string, number> } | null;
  current_configuration: "PENDING" | "ACTIVE" | "CHANGED";
};

export type GatewayModelOnboardingResult = ModelOnboardingResult & { request_digest: string };
