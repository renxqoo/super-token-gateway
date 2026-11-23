import { Hono, type Context, type Handler, type MiddlewareHandler } from "hono";
import { bodyLimit } from "hono/body-limit";
import type { SQL } from "bun";
import { z } from "zod";
import { GatewayError, pricingSchema, usageSchema } from "@gateway/contracts";
import {
  canonical,
  DomainError,
  operations,
  type Operation,
  type Pricing,
  type Usage,
} from "@gateway/kernel";
import { createPluginReleaseRepository, getApiKeySpend, resetApiKeySpend } from "@gateway/ledger";
import type {
  AdapterOverrides,
  AsyncTaskRefundEligibility,
  AsyncTaskStatus,
  ChannelKind,
  ChargePolicy,
  createAsyncTasksRepository,
  createCatalogRepository,
  createLedgerEngine,
  createLedgerQueries,
  DeclarativeProviderConfig,
  LotKind,
  PluginCapability,
  PluginKind,
  PluginPipelineStep,
  PluginReleaseTarget,
  PriceBasis,
  PriceSide,
  ProviderPluginConfig,
  RouteRetryPolicy,
  RoutingStrategy,
  SpendLimitWindow,
  WalletPolicy,
  WasmProviderConfig,
  WebhookPluginConfig,
} from "@gateway/ledger";
import {
  assertSafeEndpointTemplate,
  assertSafeFieldPath,
  builtinProviderRevisionId,
  createWasmtimeExecutor,
  getBuiltinPlugin,
  listBuiltinPlugins,
  resolveWasmtimeLibraryPath,
  resolveProviderPlugin,
  type PluginHook,
  type PluginPipelineEngine,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  createKey,
  encrypt,
  validateAuthCredentials,
  validateBaseUrl,
  validateCidr,
  verifier,
  type ClusterCoordinator,
  type SmartRouter,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import {
  authenticateInternal,
  type InternalPrincipal,
  type InternalRole,
} from "./internal-auth.ts";
import type { SnapshotManager } from "./snapshot.ts";
import { validatePluginRelease } from "./plugin-release-validation.ts";
import { previewProviderRequest } from "./upstream.ts";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type LedgerQueries = ReturnType<typeof createLedgerQueries>;
type AsyncTasksRepo = ReturnType<typeof createAsyncTasksRepository>;

const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
const multiplierIntSchema = z.string().regex(/^[1-9][0-9]{0,12}$/);
const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
const operationEnum = z.enum(operations);
const providerNameSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{1,63}$/);
const headerNameSchema = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/);
const headerValueSchema = z
  .string()
  .max(2048)
  .regex(/^[\t\x20-\x7e]*$/, "Header value must contain only printable ASCII characters");

const safeFieldPathSchema = z
  .string()
  .min(1)
  .max(128)
  .superRefine((val, ctx) => {
    try {
      assertSafeFieldPath(val);
    } catch (err) {
      ctx.addIssue({
        code: "custom",
        message: err instanceof Error ? err.message : "Invalid field path",
      });
    }
  });

const safeEndpointPathSchema = z
  .string()
  .min(1)
  .max(512)
  .superRefine((val, ctx) => {
    try {
      assertSafeEndpointTemplate(val);
    } catch (err) {
      ctx.addIssue({
        code: "custom",
        message: err instanceof Error ? err.message : "Invalid endpoint path template",
      });
    }
  });

const safeRegexSchema = z
  .string()
  .min(1)
  .max(128)
  .superRefine((val, ctx) => {
    if (/\([^)]*[+*][^)]*\)[+*?]/.test(val)) {
      ctx.addIssue({ code: "custom", message: "Nested quantifiers are not allowed in regex" });
      return;
    }
    try {
      new RegExp(val);
    } catch {
      ctx.addIssue({ code: "custom", message: "Invalid regular expression" });
    }
  });

const authSchemeSchema = z.discriminatedUnion("scheme", [
  z.object({ scheme: z.literal("none") }).strict(),
  z.object({ scheme: z.literal("bearer") }).strict(),
  z
    .object({
      scheme: z.literal("header"),
      header_name: headerNameSchema,
      prefix: headerValueSchema.optional(),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("query"),
      param_name: z.string().regex(/^[A-Za-z0-9_.-]{1,64}$/),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("azure_key"),
      default_api_version: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}(?:-preview)?$/)
        .optional(),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("aws_sigv4"),
      service: z.string().regex(/^[a-z0-9-]{1,64}$/),
    })
    .strict(),
  z.object({ scheme: z.literal("gcp_vertex_oauth") }).strict(),
  z
    .object({
      scheme: z.literal("hmac_sha256"),
      signature_header: headerNameSchema,
      timestamp_header: headerNameSchema,
      key_id_header: headerNameSchema.optional(),
      signature_prefix: headerValueSchema.optional(),
      encoding: z.enum(["hex", "base64"]).optional(),
    })
    .strict(),
]);

const bodyTransformRuleSchema = z.discriminatedUnion("op", [
  z
    .object({
      op: z.literal("rename"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z
    .object({
      op: z.literal("move_to"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z.object({ op: z.literal("drop"), path: safeFieldPathSchema }).strict(),
  z
    .object({ op: z.literal("set_default"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
  z.object({ op: z.literal("force_set"), path: safeFieldPathSchema, value: z.unknown() }).strict(),
  z
    .object({
      op: z.literal("clamp"),
      path: safeFieldPathSchema,
      min: z.number().finite().optional(),
      max: z.number().finite().optional(),
    })
    .strict(),
  z
    .object({ op: z.literal("delete_if_equals"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
]);

const responseTransformRuleSchema = z.discriminatedUnion("op", [
  z
    .object({
      op: z.literal("rename"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z.object({ op: z.literal("drop"), path: safeFieldPathSchema }).strict(),
  z
    .object({ op: z.literal("set_default"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
]);

const errorMatchRuleSchema = z
  .object({
    when_http_status: z.array(z.number().int().min(100).max(599)).min(1).max(20).optional(),
    path: safeFieldPathSchema,
    equals: z.union([z.string(), z.number(), z.boolean()]).optional(),
    not_equals: z.union([z.string(), z.number(), z.boolean()]).optional(),
    in: z
      .array(z.union([z.string(), z.number()]))
      .min(1)
      .max(64)
      .optional(),
    regex: safeRegexSchema.optional(),
    retry_kind: z.enum(["retryable", "rate_limit", "auth_error", "client_error"]),
    http_status: z.number().int().min(400).max(599).optional(),
    message_path: safeFieldPathSchema.optional(),
    retry_after_path: safeFieldPathSchema.optional(),
  })
  .strict()
  .refine(
    (r) =>
      r.equals !== undefined ||
      r.not_equals !== undefined ||
      r.in !== undefined ||
      r.regex !== undefined,
    {
      message: "error_rules entry must specify equals, not_equals, in, or regex",
    },
  );

const usageAliasRuleSchema = z
  .object({
    target: z.enum([
      "input_tokens",
      "output_tokens",
      "cached_input_tokens",
      "cache_write_tokens",
      "cache_write_5m_tokens",
      "cache_write_1h_tokens",
      "reasoning_tokens",
      "requests",
      "images",
      "audio_ms",
      "audio_seconds",
      "video_ms",
      "video_seconds",
      "music_ms",
      "characters",
    ]),
    source_path: safeFieldPathSchema,
  })
  .strict();

const endpointTemplateSchema = z
  .object({
    path: safeEndpointPathSchema,
    stream_path: safeEndpointPathSchema.optional(),
    query: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
    stream_query: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
  })
  .strict();

const adapterOverridesSchema = z
  .object({
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    query_params: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
    path_override: safeEndpointPathSchema.optional(),
    request_transforms: z.array(bodyTransformRuleSchema).max(64).optional(),
    response_transforms: z.array(responseTransformRuleSchema).max(64).optional(),
    error_rules: z.array(errorMatchRuleSchema).max(64).optional(),
    usage_aliases: z.array(usageAliasRuleSchema).max(32).optional(),
  })
  .strict();

const wireDialectSchema = z.enum([
  "openai_chat",
  "openai_responses",
  "anthropic_messages",
  "gemini",
  "passthrough",
]);
const usageDialectSchema = z.enum(["openai", "anthropic", "gemini", "custom"]);
const streamFramingSchema = z.enum(["sse", "bedrock_eventstream"]);

const asyncTaskProviderConfigSchema = z
  .object({
    query_path: safeEndpointPathSchema,
    query_param: z
      .string()
      .regex(/^[A-Za-z0-9_.-]{1,64}$/)
      .optional(),
    task_id_path: safeFieldPathSchema.optional(),
    status_path: safeFieldPathSchema.optional(),
    succeeded_values: z.array(z.string().min(1).max(64)).min(1).max(32).optional(),
    failed_values: z.array(z.string().min(1).max(64)).min(1).max(32).optional(),
    error_code_path: safeFieldPathSchema.optional(),
    error_message_path: safeFieldPathSchema.optional(),
    refundable_system_error_codes: z
      .array(z.union([z.string(), z.number()]))
      .max(64)
      .optional(),
    non_refundable_error_codes: z
      .array(z.union([z.string(), z.number()]))
      .max(64)
      .optional(),
    billed_indicator_path: safeFieldPathSchema.optional(),
  })
  .strict();

const endpointKeySet = new Set<string>([...operations, "default"]);
const declarativeProviderConfigSchema = z
  .object({
    base_dialect: wireDialectSchema,
    usage_dialect: usageDialectSchema,
    stream_framing: streamFramingSchema.optional(),
    auth: authSchemeSchema,
    endpoints: z
      .record(z.string(), endpointTemplateSchema)
      .refine((rec) => Object.keys(rec).length > 0, {
        message: "endpoints must define at least one operation or default",
      })
      .refine((rec) => Object.keys(rec).every((k) => endpointKeySet.has(k)), {
        message: "endpoints contains unsupported operation key",
      }),
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    model_field: safeFieldPathSchema.nullable().optional(),
    stream_field: safeFieldPathSchema.nullable().optional(),
    include_stream_usage_option: z.boolean().optional(),
    request_transforms: z.array(bodyTransformRuleSchema).max(64).optional(),
    response_transforms: z.array(responseTransformRuleSchema).max(64).optional(),
    error_rules: z.array(errorMatchRuleSchema).max(64).optional(),
    usage_aliases: z.array(usageAliasRuleSchema).max(32).optional(),
    reasoning_field: safeFieldPathSchema.optional(),
    async_task_config: asyncTaskProviderConfigSchema.optional(),
  })
  .strict();

const wasmProviderConfigSchema = z
  .object({
    base_dialect: wireDialectSchema,
    usage_dialect: usageDialectSchema,
    stream_framing: streamFramingSchema.optional(),
    auth: authSchemeSchema,
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    hooks: z
      .array(z.enum(["adapt_request", "adapt_response", "adapt_stream_event"]))
      .min(1)
      .max(3)
      .refine((hooks) => hooks.includes("adapt_request"), {
        message: "WASM provider hooks must include adapt_request",
      }),
    grants: z.array(z.literal("extract_usage")).max(4).optional(),
    fuel_budget: z.number().int().min(10_000).max(50_000_000).optional(),
    max_memory_bytes: z
      .number()
      .int()
      .min(65_536)
      .max(64 * 1024 * 1024)
      .optional(),
    failure_policy: z.literal("FAIL_CLOSED").optional(),
  })
  .strict();

const sourceRevisionSchema = z.number().int().min(1).max(2_147_483_647).optional();

const upsertProjectSchema = z
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

const upsertApiKeySchema = z
  .object({
    id: z.uuid().optional(),
    project_id: z.uuid(),
    name: z.string().min(1).max(120).default("default"),
    raw_key: z.string().min(16).max(256).optional(),
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

const resetApiKeySpendSchema = z
  .object({
    reason: z.string().max(500).optional(),
  })
  .strict();

const upsertPricingOverrideSchema = z
  .object({
    id: z.uuid().optional(),
    project_id: z.uuid(),
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

const upsertAccountSchema = z
  .object({
    id: z.uuid().optional(),
    name: z.string().min(1).max(120),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
    expected_state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).optional(),
    source_revision: sourceRevisionSchema,
  })
  .strict();

const upsertChannelSchema = z
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

const upsertModelSchema = z
  .object({
    name: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/),
    operations: z.array(operationEnum).min(1),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32768),
    enabled: z.boolean().default(true),
    source_revision: sourceRevisionSchema,
  })
  .strict();

const createPriceSchema = z
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

const pluginStepSchema = z
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

const upsertRouteSchema = z
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

const webhookConfigSchema = z
  .object({
    url: z.url().max(1000),
    headers: z.record(headerNameSchema, headerValueSchema).optional(),
    timeout_ms: z.number().int().min(50).max(30_000).optional(),
    stream_mode: z.enum(["HOLD_UNTIL_APPROVED", "STREAM_AND_CUTOFF"]).optional(),
    window_chars: z.number().int().min(1).max(4096).optional(),
    window_ms: z.number().int().min(10).max(5_000).optional(),
    max_buffer_bytes: z.number().int().min(64).max(16384).optional(),
    max_concurrency: z.number().int().min(1).max(256).optional(),
    max_response_bytes: z
      .number()
      .int()
      .min(1024)
      .max(4 * 1024 * 1024)
      .optional(),
    failure_policy: z.enum(["FAIL_CLOSED", "FAIL_OPEN"]).optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
    circuit_failure_threshold: z.number().int().min(1).max(100).optional(),
    circuit_open_ms: z.number().int().min(1000).max(300_000).optional(),
  })
  .strict();

const pluginManifestSchema = z
  .object({
    hooks: z
      .array(z.enum(["on_request", "on_response", "on_stream_chunk", "on_stream_end"]))
      .max(4)
      .optional(),
    max_memory_bytes: z
      .number()
      .int()
      .min(65_536)
      .max(64 * 1024 * 1024)
      .optional(),
    fuel_budget: z.number().int().min(10_000).max(50_000_000).optional(),
    fuel_per_byte: z.number().int().min(0).max(5_000).optional(),
    max_input_bytes: z
      .number()
      .int()
      .min(1024)
      .max(4 * 1024 * 1024)
      .optional(),
    deterministic_text_only: z.boolean().optional(),
    window_bytes: z.number().int().min(0).max(4096).optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
    failure_policy: z.enum(["FAIL_CLOSED", "FAIL_OPEN"]).optional(),
  })
  .strict();

const upsertPluginSchema = z
  .object({
    id: z.uuid().optional(),
    name: providerNameSchema,
    version: z.string().min(1).max(64),
    capability: z.enum(["CONTENT", "PROVIDER"]).default("CONTENT"),
    kind: z.enum(["WASM", "WEBHOOK", "DECLARATIVE"]),
    wasm_bytes_base64: z.string().min(4).optional(),
    webhook_config: webhookConfigSchema.nullable().optional(),
    provider_config: z
      .union([declarativeProviderConfigSchema, wasmProviderConfigSchema])
      .nullable()
      .optional(),
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: pluginManifestSchema.default({}),
  })
  .strict();

const installBuiltinPluginSchema = z
  .object({
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: pluginManifestSchema.optional(),
    provider_config: declarativeProviderConfigSchema.partial().optional(),
  })
  .strict();

const pluginReleaseSchema = z
  .object({
    id: z.uuid(),
    expected_epoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    targets: z
      .array(
        z.union([
          z.object({ channel_id: z.uuid(), provider_plugin_id: z.uuid() }).strict(),
          z.object({ plugin_id: z.uuid() }).strict(),
          z
            .object({ route_id: z.uuid(), plugin_pipeline: z.array(pluginStepSchema).max(20) })
            .strict(),
        ]),
      )
      .min(1)
      .max(50),
    samples: z
      .record(
        z.uuid(),
        z
          .object({
            request: z.record(z.string(), z.unknown()).optional(),
            response: z.record(z.string(), z.unknown()).optional(),
            event: z
              .object({ name: z.string().max(256), data: z.string().max(65_536) })
              .strict()
              .optional(),
            text: z.string().max(65_536).optional(),
            webhook_reply: z.record(z.string(), z.unknown()).optional(),
          })
          .strict(),
      )
      .default({}),
  })
  .strict();

const rollbackPluginReleaseSchema = z
  .object({
    id: z.uuid(),
    expected_epoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    samples: pluginReleaseSchema.shape.samples,
  })
  .strict();

const testPluginSchema = z
  .object({
    hook: z.enum(["on_request", "on_response", "on_stream_chunk", "on_stream_end"]),
    text: z.string().max(65_536),
    model: z.string().min(1).max(150).default("test-model"),
    operation: operationEnum.default("chat"),
  })
  .strict();

const previewProviderSchema = z
  .object({
    provider: providerNameSchema,
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000),
    operation: operationEnum.default("chat"),
    upstream_model: z.string().min(1).max(200),
    stream: z.boolean().default(false),
    client_body: z.record(z.string(), z.unknown()),
    channel_adapter_config: adapterOverridesSchema.optional(),
    adapter_overrides: adapterOverridesSchema.optional(),
    mock_upstream_response: z
      .object({
        status: z.number().int().min(100).max(599).default(200),
        headers: z.record(z.string(), z.string()).optional(),
        body: z.record(z.string(), z.unknown()),
      })
      .strict()
      .optional(),
  })
  .strict();

const creditWalletSchema = z
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

const creditPolicySchema = z
  .object({
    currency: currencySchema.default("USD"),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema,
    policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
  })
  .strict();

const refundChargeSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const adjustChargeSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const adjustWalletSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    lot_kind: z.enum(["CASH", "GRANT"]).optional(),
    force_debt: z.boolean().optional(),
    reason: z.string().min(1).max(500),
    actor: z.string().min(1).max(120).optional(),
  })
  .strict();

const setWalletFreezeSchema = z
  .object({
    currency: currencySchema.default("USD"),
    frozen: z.boolean(),
    reason: z.string().min(1).max(500),
    actor: z.string().min(1).max(120).optional(),
  })
  .strict();

const rechargeSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

const adjustSupplierBalanceSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    allow_negative: z.boolean().optional(),
    reason: z.string().min(1).max(500),
    actor: z.string().min(1).max(120).optional(),
  })
  .strict();

const resolveRequestSchema = z
  .object({
    action: z.enum(["SETTLE", "RELEASE"]),
    billing_price_id: z.uuid().optional(),
    usage: usageSchema.optional(),
    supplier_cost_atoms: nonNegativeAtomsSchema.optional(),
    supplier_cost_version_id: z.uuid().optional(),
    reason: z.string().min(1).max(500),
    actor: z.string().min(1).max(120).optional(),
  })
  .strict();

const supplierResolutionFields = {
  reason: resolveRequestSchema.shape.reason,
  actor: resolveRequestSchema.shape.actor,
  evidence_ref: z.string().trim().min(1).max(512),
};

const resolveSupplierAttemptSchema = z.discriminatedUnion("action", [
  z.object({ ...supplierResolutionFields, action: z.literal("RELEASE") }).strict(),
  z
    .object({
      ...supplierResolutionFields,
      action: z.literal("SETTLE"),
      supplier_cost_atoms: nonNegativeAtomsSchema,
    })
    .strict(),
]);

const claimEventsSchema = z
  .object({
    consumer: z.string().min(1).max(120).default("default"),
    limit: z.number().int().min(1).max(500).default(100),
  })
  .strict();

const ackEventsSchema = z
  .object({
    consumer: z.string().min(1).max(120).default("default"),
    lease_token: z.uuid(),
    through_seq: nonNegativeAtomsSchema,
  })
  .strict();

const uuidParam = z.uuid();

function parseBody<T>(schema: z.ZodType<T>, raw: unknown): T {
  const res = schema.safeParse(raw);
  if (!res.success) {
    const first = res.error.issues[0];
    throw new GatewayError(
      400,
      "validation_error",
      first ? `${first.path.join(".") || "body"}: ${first.message}` : "Invalid request payload",
    );
  }
  return res.data;
}

function parseUuid(value: string | undefined, label: string): string {
  const res = uuidParam.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

type AdminEnv = { Variables: { principal: InternalPrincipal } };
type AdminContext = Context<AdminEnv>;

function resolutionActor(c: AdminContext, value?: string): string {
  const principal = c.get("principal");
  if (!principal.legacy) return principal.id;
  if (!value)
    throw new GatewayError(400, "validation_error", "actor is required for legacy credentials");
  return value;
}

function requireConsumer(c: AdminContext, consumer: string): void {
  const principal = c.get("principal");
  if (!principal.legacy && !principal.consumers.includes(consumer)) {
    throw new GatewayError(403, "forbidden", "Consumer is outside the credential scope");
  }
}

export function createInternalApi(deps: {
  db: SQL;
  onAuditFailure: () => void;
  config: GatewayConfig;
  catalog: CatalogRepo;
  ledger: LedgerEngine;
  queries: LedgerQueries;
  asyncTasks: AsyncTasksRepo;
  snapshot: SnapshotManager;
  router: SmartRouter;
  cluster: ClusterCoordinator;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
}) {
  const app = new Hono<AdminEnv>();
  const pluginReleases = createPluginReleaseRepository(deps.db);
  const requestBodies = new Map<string, { schema: z.ZodType; required: boolean }>();
  const routeRoles = new Map<string, InternalRole>();

  const checkBodySize = bodyLimit({
    maxSize: deps.config.maxBodyBytes,
    onError: (c) =>
      c.json(
        { error: { code: "body_too_large", message: "Payload exceeds the configured limit" } },
        413,
      ),
  });

  function authorize(
    method: string,
    path: string,
    role: InternalRole,
  ): MiddlewareHandler<AdminEnv> {
    const action = `${method} ${path}`;
    routeRoles.set(action, role);
    return async (c, next) => {
      const principal = authenticateInternal(
        c.req.header("authorization"),
        deps.config.internalCredentials,
        deps.config.adminToken,
      );
      if (!principal)
        return c.json(
          { error: { code: "unauthorized", message: "Invalid internal credential" } },
          401,
        );
      if (!principal.roles.includes(role))
        return c.json({ error: { code: "forbidden", message: "Insufficient internal role" } }, 403);
      c.set("principal", principal);
      if (method === "GET") return next();
      const requestId = crypto.randomUUID();
      const target = path.replace(/:([A-Za-z0-9]+)/g, (parameter, name: string) => {
        const value = c.req.param(name);
        return value && /^[A-Za-z0-9_.-]{1,120}$/.test(value) ? value : parameter;
      });
      try {
        await deps.db`WITH started AS (
          INSERT INTO public.admin_audit_events (request_id, phase, actor, action, target)
          VALUES (${requestId}, 'STARTED', ${principal.id}, ${action}, ${target})
        ) INSERT INTO public.admin_audit_pending (request_id) VALUES (${requestId})`;
      } catch {
        deps.onAuditFailure();
        return c.json(
          { error: { code: "audit_unavailable", message: "Administrative audit is unavailable" } },
          503,
        );
      }
      c.header("x-admin-request-id", requestId);
      await next();
      try {
        await deps.db`WITH completed AS (
          INSERT INTO public.admin_audit_events (request_id, phase, actor, action, target, status)
          VALUES (${requestId}, 'COMPLETED', ${principal.id}, ${action}, ${target}, ${c.res.status})
        ) DELETE FROM public.admin_audit_pending WHERE request_id = ${requestId}`;
      } catch {
        deps.onAuditFailure();
        console.error(
          JSON.stringify({
            service: "gateway",
            event: "admin_audit_completion_failed",
            request_id: requestId,
          }),
        );
      }
    };
  }

  function route(
    method: "GET" | "POST",
    path: string,
    role: InternalRole,
    handle: Handler<AdminEnv>,
  ): void {
    app.on(method, path, authorize(method, path, role), handle);
  }

  function jsonRoute<T>(
    method: "POST" | "PUT",
    path: string,
    role: InternalRole,
    schema: z.ZodType<T>,
    handle: (c: AdminContext, body: T) => ReturnType<Handler<AdminEnv>>,
  ): void {
    requestBodies.set(`${method} ${path}`, { schema, required: true });
    app.on(method, path, authorize(method, path, role), checkBodySize, async (c) =>
      handle(c, parseBody(schema, await c.req.json())),
    );
  }

  const onConfigUpdated = async (epoch: number) => {
    deps.cluster.publishConfigEpoch(epoch);
    await deps.snapshot.refreshTo(epoch).catch(() => {
      console.error(
        JSON.stringify({ service: "gateway", event: "snapshot_refresh_after_write_failed", epoch }),
      );
    });
  };

  function extractPgErrno(err: unknown): string | null {
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

  app.onError((err, c) => {
    const pgErrno = extractPgErrno(err);
    if (pgErrno) {
      if (pgErrno === "23505") {
        return c.json(
          { error: { code: "conflict", message: "A conflicting record already exists" } },
          409,
        );
      }
      if (pgErrno === "23503") {
        return c.json(
          { error: { code: "reference_error", message: "Referenced entity does not exist" } },
          400,
        );
      }
      if (pgErrno.startsWith("08") || ["40P01", "40001", "55P03", "57014"].includes(pgErrno)) {
        return c.json(
          {
            error: {
              code: "ledger_unavailable",
              message: "The ledger operation could not complete",
            },
          },
          503,
        );
      }
      return c.json(
        { error: { code: "ledger_error", message: "The ledger operation failed" } },
        500,
      );
    }
    if (err instanceof DomainError && ["not_found", "wallet_not_found"].includes(err.code)) {
      return c.json({ error: { code: err.code, message: err.message } }, 404);
    }
    if (
      err instanceof DomainError &&
      [
        "plugin_revision_conflict",
        "plugin_release_conflict",
        "stale_revision",
        "price_version_conflict",
        "reconcile_required_conflict",
        "insufficient_balance",
        "credit_limit_exceeded",
        "supplier_insufficient_balance",
        "adjustment_conflict",
      ].includes(err.code)
    ) {
      return c.json({ error: { code: err.code, message: err.message } }, 409);
    }
    const status = err instanceof GatewayError ? err.status : 400;
    const code =
      err instanceof GatewayError ||
      (err instanceof DomainError &&
        ["invalid_plugin_binding", "invalid_channel_secret", "invalid_price_override"].includes(
          err.code,
        ))
        ? err.code
        : "internal_api_error";
    return c.json({ error: { code, message: err.message } }, status as 400);
  });

  route("GET", "/internal/v1/health", "ops", (c) =>
    c.json({
      ok: true,
      epoch: deps.snapshot.isLoaded() ? deps.snapshot.get().epoch : 0,
      redis_connected: deps.cluster.isRedisConnected(),
      channels: deps.router.snapshot(),
    }),
  );

  route("GET", "/internal/v1/health/channels", "ops", (c) =>
    c.json({ channels: deps.router.snapshot() }),
  );

  jsonRoute("PUT", "/internal/v1/projects", "catalog", upsertProjectSchema, async (c, body) => {
    const rec = await deps.catalog.upsertProject({
      ...(body.id ? { id: body.id } : {}),
      external_ref: body.external_ref ?? null,
      name: body.name,
      currency: body.currency,
      rpm: body.rpm,
      tpm: body.tpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  jsonRoute("PUT", "/internal/v1/api-keys", "security", upsertApiKeySchema, async (c, body) => {
    const validatedCidrs = body.allowed_cidrs.map((cidr) => validateCidr(cidr));
    const rawKey = body.raw_key ?? createKey();
    const rec = await deps.catalog.upsertApiKey({
      ...(body.id ? { id: body.id } : {}),
      project_id: body.project_id,
      name: body.name,
      verifier: verifier(rawKey, deps.config.keyPepper),
      prefix: rawKey.slice(0, 12),
      role: body.role,
      allowed_cidrs: validatedCidrs,
      models: body.models,
      operations: body.operations,
      enabled: body.enabled,
      expires_at: body.expires_at ?? null,
      spend_limit_atoms: body.spend_limit_atoms ?? null,
      spend_limit_window: body.spend_limit_window as SpendLimitWindow,
      ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
    });
    await onConfigUpdated(rec.config_epoch);
    const { created, verifier: _verifier, ...publicRec } = rec;
    return c.json(created ? { ...publicRec, key: rawKey } : publicRec);
  });

  route("GET", "/internal/v1/api-keys/:keyId/spend", "auditor", async (c) => {
    const keyId = parseUuid(c.req.param("keyId"), "keyId");
    const spend = await getApiKeySpend(deps.db, keyId);
    if (!spend) return c.json({ error: { code: "not_found", message: "API key not found" } }, 404);
    return c.json(spend);
  });

  jsonRoute(
    "POST",
    "/internal/v1/api-keys/:keyId/reset-spend",
    "security",
    resetApiKeySpendSchema,
    async (c) => {
      const keyId = parseUuid(c.req.param("keyId"), "keyId");
      const spend = await resetApiKeySpend(deps.db, keyId);
      if (!spend) {
        return c.json({ error: { code: "not_found", message: "API key not found" } }, 404);
      }
      return c.json(spend);
    },
  );

  route("GET", "/internal/v1/upstream-accounts", "auditor", async (c) => {
    const items = await deps.catalog.listUpstreamAccounts();
    return c.json({ items });
  });

  jsonRoute(
    "PUT",
    "/internal/v1/upstream-accounts",
    "catalog",
    upsertAccountSchema,
    async (c, body) => {
      const rec = await deps.catalog.upsertUpstreamAccount({
        ...(body.id ? { id: body.id } : {}),
        name: body.name,
        ...(body.state !== undefined ? { state: body.state } : {}),
        ...(body.expected_state !== undefined ? { expected_state: body.expected_state } : {}),
        ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
      });
      await onConfigUpdated(rec.config_epoch);
      return c.json(rec);
    },
  );

  route("GET", "/internal/v1/channels", "auditor", async (c) => {
    const items = await deps.catalog.listChannels();
    return c.json({ items });
  });

  jsonRoute("PUT", "/internal/v1/channels", "catalog", upsertChannelSchema, async (c, body) => {
    const id = body.id ?? crypto.randomUUID();
    const kind = body.kind as ChannelKind;
    validateBaseUrl(
      body.base_url,
      deps.config.allowPrivateUpstreams,
      deps.config.privateUpstreamCidrs,
    );
    const snap = deps.snapshot.isLoaded() ? deps.snapshot.get() : null;
    const existing = snap?.channels.get(id);
    const providerPluginId =
      body.provider_plugin_id ?? (existing?.kind === kind ? existing.provider_plugin_id : null);
    const resolved = resolveProviderPlugin(kind, providerPluginId, snap);
    if (!resolved) {
      throw new GatewayError(400, "unknown_provider", `Unknown provider plugin: ${kind}`);
    }
    let secretCiphertext: string | undefined;
    if (body.api_key !== undefined) {
      validateAuthCredentials(resolved.config.auth, body.api_key);
      secretCiphertext = encrypt(body.api_key, deps.config.encryptionKey, id);
    } else if (!body.id) {
      validateAuthCredentials(resolved.config.auth, "");
      secretCiphertext = encrypt("", deps.config.encryptionKey, id);
    } else if (!existing && resolved.config.auth.scheme === "none") {
      secretCiphertext = encrypt("", deps.config.encryptionKey, id);
    }
    const rec = await deps.catalog.upsertChannel({
      id,
      account_id: body.account_id,
      name: body.name,
      kind,
      ...(body.provider_plugin_id === undefined && body.id && existing
        ? {}
        : {
            provider_plugin_id:
              resolved.source === "builtin"
                ? builtinProviderRevisionId(resolved.preset.digest)
                : resolved.plugin_id,
          }),
      base_url: body.base_url,
      ...(secretCiphertext !== undefined ? { secret_ciphertext: secretCiphertext } : {}),
      rpm: body.rpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      routing_policy: body.routing_policy,
      adapter_config: body.adapter_config as AdapterOverrides,
      ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, secret_ciphertext: undefined });
  });

  jsonRoute("PUT", "/internal/v1/models", "catalog", upsertModelSchema, async (c, body) => {
    const rec = await deps.catalog.upsertModel({
      name: body.name,
      operations: body.operations as Operation[],
      routing_strategy: body.routing_strategy as RoutingStrategy,
      max_output_tokens: body.max_output_tokens,
      enabled: body.enabled,
      ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  route("GET", "/internal/v1/prices/:priceId", "auditor", async (c) => {
    const priceId = parseUuid(c.req.param("priceId"), "priceId");
    const rec = await deps.catalog.getPriceVersion(priceId);
    if (!rec) {
      return c.json({ error: { code: "not_found", message: "Price version not found" } }, 404);
    }
    return c.json(rec);
  });

  jsonRoute("POST", "/internal/v1/prices", "finance", createPriceSchema, async (c, body) => {
    const rec = await deps.catalog.createPriceVersion({
      ...(body.id ? { id: body.id } : {}),
      side: body.side as PriceSide,
      currency: body.currency,
      basis: body.basis as PriceBasis,
      rates: body.rates as Pricing["rates"],
      ceilings: body.ceilings,
    });
    if (!rec.replayed) await onConfigUpdated(rec.config_epoch);
    return c.json(rec, rec.replayed ? 200 : 201);
  });

  jsonRoute(
    "PUT",
    "/internal/v1/pricing-overrides",
    "finance",
    upsertPricingOverrideSchema,
    async (c, body) => {
      const rec = await deps.catalog.upsertPricingOverride({
        ...(body.id ? { id: body.id } : {}),
        project_id: body.project_id,
        model_name: body.model_name,
        operation: body.operation,
        override_kind: body.override_kind,
        billing_price_id: body.billing_price_id ?? null,
        multiplier_numerator: body.multiplier_numerator ?? null,
        multiplier_denominator: body.multiplier_denominator ?? null,
        enabled: body.enabled,
        ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
      });
      await onConfigUpdated(rec.config_epoch);
      return c.json(rec);
    },
  );

  route("GET", "/internal/v1/pricing-overrides", "auditor", async (c) => {
    const projectId = c.req.query("project_id");
    const items = await deps.catalog.listPricingOverrides(
      projectId ? parseUuid(projectId, "project_id") : undefined,
    );
    return c.json({ items });
  });

  jsonRoute("PUT", "/internal/v1/routes", "catalog", upsertRouteSchema, async (c, body) => {
    const rec = await deps.catalog.upsertRoute({
      ...(body.id ? { id: body.id } : {}),
      model_name: body.model_name,
      channel_id: body.channel_id,
      upstream_model: body.upstream_model,
      operations: body.operations as Operation[],
      priority: body.priority,
      weight: body.weight,
      billing_price_id: body.billing_price_id,
      cost_version_id: body.cost_version_id,
      request_policy: body.request_policy,
      adapter_overrides: body.adapter_overrides as AdapterOverrides,
      ...(body.plugin_pipeline
        ? { plugin_pipeline: body.plugin_pipeline as PluginPipelineStep[] }
        : {}),
      retry_policy: body.retry_policy as RouteRetryPolicy,
      enabled: body.enabled,
      ...(body.source_revision !== undefined ? { source_revision: body.source_revision } : {}),
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  jsonRoute("PUT", "/internal/v1/plugins", "security", upsertPluginSchema, async (c, body) => {
    const capability = body.capability as PluginCapability;
    let validatedProviderConfig: ProviderPluginConfig | null = null;

    if (capability === "CONTENT") {
      if (body.kind === "DECLARATIVE") {
        throw new GatewayError(
          400,
          "validation_error",
          "CONTENT plugins must use kind WASM or WEBHOOK",
        );
      }
      if (body.provider_config) {
        throw new GatewayError(
          400,
          "validation_error",
          "CONTENT plugins cannot define provider_config",
        );
      }
    } else {
      if (body.kind === "WEBHOOK") {
        throw new GatewayError(
          400,
          "validation_error",
          "PROVIDER plugins must use kind DECLARATIVE or WASM",
        );
      }
      if (!body.provider_config) {
        throw new GatewayError(
          400,
          "validation_error",
          "provider_config is required for PROVIDER plugins",
        );
      }
      validatedProviderConfig =
        body.kind === "DECLARATIVE"
          ? (parseBody(
              declarativeProviderConfigSchema,
              body.provider_config,
            ) as DeclarativeProviderConfig)
          : (parseBody(wasmProviderConfigSchema, body.provider_config) as WasmProviderConfig);
    }

    if (body.kind === "WEBHOOK") {
      if (!body.webhook_config) {
        throw new GatewayError(400, "validation_error", "webhook_config is required for WEBHOOK");
      }
      validateBaseUrl(
        body.webhook_config.url,
        deps.config.allowPrivateUpstreams,
        deps.config.privateUpstreamCidrs,
      );
    }

    const wasmBytes = body.wasm_bytes_base64
      ? new Uint8Array(Buffer.from(body.wasm_bytes_base64, "base64"))
      : null;
    if (body.kind === "WASM") {
      if (!wasmBytes || wasmBytes.byteLength === 0) {
        throw new GatewayError(400, "validation_error", "wasm_bytes_base64 is required for WASM");
      }
      let mod: WebAssembly.Module;
      try {
        mod = new WebAssembly.Module(wasmBytes);
      } catch (err) {
        throw new GatewayError(
          400,
          "validation_error",
          `Invalid WebAssembly binary: ${err instanceof Error ? err.message : "compile failed"}`,
        );
      }
      if (WebAssembly.Module.imports(mod).length > 0) {
        throw new GatewayError(
          400,
          "validation_error",
          "WASM plugin must not declare host imports",
        );
      }
      if (
        capability === "PROVIDER" &&
        validatedProviderConfig &&
        "hooks" in validatedProviderConfig
      ) {
        const exportMap = new Map(WebAssembly.Module.exports(mod).map((e) => [e.name, e.kind]));
        if (exportMap.get("memory") !== "memory" || exportMap.get("stg_alloc") !== "function") {
          throw new GatewayError(
            400,
            "validation_error",
            "PROVIDER WASM plugin must export memory and stg_alloc",
          );
        }
        for (const hook of validatedProviderConfig.hooks) {
          if (exportMap.get(hook) !== "function") {
            throw new GatewayError(
              400,
              "validation_error",
              `PROVIDER WASM plugin missing exported hook function: ${hook}`,
            );
          }
        }
      }
    }

    const rec = await deps.catalog.upsertPlugin({
      ...(body.id ? { id: body.id } : {}),
      name: body.name,
      version: body.version,
      capability,
      kind: body.kind as PluginKind,
      wasm_bytes: body.kind === "WASM" ? wasmBytes : null,
      webhook_config:
        body.kind === "WEBHOOK"
          ? ((body.webhook_config ?? null) as WebhookPluginConfig | null)
          : null,
      provider_config: validatedProviderConfig,
      signature: body.signature ?? null,
      signing_key_id: body.signing_key_id ?? null,
      manifest: body.manifest,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, wasm_bytes: undefined });
  });

  route("GET", "/internal/v1/plugins", "security", async (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    const items = await deps.catalog.listPlugins(cap);
    return c.json({ items });
  });

  function releaseDigest(value: unknown): string {
    return new Bun.CryptoHasher("sha256").update(canonical(value)).digest("hex");
  }

  async function replayRelease(id: string, actor: string, digest: string) {
    const existing = await pluginReleases.get(id);
    if (!existing) return null;
    if (existing.actor !== actor || existing.request_digest !== digest) {
      throw new GatewayError(
        409,
        "plugin_release_conflict",
        "Release identifier already belongs to another request",
      );
    }
    return existing;
  }

  async function reportRelease(
    release: NonNullable<Awaited<ReturnType<typeof pluginReleases.get>>>,
  ) {
    const { request_digest: _, ...record } = release;
    return {
      ...record,
      local_applied_epoch: deps.snapshot.get().epoch,
      nodes: await pluginReleases.confirmations(release),
    };
  }

  function releaseTargets(
    targets: z.output<typeof pluginReleaseSchema.shape.targets>,
  ): PluginReleaseTarget[] {
    return targets.map((target) =>
      "plugin_id" in target ? { ...target, available: true } : (target as PluginReleaseTarget),
    );
  }

  function requireReleaseEpoch(expected: number) {
    const snapshot = deps.snapshot.get();
    if (snapshot.epoch !== expected || !deps.snapshot.isFresh()) {
      throw new GatewayError(
        409,
        "plugin_release_conflict",
        "Validate against the current configuration epoch",
      );
    }
    return snapshot;
  }

  let activePreflights = 0;
  async function preflight(
    snapshot: ReturnType<typeof requireReleaseEpoch>,
    targets: PluginReleaseTarget[],
    samples: z.output<typeof pluginReleaseSchema.shape.samples>,
  ) {
    if (activePreflights >= 2) {
      throw new GatewayError(503, "plugin_preflight_busy", "Plugin validation capacity is full");
    }
    activePreflights++;
    let executor: WasmRuntimeExecutor | null = null;
    try {
      const library = resolveWasmtimeLibraryPath(deps.config.wasmLibPath);
      executor = createWasmtimeExecutor({
        poolSize: 1,
        coldCompileTimeoutMs: 5000,
        ...(library ? { wasmtimeLibraryPath: library } : {}),
      });
      return await validatePluginRelease({
        snapshot,
        targets,
        getPlugin: deps.catalog.getPlugin,
        decrypt: (channel) => deps.snapshot.decryptChannelSecret(channel),
        executor,
        signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem,
        samples,
      });
    } catch (error) {
      if (error instanceof GatewayError) throw error;
      throw new GatewayError(422, "plugin_preflight_failed", "Plugin execution preflight failed");
    } finally {
      executor?.close();
      activePreflights--;
    }
  }

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases/validate",
    "security",
    pluginReleaseSchema,
    async (c, body) => {
      const snapshot = requireReleaseEpoch(body.expected_epoch);
      const changes = await preflight(snapshot, releaseTargets(body.targets), body.samples);
      return c.json({ config_epoch: snapshot.epoch, changes });
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases",
    "security",
    pluginReleaseSchema,
    async (c, body) => {
      const actor = c.get("principal").id;
      const digest = releaseDigest(body);
      const existing = await replayRelease(body.id, actor, digest);
      if (existing) return c.json(await reportRelease(existing));
      let changes;
      try {
        const snapshot = requireReleaseEpoch(body.expected_epoch);
        changes = await preflight(snapshot, releaseTargets(body.targets), body.samples);
      } catch (error) {
        const replay = await replayRelease(body.id, actor, digest);
        if (replay) return c.json(await reportRelease(replay));
        throw error;
      }
      const result = await pluginReleases.publish({
        id: body.id,
        actor,
        request_digest: digest,
        expected_epoch: body.expected_epoch,
        changes,
        rollback_of: null,
      });
      await onConfigUpdated(result.config_epoch);
      return c.json(await reportRelease(result));
    },
  );

  route("GET", "/internal/v1/plugin-releases/:releaseId", "security", async (c) => {
    const release = await pluginReleases.get(parseUuid(c.req.param("releaseId"), "releaseId"));
    if (!release) throw new GatewayError(404, "not_found", "Plugin release not found");
    return c.json(await reportRelease(release));
  });

  jsonRoute(
    "POST",
    "/internal/v1/plugin-releases/:releaseId/rollback",
    "security",
    rollbackPluginReleaseSchema,
    async (c, body) => {
      const actor = c.get("principal").id;
      const releaseId = parseUuid(c.req.param("releaseId"), "releaseId");
      const digest = releaseDigest({ ...body, rollback_of: releaseId });
      const existing = await replayRelease(body.id, actor, digest);
      if (existing) return c.json(await reportRelease(existing));
      const snapshot = requireReleaseEpoch(body.expected_epoch);
      const release = await pluginReleases.get(releaseId);
      if (!release) throw new GatewayError(404, "not_found", "Plugin release not found");
      await preflight(
        snapshot,
        release.changes.map(({ before }) => before),
        body.samples,
      );
      const result = await pluginReleases.publish({
        id: body.id,
        actor,
        request_digest: digest,
        expected_epoch: body.expected_epoch,
        changes: release.changes.map(({ before, after }) => ({ before: after, after: before })),
        rollback_of: releaseId,
      });
      await onConfigUpdated(result.config_epoch);
      return c.json(await reportRelease(result));
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugins/:pluginId/revoke",
    "security",
    z.object({ reason: z.string().trim().min(1).max(1024) }).strict(),
    async (c, body) => {
      const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
      const revoked = await pluginReleases.revoke(pluginId, c.get("principal").id, body.reason);
      if (!revoked) throw new GatewayError(404, "not_found", "Plugin revision not found");
      await onConfigUpdated(revoked.config_epoch);
      return c.json(revoked);
    },
  );

  route("GET", "/internal/v1/plugins/builtins", "security", (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    return c.json({ items: listBuiltinPlugins(cap) });
  });

  requestBodies.set("POST /internal/v1/plugins/builtins/:name/install", {
    schema: installBuiltinPluginSchema,
    required: false,
  });
  app.on(
    "POST",
    "/internal/v1/plugins/builtins/:name/install",
    authorize("POST", "/internal/v1/plugins/builtins/:name/install", "security"),
    checkBodySize,
    async (c) => {
      const builtin = getBuiltinPlugin(c.req.param("name"));
      if (!builtin) {
        return c.json({ error: { code: "not_found", message: "Built-in plugin not found" } }, 404);
      }
      const rawBody = await c.req.text();
      const parsedBody = rawBody.trim()
        ? parseBody(installBuiltinPluginSchema, JSON.parse(rawBody))
        : parseBody(installBuiltinPluginSchema, {});
      const manifest = parsedBody.manifest
        ? { ...builtin.manifest, ...parsedBody.manifest }
        : { ...builtin.manifest };
      let providerConfig: ProviderPluginConfig | null = null;
      if (builtin.capability === "PROVIDER") {
        const mergedCfg = parsedBody.provider_config
          ? { ...builtin.provider_config, ...parsedBody.provider_config }
          : builtin.provider_config;
        providerConfig = parseBody(
          declarativeProviderConfigSchema,
          mergedCfg,
        ) as DeclarativeProviderConfig;
      }
      const rec = await deps.catalog.upsertPlugin({
        name: builtin.name,
        version: builtin.version,
        capability: builtin.capability,
        kind: builtin.kind,
        wasm_bytes: builtin.capability === "CONTENT" ? builtin.wasm_bytes : null,
        webhook_config: null,
        provider_config: providerConfig,
        signature: parsedBody.signature ?? null,
        signing_key_id: parsedBody.signing_key_id ?? null,
        manifest,
      });
      await onConfigUpdated(rec.config_epoch);
      return c.json({ ...rec, wasm_bytes: undefined });
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/providers/preview",
    "catalog",
    previewProviderSchema,
    async (c, body) => {
      const snap = deps.snapshot.isLoaded()
        ? deps.snapshot.get()
        : { plugins: new Map(), providerPluginsByName: new Map() };
      const preview = await previewProviderRequest({
        provider: body.provider,
        ...(body.provider_plugin_id !== undefined
          ? { providerPluginId: body.provider_plugin_id }
          : {}),
        baseUrl: body.base_url,
        operation: body.operation as Operation,
        upstreamModel: body.upstream_model,
        stream: body.stream,
        clientBody: body.client_body,
        ...(body.channel_adapter_config
          ? { channelAdapterConfig: body.channel_adapter_config as AdapterOverrides }
          : {}),
        ...(body.adapter_overrides
          ? { routeAdapterOverrides: body.adapter_overrides as AdapterOverrides }
          : {}),
        ...(body.mock_upstream_response
          ? { mockUpstreamResponse: body.mock_upstream_response }
          : {}),
        snapshot: snap,
        wasmExecutor: deps.wasmExecutor,
        ...(deps.config.pluginSigningPublicKeyPem
          ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
          : {}),
      });
      return c.json(preview);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/plugins/:pluginId/test",
    "security",
    testPluginSchema,
    async (c, body) => {
      const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
      const snap = deps.snapshot.get();
      const plugin = snap.plugins.get(pluginId);
      if (!plugin) {
        return c.json(
          { error: { code: "not_found", message: "Enabled plugin not found in active snapshot" } },
          404,
        );
      }
      const res = await deps.pluginEngine.runUnary(
        body.hook as PluginHook,
        [{ plugin_id: plugin.id, priority: 1 }],
        snap.plugins,
        {
          request_id: crypto.randomUUID(),
          project_id: "dry-run",
          model: body.model,
          operation: body.operation,
        },
        body.text,
        c.req.raw.signal,
      );
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:projectId/credits",
    "finance",
    creditWalletSchema,
    async (c, body) => {
      const projectId = parseUuid(c.req.param("projectId"), "projectId");
      const res = await deps.ledger.creditWallet({
        project_id: projectId,
        currency: body.currency,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        kind: body.kind as LotKind,
        ...(body.priority !== undefined ? { priority: body.priority } : {}),
        expires_at: body.expires_at ?? null,
        ...(body.models ? { models: body.models } : {}),
        ...(body.operations ? { operations: body.operations } : {}),
        ...(body.reason ? { reason: body.reason } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:projectId/credit-policy",
    "finance",
    creditPolicySchema,
    async (c, body) => {
      const projectId = parseUuid(c.req.param("projectId"), "projectId");
      const res = await deps.ledger.configureWalletCredit({
        project_id: projectId,
        currency: body.currency,
        expected_version: body.expected_version,
        credit_limit_atoms: body.credit_limit_atoms,
        ...(body.policy ? { policy: body.policy as WalletPolicy } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:projectId/adjustments",
    "finance",
    adjustWalletSchema,
    async (c, body) => {
      const projectId = parseUuid(c.req.param("projectId"), "projectId");
      const res = await deps.ledger.adjustWallet({
        project_id: projectId,
        currency: body.currency,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        ...(body.lot_kind ? { lot_kind: body.lot_kind as LotKind } : {}),
        ...(body.force_debt !== undefined ? { force_debt: body.force_debt } : {}),
        reason: body.reason,
        actor: resolutionActor(c, body.actor),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:projectId/freeze",
    "finance",
    setWalletFreezeSchema,
    async (c, body) => {
      const projectId = parseUuid(c.req.param("projectId"), "projectId");
      const res = await deps.ledger.setWalletFreeze({
        project_id: projectId,
        currency: body.currency,
        frozen: body.frozen,
        reason: body.reason,
        actor: resolutionActor(c, body.actor),
      });
      return c.json(res);
    },
  );

  route("GET", "/internal/v1/wallets/:projectId", "auditor", async (c) => {
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const includeLots = c.req.query("include_lots") === "true";
    const wallet = includeLots
      ? await deps.ledger.getWalletWithLots(projectId, currency)
      : await deps.ledger.getWallet(projectId, currency);
    if (!wallet) return c.json({ error: { code: "not_found", message: "Wallet not found" } }, 404);
    return c.json(wallet);
  });

  jsonRoute(
    "POST",
    "/internal/v1/charges/:chargeId/refunds",
    "finance",
    refundChargeSchema,
    async (c, body) => {
      const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
      const res = await deps.ledger.refundCharge({
        charge_id: chargeId,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        reason: body.reason,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/charges/:chargeId/adjustments",
    "finance",
    adjustChargeSchema,
    async (c, body) => {
      const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
      const res = await deps.ledger.adjustCharge({
        charge_id: chargeId,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        reason: body.reason,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/upstream-accounts/:accountId/recharges",
    "finance",
    rechargeSupplierSchema,
    async (c, body) => {
      const accountId = parseUuid(c.req.param("accountId"), "accountId");
      const res = await deps.ledger.rechargeSupplier({
        account_id: accountId,
        currency: body.currency,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        ...(body.reason ? { reason: body.reason } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/upstream-accounts/:accountId/adjustments",
    "finance",
    adjustSupplierBalanceSchema,
    async (c, body) => {
      const accountId = parseUuid(c.req.param("accountId"), "accountId");
      const res = await deps.ledger.adjustSupplierBalance({
        account_id: accountId,
        currency: body.currency,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        ...(body.allow_negative !== undefined ? { allow_negative: body.allow_negative } : {}),
        reason: body.reason,
        actor: resolutionActor(c, body.actor),
      });
      if (res.config_epoch !== undefined) {
        await onConfigUpdated(res.config_epoch);
      }
      return c.json(res);
    },
  );

  route("GET", "/internal/v1/upstream-accounts/:accountId/balance", "auditor", async (c) => {
    const accountId = parseUuid(c.req.param("accountId"), "accountId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const bal = await deps.ledger.getSupplierBalance(accountId, currency);
    return c.json(bal);
  });

  jsonRoute(
    "POST",
    "/internal/v1/requests/:requestId/resolve",
    "finance",
    resolveRequestSchema,
    async (c, body) => {
      const requestId = parseUuid(c.req.param("requestId"), "requestId");
      const res = await deps.ledger.resolveRequest({
        request_id: requestId,
        action: body.action,
        ...(body.billing_price_id ? { billing_price_id: body.billing_price_id } : {}),
        ...(body.usage ? { usage: body.usage as Usage } : {}),
        ...(body.supplier_cost_atoms ? { supplier_cost_atoms: body.supplier_cost_atoms } : {}),
        ...(body.supplier_cost_version_id
          ? { supplier_cost_version_id: body.supplier_cost_version_id }
          : {}),
        reason: body.reason,
        actor: resolutionActor(c, body.actor),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/requests/:requestId/attempts/:attemptId/resolve",
    "finance",
    resolveSupplierAttemptSchema,
    async (c, body) => {
      const requestId = parseUuid(c.req.param("requestId"), "requestId");
      const attemptId = parseUuid(c.req.param("attemptId"), "attemptId");
      const result = await deps.ledger.resolveSupplierAttempt({
        ...body,
        actor: resolutionActor(c, body.actor),
        request_id: requestId,
        attempt_id: attemptId,
      });
      if (result.ok) return c.json({ replayed: result.replayed, state: result.state });
      const status =
        result.code === "supplier_attempt_not_found"
          ? 404
          : result.code === "supplier_attempt_conflict"
            ? 409
            : 422;
      return c.json(
        { error: { code: result.code, message: "Supplier attempt resolution failed" } },
        status,
      );
    },
  );

  route("GET", "/internal/v1/charges", "auditor", async (c) => {
    const projectId = c.req.query("project_id");
    const apiKeyId = c.req.query("api_key_id");
    const outcome = c.req.query("outcome");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listCharges({
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
      ...(apiKeyId ? { api_key_id: parseUuid(apiKeyId, "api_key_id") } : {}),
      ...(outcome
        ? {
            outcome: parseBody(
              z.enum(["SETTLED", "RELEASED", "DEBT", "MODERATION_BLOCKED", "UNKNOWN_RESOLVED"]),
              outcome,
            ),
          }
        : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/async-tasks", "auditor", async (c) => {
    const projectId = c.req.query("project_id");
    const status = c.req.query("status");
    const eligibility = c.req.query("refund_eligibility");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.asyncTasks.listTasks({
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
      ...(status
        ? {
            status: parseBody(
              z.enum(["QUEUED", "PROCESSING", "SUCCEEDED", "FAILED", "CANCELLED"]),
              status,
            ) as AsyncTaskStatus,
          }
        : {}),
      ...(eligibility
        ? {
            refund_eligibility: parseBody(
              z.enum([
                "PENDING",
                "AUTO_REFUNDED",
                "DENIED_USER_CANCELLED",
                "DENIED_POLICY_OR_CLIENT",
                "DENIED_UPSTREAM_BILLED",
                "DENIED_ABUSE_LIMIT",
                "MANUAL_REVIEW",
                "NOT_APPLICABLE",
              ]),
              eligibility,
            ) as AsyncTaskRefundEligibility,
          }
        : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/funding-operations", "auditor", async (c) => {
    const side = c.req.query("side");
    const entityId = c.req.query("entity_id");
    const projectId = c.req.query("project_id");
    const accountId = c.req.query("account_id");
    const kind = c.req.query("kind");
    const currency = c.req.query("currency");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listFundingOperations({
      ...(side ? { side: parseBody(z.enum(["CUSTOMER", "SUPPLIER"]), side) } : {}),
      ...(entityId ? { entity_id: parseUuid(entityId, "entity_id") } : {}),
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
      ...(accountId ? { account_id: parseUuid(accountId, "account_id") } : {}),
      ...(kind
        ? {
            kind: parseBody(
              z.enum(["CREDIT", "REFUND", "ADJUST", "SUPPLIER_RECHARGE", "SUPPLIER_ADJUST"]),
              kind,
            ),
          }
        : {}),
      ...(currency ? { currency: parseBody(currencySchema, currency) } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/finance/summary", "auditor", async (c) => {
    const projectId = c.req.query("project_id");
    const currency = c.req.query("currency");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const res = await deps.queries.getFinanceSummary({
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
      ...(currency ? { currency: parseBody(currencySchema, currency) } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/ledger-entries", "auditor", async (c) => {
    const side = c.req.query("side");
    const entityId = c.req.query("entity_id");
    const requestId = c.req.query("request_id");
    const kind = c.req.query("kind");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listLedgerEntries({
      ...(side ? { side: parseBody(z.enum(["CUSTOMER", "SUPPLIER"]), side) } : {}),
      ...(entityId ? { entity_id: parseUuid(entityId, "entity_id") } : {}),
      ...(requestId ? { request_id: parseUuid(requestId, "request_id") } : {}),
      ...(kind ? { kind } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/holds", "auditor", async (c) => {
    const state = c.req.query("state");
    const projectId = c.req.query("project_id");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listHolds({
      ...(state
        ? {
            state: parseBody(z.enum(["HELD", "SETTLED", "RELEASED", "UNKNOWN", "CONFLICT"]), state),
          }
        : {}),
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/requests/:requestId", "auditor", async (c) => {
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const res = await deps.queries.getRequestBilling(requestId);
    if (!res.hold && !res.charge) {
      return c.json({ error: { code: "not_found", message: "Request billing not found" } }, 404);
    }
    return c.json(res);
  });

  jsonRoute("POST", "/internal/v1/events/claim", "events", claimEventsSchema, async (c, body) => {
    requireConsumer(c, body.consumer);
    const res = await deps.ledger.claimOutboxEvents(body.consumer, body.limit);
    return c.json(res);
  });

  jsonRoute("POST", "/internal/v1/events/ack", "events", ackEventsSchema, async (c, body) => {
    requireConsumer(c, body.consumer);
    const ok = await deps.ledger.ackOutboxEvents(body.consumer, body.lease_token, body.through_seq);
    return c.json({ ok });
  });

  route("GET", "/internal/v1/audit/conservation", "auditor", async (c) => {
    const audit = await deps.ledger.auditConservation();
    return c.json(audit, audit.ok ? 200 : 500);
  });

  return { app, requestBodies, routeRoles };
}

export type InternalChargePolicy = ChargePolicy;
