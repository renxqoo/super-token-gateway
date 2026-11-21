import { Hono } from "hono";
import { z } from "zod";
import { GatewayError, pricingSchema, usageSchema } from "@gateway/contracts";
import { operations, type Operation, type Pricing, type Usage } from "@gateway/kernel";
import type {
  AdapterOverrides,
  ChannelKind,
  ChargePolicy,
  createCatalogRepository,
  createLedgerEngine,
  createLedgerQueries,
  DeclarativeProviderConfig,
  LotKind,
  PluginCapability,
  PluginKind,
  PluginPipelineStep,
  PriceBasis,
  PriceSide,
  ProviderPluginConfig,
  RouteRetryPolicy,
  RoutingStrategy,
  WalletPolicy,
  WasmProviderConfig,
  WebhookPluginConfig,
} from "@gateway/ledger";
import {
  assertSafeEndpointTemplate,
  assertSafeFieldPath,
  getBuiltinPlugin,
  listBuiltinPlugins,
  resolveProviderPlugin,
  type PluginHook,
  type PluginPipelineEngine,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  createKey,
  encrypt,
  secureEqual,
  validateAuthCredentials,
  validateBaseUrl,
  validateCidr,
  verifier,
  type ClusterCoordinator,
  type SmartRouter,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { SnapshotManager } from "./snapshot.ts";
import { previewProviderRequest } from "./upstream.ts";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type LedgerQueries = ReturnType<typeof createLedgerQueries>;

const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
const operationEnum = z.enum(operations);
const providerNameSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{1,63}$/);
const headerNameSchema = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/);
const headerValueSchema = z
  .string()
  .max(2048)
  .refine((v) => !v.includes("\r") && !v.includes("\n"), {
    message: "Header value must not contain CR/LF",
  });

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
  .refine((r) => r.equals !== undefined || r.in !== undefined || r.regex !== undefined, {
    message: "error_rules entry must specify equals, in, or regex",
  });

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
  })
  .strict();

const upsertAccountSchema = z
  .object({
    id: z.uuid().optional(),
    name: z.string().min(1).max(120),
    state: z.enum(["ACTIVE", "DISABLED", "RECONCILE_REQUIRED"]).default("ACTIVE"),
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
    api_key: z.string().max(16384).default(""),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    enabled: z.boolean().default(true),
    routing_policy: z.record(z.string(), z.unknown()).default({}),
    adapter_config: adapterOverridesSchema.default({}),
  })
  .strict();

const upsertModelSchema = z
  .object({
    name: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,149}$/),
    operations: z.array(operationEnum).min(1),
    routing_strategy: z.enum(["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"]).default("WEIGHTED"),
    max_output_tokens: z.number().int().min(1).max(10_000_000).default(32768),
    enabled: z.boolean().default(true),
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
    plugin_pipeline: z.array(pluginStepSchema).max(20).default([]),
    retry_policy: retryPolicySchema.default({}),
    enabled: z.boolean().default(true),
  })
  .strict();

const webhookConfigSchema = z
  .object({
    url: z.url().max(1000),
    headers: z.record(z.string(), z.string()).optional(),
    timeout_ms: z.number().int().min(50).max(30_000).optional(),
    stream_mode: z.enum(["HOLD_UNTIL_APPROVED", "STREAM_AND_CUTOFF"]).optional(),
    window_chars: z.number().int().min(1).max(65536).optional(),
    window_ms: z.number().int().min(10).max(10_000).optional(),
    max_buffer_bytes: z.number().int().min(64).max(65536).optional(),
    failure_policy: z.enum(["FAIL_CLOSED", "FAIL_OPEN"]).optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
    circuit_failure_threshold: z.number().int().min(1).max(100).optional(),
    circuit_open_ms: z.number().int().min(1000).max(300_000).optional(),
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
    provider_config: z.record(z.string(), z.unknown()).nullable().optional(),
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: z.record(z.string(), z.unknown()).default({}),
    enabled: z.boolean().default(true),
  })
  .strict();

const installBuiltinPluginSchema = z
  .object({
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: z.record(z.string(), z.unknown()).optional(),
    provider_config: z.record(z.string(), z.unknown()).optional(),
    enabled: z.boolean().default(true),
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

const rechargeSupplierSchema = z
  .object({
    currency: currencySchema.default("USD"),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

const resolveRequestSchema = z
  .object({
    action: z.enum(["SETTLE", "RELEASE"]),
    billing_price_id: z.uuid().optional(),
    usage: usageSchema.optional(),
    supplier_cost_atoms: nonNegativeAtomsSchema.optional(),
    reason: z.string().min(1).max(500),
    actor: z.string().min(1).max(120),
  })
  .strict();

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

function parseUuid(value: string, label: string): string {
  const res = uuidParam.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

export function createInternalApi(deps: {
  config: GatewayConfig;
  catalog: CatalogRepo;
  ledger: LedgerEngine;
  queries: LedgerQueries;
  snapshot: SnapshotManager;
  router: SmartRouter;
  cluster: ClusterCoordinator;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
}): Hono {
  const app = new Hono();

  const onConfigUpdated = async (epoch: number) => {
    await deps.snapshot.load();
    deps.cluster.publishConfigEpoch(epoch);
  };

  app.onError((err, c) => {
    const status = err instanceof GatewayError ? err.status : 400;
    const code = err instanceof GatewayError ? err.code : "internal_api_error";
    return c.json({ error: { code, message: err.message } }, status as 400);
  });

  app.use("/internal/v1/*", async (c, next) => {
    const auth = c.req.header("authorization") ?? "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    if (!token || !secureEqual(token, deps.config.adminToken)) {
      return c.json({ error: { code: "unauthorized", message: "Invalid admin token" } }, 401);
    }
    await next();
  });

  app.get("/internal/v1/health", (c) =>
    c.json({
      ok: true,
      epoch: deps.snapshot.isLoaded() ? deps.snapshot.get().epoch : 0,
      redis_connected: deps.cluster.isRedisConnected(),
      channels: deps.router.snapshot(),
    }),
  );

  app.get("/internal/v1/health/channels", (c) => c.json({ channels: deps.router.snapshot() }));

  app.put("/internal/v1/projects", async (c) => {
    const body = parseBody(upsertProjectSchema, await c.req.json());
    const rec = await deps.catalog.upsertProject({
      ...(body.id ? { id: body.id } : {}),
      external_ref: body.external_ref ?? null,
      name: body.name,
      currency: body.currency,
      rpm: body.rpm,
      tpm: body.tpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  app.put("/internal/v1/api-keys", async (c) => {
    const body = parseBody(upsertApiKeySchema, await c.req.json());
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
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, key: rawKey });
  });

  app.put("/internal/v1/upstream-accounts", async (c) => {
    const body = parseBody(upsertAccountSchema, await c.req.json());
    const rec = await deps.catalog.upsertUpstreamAccount({
      ...(body.id ? { id: body.id } : {}),
      name: body.name,
      state: body.state,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  app.put("/internal/v1/channels", async (c) => {
    const body = parseBody(upsertChannelSchema, await c.req.json());
    const id = body.id ?? crypto.randomUUID();
    const kind = body.kind as ChannelKind;
    validateBaseUrl(
      body.base_url,
      deps.config.allowPrivateUpstreams,
      deps.config.privateUpstreamCidrs,
    );
    const snap = deps.snapshot.isLoaded() ? deps.snapshot.get() : null;
    const resolved = resolveProviderPlugin(kind, body.provider_plugin_id ?? null, snap);
    if (!resolved) {
      throw new GatewayError(400, "unknown_provider", `Unknown provider plugin: ${kind}`);
    }
    validateAuthCredentials(resolved.config.auth, body.api_key);
    const secretCiphertext = encrypt(body.api_key, deps.config.encryptionKey, id);
    const rec = await deps.catalog.upsertChannel({
      id,
      account_id: body.account_id,
      name: body.name,
      kind,
      provider_plugin_id: body.provider_plugin_id ?? null,
      base_url: body.base_url,
      secret_ciphertext: secretCiphertext,
      rpm: body.rpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      routing_policy: body.routing_policy,
      adapter_config: body.adapter_config as AdapterOverrides,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, secret_ciphertext: undefined });
  });

  app.put("/internal/v1/models", async (c) => {
    const body = parseBody(upsertModelSchema, await c.req.json());
    const rec = await deps.catalog.upsertModel({
      name: body.name,
      operations: body.operations as Operation[],
      routing_strategy: body.routing_strategy as RoutingStrategy,
      max_output_tokens: body.max_output_tokens,
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  app.post("/internal/v1/prices", async (c) => {
    const body = parseBody(createPriceSchema, await c.req.json());
    const rec = await deps.catalog.createPriceVersion({
      ...(body.id ? { id: body.id } : {}),
      side: body.side as PriceSide,
      currency: body.currency,
      basis: body.basis as PriceBasis,
      rates: body.rates as Pricing["rates"],
      ceilings: body.ceilings,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec, 201);
  });

  app.put("/internal/v1/routes", async (c) => {
    const body = parseBody(upsertRouteSchema, await c.req.json());
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
      plugin_pipeline: body.plugin_pipeline as PluginPipelineStep[],
      retry_policy: body.retry_policy as RouteRetryPolicy,
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  app.put("/internal/v1/plugins", async (c) => {
    const body = parseBody(upsertPluginSchema, await c.req.json());
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
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, wasm_bytes: undefined });
  });

  app.get("/internal/v1/plugins", async (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    const items = await deps.catalog.listPlugins(cap);
    return c.json({ items });
  });

  app.get("/internal/v1/plugins/builtins", (c) => {
    const rawCap = c.req.query("capability");
    const cap = rawCap
      ? (parseBody(z.enum(["CONTENT", "PROVIDER"]), rawCap) as PluginCapability)
      : undefined;
    return c.json({ items: listBuiltinPlugins(cap) });
  });

  app.post("/internal/v1/plugins/builtins/:name/install", async (c) => {
    const builtin = getBuiltinPlugin(c.req.param("name"));
    if (!builtin) {
      return c.json({ error: { code: "not_found", message: "Built-in plugin not found" } }, 404);
    }
    const rawBody = await c.req.text();
    const parsedBody = rawBody.trim()
      ? parseBody(installBuiltinPluginSchema, JSON.parse(rawBody))
      : { enabled: true };
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
      enabled: parsedBody.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, wasm_bytes: undefined });
  });

  app.post("/internal/v1/providers/preview", async (c) => {
    const body = parseBody(previewProviderSchema, await c.req.json());
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
      ...(body.mock_upstream_response ? { mockUpstreamResponse: body.mock_upstream_response } : {}),
      snapshot: snap,
      wasmExecutor: deps.wasmExecutor,
      ...(deps.config.pluginSigningPublicKeyPem
        ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
        : {}),
    });
    return c.json(preview);
  });

  app.post("/internal/v1/plugins/:pluginId/test", async (c) => {
    const pluginId = parseUuid(c.req.param("pluginId"), "pluginId");
    const body = parseBody(testPluginSchema, await c.req.json());
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
  });

  app.post("/internal/v1/wallets/:projectId/credits", async (c) => {
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = parseBody(creditWalletSchema, await c.req.json());
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
  });

  app.post("/internal/v1/wallets/:projectId/credit-policy", async (c) => {
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = parseBody(creditPolicySchema, await c.req.json());
    const res = await deps.ledger.configureWalletCredit({
      project_id: projectId,
      currency: body.currency,
      expected_version: body.expected_version,
      credit_limit_atoms: body.credit_limit_atoms,
      ...(body.policy ? { policy: body.policy as WalletPolicy } : {}),
    });
    return c.json(res);
  });

  app.get("/internal/v1/wallets/:projectId", async (c) => {
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const wallet = await deps.ledger.getWallet(projectId, currency);
    if (!wallet) return c.json({ error: { code: "not_found", message: "Wallet not found" } }, 404);
    return c.json(wallet);
  });

  app.post("/internal/v1/charges/:chargeId/refunds", async (c) => {
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = parseBody(refundChargeSchema, await c.req.json());
    const res = await deps.ledger.refundCharge({
      charge_id: chargeId,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      reason: body.reason,
    });
    return c.json(res);
  });

  app.post("/internal/v1/charges/:chargeId/adjustments", async (c) => {
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = parseBody(adjustChargeSchema, await c.req.json());
    const res = await deps.ledger.adjustCharge({
      charge_id: chargeId,
      external_ref: body.external_ref,
      delta_atoms: body.delta_atoms,
      reason: body.reason,
    });
    return c.json(res);
  });

  app.post("/internal/v1/upstream-accounts/:accountId/recharges", async (c) => {
    const accountId = parseUuid(c.req.param("accountId"), "accountId");
    const body = parseBody(rechargeSupplierSchema, await c.req.json());
    const res = await deps.ledger.rechargeSupplier({
      account_id: accountId,
      currency: body.currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      ...(body.reason ? { reason: body.reason } : {}),
    });
    return c.json(res);
  });

  app.get("/internal/v1/upstream-accounts/:accountId/balance", async (c) => {
    const accountId = parseUuid(c.req.param("accountId"), "accountId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const bal = await deps.ledger.getSupplierBalance(accountId, currency);
    return c.json(bal);
  });

  app.post("/internal/v1/requests/:requestId/resolve", async (c) => {
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const body = parseBody(resolveRequestSchema, await c.req.json());
    const res = await deps.ledger.resolveRequest({
      request_id: requestId,
      action: body.action,
      ...(body.billing_price_id ? { billing_price_id: body.billing_price_id } : {}),
      ...(body.usage ? { usage: body.usage as Usage } : {}),
      ...(body.supplier_cost_atoms ? { supplier_cost_atoms: body.supplier_cost_atoms } : {}),
      reason: body.reason,
      actor: body.actor,
    });
    return c.json(res);
  });

  app.get("/internal/v1/charges", async (c) => {
    const projectId = c.req.query("project_id");
    const outcome = c.req.query("outcome");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listCharges({
      ...(projectId ? { project_id: parseUuid(projectId, "project_id") } : {}),
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

  app.get("/internal/v1/ledger-entries", async (c) => {
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

  app.get("/internal/v1/holds", async (c) => {
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

  app.get("/internal/v1/requests/:requestId", async (c) => {
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const res = await deps.queries.getRequestBilling(requestId);
    if (!res.hold && !res.charge) {
      return c.json({ error: { code: "not_found", message: "Request billing not found" } }, 404);
    }
    return c.json(res);
  });

  app.post("/internal/v1/events/claim", async (c) => {
    const body = parseBody(claimEventsSchema, await c.req.json());
    const res = await deps.ledger.claimOutboxEvents(body.consumer, body.limit);
    return c.json(res);
  });

  app.post("/internal/v1/events/ack", async (c) => {
    const body = parseBody(ackEventsSchema, await c.req.json());
    const ok = await deps.ledger.ackOutboxEvents(body.consumer, body.lease_token, body.through_seq);
    return c.json({ ok });
  });

  app.get("/internal/v1/audit/conservation", async (c) => {
    const audit = await deps.ledger.auditConservation();
    return c.json(audit, audit.ok ? 200 : 500);
  });

  return app;
}

export type InternalChargePolicy = ChargePolicy;
