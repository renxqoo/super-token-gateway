import { Hono } from "hono";
import { z } from "zod";
import { GatewayError, pricingSchema, usageSchema } from "@gateway/contracts";
import { operations, type Operation, type Pricing, type Usage } from "@gateway/kernel";
import type {
  ChannelKind,
  ChargePolicy,
  createCatalogRepository,
  createLedgerEngine,
  createLedgerQueries,
  LotKind,
  PluginKind,
  PluginPipelineStep,
  PriceBasis,
  PriceSide,
  RouteRetryPolicy,
  RoutingStrategy,
  WalletPolicy,
  WebhookPluginConfig,
} from "@gateway/ledger";
import {
  createKey,
  encrypt,
  secureEqual,
  validateBaseUrl,
  validateCidr,
  validateCloudCredentials,
  verifier,
  type ClusterCoordinator,
  type SmartRouter,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { SnapshotManager } from "./snapshot.ts";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type LedgerQueries = ReturnType<typeof createLedgerQueries>;

const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
const operationEnum = z.enum(operations);
const channelKindEnum = z.enum([
  "openai",
  "anthropic",
  "gemini",
  "azure",
  "bedrock",
  "vertex",
  "deepseek",
  "ollama",
  "openai_compatible",
]);

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
    kind: channelKindEnum,
    base_url: z.url().max(1000),
    api_key: z.string().max(16384).default(""),
    rpm: z.number().int().min(1).max(10_000_000).default(1_000_000),
    concurrency: z.number().int().min(1).max(100_000).default(1000),
    enabled: z.boolean().default(true),
    routing_policy: z.record(z.string(), z.unknown()).default({}),
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
    name: z.string().min(1).max(120),
    version: z.string().min(1).max(64),
    kind: z.enum(["WASM", "WEBHOOK"]),
    wasm_bytes_base64: z.string().min(4).optional(),
    webhook_config: webhookConfigSchema.nullable().optional(),
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: z.record(z.string(), z.unknown()).default({}),
    enabled: z.boolean().default(true),
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
    if (kind === "azure" || kind === "bedrock" || kind === "vertex") {
      validateCloudCredentials(kind, body.api_key);
    }
    const secretCiphertext = encrypt(body.api_key, deps.config.encryptionKey, id);
    const rec = await deps.catalog.upsertChannel({
      id,
      account_id: body.account_id,
      name: body.name,
      kind,
      base_url: body.base_url,
      secret_ciphertext: secretCiphertext,
      rpm: body.rpm,
      concurrency: body.concurrency,
      enabled: body.enabled,
      routing_policy: body.routing_policy,
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
      plugin_pipeline: body.plugin_pipeline as PluginPipelineStep[],
      retry_policy: body.retry_policy as RouteRetryPolicy,
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json(rec);
  });

  app.put("/internal/v1/plugins", async (c) => {
    const body = parseBody(upsertPluginSchema, await c.req.json());
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
    if (body.kind === "WASM" && (!wasmBytes || wasmBytes.byteLength === 0)) {
      throw new GatewayError(400, "validation_error", "wasm_bytes_base64 is required for WASM");
    }
    const rec = await deps.catalog.upsertPlugin({
      ...(body.id ? { id: body.id } : {}),
      name: body.name,
      version: body.version,
      kind: body.kind as PluginKind,
      wasm_bytes: wasmBytes,
      webhook_config: (body.webhook_config ?? null) as WebhookPluginConfig | null,
      signature: body.signature ?? null,
      signing_key_id: body.signing_key_id ?? null,
      manifest: body.manifest,
      enabled: body.enabled,
    });
    await onConfigUpdated(rec.config_epoch);
    return c.json({ ...rec, wasm_bytes: undefined });
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
