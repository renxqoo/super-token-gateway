import { Hono } from "hono";
import { GatewayError, object } from "@gateway/contracts";
import type { Operation, Pricing, Usage } from "@gateway/kernel";
import type {
  ChannelKind,
  ChargePolicy,
  createCatalogRepository,
  createLedgerEngine,
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
  validateCloudCredentials,
  verifier,
  type SmartRouter,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { SnapshotManager } from "./snapshot.ts";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;
type LedgerEngine = ReturnType<typeof createLedgerEngine>;

export function createInternalApi(deps: {
  config: GatewayConfig;
  catalog: CatalogRepo;
  ledger: LedgerEngine;
  snapshot: SnapshotManager;
  router: SmartRouter;
}): Hono {
  const app = new Hono();

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
      channels: deps.router.snapshot(),
    }),
  );

  app.get("/internal/v1/health/channels", (c) => c.json({ channels: deps.router.snapshot() }));

  app.put("/internal/v1/projects", async (c) => {
    const body = object(await c.req.json());
    const rec = await deps.catalog.upsertProject({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      external_ref: typeof body.external_ref === "string" ? body.external_ref : null,
      name: String(body.name ?? ""),
      currency: typeof body.currency === "string" ? body.currency : "USD",
      rpm: Number(body.rpm ?? 6000),
      tpm: Number(body.tpm ?? 10_000_000),
      concurrency: Number(body.concurrency ?? 1000),
      enabled: body.enabled !== false,
    });
    await deps.snapshot.load();
    return c.json(rec);
  });

  app.put("/internal/v1/api-keys", async (c) => {
    const body = object(await c.req.json());
    const rawKey = typeof body.raw_key === "string" && body.raw_key ? body.raw_key : createKey();
    const rec = await deps.catalog.upsertApiKey({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      project_id: String(body.project_id),
      name: String(body.name ?? "default"),
      verifier: verifier(rawKey, deps.config.keyPepper),
      prefix: rawKey.slice(0, 12),
      role: (body.role as "ADMIN" | "BILLING" | "INVOKER" | "VIEWER") ?? "INVOKER",
      allowed_cidrs: Array.isArray(body.allowed_cidrs) ? (body.allowed_cidrs as string[]) : [],
      models: Array.isArray(body.models) ? (body.models as string[]) : [],
      operations: Array.isArray(body.operations) ? (body.operations as string[]) : [],
      enabled: body.enabled !== false,
      expires_at: typeof body.expires_at === "string" ? body.expires_at : null,
    });
    await deps.snapshot.load();
    return c.json({ ...rec, key: rawKey });
  });

  app.put("/internal/v1/upstream-accounts", async (c) => {
    const body = object(await c.req.json());
    const rec = await deps.catalog.upsertUpstreamAccount({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      name: String(body.name ?? ""),
      state: (body.state as "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED") ?? "ACTIVE",
    });
    await deps.snapshot.load();
    return c.json(rec);
  });

  app.put("/internal/v1/channels", async (c) => {
    const body = object(await c.req.json());
    const id = typeof body.id === "string" ? body.id : crypto.randomUUID();
    const kind = String(body.kind) as ChannelKind;
    const baseUrl = String(body.base_url);
    validateBaseUrl(baseUrl, deps.config.allowPrivateUpstreams, deps.config.privateUpstreamCidrs);
    const apiKey = String(body.api_key ?? "");
    if (kind === "azure" || kind === "bedrock" || kind === "vertex") {
      validateCloudCredentials(kind, apiKey);
    }
    const secretCiphertext = encrypt(apiKey, deps.config.encryptionKey, id);
    const rec = await deps.catalog.upsertChannel({
      id,
      account_id: String(body.account_id),
      name: String(body.name ?? ""),
      kind,
      base_url: baseUrl,
      secret_ciphertext: secretCiphertext,
      rpm: Number(body.rpm ?? 1_000_000),
      concurrency: Number(body.concurrency ?? 1000),
      enabled: body.enabled !== false,
      routing_policy:
        typeof body.routing_policy === "object" && body.routing_policy !== null
          ? (body.routing_policy as Record<string, unknown>)
          : {},
    });
    await deps.snapshot.load();
    return c.json({ ...rec, secret_ciphertext: undefined });
  });

  app.put("/internal/v1/models", async (c) => {
    const body = object(await c.req.json());
    const rec = await deps.catalog.upsertModel({
      name: String(body.name),
      operations: (body.operations as Operation[]) ?? ["chat"],
      routing_strategy: (body.routing_strategy as RoutingStrategy) ?? "WEIGHTED",
      max_output_tokens: Number(body.max_output_tokens ?? 32768),
      enabled: body.enabled !== false,
    });
    await deps.snapshot.load();
    return c.json(rec);
  });

  app.post("/internal/v1/prices", async (c) => {
    const body = object(await c.req.json());
    const rec = await deps.catalog.createPriceVersion({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      side: body.side as PriceSide,
      currency: String(body.currency),
      basis: (body.basis as PriceBasis) ?? "PROVIDER_USAGE",
      rates: body.rates as Pricing["rates"],
      ceilings: (body.ceilings as Record<string, string>) ?? {},
    });
    await deps.snapshot.load();
    return c.json(rec, 201);
  });

  app.put("/internal/v1/routes", async (c) => {
    const body = object(await c.req.json());
    const rec = await deps.catalog.upsertRoute({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      model_name: String(body.model_name),
      channel_id: String(body.channel_id),
      upstream_model: String(body.upstream_model),
      operations: (body.operations as Operation[]) ?? ["chat"],
      priority: Number(body.priority ?? 0),
      weight: Number(body.weight ?? 1),
      billing_price_id: String(body.billing_price_id),
      cost_version_id: String(body.cost_version_id),
      request_policy: (body.request_policy as Record<string, unknown>) ?? {},
      plugin_pipeline: (body.plugin_pipeline as PluginPipelineStep[]) ?? [],
      retry_policy: (body.retry_policy as RouteRetryPolicy) ?? {},
      enabled: body.enabled !== false,
    });
    await deps.snapshot.load();
    return c.json(rec);
  });

  app.put("/internal/v1/plugins", async (c) => {
    const body = object(await c.req.json());
    const wasmBytes =
      typeof body.wasm_bytes_base64 === "string"
        ? new Uint8Array(Buffer.from(body.wasm_bytes_base64, "base64"))
        : null;
    const rec = await deps.catalog.upsertPlugin({
      ...(typeof body.id === "string" ? { id: body.id } : {}),
      name: String(body.name),
      version: String(body.version),
      kind: body.kind as PluginKind,
      wasm_bytes: wasmBytes,
      webhook_config: (body.webhook_config as WebhookPluginConfig | null) ?? null,
      signature: typeof body.signature === "string" ? body.signature : null,
      signing_key_id: typeof body.signing_key_id === "string" ? body.signing_key_id : null,
      manifest: (body.manifest as Record<string, unknown>) ?? {},
      enabled: body.enabled !== false,
    });
    await deps.snapshot.load();
    return c.json({ ...rec, wasm_bytes: undefined });
  });

  app.post("/internal/v1/wallets/:projectId/credits", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.creditWallet({
      project_id: c.req.param("projectId"),
      currency: String(body.currency ?? "USD"),
      external_ref: String(body.external_ref),
      amount_atoms: String(body.amount_atoms),
      kind: (body.kind as LotKind) ?? "CASH",
      ...(typeof body.priority === "number" ? { priority: body.priority } : {}),
      expires_at: typeof body.expires_at === "string" ? body.expires_at : null,
      ...(Array.isArray(body.models) ? { models: body.models as string[] } : {}),
      ...(Array.isArray(body.operations) ? { operations: body.operations as string[] } : {}),
      ...(typeof body.reason === "string" ? { reason: body.reason } : {}),
    });
    return c.json(res);
  });

  app.post("/internal/v1/wallets/:projectId/credit-policy", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.configureWalletCredit({
      project_id: c.req.param("projectId"),
      currency: String(body.currency ?? "USD"),
      expected_version: String(body.expected_version),
      credit_limit_atoms: String(body.credit_limit_atoms),
      ...(typeof body.policy === "string" ? { policy: body.policy as WalletPolicy } : {}),
    });
    return c.json(res);
  });

  app.get("/internal/v1/wallets/:projectId", async (c) => {
    const currency = c.req.query("currency") ?? "USD";
    const wallet = await deps.ledger.getWallet(c.req.param("projectId"), currency);
    if (!wallet) return c.json({ error: { code: "not_found", message: "Wallet not found" } }, 404);
    return c.json(wallet);
  });

  app.post("/internal/v1/charges/:chargeId/refunds", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.refundCharge({
      charge_id: c.req.param("chargeId"),
      external_ref: String(body.external_ref),
      amount_atoms: String(body.amount_atoms),
      reason: String(body.reason ?? "refund"),
    });
    return c.json(res);
  });

  app.post("/internal/v1/charges/:chargeId/adjustments", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.adjustCharge({
      charge_id: c.req.param("chargeId"),
      external_ref: String(body.external_ref),
      delta_atoms: String(body.delta_atoms),
      reason: String(body.reason ?? "adjustment"),
    });
    return c.json(res);
  });

  app.post("/internal/v1/upstream-accounts/:accountId/recharges", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.rechargeSupplier({
      account_id: c.req.param("accountId"),
      currency: String(body.currency ?? "USD"),
      external_ref: String(body.external_ref),
      amount_atoms: String(body.amount_atoms),
      ...(typeof body.reason === "string" ? { reason: body.reason } : {}),
    });
    return c.json(res);
  });

  app.get("/internal/v1/upstream-accounts/:accountId/balance", async (c) => {
    const currency = c.req.query("currency") ?? "USD";
    const bal = await deps.ledger.getSupplierBalance(c.req.param("accountId"), currency);
    return c.json(bal);
  });

  app.post("/internal/v1/requests/:requestId/resolve", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.resolveRequest({
      request_id: c.req.param("requestId"),
      action: body.action as "SETTLE" | "RELEASE",
      ...(typeof body.billing_price_id === "string"
        ? { billing_price_id: body.billing_price_id }
        : {}),
      ...(body.usage !== undefined ? { usage: body.usage as Usage } : {}),
      ...(typeof body.supplier_cost_atoms === "string"
        ? { supplier_cost_atoms: body.supplier_cost_atoms }
        : {}),
      reason: String(body.reason ?? "manual_resolution"),
      actor: String(body.actor ?? "admin"),
    });
    return c.json(res);
  });

  app.post("/internal/v1/events/claim", async (c) => {
    const body = object(await c.req.json());
    const res = await deps.ledger.claimOutboxEvents(
      String(body.consumer ?? "default"),
      Number(body.limit ?? 100),
    );
    return c.json(res);
  });

  app.post("/internal/v1/events/ack", async (c) => {
    const body = object(await c.req.json());
    const ok = await deps.ledger.ackOutboxEvents(
      String(body.consumer ?? "default"),
      String(body.lease_token),
      String(body.through_seq),
    );
    return c.json({ ok });
  });

  app.get("/internal/v1/audit/conservation", async (c) => {
    const audit = await deps.ledger.auditConservation();
    return c.json(audit, audit.ok ? 200 : 500);
  });

  return app;
}

export type InternalChargePolicy = ChargePolicy;
