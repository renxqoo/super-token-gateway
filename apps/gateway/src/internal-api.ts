import { Hono, type Handler, type MiddlewareHandler } from "hono";
import { bodyLimit } from "hono/body-limit";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { DomainError, type Operation, type Pricing } from "@gateway/kernel";
import { createPluginReleaseRepository, getApiKeySpend, resetApiKeySpend } from "@gateway/ledger";
import type {
  AdapterOverrides,
  ChannelKind,
  ChargePolicy,
  PluginPipelineStep,
  PriceBasis,
  PriceSide,
  RouteRetryPolicy,
  RoutingStrategy,
  SpendLimitWindow,
} from "@gateway/ledger";
import { builtinProviderRevisionId, resolveProviderPlugin } from "@gateway/plugin-engine";
import {
  createKey,
  encrypt,
  validateAuthCredentials,
  validateBaseUrl,
  validateCidr,
  verifier,
} from "@gateway/runtime";
import { authenticateInternal, type InternalRole } from "./internal-auth.ts";
import { registerInternalFinanceRoutes } from "./internal-finance-routes.ts";
import { registerInternalPluginRoutes } from "./internal-plugin-routes.ts";
import { type AdminEnv, type AdminContext, parseBody, parseUuid } from "./internal-request.ts";
import {
  upsertBillingAccountSchema,
  consoleOrganizationAdmissionSchema,
  upsertApiKeySchema,
  resetApiKeySpendSchema,
  upsertAccountSchema,
  upsertChannelSchema,
  upsertModelSchema,
  createPriceSchema,
  upsertPricingOverrideSchema,
  upsertRouteSchema,
} from "./internal-catalog-schemas.ts";
import { billingAccountFilter } from "./internal-finance-schemas.ts";

import type { InternalApiDeps } from "./internal-route-types.ts";

export function createInternalApi(deps: InternalApiDeps) {
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
        "console_billing_account_binding_conflict",
        "api_key_scope_conflict",
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

  jsonRoute(
    "PUT",
    "/internal/v1/billing-accounts",
    "catalog",
    upsertBillingAccountSchema,
    async (c, body) => {
      const rec = await deps.catalog.upsertBillingAccount({
        ...(body.id ? { id: body.id } : {}),
        ...(body.external_ref !== undefined ? { external_ref: body.external_ref } : {}),
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
    },
  );

  jsonRoute(
    "PUT",
    "/internal/v1/console/organizations/:organizationId/admission",
    "security",
    consoleOrganizationAdmissionSchema,
    async (c, body) => {
      const id = parseUuid(c.req.param("organizationId"), "organizationId");
      return c.json(
        await deps.catalog.setConsoleOrganizationAdmission(
          id,
          body.suspended,
          body.source_revision,
        ),
      );
    },
  );

  jsonRoute("PUT", "/internal/v1/api-keys", "security", upsertApiKeySchema, async (c, body) => {
    const validatedCidrs = body.allowed_cidrs.map((cidr) => validateCidr(cidr));
    const rawKey = body.raw_key ?? createKey();
    const rec = await deps.catalog.upsertApiKey({
      ...(body.id ? { id: body.id } : {}),
      billing_account_id: body.billing_account_id,
      ...(body.rotated_from_key_id ? { rotated_from_key_id: body.rotated_from_key_id } : {}),
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
      ...(body.adaptive_routing !== undefined ? { adaptive_routing: body.adaptive_routing } : {}),
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
        billing_account_id: body.billing_account_id,
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
    const billingAccountId = billingAccountFilter(c);
    const items = await deps.catalog.listPricingOverrides(billingAccountId);
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

  registerInternalPluginRoutes({
    jsonRoute,
    deps,
    onConfigUpdated,
    route,
    pluginReleases,
    requestBodies,
    app,
    authorize,
    checkBodySize,
  });

  registerInternalFinanceRoutes({ jsonRoute, deps, route, onConfigUpdated });

  return { app, requestBodies, routeRoles };
}

export type InternalChargePolicy = ChargePolicy;
