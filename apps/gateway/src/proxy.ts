import type { Context } from "hono";
import { GatewayError, object, optionalObject } from "@gateway/contracts";
import {
  canonical,
  quoteReservation,
  type Operation,
  type Protocol,
  type ProviderKind,
  type Usage,
} from "@gateway/kernel";
import type {
  ApiKeyRecord,
  ChannelKind,
  ChannelRecord,
  ConfigSnapshot,
  createLedgerEngine,
  ModelRecord,
  PluginAuditEntry,
  PriceVersionRecord,
  ProjectRecord,
  RouteRecord,
  UpstreamAccountRecord,
} from "@gateway/ledger";
import type { PluginPipelineEngine } from "@gateway/plugin-engine";
import {
  copyExact,
  decodeResponse,
  encodeResponse,
  encodeSSE,
  extractUsage,
  parseClientJson,
  parseProviderJson,
  requestCharacters,
  SSEParser,
  StreamTranslator,
  stringifyExact,
} from "@gateway/protocol";
import {
  clientAddress,
  inCidrs,
  Lifecycle,
  LocalRateLimiter,
  readBounded,
  sha256,
  SmartRouter,
  UpstreamPreDispatchError,
  verifier,
  VertexTokens,
  type createTransport,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { SnapshotManager } from "./snapshot.ts";
import { rejectUnmeteredTools } from "./tool-policy.ts";
import {
  BedrockEventStreamDecoder,
  buildTextDeltaSse,
  extractChunkTextFromEncodedSse,
  extractRequestText,
  extractResponseText,
  isConversationalOperation,
  parseRetryAfterMs,
  prepareUpstreamRequest,
  replaceResponseText,
} from "./upstream.ts";

type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type Transport = ReturnType<typeof createTransport>;

type HydratedCandidate = {
  id: string;
  channel_id: string;
  account_id: string;
  priority: number;
  weight: number;
  operations: readonly Operation[];
  enabled: boolean;
  channel_enabled: boolean;
  account_active: boolean;
  route: RouteRecord;
  channel: ChannelRecord;
  account: UpstreamAccountRecord;
  billingPrice: PriceVersionRecord;
  costPrice: PriceVersionRecord;
};

export type ProxyDependencies = {
  config: GatewayConfig;
  snapshot: SnapshotManager;
  ledger: LedgerEngine;
  router: SmartRouter;
  rateLimiter: LocalRateLimiter;
  transport: Transport;
  vertexTokens: VertexTokens;
  pluginEngine: PluginPipelineEngine;
  lifecycle: Lifecycle;
  metrics: {
    requestsTotal: number;
    failoversTotal: number;
    moderationBlocksTotal: number;
  };
};

function extractApiKey(c: Context, operation: Operation): string | null {
  const auth = c.req.header("authorization");
  if (auth && auth.toLowerCase().startsWith("bearer ")) {
    const token = auth.slice(7).trim();
    if (token) return token;
  }
  if (operation === "messages") {
    const xKey = c.req.header("x-api-key")?.trim();
    if (xKey) return xKey;
  }
  if (operation === "gemini") {
    const gKey = c.req.header("x-goog-api-key")?.trim() || c.req.query("key")?.trim();
    if (gKey) return gKey;
  }
  return null;
}

export function authenticateRequest(
  c: Context,
  snap: ConfigSnapshot,
  config: GatewayConfig,
  operation: Operation,
  peerAddress?: string,
): { key: ApiKeyRecord; project: ProjectRecord } {
  const rawKey = extractApiKey(c, operation);
  if (!rawKey) throw new GatewayError(401, "unauthorized", "Missing API key");
  const digest = verifier(rawKey, config.keyPepper);
  const key = snap.keysByVerifier.get(digest);
  if (!key || !key.enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
  if (key.expires_at && Date.parse(key.expires_at) <= Date.now()) {
    throw new GatewayError(401, "key_expired", "API key has expired");
  }
  if (key.role === "VIEWER") {
    throw new GatewayError(403, "forbidden", "API key role cannot invoke models");
  }
  if (key.allowed_cidrs.length > 0) {
    const addr = clientAddress(
      peerAddress,
      c.req.header("x-forwarded-for") ?? null,
      config.trustedProxyCidrs,
    );
    if (!addr || !inCidrs(addr, key.allowed_cidrs)) {
      throw new GatewayError(403, "ip_not_allowed", "Client IP address is not permitted");
    }
  }
  if (key.operations.length > 0 && !key.operations.includes(operation)) {
    throw new GatewayError(403, "operation_not_allowed", "API key cannot invoke this operation");
  }
  const project = snap.projects.get(key.project_id);
  if (!project || !project.enabled) {
    throw new GatewayError(403, "project_disabled", "Project is disabled");
  }
  return { key, project };
}

function resolveOutputBound(body: Record<string, unknown>, model: ModelRecord): number {
  const gen = optionalObject(body.generationConfig);
  const raw =
    body.max_completion_tokens ?? body.max_tokens ?? body.max_output_tokens ?? gen.maxOutputTokens;
  if (typeof raw === "number" && Number.isSafeInteger(raw) && raw > 0) {
    return Math.min(raw, model.max_output_tokens);
  }
  return model.max_output_tokens;
}

function toProviderKind(kind: ChannelKind): ProviderKind {
  return kind === "openai_compatible" ? "openai" : kind;
}

function quoteForPrice(
  price: PriceVersionRecord,
  measurement: { inputCharacters?: string | undefined; images?: unknown },
  outputBound: number,
): string {
  const quote = quoteReservation(
    [
      {
        provider_id: price.id,
        id: price.id,
        currency: price.currency,
        rates: price.rates,
        ceilings: price.ceilings as Usage,
        basis: price.basis,
      },
    ],
    price.currency,
    measurement,
    outputBound,
  );
  return quote.amount_atoms;
}

function mapHoldError(code: string, message: string): GatewayError {
  if (code === "insufficient_balance") return new GatewayError(402, code, message);
  if (code === "wallet_frozen" || code === "wallet_not_found") {
    return new GatewayError(403, code, message);
  }
  if (code === "idempotency_conflict") return new GatewayError(409, code, message);
  if (code === "supplier_unavailable" || code === "supplier_insufficient_balance") {
    return new GatewayError(503, code, message);
  }
  return new GatewayError(400, code, message);
}

function isRetryableStatus(status: number): "rate_limit" | "auth_error" | "retryable" | null {
  if (status === 429) return "rate_limit";
  if (status === 401 || status === 403) return "auth_error";
  if (status === 500 || status === 502 || status === 503 || status === 504 || status === 529) {
    return "retryable";
  }
  return null;
}

function buildUsageForOperation(
  operation: Operation,
  basis: PriceVersionRecord["basis"],
  rawUsage: Usage | undefined,
  inputCharacters: number | undefined,
): Usage | null {
  if (basis === "REQUEST_COMPLETION") return { requests: "1" };
  if (basis === "INPUT_CHARACTERS") {
    return { characters: String(inputCharacters ?? 0), requests: "1" };
  }
  if (rawUsage) return rawUsage;
  if (operation === "images") return { images: "1" };
  return null;
}

function buildModerationUsageFallback(
  observed: Usage | undefined,
  inputChars: number,
  outputChars: number,
): Usage {
  if (observed) return observed;
  return {
    input_tokens: String(Math.max(1, Math.ceil(inputChars / 4))),
    output_tokens: String(Math.max(1, Math.ceil(outputChars / 4))),
  };
}

function errorJson(c: Context, err: unknown, requestId?: string): Response {
  const gw =
    err instanceof GatewayError
      ? err
      : new GatewayError(500, "internal_error", err instanceof Error ? err.message : "Error");
  const headers: Record<string, string> = {};
  if (requestId) headers["x-request-id"] = requestId;
  return c.json(
    {
      error: {
        code: gw.code,
        message: gw.message,
        ...(requestId ? { request_id: requestId } : {}),
      },
    },
    gw.status as 400,
    headers,
  );
}

export async function handleInferenceRequest(
  c: Context,
  deps: ProxyDependencies,
  operation: Operation,
  pathOverride?: { model: string; stream: boolean },
  peerAddress?: string,
): Promise<Response> {
  const requestId = crypto.randomUUID();
  deps.metrics.requestsTotal++;

  const guard = deps.lifecycle.enter();
  let guardReleased = false;
  const releaseGuard = () => {
    if (!guardReleased) {
      guardReleased = true;
      guard.release();
    }
  };

  let releaseProjectLimit: (() => void) | null = null;
  let releaseChannelLimit: (() => void) | null = null;
  const releaseLimiters = () => {
    releaseChannelLimit?.();
    releaseChannelLimit = null;
    releaseProjectLimit?.();
    releaseProjectLimit = null;
  };

  try {
    const snap = deps.snapshot.get();
    const { key, project } = authenticateRequest(c, snap, deps.config, operation, peerAddress);

    const rawBytes = await readBounded(c.req.raw.body, deps.config.maxBodyBytes, c.req.raw.signal);
    const parsedRaw = object(
      parseClientJson(new TextDecoder("utf-8", { fatal: true }).decode(rawBytes)),
    );
    const clientBody: Record<string, unknown> = pathOverride
      ? copyExact(parsedRaw, {
          ...parsedRaw,
          model: pathOverride.model,
          stream: pathOverride.stream,
        })
      : parsedRaw;

    const modelName = typeof clientBody.model === "string" ? clientBody.model.trim() : "";
    if (!modelName) throw new GatewayError(400, "invalid_request", "Missing model");
    if (key.models.length > 0 && !key.models.includes(modelName)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    const model = snap.models.get(modelName);
    if (!model || !model.enabled || !model.operations.includes(operation)) {
      throw new GatewayError(404, "model_not_found", "Model or operation is unavailable");
    }

    rejectUnmeteredTools(clientBody, operation);

    const isStream = Boolean(clientBody.stream === true);
    const outputBound = resolveOutputBound(clientBody, model);
    const chars = requestCharacters(operation, clientBody);
    const measurement = {
      inputCharacters: chars === undefined ? undefined : String(chars),
      images: operation === "images" ? (clientBody.n ?? 1) : undefined,
    };

    const estimatedTokens = Math.max(
      1,
      Math.ceil((chars ?? 200) / 4) + Math.min(outputBound, 4096),
    );
    const projAcq = deps.rateLimiter.acquire(
      `project:${project.id}`,
      { rpm: project.rpm, tpm: project.tpm, concurrency: project.concurrency },
      estimatedTokens,
    );
    if (!projAcq.ok) {
      throw new GatewayError(429, `rate_limit_${projAcq.reason}`, "Project rate limit exceeded");
    }
    releaseProjectLimit = projAcq.release;

    const rawRoutes = snap.routesByModel.get(model.name) ?? [];
    const hydrated: HydratedCandidate[] = [];
    for (const r of rawRoutes) {
      const channel = snap.channels.get(r.channel_id);
      if (!channel) continue;
      const account = snap.accounts.get(channel.account_id);
      if (!account) continue;
      const billingPrice = snap.prices.get(r.billing_price_id);
      const costPrice = snap.prices.get(r.cost_version_id);
      if (!billingPrice || !costPrice) continue;
      if (billingPrice.currency !== project.currency) continue;
      hydrated.push({
        id: r.id,
        channel_id: channel.id,
        account_id: account.id,
        priority: r.priority,
        weight: r.weight,
        operations: r.operations,
        enabled: r.enabled,
        channel_enabled: channel.enabled,
        account_active: account.state === "ACTIVE",
        route: r,
        channel,
        account,
        billingPrice,
        costPrice,
      });
    }

    const selected = deps.router.selectCandidates(
      model.name,
      hydrated,
      model.routing_strategy,
      operation,
    );
    if (selected.length === 0) {
      throw new GatewayError(503, "no_route", "No healthy channel available for this model");
    }

    const releaseUnusedProbes = (fromIndex: number) => {
      for (let i = fromIndex; i < selected.length; i++) {
        if (selected[i]!.isProbe) {
          deps.router.releaseProbe(selected[i]!.route.channel_id);
        }
      }
    };

    const preRequestAudit: PluginAuditEntry[] = [];
    const primarySteps = selected[0]!.route.route.plugin_pipeline;
    if (primarySteps.length > 0) {
      const reqText = extractRequestText(clientBody, operation);
      const preRes = await deps.pluginEngine.runUnary(
        "on_request",
        primarySteps,
        snap.plugins,
        { request_id: requestId, project_id: project.id, model: model.name, operation },
        reqText,
        c.req.raw.signal,
      );
      preRequestAudit.push(...preRes.audit);
      if (preRes.blocked) {
        releaseUnusedProbes(0);
        deps.metrics.moderationBlocksTotal++;
        return c.json(
          {
            error: {
              code: "content_blocked",
              rule_code: preRes.rule_code,
              message: preRes.safe_message,
              request_id: requestId,
            },
          },
          (preRes.http_status ?? 400) as 400,
          { "x-request-id": requestId },
        );
      }
    }

    const idempotencyKey = c.req.header("idempotency-key")?.trim() || null;
    const fingerprint = sha256(
      canonical({ project_id: project.id, operation, model: model.name, body: clientBody }),
    );
    const expiresAt = new Date(Date.now() + deps.config.holdTtlMs).toISOString();

    let activeIndex = -1;
    let activeAttemptId = "";
    let holdCustomerAtoms = "0";
    let lastHoldErr: GatewayError | null = null;

    for (let idx = 0; idx < selected.length; idx++) {
      const cand = selected[idx]!.route;
      let customerAtoms: string;
      let supplierAtoms: string;
      try {
        customerAtoms = quoteForPrice(cand.billingPrice, measurement, outputBound);
        supplierAtoms = quoteForPrice(cand.costPrice, measurement, outputBound);
      } catch (err) {
        lastHoldErr =
          err instanceof GatewayError
            ? err
            : new GatewayError(
                422,
                "billing_ceiling",
                err instanceof Error ? err.message : "Quote error",
              );
        if (selected[idx]!.isProbe) deps.router.releaseProbe(cand.channel_id);
        continue;
      }
      const attemptId = crypto.randomUUID();
      const holdRes = await deps.ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
        project_id: project.id,
        currency: project.currency,
        operation,
        model: model.name,
        idempotency_key: idempotencyKey,
        fingerprint,
        customer_atoms: customerAtoms,
        supplier_account_id: cand.account.id,
        supplier_channel_id: cand.channel.id,
        supplier_currency: cand.costPrice.currency,
        supplier_atoms: supplierAtoms,
        expires_at: expiresAt,
      });
      if (holdRes.ok) {
        if (holdRes.replayed && holdRes.existing_state && holdRes.existing_state !== "HELD") {
          releaseUnusedProbes(idx);
          return c.json(
            {
              error: {
                code: "idempotency_replayed",
                message: `Request already finalized with state ${holdRes.existing_state}`,
                request_id: holdRes.request_id,
              },
            },
            409,
            { "x-request-id": holdRes.request_id },
          );
        }
        activeIndex = idx;
        activeAttemptId = attemptId;
        holdCustomerAtoms = holdRes.held_atoms;
        break;
      }
      if (selected[idx]!.isProbe) deps.router.releaseProbe(cand.channel_id);
      if (
        holdRes.code === "supplier_unavailable" ||
        holdRes.code === "supplier_insufficient_balance"
      ) {
        lastHoldErr = mapHoldError(holdRes.code, holdRes.message);
        continue;
      }
      releaseUnusedProbes(idx + 1);
      throw mapHoldError(holdRes.code, holdRes.message);
    }

    if (activeIndex < 0) {
      throw lastHoldErr ?? new GatewayError(503, "no_route", "No funded route available");
    }

    const maxAttempts = Math.min(
      selected.length - activeIndex,
      Math.max(1, selected[activeIndex]!.route.route.retry_policy.max_attempts ?? 3),
    );
    let anyUnknownAttempt = false;
    let attemptsMade = 0;
    let lastErrorStatus = 502;
    let lastErrorCode = "upstream_failed";
    let lastErrorMessage = "All upstream channels failed";
    let pendingSwitchFrom: { attemptId: string; outcome: "FAILED" | "UNKNOWN" } | null = null;

    for (
      let cursor = activeIndex;
      cursor < selected.length && attemptsMade < maxAttempts;
      cursor++
    ) {
      const entry = selected[cursor]!;
      const cand = entry.route;

      if (pendingSwitchFrom) {
        const nextCustomerAtoms = quoteForPrice(cand.billingPrice, measurement, outputBound);
        const nextSupplierAtoms = quoteForPrice(cand.costPrice, measurement, outputBound);
        const nextAttemptId = crypto.randomUUID();
        const sw = await deps.ledger.switchSupplierAttempt({
          request_id: requestId,
          failed_attempt_id: pendingSwitchFrom.attemptId,
          failed_outcome: pendingSwitchFrom.outcome,
          next_attempt_id: nextAttemptId,
          next_account_id: cand.account.id,
          next_channel_id: cand.channel.id,
          next_currency: cand.costPrice.currency,
          next_supplier_atoms: nextSupplierAtoms,
          ...(BigInt(nextCustomerAtoms) > BigInt(holdCustomerAtoms)
            ? { required_customer_atoms: nextCustomerAtoms }
            : {}),
        });
        if (!sw.ok) {
          if (entry.isProbe) deps.router.releaseProbe(cand.channel_id);
          if (sw.code === "supplier_unavailable" || sw.code === "supplier_insufficient_balance") {
            continue;
          }
          releaseUnusedProbes(cursor + 1);
          if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
          else await deps.ledger.releaseHold(requestId);
          throw mapHoldError(sw.code, sw.message);
        }
        activeAttemptId = nextAttemptId;
        holdCustomerAtoms = sw.held_atoms;
        pendingSwitchFrom = null;
        deps.metrics.failoversTotal++;
      }

      const chLimit = deps.rateLimiter.acquire(
        `channel:${cand.channel.id}`,
        {
          rpm: cand.channel.rpm,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: cand.channel.concurrency,
        },
        1,
      );
      if (!chLimit.ok) {
        if (entry.isProbe) deps.router.releaseProbe(cand.channel_id);
        pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        lastErrorStatus = 429;
        lastErrorCode = "channel_saturated";
        lastErrorMessage = "Upstream channel concurrency or RPM limit reached";
        continue;
      }
      releaseChannelLimit = chLimit.release;
      attemptsMade++;

      const attemptTimeoutMs =
        cand.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs;
      const attemptAbort = new AbortController();
      const timeoutTimer = setTimeout(
        () => attemptAbort.abort(new Error("attempt_timeout")),
        attemptTimeoutMs,
      );
      const onClientAbort = () => attemptAbort.abort(c.req.raw.signal.reason);
      const onShutdownAbort = () => attemptAbort.abort(guard.controller.signal.reason);
      c.req.raw.signal.addEventListener("abort", onClientAbort, { once: true });
      guard.controller.signal.addEventListener("abort", onShutdownAbort, { once: true });

      const startedAt = performance.now();
      let upstreamResp: Response;
      let prepared: Awaited<ReturnType<typeof prepareUpstreamRequest>>;
      try {
        const secret = deps.snapshot.decryptChannelSecret(cand.channel);
        prepared = await prepareUpstreamRequest({
          channel: cand.channel,
          route: cand.route,
          secret,
          operation,
          clientBody,
          isStream,
          vertexTokens: deps.vertexTokens,
          signal: attemptAbort.signal,
        });
      } catch (err) {
        clearTimeout(timeoutTimer);
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        releaseChannelLimit();
        releaseChannelLimit = null;
        if (entry.isProbe) deps.router.releaseProbe(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
        else await deps.ledger.releaseHold(requestId);
        throw err;
      }

      try {
        upstreamResp = await deps.transport.fetch(
          prepared.url,
          {
            method: "POST",
            headers: prepared.headers,
            body: Buffer.from(prepared.body),
            signal: attemptAbort.signal,
            dispatchDeadline: performance.now() + attemptTimeoutMs,
          },
          deps.config.allowPrivateUpstreams,
          deps.config.privateUpstreamCidrs,
        );
      } catch (err) {
        clearTimeout(timeoutTimer);
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        releaseChannelLimit();
        releaseChannelLimit = null;
        const latencyMs = performance.now() - startedAt;
        const isPreDispatch =
          err instanceof UpstreamPreDispatchError ||
          (err instanceof Error &&
            /ECONNREFUSED|ENOTFOUND|ECONNRESET|Unable to connect|ConnectionRefused/i.test(
              err.message,
            ));
        if (c.req.raw.signal.aborted) {
          releaseUnusedProbes(cursor + 1);
          if (isPreDispatch && !anyUnknownAttempt) await deps.ledger.releaseHold(requestId);
          else await deps.ledger.markUnknown(requestId);
          return c.json(
            { error: { code: "client_aborted", message: "Client disconnected" } },
            499 as 400,
          );
        }
        deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
        const failedOutcome: "FAILED" | "UNKNOWN" = isPreDispatch ? "FAILED" : "UNKNOWN";
        if (failedOutcome === "UNKNOWN") anyUnknownAttempt = true;
        const allowAmbiguousRetry = Boolean(cand.route.retry_policy.retry_on_ambiguous);
        lastErrorStatus = isPreDispatch ? 503 : 504;
        lastErrorCode = isPreDispatch ? "upstream_unreachable" : "upstream_timeout";
        lastErrorMessage = err instanceof Error ? err.message : "Upstream request failed";
        if (isPreDispatch || allowAmbiguousRetry) {
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: failedOutcome };
          continue;
        }
        releaseUnusedProbes(cursor + 1);
        await deps.ledger.markUnknown(requestId);
        throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
      }

      const latencyMs = performance.now() - startedAt;
      if (!upstreamResp.ok) {
        clearTimeout(timeoutTimer);
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        void upstreamResp.body?.cancel().catch(() => undefined);
        releaseChannelLimit();
        releaseChannelLimit = null;
        const retryKind = isRetryableStatus(upstreamResp.status);
        if (retryKind) {
          const retryAfterMs = parseRetryAfterMs(upstreamResp.headers.get("retry-after"));
          deps.router.recordFailure(cand.channel.id, retryKind, retryAfterMs, latencyMs);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
          lastErrorStatus = upstreamResp.status === 429 ? 429 : 502;
          lastErrorCode =
            retryKind === "rate_limit"
              ? "upstream_rate_limited"
              : retryKind === "auth_error"
                ? "upstream_auth_error"
                : "upstream_error";
          lastErrorMessage = `Upstream returned HTTP ${upstreamResp.status}`;
          continue;
        }
        if (entry.isProbe) deps.router.releaseProbe(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
        else await deps.ledger.releaseHold(requestId);
        return c.json(
          {
            error: {
              code: "upstream_client_error",
              message: `Upstream rejected request with HTTP ${upstreamResp.status}`,
              request_id: requestId,
            },
          },
          upstreamResp.status as 400,
          { "x-request-id": requestId },
        );
      }

      releaseUnusedProbes(cursor + 1);

      if (!isStream) {
        try {
          const respBytes = await readBounded(
            upstreamResp.body,
            deps.config.maxBodyBytes,
            attemptAbort.signal,
          );
          const rawJson = object(
            parseProviderJson(new TextDecoder("utf-8", { fatal: true }).decode(respBytes)),
          );
          const providerKind = toProviderKind(cand.channel.kind);
          const rawUsage = extractUsage(rawJson, providerKind);
          const usage = buildUsageForOperation(operation, cand.billingPrice.basis, rawUsage, chars);
          if (!usage) {
            await deps.ledger.markUnknown(requestId);
            deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
            throw new GatewayError(
              502,
              "invalid_upstream_usage",
              "Upstream omitted usage metadata",
            );
          }

          let clientJson: Record<string, unknown>;
          if (
            isConversationalOperation(operation) &&
            prepared.upstreamProtocol &&
            prepared.upstreamProtocol !== operation
          ) {
            const completion = decodeResponse(rawJson, prepared.upstreamProtocol, providerKind);
            clientJson = encodeResponse(completion, operation, model.name);
          } else {
            clientJson = copyExact(rawJson, { ...rawJson });
            if (typeof clientJson.model === "string") clientJson.model = model.name;
          }

          let moderationBlocked = false;
          let chargePolicy: "CHARGE_CUSTOMER" | "PLATFORM_ABSORB" = "CHARGE_CUSTOMER";
          let blockRule = "";
          let blockStatus = 400;
          let blockMessage = "Content blocked";
          const allAudit: PluginAuditEntry[] = [...preRequestAudit];

          if (cand.route.plugin_pipeline.length > 0 && isConversationalOperation(operation)) {
            const respText = extractResponseText(clientJson, operation);
            const postRes = await deps.pluginEngine.runUnary(
              "on_response",
              cand.route.plugin_pipeline,
              snap.plugins,
              { request_id: requestId, project_id: project.id, model: model.name, operation },
              respText,
              attemptAbort.signal,
            );
            allAudit.push(...postRes.audit);
            if (postRes.blocked) {
              moderationBlocked = true;
              chargePolicy = postRes.charge_policy;
              blockRule = postRes.rule_code ?? "moderation_blocked";
              blockStatus = postRes.http_status ?? 400;
              blockMessage = postRes.safe_message ?? "Content blocked by moderation policy";
              deps.metrics.moderationBlocksTotal++;
            } else if (postRes.text !== respText) {
              clientJson = replaceResponseText(clientJson, operation, postRes.text);
            }
          }

          const commitRes = await deps.ledger.commitTerminal({
            request_id: requestId,
            selected_attempt_id: activeAttemptId,
            channel_id: cand.channel.id,
            billing_price_id: cand.billingPrice.id,
            cost_version_id: cand.costPrice.id,
            usage,
            billing_pricing: {
              currency: cand.billingPrice.currency,
              rates: cand.billingPrice.rates,
            },
            billing_basis: cand.billingPrice.basis,
            ...(chars !== undefined ? { input_characters: String(chars) } : {}),
            supplier_pricing: {
              currency: cand.costPrice.currency,
              rates: cand.costPrice.rates,
            },
            supplier_currency: cand.costPrice.currency,
            plugin_audit: allAudit,
            charge_policy: chargePolicy,
            moderation_blocked: moderationBlocked,
          });
          if (!commitRes.ok) {
            throw new GatewayError(500, "ledger_commit_failed", commitRes.message);
          }

          deps.router.recordSuccess(cand.channel.id, latencyMs);
          if (moderationBlocked) {
            return c.json(
              {
                error: {
                  code: "content_blocked",
                  rule_code: blockRule,
                  message: blockMessage,
                  request_id: requestId,
                },
              },
              blockStatus as 400,
              { "x-request-id": requestId, "x-gateway-channel": cand.channel.name },
            );
          }
          return new Response(stringifyExact(clientJson), {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-request-id": requestId,
              "x-gateway-channel": cand.channel.name,
            },
          });
        } finally {
          clearTimeout(timeoutTimer);
          c.req.raw.signal.removeEventListener("abort", onClientAbort);
          guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        }
      }

      clearTimeout(timeoutTimer);
      if (!isConversationalOperation(operation) || !prepared.upstreamProtocol) {
        void upstreamResp.body?.cancel().catch(() => undefined);
        await deps.ledger.releaseHold(requestId);
        throw new GatewayError(
          422,
          "unsupported_stream",
          "Streaming not supported for this operation",
        );
      }

      const clientProtocol: Protocol = operation;
      const upstreamProtocol: Protocol = prepared.upstreamProtocol;
      const streamId = `gen_${requestId.replaceAll("-", "")}`;
      const hasPlugins = cand.route.plugin_pipeline.length > 0;
      const upstreamBody = upstreamResp.body;
      const capturedProjectLimit = releaseProjectLimit;
      const capturedChannelLimit = releaseChannelLimit;
      releaseProjectLimit = null;
      releaseChannelLimit = null;

      let streamCleanupDone = false;
      const cleanupStream = () => {
        if (streamCleanupDone) return;
        streamCleanupDone = true;
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        capturedChannelLimit?.();
        capturedProjectLimit?.();
        releaseGuard();
      };

      const bodyStream = new ReadableStream<Uint8Array>({
        async start(controller) {
          const reader = upstreamBody?.getReader();
          const parser = new SSEParser();
          const bedrockDecoder = prepared.isBedrockStream ? new BedrockEventStreamDecoder() : null;
          const translator = new StreamTranslator(
            upstreamProtocol,
            clientProtocol,
            toProviderKind(cand.channel.kind),
            model.name,
          );
          const session = hasPlugins
            ? deps.pluginEngine.createStreamSession(
                cand.route.plugin_pipeline,
                snap.plugins,
                { request_id: requestId, project_id: project.id, model: model.name, operation },
                () => attemptAbort.abort(new Error("moderation_blocked")),
                attemptAbort.signal,
              )
            : null;
          const heldTerminalChunks: Uint8Array[] = [];
          let observedOutputChars = 0;
          let streamFailed = false;
          let blockedEarly = false;

          try {
            if (!reader)
              throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
            while (true) {
              const step = await reader.read();
              if (step.done) break;
              const events = bedrockDecoder
                ? bedrockDecoder.feed(step.value)
                : parser.feed(step.value, false);
              for (const ev of events) {
                const translated = translator.consume(ev);
                for (const chunk of translated.chunks) {
                  if (translated.terminal || translator.holdTerminal) {
                    heldTerminalChunks.push(chunk);
                    continue;
                  }
                  if (!session) {
                    controller.enqueue(chunk);
                    continue;
                  }
                  const deltaText = extractChunkTextFromEncodedSse(chunk, clientProtocol);
                  if (deltaText === null) {
                    controller.enqueue(chunk);
                    continue;
                  }
                  observedOutputChars += deltaText.length;
                  const mod = await session.push(deltaText);
                  if (mod.blocked) {
                    blockedEarly = true;
                    break;
                  }
                  if (mod.emit.length > 0) {
                    controller.enqueue(
                      buildTextDeltaSse(clientProtocol, model.name, mod.emit, streamId),
                    );
                  }
                }
                if (blockedEarly) break;
              }
              if (blockedEarly) {
                await reader.cancel().catch(() => undefined);
                break;
              }
            }

            if (!blockedEarly && !bedrockDecoder) {
              const tailEvents = parser.feed(new Uint8Array(), true);
              for (const ev of tailEvents) {
                const translated = translator.consume(ev);
                for (const chunk of translated.chunks) {
                  heldTerminalChunks.push(chunk);
                }
              }
            }

            if (session) {
              const finalMod = await session.finalize();
              if (finalMod.blocked) {
                deps.metrics.moderationBlocksTotal++;
                const modUsage = buildModerationUsageFallback(
                  translator.usage,
                  chars ?? 0,
                  observedOutputChars,
                );
                await deps.ledger.commitTerminal({
                  request_id: requestId,
                  selected_attempt_id: activeAttemptId,
                  channel_id: cand.channel.id,
                  billing_price_id: cand.billingPrice.id,
                  cost_version_id: cand.costPrice.id,
                  usage: modUsage,
                  billing_pricing: {
                    currency: cand.billingPrice.currency,
                    rates: cand.billingPrice.rates,
                  },
                  billing_basis: cand.billingPrice.basis,
                  ...(chars !== undefined ? { input_characters: String(chars) } : {}),
                  supplier_pricing: {
                    currency: cand.costPrice.currency,
                    rates: cand.costPrice.rates,
                  },
                  supplier_currency: cand.costPrice.currency,
                  plugin_audit: [...preRequestAudit, ...finalMod.audit],
                  charge_policy: finalMod.charge_policy,
                  moderation_blocked: true,
                });
                deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
                controller.enqueue(
                  encodeSSE(
                    {
                      error: {
                        code: "content_blocked",
                        rule_code: finalMod.rule_code,
                        message: finalMod.safe_message,
                        request_id: requestId,
                      },
                    },
                    "error",
                  ),
                );
                controller.close();
                return;
              }
              if (finalMod.emit.length > 0) {
                controller.enqueue(
                  buildTextDeltaSse(clientProtocol, model.name, finalMod.emit, streamId),
                );
              }
            }

            const finalTranslatorChunks = translator.finalize();
            heldTerminalChunks.push(...finalTranslatorChunks);

            const usage = buildUsageForOperation(
              operation,
              cand.billingPrice.basis,
              translator.usage,
              chars,
            );
            if (!usage) {
              streamFailed = true;
              deps.router.recordFailure(
                cand.channel.id,
                "retryable",
                undefined,
                performance.now() - startedAt,
              );
              await deps.ledger.markUnknown(requestId);
              controller.enqueue(
                encodeSSE(
                  {
                    error: {
                      code: "invalid_upstream_usage",
                      message: "Stream finished without usage metadata",
                      request_id: requestId,
                    },
                  },
                  "error",
                ),
              );
              controller.close();
              return;
            }

            const commitRes = await deps.ledger.commitTerminal({
              request_id: requestId,
              selected_attempt_id: activeAttemptId,
              channel_id: cand.channel.id,
              billing_price_id: cand.billingPrice.id,
              cost_version_id: cand.costPrice.id,
              usage,
              billing_pricing: {
                currency: cand.billingPrice.currency,
                rates: cand.billingPrice.rates,
              },
              billing_basis: cand.billingPrice.basis,
              ...(chars !== undefined ? { input_characters: String(chars) } : {}),
              supplier_pricing: {
                currency: cand.costPrice.currency,
                rates: cand.costPrice.rates,
              },
              supplier_currency: cand.costPrice.currency,
              plugin_audit: preRequestAudit,
              charge_policy: "CHARGE_CUSTOMER",
              moderation_blocked: false,
            });
            if (!commitRes.ok) {
              throw new GatewayError(500, "ledger_commit_failed", commitRes.message);
            }

            deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
            for (const chunk of heldTerminalChunks) {
              controller.enqueue(chunk);
            }
            controller.close();
          } catch (err) {
            if (!streamFailed) {
              streamFailed = true;
              deps.router.recordFailure(
                cand.channel.id,
                "retryable",
                undefined,
                performance.now() - startedAt,
              );
              await deps.ledger.markUnknown(requestId).catch(() => undefined);
            }
            try {
              const code = err instanceof GatewayError ? err.code : "stream_interrupted";
              const msg = err instanceof Error ? err.message : "Stream failed";
              controller.enqueue(
                encodeSSE({ error: { code, message: msg, request_id: requestId } }, "error"),
              );
              controller.close();
            } catch {
              void 0;
            }
          } finally {
            reader?.releaseLock();
            cleanupStream();
          }
        },
        cancel() {
          attemptAbort.abort(new Error("client_cancelled"));
          void upstreamBody?.cancel().catch(() => undefined);
          cleanupStream();
        },
      });

      return new Response(bodyStream, {
        status: 200,
        headers: {
          "content-type": "text/event-stream; charset=utf-8",
          "cache-control": "no-cache",
          "x-request-id": requestId,
          "x-gateway-channel": cand.channel.name,
        },
      });
    }

    releaseUnusedProbes(activeIndex + attemptsMade);
    if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
    else await deps.ledger.releaseHold(requestId);
    throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
  } catch (err) {
    return errorJson(c, err, requestId);
  } finally {
    releaseLimiters();
    releaseGuard();
  }
}
