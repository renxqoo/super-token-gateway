import type { Context } from "hono";
import { GatewayError, object, optionalObject } from "@gateway/contracts";
import {
  canonical,
  quoteReservation,
  type Operation,
  type Protocol,
  type Usage,
} from "@gateway/kernel";
import type {
  ApiKeyRecord,
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
import {
  applyResponseTransforms,
  evaluateErrorRules,
  invokeWasmProviderResponse,
  invokeWasmProviderStreamEvent,
  type PluginPipelineEngine,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
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
  validateUsageObject,
} from "@gateway/protocol";
import {
  clientAddress,
  inCidrs,
  Lifecycle,
  readBounded,
  sha256,
  SmartRouter,
  UpstreamPreDispatchError,
  verifier,
  VertexTokens,
  type ClusterCoordinator,
  type createTransport,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import { parseOrCreateTrace, type GatewayMetrics, type TraceContext } from "./observability.ts";
import type { SnapshotManager } from "./snapshot.ts";
import { rejectUnmeteredTools } from "./tool-policy.ts";
import {
  BedrockEventStreamDecoder,
  buildTextDeltaSse,
  extractChunkTextFromEncodedSse,
  extractResponseText,
  isConversationalOperation,
  mergeUsageWithAliases,
  parseRetryAfterMs,
  prepareUpstreamRequest,
  replaceResponseText,
  transformRequestTexts,
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
  cluster: ClusterCoordinator;
  transport: Transport;
  vertexTokens: VertexTokens;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
  lifecycle: Lifecycle;
  metrics: GatewayMetrics;
};

const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();

let authCacheEpoch = -1;
let authCachePepper = "";
const verifiedKeyCache = new Map<string, ApiKeyRecord>();
const quoteCache = new Map<string, string>();

function cachedVerifierLookup(
  rawKey: string,
  snap: ConfigSnapshot,
  keyPepper: string,
): ApiKeyRecord | undefined {
  if (authCacheEpoch !== snap.epoch || authCachePepper !== keyPepper) {
    authCacheEpoch = snap.epoch;
    authCachePepper = keyPepper;
    verifiedKeyCache.clear();
  }
  const hit = verifiedKeyCache.get(rawKey);
  if (hit !== undefined) return hit;
  const digest = verifier(rawKey, keyPepper);
  const key = snap.keysByVerifier.get(digest);
  if (key) {
    if (verifiedKeyCache.size >= 4096) verifiedKeyCache.clear();
    verifiedKeyCache.set(rawKey, key);
  }
  return key;
}

async function readRequestBytesFast(
  req: Request,
  maxBytes: number,
  signal: AbortSignal,
): Promise<Uint8Array> {
  const clHeader = req.headers.get("content-length");
  if (clHeader !== null && /^\d+$/.test(clHeader)) {
    const cl = Number(clHeader);
    if (cl > maxBytes) {
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    if (!signal.aborted && cl <= 262_144) {
      const bytes = await req.bytes();
      if (bytes.byteLength > maxBytes) {
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      }
      return bytes;
    }
  }
  return readBounded(req.body, maxBytes, signal);
}

async function readResponseBytesFast(
  resp: Response,
  maxBytes: number,
  signal: AbortSignal,
): Promise<Uint8Array> {
  const clHeader = resp.headers.get("content-length");
  if (clHeader !== null && /^\d+$/.test(clHeader)) {
    const cl = Number(clHeader);
    if (cl > maxBytes) {
      void resp.body?.cancel().catch(() => undefined);
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    if (!signal.aborted && !resp.headers.has("content-encoding") && cl <= 262_144) {
      const bytes = await resp.bytes();
      if (bytes.byteLength > maxBytes) {
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      }
      return bytes;
    }
  }
  return readBounded(resp.body, maxBytes, signal);
}

function extractApiKey(c: Context, operation?: Operation): string | null {
  const auth = c.req.header("authorization");
  if (auth && auth.toLowerCase().startsWith("bearer ")) {
    const token = auth.slice(7).trim();
    if (token) return token;
  }
  if (operation === "messages" || operation === undefined) {
    const xKey = c.req.header("x-api-key")?.trim();
    if (xKey) return xKey;
  }
  if (operation === "gemini" || operation === undefined) {
    const gKey = c.req.header("x-goog-api-key")?.trim() || c.req.query("key")?.trim();
    if (gKey) return gKey;
  }
  return null;
}

export function authenticateRequest(
  c: Context,
  snap: ConfigSnapshot,
  config: GatewayConfig,
  operation?: Operation,
  peerAddress?: string,
): { key: ApiKeyRecord; project: ProjectRecord } {
  const rawKey = extractApiKey(c, operation);
  if (!rawKey) throw new GatewayError(401, "unauthorized", "Missing API key");
  const key = cachedVerifierLookup(rawKey, snap, config.keyPepper);
  if (!key || !key.enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
  if (key.expires_at && Date.parse(key.expires_at) <= Date.now()) {
    throw new GatewayError(401, "key_expired", "API key has expired");
  }
  if (operation !== undefined && key.role === "VIEWER") {
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
  if (operation !== undefined && key.operations.length > 0 && !key.operations.includes(operation)) {
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

function quoteForPrice(
  price: PriceVersionRecord,
  measurement: { inputCharacters?: string | undefined; images?: unknown },
  outputBound: number,
): string {
  const cacheable = price.basis === "PROVIDER_USAGE" && measurement.images === undefined;
  if (cacheable) {
    const ck = `${price.id}:${outputBound}`;
    const hit = quoteCache.get(ck);
    if (hit !== undefined) return hit;
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
    if (quoteCache.size >= 4096) quoteCache.clear();
    quoteCache.set(ck, quote.amount_atoms);
    return quote.amount_atoms;
  }
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
  if (operation === "images" || operation === "image_edits") return { images: "1" };
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

function responseHeaders(
  requestId: string,
  trace: TraceContext,
  extra: Record<string, string> = {},
): Record<string, string> {
  return {
    "x-request-id": requestId,
    "x-trace-id": trace.traceId,
    traceparent: trace.traceparent,
    ...extra,
  };
}

function errorJson(c: Context, err: unknown, requestId: string, trace: TraceContext): Response {
  const gw =
    err instanceof GatewayError
      ? err
      : new GatewayError(500, "internal_error", err instanceof Error ? err.message : "Error");
  return c.json(
    { error: { code: gw.code, message: gw.message, request_id: requestId } },
    gw.status as 400,
    responseHeaders(requestId, trace),
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
  const trace = parseOrCreateTrace(c.req.header("traceparent"));
  const requestStart = performance.now();
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

  let logProjectId: string | null = null;
  let logModel: string | null = null;
  let logStream = false;
  let logChannel: string | null = null;
  let logAttempts = 0;
  let logFailover = false;
  let logModBlocked = false;
  let logTtftMs: number | null = null;
  let logged = false;
  const acquiredProbes = new Set<string>();
  let holdAcquired = false;
  let holdFinalized = false;
  let anyUnknownAttempt = false;

  const releaseAllPendingProbes = () => {
    for (const chId of acquiredProbes) {
      deps.router.releaseProbe(chId);
    }
    acquiredProbes.clear();
  };

  const releaseProbeFor = (channelId: string) => {
    if (acquiredProbes.delete(channelId)) {
      deps.router.releaseProbe(channelId);
    }
  };

  const markProbeCompleted = (channelId: string) => {
    acquiredProbes.delete(channelId);
  };

  const emitAccessLog = (status: number, errorCode: string | null) => {
    if (logged) return;
    logged = true;
    deps.metrics.observeRequest({
      trace_id: trace.traceId,
      request_id: requestId,
      project_id: logProjectId,
      model: logModel,
      operation,
      stream: logStream,
      channel: logChannel,
      status,
      attempts: logAttempts,
      failover: logFailover,
      moderation_blocked: logModBlocked,
      duration_ms: performance.now() - requestStart,
      ttft_ms: logTtftMs,
      error_code: errorCode,
    });
  };

  try {
    const snap = deps.snapshot.get();
    const { key, project } = authenticateRequest(c, snap, deps.config, operation, peerAddress);
    logProjectId = project.id;

    const contentType = c.req.header("content-type") ?? "";
    const isMultipartOp =
      (operation === "transcription" ||
        operation === "translation" ||
        operation === "image_edits") &&
      contentType.toLowerCase().includes("multipart/form-data");

    let clientBody: Record<string, unknown>;
    let multipartEntries: [string, string | File][] | undefined;

    if (isMultipartOp) {
      const form = await c.req.raw.formData();
      const entries: [string, string | File][] = [];
      const meta: Record<string, unknown> = {};
      let totalFileBytes = 0;
      for (const [k, v] of form.entries()) {
        if (typeof v === "string") {
          entries.push([k, v]);
          meta[k] = v;
        } else {
          const file = v as File;
          totalFileBytes += file.size;
          if (totalFileBytes > deps.config.maxBodyBytes) {
            throw new GatewayError(413, "body_too_large", "Multipart upload exceeds limit");
          }
          entries.push([k, file]);
          meta[k] = { filename: file.name, size: file.size, type: file.type };
        }
      }
      multipartEntries = entries;
      clientBody = {
        ...meta,
        stream: meta.stream === "true" || meta.stream === true,
      };
    } else {
      const rawBytes = await readRequestBytesFast(
        c.req.raw,
        deps.config.maxBodyBytes,
        c.req.raw.signal,
      );
      const parsedRaw = object(parseClientJson(utf8FatalDecoder.decode(rawBytes)));
      clientBody = pathOverride
        ? copyExact(parsedRaw, {
            ...parsedRaw,
            model: pathOverride.model,
            stream: pathOverride.stream,
          })
        : parsedRaw;
    }

    const modelName = typeof clientBody.model === "string" ? clientBody.model.trim() : "";
    if (!modelName) throw new GatewayError(400, "invalid_request", "Missing model");
    logModel = modelName;
    if (key.models.length > 0 && !key.models.includes(modelName)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    const model = snap.models.get(modelName);
    if (!model || !model.enabled || !model.operations.includes(operation)) {
      throw new GatewayError(404, "model_not_found", "Model or operation is unavailable");
    }

    rejectUnmeteredTools(clientBody, operation);

    const isStream =
      clientBody.stream === true || (operation === "speech" && clientBody.stream_format === "sse");
    logStream = isStream;
    const outputBound = resolveOutputBound(clientBody, model);

    const estimatedTokens = Math.max(
      1,
      Math.ceil((requestCharacters(operation, clientBody) ?? 200) / 4) +
        Math.min(outputBound, 4096),
    );
    const projAcq = await deps.cluster.acquireRateLimit(
      `project:${project.id}`,
      { rpm: project.rpm, tpm: project.tpm, concurrency: project.concurrency },
      estimatedTokens,
      deps.config.holdTtlMs,
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
    for (const entry of selected) {
      if (entry.isProbe) acquiredProbes.add(entry.route.channel_id);
    }

    const releaseUnusedProbes = (fromIndex: number) => {
      for (let i = fromIndex; i < selected.length; i++) {
        if (selected[i]!.isProbe) {
          releaseProbeFor(selected[i]!.route.channel_id);
        }
      }
    };

    const preRequestAudit: PluginAuditEntry[] = [];
    const primarySteps = selected[0]!.route.route.plugin_pipeline;
    if (primarySteps.length > 0 && !isMultipartOp) {
      let preBlocked: {
        rule_code?: string;
        http_status?: number;
        safe_message?: string;
      } | null = null;
      const transformed = await transformRequestTexts(clientBody, operation, async (partText) => {
        const preRes = await deps.pluginEngine.runUnary(
          "on_request",
          primarySteps,
          snap.plugins,
          { request_id: requestId, project_id: project.id, model: model.name, operation },
          partText,
          c.req.raw.signal,
        );
        preRequestAudit.push(...preRes.audit);
        if (preRes.blocked) {
          preBlocked = {
            ...(preRes.rule_code !== undefined ? { rule_code: preRes.rule_code } : {}),
            ...(preRes.http_status !== undefined ? { http_status: preRes.http_status } : {}),
            ...(preRes.safe_message !== undefined ? { safe_message: preRes.safe_message } : {}),
          };
          return { blocked: true, text: "" };
        }
        return { blocked: false, text: preRes.text };
      });
      if (transformed.blocked && preBlocked) {
        const blk = preBlocked as {
          rule_code?: string;
          http_status?: number;
          safe_message?: string;
        };
        releaseUnusedProbes(0);
        deps.metrics.moderationBlocksTotal++;
        logModBlocked = true;
        const blockStatus = blk.http_status ?? 400;
        emitAccessLog(blockStatus, blk.rule_code ?? "content_blocked");
        return c.json(
          {
            error: {
              code: "content_blocked",
              rule_code: blk.rule_code,
              message: blk.safe_message,
              request_id: requestId,
            },
          },
          blockStatus as 400,
          responseHeaders(requestId, trace),
        );
      }
      clientBody = transformed.body;
    }

    const chars = requestCharacters(operation, clientBody);
    const measurement = {
      inputCharacters: chars === undefined ? undefined : String(chars),
      images:
        operation === "images" || operation === "image_edits"
          ? Number(clientBody.n ?? 1)
          : undefined,
    };

    const idempotencyKey = c.req.header("idempotency-key")?.trim() || null;
    const fingerprint =
      idempotencyKey !== null
        ? sha256(
            canonical({ project_id: project.id, operation, model: model.name, body: clientBody }),
          )
        : "";
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
        releaseProbeFor(cand.channel_id);
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
        if (holdRes.replayed) {
          releaseUnusedProbes(idx);
          const inProgress = holdRes.existing_state === "HELD";
          const code = inProgress ? "idempotency_in_progress" : "idempotency_replayed";
          const msg = inProgress
            ? "Request with this Idempotency-Key is already in progress"
            : `Request already finalized with state ${holdRes.existing_state ?? "UNKNOWN"}`;
          emitAccessLog(409, code);
          return c.json(
            {
              error: {
                code,
                message: msg,
                request_id: holdRes.request_id,
              },
            },
            409,
            responseHeaders(holdRes.request_id, trace),
          );
        }
        holdAcquired = true;
        activeIndex = idx;
        activeAttemptId = attemptId;
        holdCustomerAtoms = holdRes.held_atoms;
        break;
      }
      releaseProbeFor(cand.channel_id);
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
      logChannel = cand.channel.name;

      if (pendingSwitchFrom) {
        let nextCustomerAtoms: string;
        let nextSupplierAtoms: string;
        try {
          nextCustomerAtoms = quoteForPrice(cand.billingPrice, measurement, outputBound);
          nextSupplierAtoms = quoteForPrice(cand.costPrice, measurement, outputBound);
        } catch (err) {
          releaseProbeFor(cand.channel_id);
          lastErrorStatus = 422;
          lastErrorCode = "billing_ceiling";
          lastErrorMessage = err instanceof Error ? err.message : "Fallback quote error";
          continue;
        }
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
          releaseProbeFor(cand.channel_id);
          if (sw.code === "supplier_unavailable" || sw.code === "supplier_insufficient_balance") {
            continue;
          }
          releaseUnusedProbes(cursor + 1);
          holdFinalized = true;
          if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
          else await deps.ledger.releaseHold(requestId);
          throw mapHoldError(sw.code, sw.message);
        }
        activeAttemptId = nextAttemptId;
        holdCustomerAtoms = sw.held_atoms;
        pendingSwitchFrom = null;
        logFailover = true;
        deps.metrics.failoversTotal++;
      }

      const chLimit = await deps.cluster.acquireRateLimit(
        `channel:${cand.channel.id}`,
        {
          rpm: cand.channel.rpm,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: cand.channel.concurrency,
        },
        1,
        deps.config.holdTtlMs,
      );
      if (!chLimit.ok) {
        releaseProbeFor(cand.channel_id);
        pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        lastErrorStatus = 429;
        lastErrorCode = "channel_saturated";
        lastErrorMessage = "Upstream channel concurrency or RPM limit reached";
        continue;
      }
      releaseChannelLimit = chLimit.release;
      attemptsMade++;
      logAttempts = attemptsMade;

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
          snapshot: snap,
          secret,
          operation,
          clientBody,
          ...(multipartEntries ? { multipartEntries } : {}),
          isStream,
          vertexTokens: deps.vertexTokens,
          wasmExecutor: deps.wasmExecutor,
          ...(deps.config.pluginSigningPublicKeyPem
            ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
            : {}),
          traceparent: trace.traceparent,
          signal: attemptAbort.signal,
        });
      } catch (err) {
        clearTimeout(timeoutTimer);
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        releaseChannelLimit();
        releaseChannelLimit = null;
        if (
          err instanceof GatewayError &&
          (err.code === "provider_plugin_unavailable" || err.code === "provider_adapter_failed")
        ) {
          deps.metrics.recordUpstreamError(cand.channel.name, err.code);
          deps.router.recordFailure(
            cand.channel.id,
            "retryable",
            undefined,
            performance.now() - startedAt,
          );
          markProbeCompleted(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
          lastErrorStatus = err.status;
          lastErrorCode = err.code;
          lastErrorMessage = err.message;
          continue;
        }
        releaseProbeFor(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        holdFinalized = true;
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
            body: prepared.body as BodyInit,
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
            /ECONNREFUSED|ENOTFOUND|Unable to connect|ConnectionRefused/i.test(err.message));
        if (c.req.raw.signal.aborted) {
          releaseProbeFor(cand.channel_id);
          releaseUnusedProbes(cursor + 1);
          holdFinalized = true;
          if (isPreDispatch && !anyUnknownAttempt) await deps.ledger.releaseHold(requestId);
          else await deps.ledger.markUnknown(requestId);
          emitAccessLog(499, "client_aborted");
          return c.json(
            { error: { code: "client_aborted", message: "Client disconnected" } },
            499 as 400,
            responseHeaders(requestId, trace),
          );
        }
        deps.metrics.recordUpstreamError(
          cand.channel.name,
          isPreDispatch ? "connect_error" : "timeout",
        );
        deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
        markProbeCompleted(cand.channel.id);
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
        holdFinalized = true;
        await deps.ledger.markUnknown(requestId);
        throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
      }

      const latencyMs = performance.now() - startedAt;
      logTtftMs = latencyMs;
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
          deps.metrics.recordUpstreamError(cand.channel.name, `http_${upstreamResp.status}`);
          deps.router.recordFailure(cand.channel.id, retryKind, retryAfterMs, latencyMs);
          markProbeCompleted(cand.channel.id);
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
        releaseProbeFor(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        holdFinalized = true;
        if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
        else await deps.ledger.releaseHold(requestId);
        emitAccessLog(upstreamResp.status, "upstream_client_error");
        return c.json(
          {
            error: {
              code: "upstream_client_error",
              message: `Upstream rejected request with HTTP ${upstreamResp.status}`,
              request_id: requestId,
            },
          },
          upstreamResp.status as 400,
          responseHeaders(requestId, trace),
        );
      }

      const upstreamContentType = upstreamResp.headers.get("content-type") ?? "application/json";
      const isUpstreamSse = upstreamContentType.toLowerCase().includes("text/event-stream");
      const isUpstreamJson = upstreamContentType.toLowerCase().includes("application/json");
      const isFakeJsonOnStream =
        isStream && !isUpstreamSse && isUpstreamJson && !prepared.isBedrockStream;

      if ((!isStream && !isUpstreamSse) || isFakeJsonOnStream) {
        let retryFromLogicalError = false;
        try {
          const respBytes = await readResponseBytesFast(
            upstreamResp,
            deps.config.maxBodyBytes,
            attemptAbort.signal,
          );

          if (!isUpstreamJson) {
            releaseUnusedProbes(cursor + 1);
            const usage = buildUsageForOperation(
              operation,
              cand.billingPrice.basis,
              undefined,
              chars,
            );
            if (!usage) {
              holdFinalized = true;
              await deps.ledger.markUnknown(requestId);
              throw new GatewayError(
                502,
                "invalid_upstream_usage",
                "Non-JSON response requires character or request pricing basis",
              );
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
            holdFinalized = true;
            deps.router.recordSuccess(cand.channel.id, latencyMs);
            markProbeCompleted(cand.channel.id);
            emitAccessLog(200, null);
            return new Response(respBytes as BodyInit, {
              status: 200,
              headers: responseHeaders(requestId, trace, {
                "content-type": upstreamContentType,
                "x-gateway-channel": cand.channel.name,
              }),
            });
          }

          const rawJson = object(parseProviderJson(utf8FatalDecoder.decode(respBytes)));
          let rawUsage: Usage | undefined;
          let clientJson: Record<string, unknown>;

          if (
            prepared.resolvedProvider.kind === "WASM" &&
            prepared.resolvedProvider.config.hooks.includes("adapt_response")
          ) {
            const respHeadersObj: Record<string, string> = {};
            upstreamResp.headers.forEach((v, k) => {
              respHeadersObj[k] = v;
            });
            let wasmResp: Awaited<ReturnType<typeof invokeWasmProviderResponse>>;
            try {
              wasmResp = await invokeWasmProviderResponse({
                executor: deps.wasmExecutor,
                plugin: prepared.resolvedProvider.plugin,
                config: prepared.resolvedProvider.config,
                ...(deps.config.pluginSigningPublicKeyPem
                  ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
                  : {}),
                input: {
                  operation,
                  upstream_model: cand.route.upstream_model,
                  client_protocol: isConversationalOperation(operation) ? operation : null,
                  status: upstreamResp.status,
                  headers: respHeadersObj,
                  body: rawJson,
                },
              });
            } catch (err) {
              releaseUnusedProbes(cursor + 1);
              holdFinalized = true;
              await deps.ledger.markUnknown(requestId);
              deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
              markProbeCompleted(cand.channel.id);
              throw new GatewayError(
                502,
                "provider_adapter_failed",
                err instanceof Error ? err.message : "WASM provider adapt_response failed",
              );
            }

            if (wasmResp.retry_kind) {
              if (wasmResp.retry_kind !== "client_error") {
                deps.metrics.recordUpstreamError(
                  cand.channel.name,
                  `logical_${wasmResp.retry_kind}`,
                );
                deps.router.recordFailure(
                  cand.channel.id,
                  wasmResp.retry_kind,
                  wasmResp.retry_after_ms,
                  latencyMs,
                );
                markProbeCompleted(cand.channel.id);
                releaseChannelLimit?.();
                releaseChannelLimit = null;
                pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
                lastErrorStatus =
                  wasmResp.status ?? (wasmResp.retry_kind === "rate_limit" ? 429 : 502);
                lastErrorCode =
                  wasmResp.retry_kind === "rate_limit"
                    ? "upstream_rate_limited"
                    : wasmResp.retry_kind === "auth_error"
                      ? "upstream_auth_error"
                      : "upstream_error";
                lastErrorMessage = wasmResp.error_message ?? "Upstream logical error";
                retryFromLogicalError = true;
              } else {
                releaseProbeFor(cand.channel_id);
                releaseUnusedProbes(cursor + 1);
                holdFinalized = true;
                if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
                else await deps.ledger.releaseHold(requestId);
                const errStatus = wasmResp.status ?? 400;
                emitAccessLog(errStatus, "upstream_client_error");
                return c.json(
                  {
                    error: {
                      code: "upstream_client_error",
                      message: wasmResp.error_message ?? "Upstream rejected request",
                      request_id: requestId,
                    },
                  },
                  errStatus as 400,
                  responseHeaders(requestId, trace),
                );
              }
            }

            if (!retryFromLogicalError) {
              releaseUnusedProbes(cursor + 1);
              if (isFakeJsonOnStream) {
                holdFinalized = true;
                await deps.ledger.markUnknown(requestId);
                throw new GatewayError(
                  502,
                  "invalid_upstream",
                  "Upstream returned JSON body instead of event stream",
                );
              }
              const adaptedBody = wasmResp.body ?? rawJson;
              try {
                if (wasmResp.usage !== undefined) {
                  rawUsage = validateUsageObject(wasmResp.usage);
                } else if (prepared.usageDialect !== "custom") {
                  rawUsage = extractUsage(adaptedBody, prepared.usageDialect);
                }
              } catch (err) {
                holdFinalized = true;
                await deps.ledger.markUnknown(requestId);
                deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
                markProbeCompleted(cand.channel.id);
                throw err;
              }
              if (
                isConversationalOperation(operation) &&
                prepared.upstreamProtocol &&
                prepared.upstreamProtocol !== operation
              ) {
                const completion = decodeResponse(
                  adaptedBody,
                  prepared.upstreamProtocol,
                  prepared.usageDialect,
                );
                clientJson = encodeResponse(completion, operation, model.name);
              } else {
                clientJson = copyExact(adaptedBody, { ...adaptedBody });
                if (typeof clientJson.model === "string") clientJson.model = model.name;
              }
            } else {
              clientJson = {};
            }
          } else {
            const matchedErr = evaluateErrorRules(
              upstreamResp.status,
              rawJson,
              prepared.mergedOverrides.error_rules,
            );
            if (matchedErr) {
              if (matchedErr.retry_kind !== "client_error") {
                deps.metrics.recordUpstreamError(
                  cand.channel.name,
                  `logical_${matchedErr.retry_kind}`,
                );
                deps.router.recordFailure(
                  cand.channel.id,
                  matchedErr.retry_kind,
                  matchedErr.retry_after_ms,
                  latencyMs,
                );
                markProbeCompleted(cand.channel.id);
                releaseChannelLimit?.();
                releaseChannelLimit = null;
                pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
                lastErrorStatus =
                  matchedErr.http_status ?? (matchedErr.retry_kind === "rate_limit" ? 429 : 502);
                lastErrorCode =
                  matchedErr.retry_kind === "rate_limit"
                    ? "upstream_rate_limited"
                    : matchedErr.retry_kind === "auth_error"
                      ? "upstream_auth_error"
                      : "upstream_error";
                lastErrorMessage = matchedErr.message ?? "Upstream logical error";
                retryFromLogicalError = true;
                clientJson = {};
              } else {
                releaseProbeFor(cand.channel_id);
                releaseUnusedProbes(cursor + 1);
                holdFinalized = true;
                if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
                else await deps.ledger.releaseHold(requestId);
                const errStatus = matchedErr.http_status ?? 400;
                emitAccessLog(errStatus, "upstream_client_error");
                return c.json(
                  {
                    error: {
                      code: "upstream_client_error",
                      message: matchedErr.message ?? "Upstream rejected request",
                      request_id: requestId,
                    },
                  },
                  errStatus as 400,
                  responseHeaders(requestId, trace),
                );
              }
            } else {
              releaseUnusedProbes(cursor + 1);
              if (isFakeJsonOnStream) {
                holdFinalized = true;
                await deps.ledger.markUnknown(requestId);
                throw new GatewayError(
                  502,
                  "invalid_upstream",
                  "Upstream returned JSON body instead of event stream",
                );
              }
              const adaptedJson =
                prepared.mergedOverrides.response_transforms.length > 0
                  ? copyExact(
                      rawJson,
                      applyResponseTransforms(
                        rawJson,
                        prepared.mergedOverrides.response_transforms,
                      ),
                    )
                  : rawJson;
              try {
                const baseUsage =
                  prepared.usageDialect === "custom"
                    ? undefined
                    : extractUsage(adaptedJson, prepared.usageDialect);
                rawUsage = mergeUsageWithAliases(
                  baseUsage,
                  adaptedJson,
                  prepared.mergedOverrides.usage_aliases,
                );
              } catch (err) {
                holdFinalized = true;
                await deps.ledger.markUnknown(requestId);
                deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
                markProbeCompleted(cand.channel.id);
                throw err;
              }
              if (
                isConversationalOperation(operation) &&
                prepared.upstreamProtocol &&
                prepared.upstreamProtocol !== operation
              ) {
                const completion = decodeResponse(
                  adaptedJson,
                  prepared.upstreamProtocol,
                  prepared.usageDialect,
                );
                clientJson = encodeResponse(completion, operation, model.name);
              } else {
                clientJson = copyExact(adaptedJson, { ...adaptedJson });
                if (typeof clientJson.model === "string") clientJson.model = model.name;
              }
            }
          }

          if (!retryFromLogicalError) {
            const usage = buildUsageForOperation(
              operation,
              cand.billingPrice.basis,
              rawUsage,
              chars,
            );
            if (!usage) {
              holdFinalized = true;
              await deps.ledger.markUnknown(requestId);
              deps.router.recordFailure(cand.channel.id, "retryable", undefined, latencyMs);
              markProbeCompleted(cand.channel.id);
              throw new GatewayError(
                502,
                "invalid_upstream_usage",
                "Upstream omitted usage metadata",
              );
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
                logModBlocked = true;
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

            holdFinalized = true;
            deps.router.recordSuccess(cand.channel.id, latencyMs);
            markProbeCompleted(cand.channel.id);
            if (moderationBlocked) {
              emitAccessLog(blockStatus, blockRule);
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
                responseHeaders(requestId, trace, { "x-gateway-channel": cand.channel.name }),
              );
            }
            emitAccessLog(200, null);
            return new Response(stringifyExact(clientJson), {
              status: 200,
              headers: responseHeaders(requestId, trace, {
                "content-type": "application/json",
                "x-gateway-channel": cand.channel.name,
              }),
            });
          }
        } finally {
          clearTimeout(timeoutTimer);
          c.req.raw.signal.removeEventListener("abort", onClientAbort);
          guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        }
        if (retryFromLogicalError) {
          continue;
        }
      }

      releaseUnusedProbes(cursor + 1);
      clearTimeout(timeoutTimer);
      holdFinalized = true;
      const streamProbeChannel = acquiredProbes.delete(cand.channel.id) ? cand.channel.id : null;
      const upstreamBody = upstreamResp.body;
      const capturedProjectLimit = releaseProjectLimit;
      const capturedChannelLimit = releaseChannelLimit;
      releaseProjectLimit = null;
      releaseChannelLimit = null;

      let streamCleanupDone = false;
      let streamProbeDone = false;
      const finishStreamProbe = () => {
        if (!streamProbeDone && streamProbeChannel) {
          streamProbeDone = true;
          deps.router.releaseProbe(streamProbeChannel);
        }
      };
      const cleanupStream = () => {
        if (streamCleanupDone) return;
        streamCleanupDone = true;
        finishStreamProbe();
        c.req.raw.signal.removeEventListener("abort", onClientAbort);
        guard.controller.signal.removeEventListener("abort", onShutdownAbort);
        capturedChannelLimit?.();
        capturedProjectLimit?.();
        releaseGuard();
      };

      const isWasmStreamProvider =
        prepared.resolvedProvider.kind === "WASM" &&
        prepared.resolvedProvider.config.hooks.includes("adapt_stream_event") &&
        isConversationalOperation(operation);

      if (
        !isConversationalOperation(operation) ||
        (!prepared.upstreamProtocol && !isWasmStreamProvider)
      ) {
        const usageDialect = prepared.usageDialect;
        let clientCancelled = false;
        const mediaStream = new ReadableStream<Uint8Array>({
          async start(controller) {
            const reader = upstreamBody?.getReader();
            const parser = new SSEParser();
            let observedUsage: Usage | undefined;
            const heldChunks: Uint8Array[] = [];
            try {
              if (!reader) {
                throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
              }
              while (true) {
                const step = await reader.read();
                if (step.done) break;
                const events = parser.feed(step.value, false);
                for (const ev of events) {
                  let isDoneEvent = false;
                  if (ev.data && ev.data !== "[DONE]") {
                    const parsed = object(parseProviderJson(ev.data));
                    const baseU =
                      usageDialect === "custom"
                        ? observedUsage
                        : (extractUsage(parsed, usageDialect, observedUsage) ?? observedUsage);
                    observedUsage =
                      mergeUsageWithAliases(
                        baseU,
                        parsed,
                        prepared.mergedOverrides.usage_aliases,
                      ) ?? baseU;
                    const t = String(parsed.type ?? ev.event);
                    if (t.endsWith(".done") || t.endsWith(".completed")) isDoneEvent = true;
                  }
                  const bytes = utf8Encoder.encode(ev.wire);
                  if (isDoneEvent) heldChunks.push(bytes);
                  else controller.enqueue(bytes);
                }
              }
              if (clientCancelled || c.req.raw.signal.aborted) {
                await deps.ledger.markUnknown(requestId).catch(() => undefined);
                emitAccessLog(499, "client_aborted");
                return;
              }
              for (const ev of parser.feed(new Uint8Array(), true)) {
                heldChunks.push(utf8Encoder.encode(ev.wire));
              }
              const usage = buildUsageForOperation(
                operation,
                cand.billingPrice.basis,
                observedUsage,
                chars,
              );
              if (!usage) {
                await deps.ledger.markUnknown(requestId);
                emitAccessLog(502, "invalid_upstream_usage");
                controller.enqueue(
                  encodeSSE(
                    {
                      error: {
                        code: "invalid_upstream_usage",
                        message: "Media stream completed without usage",
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
              streamProbeDone = true;
              deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
              for (const ch of heldChunks) controller.enqueue(ch);
              emitAccessLog(200, null);
              controller.close();
            } catch (err) {
              const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
              await deps.ledger.markUnknown(requestId).catch(() => undefined);
              if (abortedByClient) {
                emitAccessLog(499, "client_aborted");
                return;
              }
              deps.metrics.recordUpstreamError(cand.channel.name, "media_stream_error");
              streamProbeDone = true;
              deps.router.recordFailure(
                cand.channel.id,
                "retryable",
                undefined,
                performance.now() - startedAt,
              );
              emitAccessLog(502, "stream_interrupted");
              try {
                controller.enqueue(
                  encodeSSE(
                    {
                      error: {
                        code: "stream_interrupted",
                        message: err instanceof Error ? err.message : "Media stream failed",
                        request_id: requestId,
                      },
                    },
                    "error",
                  ),
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
            clientCancelled = true;
            attemptAbort.abort(new Error("client_cancelled"));
            void upstreamBody?.cancel().catch(() => undefined);
          },
        });
        return new Response(mediaStream, {
          status: 200,
          headers: responseHeaders(requestId, trace, {
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache",
            "x-gateway-channel": cand.channel.name,
          }),
        });
      }

      const clientProtocol: Protocol = operation;
      const streamId = `gen_${requestId.replaceAll("-", "")}`;
      const hasPlugins = cand.route.plugin_pipeline.length > 0;
      let clientCancelled = false;

      if (isWasmStreamProvider && prepared.resolvedProvider.kind === "WASM") {
        const wasmProvider = prepared.resolvedProvider;
        const wasmStream = new ReadableStream<Uint8Array>({
          async start(controller) {
            const reader = upstreamBody?.getReader();
            const parser = new SSEParser();
            const bedrockDecoder = prepared.isBedrockStream
              ? new BedrockEventStreamDecoder()
              : null;
            const pluginSignal = AbortSignal.any([c.req.raw.signal, guard.controller.signal]);
            const session = hasPlugins
              ? deps.pluginEngine.createStreamSession(
                  cand.route.plugin_pipeline,
                  snap.plugins,
                  { request_id: requestId, project_id: project.id, model: model.name, operation },
                  () => attemptAbort.abort(new Error("moderation_blocked")),
                  pluginSignal,
                )
              : null;
            const heldTerminalChunks: Uint8Array[] = [];
            const streamPluginAudit: PluginAuditEntry[] = [];
            let observedOutputChars = 0;
            let wasmState = "";
            let wasmUsage: Usage | undefined;
            let streamFailed = false;
            let blockedEarly = false;

            const processWasmEvent = async (ev: { event: string; data: string }) => {
              const out = await invokeWasmProviderStreamEvent({
                executor: deps.wasmExecutor,
                plugin: wasmProvider.plugin,
                config: wasmProvider.config,
                ...(deps.config.pluginSigningPublicKeyPem
                  ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
                  : {}),
                input: {
                  operation,
                  upstream_model: cand.route.upstream_model,
                  client_protocol: clientProtocol,
                  event: ev.event,
                  data: ev.data,
                  state: wasmState,
                },
              });
              if (out.state !== undefined) wasmState = out.state;
              if (out.retry_kind) {
                throw new GatewayError(
                  out.retry_kind === "rate_limit" ? 429 : 502,
                  "upstream_stream_error",
                  out.error_message ?? "Upstream stream reported error",
                );
              }
              if (out.usage !== undefined) {
                const validated = validateUsageObject(out.usage);
                wasmUsage = { ...wasmUsage, ...validated };
              }
              for (const chunkObj of out.chunks ?? []) {
                const evName =
                  clientProtocol === "messages" && typeof chunkObj.type === "string"
                    ? chunkObj.type
                    : undefined;
                const encoded = encodeSSE(chunkObj, evName);
                if (!session) {
                  controller.enqueue(encoded);
                  continue;
                }
                const deltaText = extractChunkTextFromEncodedSse(encoded, clientProtocol);
                if (deltaText === null) {
                  controller.enqueue(encoded);
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
              if (out.done && clientProtocol === "chat") {
                heldTerminalChunks.push(utf8Encoder.encode("data: [DONE]\n\n"));
              }
            };

            try {
              if (!reader) {
                throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
              }
              while (true) {
                let step: Awaited<ReturnType<typeof reader.read>>;
                try {
                  step = await reader.read();
                } catch (readErr) {
                  if (session?.isBlocked()) {
                    blockedEarly = true;
                    break;
                  }
                  throw readErr;
                }
                if (step.done) break;
                const events = bedrockDecoder
                  ? bedrockDecoder.feed(step.value)
                  : parser.feed(step.value, false);
                for (const ev of events) {
                  await processWasmEvent(ev);
                  if (blockedEarly) break;
                }
                if (blockedEarly) {
                  await reader.cancel().catch(() => undefined);
                  break;
                }
              }

              if (clientCancelled || c.req.raw.signal.aborted) {
                await deps.ledger.markUnknown(requestId).catch(() => undefined);
                emitAccessLog(499, "client_aborted");
                return;
              }

              if (!blockedEarly && !bedrockDecoder) {
                for (const ev of parser.feed(new Uint8Array(), true)) {
                  await processWasmEvent(ev);
                  if (blockedEarly) break;
                }
              }

              if (session) {
                const finalMod = await session.finalize();
                streamPluginAudit.push(...finalMod.audit);
                if (finalMod.blocked) {
                  deps.metrics.moderationBlocksTotal++;
                  logModBlocked = true;
                  const modUsage = buildModerationUsageFallback(
                    wasmUsage,
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
                    plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                    charge_policy: finalMod.charge_policy,
                    moderation_blocked: true,
                  });
                  streamProbeDone = true;
                  deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
                  emitAccessLog(400, finalMod.rule_code ?? "content_blocked");
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

              if (clientProtocol === "chat" && heldTerminalChunks.length === 0) {
                heldTerminalChunks.push(utf8Encoder.encode("data: [DONE]\n\n"));
              }

              const usage = buildUsageForOperation(
                operation,
                cand.billingPrice.basis,
                wasmUsage,
                chars,
              );
              if (!usage) {
                streamFailed = true;
                deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
                streamProbeDone = true;
                deps.router.recordFailure(
                  cand.channel.id,
                  "retryable",
                  undefined,
                  performance.now() - startedAt,
                );
                await deps.ledger.markUnknown(requestId);
                emitAccessLog(502, "invalid_upstream_usage");
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
                plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                charge_policy: "CHARGE_CUSTOMER",
                moderation_blocked: false,
              });
              if (!commitRes.ok) {
                throw new GatewayError(500, "ledger_commit_failed", commitRes.message);
              }

              streamProbeDone = true;
              deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
              for (const chunk of heldTerminalChunks) {
                controller.enqueue(chunk);
              }
              emitAccessLog(200, null);
              controller.close();
            } catch (err) {
              const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
              if (!streamFailed) {
                streamFailed = true;
                await deps.ledger.markUnknown(requestId).catch(() => undefined);
                if (!abortedByClient) {
                  deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
                  streamProbeDone = true;
                  deps.router.recordFailure(
                    cand.channel.id,
                    "retryable",
                    undefined,
                    performance.now() - startedAt,
                  );
                }
              }
              if (abortedByClient) {
                emitAccessLog(499, "client_aborted");
                return;
              }
              const code = err instanceof GatewayError ? err.code : "stream_interrupted";
              emitAccessLog(502, code);
              try {
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
            clientCancelled = true;
            attemptAbort.abort(new Error("client_cancelled"));
            void upstreamBody?.cancel().catch(() => undefined);
          },
        });
        return new Response(wasmStream, {
          status: 200,
          headers: responseHeaders(requestId, trace, {
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache",
            "x-gateway-channel": cand.channel.name,
          }),
        });
      }

      const upstreamProtocol: Protocol = prepared.upstreamProtocol!;

      const bodyStream = new ReadableStream<Uint8Array>({
        async start(controller) {
          const reader = upstreamBody?.getReader();
          const parser = new SSEParser();
          const bedrockDecoder = prepared.isBedrockStream ? new BedrockEventStreamDecoder() : null;
          const translator = new StreamTranslator(
            upstreamProtocol,
            clientProtocol,
            prepared.usageDialect,
            model.name,
          );
          const hasStreamRules =
            prepared.mergedOverrides.error_rules.length > 0 ||
            prepared.mergedOverrides.usage_aliases.length > 0;
          let streamAliasUsage: Usage | undefined;
          const pluginSignal = AbortSignal.any([c.req.raw.signal, guard.controller.signal]);
          const session = hasPlugins
            ? deps.pluginEngine.createStreamSession(
                cand.route.plugin_pipeline,
                snap.plugins,
                { request_id: requestId, project_id: project.id, model: model.name, operation },
                () => attemptAbort.abort(new Error("moderation_blocked")),
                pluginSignal,
              )
            : null;
          const heldTerminalChunks: Uint8Array[] = [];
          const streamPluginAudit: PluginAuditEntry[] = [];
          let observedOutputChars = 0;
          let streamFailed = false;
          let blockedEarly = false;

          try {
            if (!reader) {
              throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
            }
            while (true) {
              let step: Awaited<ReturnType<typeof reader.read>>;
              try {
                step = await reader.read();
              } catch (readErr) {
                if (session?.isBlocked()) {
                  blockedEarly = true;
                  break;
                }
                throw readErr;
              }
              if (step.done) break;
              const events = bedrockDecoder
                ? bedrockDecoder.feed(step.value)
                : parser.feed(step.value, false);
              for (const ev of events) {
                if (hasStreamRules && ev.data && ev.data !== "[DONE]") {
                  const parsedEv = object(parseProviderJson(ev.data));
                  const matchedErr = evaluateErrorRules(
                    200,
                    parsedEv,
                    prepared.mergedOverrides.error_rules,
                  );
                  if (matchedErr) {
                    throw new GatewayError(
                      matchedErr.http_status ??
                        (matchedErr.retry_kind === "rate_limit" ? 429 : 502),
                      "upstream_stream_error",
                      matchedErr.message ?? "Upstream stream error",
                    );
                  }
                  if (prepared.mergedOverrides.usage_aliases.length > 0) {
                    const mergedU = mergeUsageWithAliases(
                      streamAliasUsage,
                      parsedEv,
                      prepared.mergedOverrides.usage_aliases,
                    );
                    if (mergedU) streamAliasUsage = mergedU;
                  }
                }
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

            if (clientCancelled || c.req.raw.signal.aborted) {
              await deps.ledger.markUnknown(requestId).catch(() => undefined);
              emitAccessLog(499, "client_aborted");
              return;
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

            const combinedStreamUsage: Usage | undefined =
              streamAliasUsage && translator.usage
                ? validateUsageObject({ ...translator.usage, ...streamAliasUsage })
                : (translator.usage ?? streamAliasUsage);

            if (session) {
              const finalMod = await session.finalize();
              streamPluginAudit.push(...finalMod.audit);
              if (finalMod.blocked) {
                deps.metrics.moderationBlocksTotal++;
                logModBlocked = true;
                const modUsage = buildModerationUsageFallback(
                  combinedStreamUsage,
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
                  plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                  charge_policy: finalMod.charge_policy,
                  moderation_blocked: true,
                });
                streamProbeDone = true;
                deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
                emitAccessLog(400, finalMod.rule_code ?? "content_blocked");
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
              combinedStreamUsage,
              chars,
            );
            if (!usage) {
              streamFailed = true;
              deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
              streamProbeDone = true;
              deps.router.recordFailure(
                cand.channel.id,
                "retryable",
                undefined,
                performance.now() - startedAt,
              );
              await deps.ledger.markUnknown(requestId);
              emitAccessLog(502, "invalid_upstream_usage");
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
              plugin_audit: [...preRequestAudit, ...streamPluginAudit],
              charge_policy: "CHARGE_CUSTOMER",
              moderation_blocked: false,
            });
            if (!commitRes.ok) {
              throw new GatewayError(500, "ledger_commit_failed", commitRes.message);
            }

            streamProbeDone = true;
            deps.router.recordSuccess(cand.channel.id, performance.now() - startedAt);
            for (const chunk of heldTerminalChunks) {
              controller.enqueue(chunk);
            }
            emitAccessLog(200, null);
            controller.close();
          } catch (err) {
            const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
            if (!streamFailed) {
              streamFailed = true;
              await deps.ledger.markUnknown(requestId).catch(() => undefined);
              if (!abortedByClient) {
                deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
                streamProbeDone = true;
                deps.router.recordFailure(
                  cand.channel.id,
                  "retryable",
                  undefined,
                  performance.now() - startedAt,
                );
              }
            }
            if (abortedByClient) {
              emitAccessLog(499, "client_aborted");
              return;
            }
            const code = err instanceof GatewayError ? err.code : "stream_interrupted";
            emitAccessLog(502, code);
            try {
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
          clientCancelled = true;
          attemptAbort.abort(new Error("client_cancelled"));
          void upstreamBody?.cancel().catch(() => undefined);
        },
      });

      return new Response(bodyStream, {
        status: 200,
        headers: responseHeaders(requestId, trace, {
          "content-type": "text/event-stream; charset=utf-8",
          "cache-control": "no-cache",
          "x-gateway-channel": cand.channel.name,
        }),
      });
    }

    releaseAllPendingProbes();
    holdFinalized = true;
    if (anyUnknownAttempt) await deps.ledger.markUnknown(requestId);
    else await deps.ledger.releaseHold(requestId);
    throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
  } catch (err) {
    releaseAllPendingProbes();
    if (holdAcquired && !holdFinalized) {
      holdFinalized = true;
      await deps.ledger.markUnknown(requestId).catch(() => undefined);
    }
    const status = err instanceof GatewayError ? err.status : 500;
    const code = err instanceof GatewayError ? err.code : "internal_error";
    emitAccessLog(status, code);
    return errorJson(c, err, requestId, trace);
  } finally {
    releaseAllPendingProbes();
    releaseLimiters();
    releaseGuard();
  }
}
