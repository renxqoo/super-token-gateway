import type { ContentfulStatusCode } from "hono/utils/http-status";
import { GatewayError, object, optionalObject } from "@gateway/contracts";

export interface InferenceContext {
  req: {
    raw: Request;
    query(name: string): string | undefined;
  };
  json(data: unknown, status?: ContentfulStatusCode, headers?: Record<string, string>): Response;
}
import {
  canonical,
  normalizedNumber,
  type Operation,
  type Protocol,
  type Usage,
} from "@gateway/kernel";
import type {
  ApiKeyRecord,
  CommitTerminalInput,
  ConfigSnapshot,
  createAsyncTasksRepository,
  createLedgerEngine,
  ModelRecord,
  PluginAuditEntry,
  PriceVersionRecord,
  ProjectRecord,
  RouteRecord,
} from "@gateway/ledger";
import {
  applyResponseTransforms,
  describeProviderError,
  evaluateErrorRules,
  getAtPath,
  invokeWasmProviderResponse,
  invokeWasmProviderStreamEvent,
  WasmCapacityError,
  WasmPluginFaultError,
  type MatchedProviderError,
  type PluginOutcomeKind,
  type PluginPipelineEngine,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import {
  copyExact,
  decodeResponse,
  encodeResponse,
  encodeSSE,
  extractUsage,
  formatProtocolError,
  formatProtocolSseError,
  numberLiteral,
  parseClientJson,
  parseProviderJson,
  requestCharacters,
  SSEParser,
  type SSEEvent,
  StreamTranslator,
  stringifyExact,
  validateUsageObject,
} from "@gateway/protocol";
import {
  clientAddress,
  inCidrs,
  Lifecycle,
  PROJECT_API_KEY_PATTERN,
  readBounded,
  sha256,
  SmartRouter,
  UpstreamPreDispatchError,
  verifier,
  VertexTokens,
  type CancelRegistrar,
  type ClusterCoordinator,
  type createTransport,
  type FastUpstreamResult,
  type PreparedDispatch,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import { declaredRequestSize } from "./public-ingress.ts";
import type { ConsoleAdmissionCheck } from "./console-admission.ts";
import { extractUpstreamTaskId, getAsyncTaskConfig } from "./async-tasks.ts";
import { resolveEffectiveCustomerPrice } from "./pricing.ts";
import {
  fastUuid,
  ProxyRuntimeState,
  type DeadlineWheel,
  type HydratedCandidate,
} from "./proxy-state.ts";
import { createResponsesModeration } from "./responses-moderation.ts";
import {
  createStreamChunkReader,
  createStreamRelay,
  createTerminalBuffer,
  peekStream,
} from "./stream-relay.ts";
import { parseOrCreateTrace, type GatewayMetrics, type TraceContext } from "./observability.ts";
import { createRequestTrace, createTransportTiming } from "./first-text-trace.ts";
import type { createTraceQueue } from "./trace-queue.ts";
import type { ChannelFailureKind, ChannelFailureStage, TraceAttempt } from "./trace-types.ts";
import type { SnapshotManager } from "./snapshot.ts";
import { rejectUnmeteredTools } from "./tool-policy.ts";
import {
  BedrockEventStreamDecoder,
  buildTextDeltaSse,
  extractChunkTextFromEncodedSse,
  extractResponseText,
  inspectAnthropicBlockEvent,
  isConversationalOperation,
  mergeUsageSources,
  mergeUsageWithAliases,
  parseRetryAfterMs,
  prepareUpstreamRequest,
  replaceResponseText,
  resolveRouteUpstreamProtocol,
  stripTextFromSseChunk,
  transformRequestTexts,
  tryPrepareUpstreamRequestSync,
  type PreparedUpstreamRequest,
  type PrepareUpstreamOptions,
} from "./upstream.ts";

type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type AsyncTasksRepo = ReturnType<typeof createAsyncTasksRepository>;
type Transport = ReturnType<typeof createTransport>;

export type ProxyDependencies = {
  config: GatewayConfig;
  checkConsoleAdmission: ConsoleAdmissionCheck;
  snapshot: SnapshotManager;
  ledger: LedgerEngine;
  asyncTasks?: AsyncTasksRepo;
  router: SmartRouter;
  cluster: ClusterCoordinator;
  transport: Transport;
  vertexTokens: VertexTokens;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
  lifecycle: Lifecycle;
  metrics: GatewayMetrics;
  traceQueue?: ReturnType<typeof createTraceQueue>;
  state?: ProxyRuntimeState;
  bodyBufferBudget?: { reserve(bytes: number): () => void };
};

const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();
const fallbackStates = new WeakMap<ProxyDependencies, ProxyRuntimeState>();

function getProxyState(deps: ProxyDependencies): ProxyRuntimeState {
  if (deps.state) return deps.state;
  let state = fallbackStates.get(deps);
  if (!state) {
    state = new ProxyRuntimeState();
    fallbackStates.set(deps, state);
  }
  return state;
}

function getHeaderValue(headers: Headers | Record<string, string>, name: string): string | null {
  if (headers instanceof Headers) return headers.get(name);
  return headers[name] ?? null;
}

async function readRequestBytesBounded(
  req: Request,
  maxBytes: number,
  timeoutMs: number,
  deadlines: DeadlineWheel,
  isDraining: () => boolean,
  setAbortNotify: (fn: ((reason: unknown) => void) | null) => void,
  forceStream: boolean,
): Promise<Uint8Array> {
  if (req.signal.aborted) {
    throw new GatewayError(499, "client_aborted", "Client disconnected");
  }
  if (isDraining()) {
    throw new GatewayError(503, "overloaded", "Gateway is draining");
  }
  const parsedCl = declaredRequestSize(req, maxBytes) ?? -1;
  const timeoutErr = () => new GatewayError(408, "request_timeout", "Request body read timed out");

  if (!forceStream && parsedCl >= 0 && parsedCl <= 262_144) {
    return new Promise<Uint8Array>((resolve, reject) => {
      let settled = false;
      let watchingSignal = false;
      let cancelSignalWatch: (() => void) | null = null;
      const onReqAbort = () =>
        fail(new GatewayError(499, "client_aborted", "Client disconnected"), false);
      const cleanup = () => {
        cancelTimer();
        if (cancelSignalWatch) {
          cancelSignalWatch();
          cancelSignalWatch = null;
        }
        if (watchingSignal) {
          watchingSignal = false;
          req.signal.removeEventListener("abort", onReqAbort);
        }
        setAbortNotify(null);
      };
      const fail = (err: unknown, cancelBody = true) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (cancelBody) void req.body?.cancel().catch(() => undefined);
        reject(err);
      };
      setAbortNotify(() => fail(new GatewayError(503, "overloaded", "Gateway is draining")));
      const cancelTimer = deadlines.schedule(timeoutMs, () => fail(timeoutErr()));
      cancelSignalWatch = deadlines.schedule(Math.min(50, timeoutMs), () => {
        cancelSignalWatch = null;
        if (settled) return;
        if (req.signal.aborted) {
          onReqAbort();
          return;
        }
        watchingSignal = true;
        req.signal.addEventListener("abort", onReqAbort, { once: true });
      });
      req.bytes().then(
        (bytes) => {
          if (settled) return;
          settled = true;
          cleanup();
          if (bytes.byteLength > maxBytes) {
            reject(
              new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit"),
            );
            return;
          }
          resolve(bytes);
        },
        (err) => {
          if (settled) return;
          if ((err instanceof Error && err.name === "AbortError") || req.signal.aborted) {
            fail(new GatewayError(499, "client_aborted", "Client disconnected"), false);
            return;
          }
          fail(err, false);
        },
      );
    });
  }

  const controller = new AbortController();
  let timedOut = false;
  let drained = false;
  const onSignalAbort = () => controller.abort(req.signal.reason);
  req.signal.addEventListener("abort", onSignalAbort, { once: true });
  setAbortNotify((reason) => {
    drained = true;
    controller.abort(reason);
  });
  const cancelTimer = deadlines.schedule(timeoutMs, () => {
    timedOut = true;
    controller.abort(timeoutErr());
  });
  try {
    return await readBounded(req.body, maxBytes, controller.signal);
  } catch (err) {
    if (err instanceof GatewayError) throw err;
    if (timedOut) throw timeoutErr();
    if (req.signal.aborted) throw new GatewayError(499, "client_aborted", "Client disconnected");
    if (drained || isDraining()) throw new GatewayError(503, "overloaded", "Gateway is draining");
    throw err;
  } finally {
    cancelTimer();
    req.signal.removeEventListener("abort", onSignalAbort);
    setAbortNotify(null);
  }
}

async function readResponseBytesFast(
  resp: Response,
  maxBytes: number,
  signal: AbortSignal,
  bufferedBody?: Uint8Array,
): Promise<Uint8Array> {
  if (bufferedBody !== undefined) {
    signal.throwIfAborted();
    if (bufferedBody.byteLength > maxBytes) {
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    return bufferedBody;
  }
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

function extractApiKey(c: InferenceContext, operation?: Operation): string | null {
  const headers = c.req.raw.headers;
  const auth = headers.get("authorization");
  if (auth && (auth.startsWith("Bearer ") || auth.toLowerCase().startsWith("bearer "))) {
    const token = auth.slice(7).trim();
    if (token) return token;
  }
  if (operation === "messages" || operation === undefined) {
    const xKey = headers.get("x-api-key")?.trim();
    if (xKey) return xKey;
  }
  if (operation === "gemini" || operation === undefined) {
    const gKey = headers.get("x-goog-api-key")?.trim() || c.req.query("key")?.trim();
    if (gKey) return gKey;
  }
  return null;
}

export function authenticateRequest(
  c: InferenceContext,
  snap: ConfigSnapshot,
  config: GatewayConfig,
  operation?: Operation,
  peerAddress?: string | (() => string | undefined),
  state?: ProxyRuntimeState,
): { key: ApiKeyRecord; project: ProjectRecord } {
  const rawKey = extractApiKey(c, operation);
  if (!rawKey) throw new GatewayError(401, "unauthorized", "Missing API key");
  if (!PROJECT_API_KEY_PATTERN.test(rawKey)) {
    throw new GatewayError(401, "unauthorized", "Invalid API key");
  }
  const key = state
    ? state.lookupVerifiedKey(rawKey, snap, config.keyPepper)
    : snap.keysByVerifier.get(verifier(rawKey, config.keyPepper));
  if (!key || !key.enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
  if (key.expires_at && Date.parse(key.expires_at) <= Date.now()) {
    throw new GatewayError(401, "key_expired", "API key has expired");
  }
  if (operation !== undefined && key.role === "VIEWER") {
    throw new GatewayError(403, "forbidden", "API key role cannot invoke models");
  }
  if (key.allowed_cidrs.length > 0) {
    const resolvedPeer = typeof peerAddress === "function" ? peerAddress() : peerAddress;
    const addr = clientAddress(
      resolvedPeer,
      c.req.raw.headers.get("x-forwarded-for"),
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

function mapHoldError(code: string, message: string): GatewayError {
  if (code === "insufficient_balance" || code === "key_spend_limit_exceeded") {
    return new GatewayError(402, code, message);
  }
  if (code === "wallet_frozen" || code === "wallet_not_found") {
    return new GatewayError(403, code, message);
  }
  if (code === "idempotency_conflict") return new GatewayError(409, code, message);
  if (code === "ledger_busy") return new GatewayError(503, code, message);
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

function channelFailureKind(code: string): ChannelFailureKind {
  switch (code) {
    case "upstream_dns":
      return "dns";
    case "upstream_tls":
      return "tls";
    case "upstream_connect":
    case "upstream_unreachable":
      return "connect";
    case "transport_interrupted":
    case "stream_interrupted":
      return "interrupted";
    case "upstream_not_dispatched":
    case "upstream_timeout":
      return "timeout";
    case "upstream_capacity":
    case "channel_saturated":
    case "provider_capacity_unavailable":
      return "capacity";
    case "upstream_rate_limited":
      return "rate_limit";
    case "upstream_auth_error":
      return "auth_error";
    case "upstream_client_error":
      return "client_error";
    case "invalid_upstream":
    case "invalid_upstream_usage":
    case "unexpected_trailing_event":
    case "stream_truncated":
    case "empty_stream":
    case "usage_alias_conflict":
    case "conversion_loss":
    case "provider_stream_error":
      return "protocol";
    default:
      return "other";
  }
}

const mediaCompletionEvents: Partial<Record<Operation, string>> = {
  images: "image_generation.completed",
  image_edits: "image_edit.completed",
  speech: "speech.audio.done",
  transcription: "transcript.text.done",
};

function imageArtifact(value: unknown): string | undefined {
  const { b64_json, url } = optionalObject(value);
  if (
    typeof b64_json === "string" &&
    /^[A-Za-z0-9+/]+={0,2}$/.test(b64_json) &&
    b64_json.length % 4 === 0
  )
    return b64_json;
  if (typeof url !== "string" || !URL.canParse(url)) return;
  const parsed = new URL(url);
  if (parsed.protocol === "https:" || parsed.protocol === "http:") return parsed.href;
}

function recordImageArtifact(artifacts: Set<string>, value: unknown): void {
  const artifact = imageArtifact(value);
  if (!artifact) return;
  const digest = new Bun.CryptoHasher("sha256").update(artifact).digest("hex");
  if (artifacts.has(digest)) {
    throw new GatewayError(502, "invalid_upstream_usage", "Provider repeated an image result");
  }
  artifacts.add(digest);
}

function countImageArtifacts(images: unknown[]): number {
  const artifacts = new Set<string>();
  for (const image of images) recordImageArtifact(artifacts, image);
  return artifacts.size;
}

function buildUsageForOperation(
  operation: Operation,
  customerPrice: PriceVersionRecord,
  supplierPrice: PriceVersionRecord,
  rawUsage: Usage | undefined,
  deliveredImages?: number,
  authorizedImages?: number,
): Usage | null {
  if (operation === "images" || operation === "image_edits") {
    const declared = rawUsage?.images;
    const measured = deliveredImages === undefined ? undefined : String(deliveredImages);
    if (declared !== undefined && measured !== undefined && declared !== measured) {
      throw new GatewayError(
        502,
        "usage_alias_conflict",
        "Provider image count disagrees with delivered images",
      );
    }
    const quantity = measured ?? declared;
    if (
      quantity !== undefined &&
      (authorizedImages === undefined ||
        !Number.isSafeInteger(authorizedImages) ||
        BigInt(quantity) > BigInt(authorizedImages))
    ) {
      throw new GatewayError(
        502,
        "invalid_upstream_usage",
        "Provider returned more images than authorized",
      );
    }
    if (
      (!quantity || quantity === "0") &&
      ((customerPrice.basis === "PROVIDER_USAGE" && customerPrice.rates.images) ||
        (supplierPrice.basis === "PROVIDER_USAGE" && supplierPrice.rates.images))
    )
      return null;
    if (quantity !== undefined) return { ...rawUsage, images: quantity };
  }
  if (rawUsage) return rawUsage;
  if (customerPrice.basis === "PROVIDER_USAGE" || supplierPrice.basis === "PROVIDER_USAGE")
    return null;
  return { requests: "1" };
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

function errorJson(
  c: InferenceContext,
  operation: Operation,
  err: unknown,
  requestId: string,
  trace: TraceContext,
): Response {
  const gw =
    err instanceof GatewayError
      ? err
      : new GatewayError(500, "internal_error", "Internal gateway error");
  const extraHeaders: Record<string, string> =
    gw.code === "plugin_capacity_unavailable" ||
    gw.code === "provider_capacity_unavailable" ||
    gw.code === "request_body_capacity_unavailable" ||
    gw.code === "upstream_capacity"
      ? { "retry-after": "1" }
      : {};
  return c.json(
    formatProtocolError(operation, gw.status, gw.code, gw.message, requestId),
    gw.status as 400,
    responseHeaders(requestId, trace, extraHeaders),
  );
}

function sanitizeHttpErrorStatus(status: number | undefined, fallback = 400): number {
  return typeof status === "number" && Number.isInteger(status) && status >= 400 && status <= 599
    ? status
    : fallback;
}

function classifyPluginBlock(
  outcomeKind: PluginOutcomeKind | undefined,
  httpStatus: number | undefined,
  safeMessage: string | undefined,
): {
  isContentDecision: boolean;
  status: number;
  code: string;
  message: string;
  extraHeaders: Record<string, string>;
} {
  const kind = outcomeKind ?? "content_decision";
  if (kind === "host_capacity") {
    return {
      isContentDecision: false,
      status: 503,
      code: "plugin_capacity_unavailable",
      message: safeMessage ?? "Plugin runtime capacity unavailable",
      extraHeaders: { "retry-after": "1" },
    };
  }
  if (kind === "plugin_fault") {
    return {
      isContentDecision: false,
      status: 500,
      code: "plugin_execution_failed",
      message: safeMessage ?? "Plugin execution failed",
      extraHeaders: {},
    };
  }
  return {
    isContentDecision: true,
    status: sanitizeHttpErrorStatus(httpStatus, 400),
    code: "content_blocked",
    message: safeMessage ?? "Content blocked by moderation policy",
    extraHeaders: {},
  };
}

export async function handleInferenceRequest(
  c: InferenceContext,
  deps: ProxyDependencies,
  operation: Operation,
  pathOverride?: { model: string; stream: boolean },
  peerAddress?: string | (() => string | undefined),
): Promise<Response> {
  const state = getProxyState(deps);
  const requestId = fastUuid();
  const reqHeaders = c.req.raw.headers;
  const trace = parseOrCreateTrace(reqHeaders.get("traceparent") ?? undefined);
  const requestStart = performance.now();
  deps.metrics.requestsTotal++;

  let releaseGuard: (() => void) | undefined;
  let releaseBodyBuffer: (() => void) | undefined;
  let releaseLocalProjectSlot: (() => void) | null = null;
  let streamOwnsGuard = false;
  let drainAborted = false;
  let projectPermitRevoked = false;
  let channelPermitRevoked = false;
  let activeAbortNotify: ((reason: unknown) => void) | null = null;
  let activeLinkedDispose: (() => void) | null = null;

  const createLinkedController = (
    sources: readonly (AbortSignal | undefined)[],
    includeChannelPermit = false,
    attachToRequest = true,
  ) => {
    const controller = new AbortController();
    let disposed = false;
    const trigger = (reason: unknown) => {
      if (!controller.signal.aborted) controller.abort(reason);
    };
    if (drainAborted) {
      trigger(new Error("Gateway shutting down"));
    } else if (projectPermitRevoked || (includeChannelPermit && channelPermitRevoked)) {
      trigger(new Error("Shared concurrency permit could not be renewed"));
    }
    let singleExtra: AbortSignal | undefined;
    let activeSources: AbortSignal[] | undefined;
    for (let i = 0; i < sources.length; i++) {
      const src = sources[i];
      if (!src || deps.snapshot.isNoopPluginSignal(src)) continue;
      if (!singleExtra) {
        singleExtra = src;
      } else {
        if (!activeSources) activeSources = [controller.signal, singleExtra];
        activeSources.push(src);
      }
    }
    const signal = activeSources
      ? AbortSignal.any(activeSources)
      : singleExtra
        ? AbortSignal.any([controller.signal, singleExtra])
        : controller.signal;
    if (!controller.signal.aborted && attachToRequest) {
      activeAbortNotify = trigger;
    }
    const dispose = () => {
      if (disposed) return;
      disposed = true;
      if (attachToRequest && activeAbortNotify === trigger) activeAbortNotify = null;
    };
    return { controller, signal, trigger, dispose };
  };

  let releaseProjectLimit: (() => void) | null = null;
  let releaseChannelLimit: (() => void) | null = null;
  let activeDispatch: PreparedDispatch | undefined;
  let activeDispatchTiming: ReturnType<typeof createTransportTiming> | undefined;
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
  let requestTrace: ReturnType<typeof createRequestTrace> | undefined;
  let traceProtocol: Protocol | null = null;
  let traceChannel: string | undefined;
  let failureRecorded = false;
  let channelStage: ChannelFailureStage = "request_prepare";
  const recordAttemptFailure = (
    code: string,
    kind: ChannelFailureKind = channelFailureKind(code),
    details?: TraceAttempt["providerError"],
    stage = channelStage,
  ) => {
    if (!traceChannel || failureRecorded) return;
    failureRecorded = true;
    deps.metrics.recordChannelFailure(traceChannel, stage, kind);
    requestTrace?.failAttempt(performance.now(), stage, kind, code, details);
  };
  const recordPreDispatchFailure = (
    channel: string,
    code: string,
    kind: ChannelFailureKind,
    timing?: ReturnType<typeof createTransportTiming>,
    id?: string,
    startedAt?: number,
  ) => {
    deps.metrics.recordChannelFailure(channel, "pre_dispatch", kind);
    if (!deps.traceQueue || !requestTrace) return;
    const at = performance.now();
    requestTrace.recordPreDispatchFailure(
      id ?? fastUuid(),
      channel,
      startedAt ?? at,
      at,
      code,
      kind,
      timing?.values ?? createTransportTiming().values,
    );
  };
  let waitingForFirstText = false;
  const stopWaitingForFirstText = () => {
    if (!waitingForFirstText) return;
    waitingForFirstText = false;
    deps.metrics.waitingFirstText--;
  };
  let logged = false;
  const acquiredProbes = new Map<string, symbol>();
  let holdAcquired = false;
  let holdFinalized = false;
  let holdFinalizeAttempted = false;
  let anyUnknownAttempt = false;
  let activePluginSignal: AbortSignal | undefined;
  let releaseOutputAdmission: (() => void) | undefined;
  let currentAttemptDispatched = false;
  let providerRequestIds: Map<string, string> | undefined;

  const commitMeasured = async (input: CommitTerminalInput) => {
    activePluginSignal?.throwIfAborted();
    const result = await deps.ledger.commitTerminal(input).catch(() => null);
    if (!result?.ok) {
      deps.metrics.ledgerCommitFailures++;
      throw new GatewayError(503, "ledger_commit_failed", "Ledger could not confirm settlement");
    }
    holdFinalized = true;
    requestTrace?.recordSettlement(input.selected_attempt_id);
    return result;
  };

  const finalizeHold = async (mode: "release" | "unknown"): Promise<boolean> => {
    if (!holdAcquired || holdFinalized) return true;
    holdFinalizeAttempted = true;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        if (mode === "release") await deps.ledger.releaseHold(requestId);
        else await deps.ledger.markUnknown(requestId, providerRequestIds);
        holdFinalized = true;
        return true;
      } catch {
        if (attempt === 1) return false;
        await Bun.sleep(15);
      }
    }
    return false;
  };

  const releaseAllPendingProbes = () => {
    for (const [chId, probe] of acquiredProbes) {
      deps.router.releaseProbe(chId, probe);
    }
    acquiredProbes.clear();
  };

  const releaseProbeFor = (channelId: string) => {
    const probe = acquiredProbes.get(channelId);
    if (probe && acquiredProbes.delete(channelId)) deps.router.releaseProbe(channelId, probe);
  };

  const markProbeCompleted = (channelId: string) => {
    acquiredProbes.delete(channelId);
  };

  const emitAccessLog = (status: number, errorCode: string | null) => {
    if (logged) return;
    logged = true;
    const ended = performance.now();
    const durationMs = ended - requestStart;
    stopWaitingForFirstText();
    try {
      if (
        errorCode &&
        /^(?:upstream_|invalid_upstream|provider_|unexpected_trailing_event$|missing_usage$)/.test(
          errorCode,
        )
      ) {
        recordAttemptFailure(errorCode);
      }
      if (requestTrace) {
        requestTrace.endAttempt(ended);
        const measurement = requestTrace.measurement;
        if (traceProtocol && deps.config.traceMetrics)
          deps.metrics.observeStreamPhases(operation, status, measurement);
        if (deps.traceQueue && logProjectId)
          deps.traceQueue.offer({
            traceId: trace.traceId,
            spanId: trace.spanId,
            ...(trace.parentSpanId ? { parentSpanId: trace.parentSpanId } : {}),
            requestId,
            projectId: logProjectId,
            model: logModel,
            operation,
            status,
            firstTextTracked: traceProtocol !== null,
            error:
              errorCode && /^[A-Za-z0-9_.:-]{1,80}$/.test(errorCode)
                ? errorCode
                : errorCode
                  ? "request_failed"
                  : null,
            origin: performance.timeOrigin,
            start: requestStart,
            end: ended,
            ...(measurement.enqueued === undefined ? {} : { enqueued: measurement.enqueued }),
            ...(measurement.admission ? { admission: measurement.admission } : {}),
            ...requestTrace.completedAttempts,
          });
      }
    } catch {
      deps.metrics.traceCaptureFailures++;
    }
    if (!deps.metrics.isAccessLogEnabled) {
      deps.metrics.observeRequestFast(operation, status, logChannel, durationMs, logTtftMs);
      return;
    }
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
      duration_ms: durationMs,
      ttft_ms: logTtftMs,
      ...(requestTrace && traceProtocol
        ? { first_text_trace: requestTrace.record(trace.spanId) }
        : {}),
      error_code: errorCode,
    });
  };

  try {
    const guard = deps.lifecycle.enterCallback(() => {
      drainAborted = true;
      activeAbortNotify?.(new Error("Gateway shutting down"));
    });
    releaseGuard = guard.release;
    const admissionError = (quotaLost: boolean) =>
      c.req.raw.signal.aborted
        ? new GatewayError(499, "client_aborted", "Client disconnected")
        : quotaLost
          ? new GatewayError(503, "rate_limit_unavailable", "Shared concurrency permit was revoked")
          : new GatewayError(503, "overloaded", "Gateway is draining");
    if (deps.snapshot.isDiverged()) {
      throw new GatewayError(
        503,
        "config_diverged",
        "Gateway configuration epoch diverged from database lineage",
      );
    }
    if (!deps.snapshot.isFresh()) {
      throw new GatewayError(503, "config_unavailable", "Gateway configuration is stale");
    }
    const snap = deps.snapshot.get();
    const { key, project } = authenticateRequest(
      c,
      snap,
      deps.config,
      operation,
      peerAddress,
      state,
    );
    logProjectId = project.id;
    const check = deps.checkConsoleAdmission(project, key);
    if (check) await check;

    const localProjRelease = state.tryEnterProject(project.id, project.concurrency);
    if (!localProjRelease) {
      throw new GatewayError(429, "rate_limit_concurrency", "Project rate limit exceeded");
    }
    releaseLocalProjectSlot = localProjRelease;

    const contentType = reqHeaders.get("content-type") ?? "";
    const isMultipartOp =
      (operation === "transcription" ||
        operation === "translation" ||
        operation === "image_edits") &&
      contentType.toLowerCase().includes("multipart/form-data");

    const bodyReadTimeoutMs =
      deps.config.bodyReadTimeoutMs ?? Math.min(deps.config.attemptTimeoutMs, 15_000);
    let bodyReadLimit = deps.config.maxBodyBytes;
    if (deps.bodyBufferBudget) {
      bodyReadLimit = declaredRequestSize(c.req.raw, bodyReadLimit) ?? bodyReadLimit;
      releaseBodyBuffer = deps.bodyBufferBudget.reserve(bodyReadLimit);
    }
    let clientBody: Record<string, unknown>;
    let multipartEntries: [string, string | File][] | undefined;

    if (isMultipartOp) {
      const received = await readRequestBytesBounded(
        c.req.raw,
        bodyReadLimit,
        bodyReadTimeoutMs,
        state.deadlines,
        () => drainAborted,
        (fn) => {
          activeAbortNotify = fn;
        },
        true,
      );
      const form = await new Response(received as BodyInit, {
        headers: { "content-type": contentType },
      })
        .formData()
        .catch(() => {
          throw new GatewayError(400, "invalid_request", "Invalid multipart body");
        });
      const entries: [string, string | File][] = [];
      const meta: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      for (const [key, value] of form.entries() as Iterable<[string, string | File]>) {
        entries.push([key, value]);
        meta[key] =
          typeof value === "string"
            ? value
            : { filename: value.name, size: value.size, type: value.type };
      }
      multipartEntries = entries;
      clientBody = {
        ...meta,
        stream: meta.stream === "true" || meta.stream === true,
      };
    } else {
      const rawBytes = await readRequestBytesBounded(
        c.req.raw,
        bodyReadLimit,
        bodyReadTimeoutMs,
        state.deadlines,
        () => drainAborted,
        (fn) => {
          activeAbortNotify = fn;
        },
        deps.bodyBufferBudget !== undefined,
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

    if (operation === "videos" && deps.asyncTasks) {
      const activeTasks = await deps.asyncTasks.countActiveTasks(
        project.id,
        deps.config.asyncTaskCancelCooldownMs ?? 0,
      );
      const maxActive = Math.min(
        project.concurrency,
        deps.config.maxActiveAsyncTasksPerProject ?? 10,
      );
      if (activeTasks >= maxActive) {
        throw new GatewayError(
          429,
          "rate_limit_async_tasks",
          "Too many concurrent active async video tasks for this project",
        );
      }
    }

    rejectUnmeteredTools(clientBody, operation);

    const isStream =
      clientBody.stream === true || (operation === "speech" && clientBody.stream_format === "sse");
    logStream = isStream;
    traceProtocol = isStream && isConversationalOperation(operation) ? operation : null;
    if (
      deps.traceQueue ||
      (traceProtocol && (deps.config.traceMetrics || deps.metrics.isAccessLogEnabled))
    ) {
      requestTrace = createRequestTrace(
        requestStart,
        traceProtocol,
        deps.config.traceExport?.maxAttempts,
        stopWaitingForFirstText,
      );
      if (traceProtocol && deps.config.traceMetrics) {
        waitingForFirstText = true;
        deps.metrics.waitingFirstText++;
      }
    }
    const outputBound = resolveOutputBound(clientBody, model);

    const initialChars = isMultipartOp ? undefined : requestCharacters(operation, clientBody);
    const estimatedTokens = Math.max(
      1,
      Math.ceil((initialChars ?? 200) / 4) + Math.min(outputBound, 4096),
    );
    const projAcq = await deps.cluster.acquireRateLimit(
      `project:${project.id}`,
      { rpm: project.rpm, tpm: project.tpm, concurrency: project.concurrency },
      estimatedTokens,
      deps.config.holdTtlMs,
      () => {
        projectPermitRevoked = true;
        activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
      },
    );
    if (!projAcq.ok) {
      if (projAcq.reason === "unavailable") {
        throw new GatewayError(
          503,
          "rate_limit_unavailable",
          "Shared request quota is unavailable",
        );
      }
      throw new GatewayError(429, `rate_limit_${projAcq.reason}`, "Project rate limit exceeded");
    }
    releaseProjectLimit = projAcq.release;

    const baseHydrated = state.getHydratedCandidates(
      snap,
      model.name,
      operation,
      project.currency,
      () => {
        const rawRoutes = snap.routesByModel.get(model.name) ?? [];
        const list: HydratedCandidate[] = [];
        for (const r of rawRoutes) {
          const channel = snap.channels.get(r.channel_id);
          if (!channel) continue;
          const account = snap.accounts.get(channel.account_id);
          if (!account) continue;
          const billingPrice = snap.prices.get(r.billing_price_id);
          const costPrice = snap.prices.get(r.cost_version_id);
          if (!billingPrice || !costPrice) continue;
          if (billingPrice.currency !== project.currency) continue;
          const candidateUpstreamProto = resolveRouteUpstreamProtocol(snap, channel, r, operation);
          list.push({
            id: r.id,
            channel_id: channel.id,
            account_id: account.id,
            priority: r.priority,
            weight: r.weight,
            operations: r.operations,
            enabled: r.enabled,
            channel_enabled: channel.enabled,
            account_active: account.state === "ACTIVE",
            same_protocol: candidateUpstreamProto !== null && candidateUpstreamProto === operation,
            route: r,
            channel,
            account,
            billingPrice,
            costPrice,
          });
        }
        return list;
      },
    );
    let hydrated: readonly HydratedCandidate[] = baseHydrated;
    let revokedRoute: AbortSignal | undefined;
    if (snap.revokedPlugins.size > 0) {
      const filtered: HydratedCandidate[] = [];
      for (const item of baseHydrated) {
        if (item.enabled && item.channel_enabled && item.operations.includes(operation)) {
          const signal = deps.snapshot.pluginSignal(item.route, item.channel);
          if (signal.aborted) {
            revokedRoute = signal;
            continue;
          }
        }
        filtered.push(item);
      }
      hydrated = filtered;
    }

    const selected = deps.router.selectCandidates(
      model.name,
      hydrated,
      model.routing_strategy,
      operation,
    );
    if (selected.length === 0) {
      revokedRoute?.throwIfAborted();
      throw new GatewayError(503, "no_route", "No healthy channel available for this model");
    }
    for (const entry of selected) {
      if (entry.probe) acquiredProbes.set(entry.route.channel_id, entry.probe);
    }

    const releaseUnusedProbes = (fromIndex: number) => {
      for (let i = fromIndex; i < selected.length; i++) {
        if (selected[i]!.probe) {
          releaseProbeFor(selected[i]!.route.channel_id);
        }
      }
    };

    const emptyAudit: PluginAuditEntry[] = [];
    let preRequestCache: Map<
      string,
      {
        body: Record<string, unknown>;
        chars: number | undefined;
        measurement: { inputCharacters?: string | undefined; images?: number | undefined };
        audit: PluginAuditEntry[];
      }
    > | null = null;
    const pipelineKeyOf = (steps: readonly RouteRecord["plugin_pipeline"][number][]) => {
      if (steps.length === 0) return "";
      return steps
        .filter((s) => s.enabled !== false)
        .map((s) => `${s.plugin_id}:${s.priority}:${s.charge_policy ?? ""}`)
        .join("|");
    };
    const prepareRouteInput = async (candidate: HydratedCandidate) => {
      const pluginSignal = deps.snapshot.pluginSignal(candidate.route, candidate.channel);
      activePluginSignal = pluginSignal;
      pluginSignal.throwIfAborted();
      const steps = candidate.route.plugin_pipeline;
      if (steps.length === 0 && !isMultipartOp) {
        const measurement = {
          inputCharacters: initialChars === undefined ? undefined : String(initialChars),
          images:
            operation === "images" || operation === "image_edits"
              ? Number(clientBody.n ?? 1)
              : undefined,
        };
        return {
          body: clientBody,
          chars: initialChars,
          measurement,
          audit: emptyAudit,
          pluginSignal,
        };
      }
      const pipeKey = isMultipartOp ? "__multipart__" : pipelineKeyOf(steps);
      const cached = preRequestCache?.get(pipeKey);
      if (cached) {
        return { ...cached, pluginSignal };
      }
      let body = clientBody;
      const localAudit: PluginAuditEntry[] = [];
      if (steps.length > 0 && !isMultipartOp) {
        if (
          deps.metrics.isTenantPlatformAbsorbExceeded(
            project.id,
            deps.config.platformAbsorbMaxCountPerMinute ?? 100,
            deps.config.platformAbsorbMaxAtomsPerMinute,
          )
        ) {
          releaseUnusedProbes(0);
          emitAccessLog(503, "plugin_capacity_unavailable");
          return c.json(
            formatProtocolError(
              operation,
              503,
              "plugin_capacity_unavailable",
              "Tenant plugin fault absorption budget exceeded",
              requestId,
            ),
            503,
            responseHeaders(requestId, trace, { "retry-after": "5" }),
          );
        }
        let preBlocked: {
          outcome_kind?: PluginOutcomeKind;
          rule_code?: string;
          http_status?: number;
          safe_message?: string;
        } | null = null;
        const policyLinked = createLinkedController([c.req.raw.signal, pluginSignal]);
        let transformed: Awaited<ReturnType<typeof transformRequestTexts>>;
        try {
          transformed = await transformRequestTexts(clientBody, operation, async (partText) => {
            const preRes = await deps.pluginEngine.runUnary(
              "on_request",
              steps,
              snap.plugins,
              { request_id: requestId, project_id: project.id, model: model.name, operation },
              partText,
              policyLinked.signal,
            );
            localAudit.push(...preRes.audit);
            if (preRes.blocked) {
              preBlocked = {
                ...(preRes.outcome_kind !== undefined ? { outcome_kind: preRes.outcome_kind } : {}),
                ...(preRes.rule_code !== undefined ? { rule_code: preRes.rule_code } : {}),
                ...(preRes.http_status !== undefined ? { http_status: preRes.http_status } : {}),
                ...(preRes.safe_message !== undefined ? { safe_message: preRes.safe_message } : {}),
              };
              return { blocked: true, text: "" };
            }
            return { blocked: false, text: preRes.text };
          });
        } finally {
          policyLinked.dispose();
        }
        if (transformed.blocked && preBlocked) {
          const blk = preBlocked as {
            outcome_kind?: PluginOutcomeKind;
            rule_code?: string;
            http_status?: number;
            safe_message?: string;
          };
          releaseUnusedProbes(0);
          const classified = classifyPluginBlock(
            blk.outcome_kind,
            blk.http_status,
            blk.safe_message,
          );
          if (classified.isContentDecision) {
            deps.metrics.moderationBlocksTotal++;
            logModBlocked = true;
          }
          emitAccessLog(classified.status, blk.rule_code ?? classified.code);
          return c.json(
            formatProtocolError(
              operation,
              classified.status,
              classified.code,
              classified.message,
              requestId,
              blk.rule_code ? { rule_code: blk.rule_code } : undefined,
            ),
            classified.status as 400,
            responseHeaders(requestId, trace, classified.extraHeaders),
          );
        }
        body = transformed.body;
      }

      const chars =
        body === clientBody && !isMultipartOp ? initialChars : requestCharacters(operation, body);
      const measurement = {
        inputCharacters: chars === undefined ? undefined : String(chars),
        images:
          operation === "images" || operation === "image_edits" ? Number(body.n ?? 1) : undefined,
      };
      const entry = { body, chars, measurement, audit: localAudit };
      if (!preRequestCache) preRequestCache = new Map();
      preRequestCache.set(pipeKey, entry);
      return { ...entry, pluginSignal };
    };
    const firstCand = selected[0]!.route;
    let firstInput: Awaited<ReturnType<typeof prepareRouteInput>>;
    if (firstCand.route.plugin_pipeline.length === 0 && !isMultipartOp) {
      const pluginSignal = deps.snapshot.pluginSignal(firstCand.route, firstCand.channel);
      activePluginSignal = pluginSignal;
      pluginSignal.throwIfAborted();
      firstInput = {
        body: clientBody,
        chars: initialChars,
        measurement: {
          inputCharacters: initialChars === undefined ? undefined : String(initialChars),
          images:
            operation === "images" || operation === "image_edits"
              ? Number(clientBody.n ?? 1)
              : undefined,
        },
        audit: emptyAudit,
        pluginSignal,
      };
    } else {
      firstInput = await prepareRouteInput(firstCand);
    }
    if (firstInput instanceof Response) return firstInput;
    let activeInput = firstInput;
    let activePrepared: PreparedUpstreamRequest | null = null;

    function upstreamOptions(
      cand: HydratedCandidate,
      body: Record<string, unknown>,
      canMutateClientBody = false,
    ): Omit<PrepareUpstreamOptions, "signal"> {
      return {
        channel: cand.channel,
        route: cand.route,
        snapshot: snap,
        secret: deps.snapshot.decryptChannelSecret(cand.channel),
        operation,
        clientBody: body,
        outputBound,
        isStream,
        canMutateClientBody,
        vertexTokens: deps.vertexTokens,
        wasmExecutor: deps.wasmExecutor,
        tenantId: project.id,
        onFieldDropped(source, target, field) {
          deps.metrics.recordProtocolFieldDropped(source, target, field);
        },
        onFieldRejected(source, target, field) {
          deps.metrics.recordProtocolFieldRejected(source, target, field);
        },
        traceparent: trace.traceparent,
        ...(multipartEntries ? { multipartEntries } : {}),
        ...(deps.config.pluginSigningPublicKeyPem
          ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
          : {}),
      };
    }

    function prepareForReservation(
      cand: HydratedCandidate,
      body: Record<string, unknown>,
      customerPrice: PriceVersionRecord,
    ): PreparedUpstreamRequest | null {
      try {
        return tryPrepareUpstreamRequestSync({
          ...upstreamOptions(cand, body),
          estimateInputTokens:
            operation === "chat" &&
            cand.same_protocol &&
            Boolean(customerPrice.rates.input_tokens || cand.costPrice.rates.input_tokens),
        });
      } catch {
        return null;
      }
    }

    const idempotencyKey = reqHeaders.get("idempotency-key")?.trim() || null;
    let fingerprint = "";
    if (idempotencyKey !== null) {
      const body = multipartEntries
        ? await Promise.all(
            multipartEntries.map(async ([field, value]) => [
              field,
              typeof value === "string"
                ? value
                : {
                    filename: value.name,
                    type: value.type,
                    sha256: new Bun.CryptoHasher("sha256")
                      .update(await value.arrayBuffer())
                      .digest("hex"),
                  },
            ]),
          )
        : firstInput.body;
      fingerprint = sha256(
        canonical({ project_id: project.id, operation, model: model.name, body }, (parent, key) => {
          const literal = numberLiteral(parent, key);
          if (!literal) return undefined;
          const rounded = String(Number(literal));
          if (literal === rounded) return undefined;
          if (!Number.isFinite(Number(literal))) return literal;
          const exact = normalizedNumber(literal);
          const value = normalizedNumber(rounded);
          return exact.digits === value.digits &&
            exact.exponent === value.exponent &&
            exact.negative === value.negative
            ? undefined
            : literal;
        }),
      );
    }
    const maxStreamDurationMs = deps.config.streamMaxDurationMs ?? 300_000;
    const firstByteDeadlineMs =
      deps.config.firstByteTimeoutMs !== undefined
        ? requestStart + deps.config.firstByteTimeoutMs
        : null;
    const admitOutputPolicy = (candidate: HydratedCandidate) => {
      releaseOutputAdmission?.();
      releaseOutputAdmission = undefined;
      if (!isConversationalOperation(operation) || !candidate.route.plugin_pipeline.length)
        return undefined;
      const admission = deps.pluginEngine.admitOutputPolicies(
        candidate.route.plugin_pipeline,
        snap.plugins,
        requestId,
        isStream,
      );
      if (admission.ok) {
        releaseOutputAdmission = admission.release;
        return undefined;
      }
      return admission;
    };
    const replayedResponse = (priorId: string, priorState: string | undefined) => {
      const inProgress = priorState === "HELD";
      const code = inProgress ? "idempotency_in_progress" : "idempotency_replayed";
      const message = inProgress
        ? "Request with this Idempotency-Key is already in progress"
        : `Request already finalized with state ${priorState ?? "UNKNOWN"}`;
      emitAccessLog(409, code);
      return c.json(
        formatProtocolError(operation, 409, code, message, priorId),
        409,
        responseHeaders(priorId, trace),
      );
    };
    const rejectOutputPolicy = async (failure: {
      kind: "WASM" | "WEBHOOK";
      code: string;
      message: string;
    }) => {
      if (failure.kind === "WASM") deps.metrics.wasmAdmissionRejections++;
      else deps.metrics.webhookAdmissionRejections++;
      await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
      const classified = classifyPluginBlock("host_capacity", undefined, failure.message);
      emitAccessLog(classified.status, failure.code);
      return c.json(
        formatProtocolError(
          operation,
          classified.status,
          classified.code,
          classified.message,
          requestId,
          { rule_code: failure.code },
        ),
        classified.status as 400,
        responseHeaders(requestId, trace, classified.extraHeaders),
      );
    };
    const reserveDispatch = async (
      candidate: HydratedCandidate,
      prepared: PreparedUpstreamRequest | null,
      pluginSignal: AbortSignal,
    ) => {
      if (!prepared) return;
      const linked = createLinkedController([c.req.raw.signal, pluginSignal]);
      const timing = requestTrace ? createTransportTiming() : undefined;
      const startedAt = timing ? performance.now() : 0;
      if (timing) requestTrace?.observeAdmission(timing.values);
      try {
        activeDispatch = await deps.transport.prepareDispatch(
          prepared.url,
          typeof prepared.body === "string"
            ? Buffer.byteLength(prepared.body)
            : prepared.body.byteLength,
          {
            signal: linked.signal,
            ...(timing ? { onTiming: timing.observe } : {}),
            dispatchDeadline: Math.min(
              firstByteDeadlineMs ?? Infinity,
              performance.now() +
                (candidate.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs),
            ),
          },
          deps.config.allowPrivateUpstreams,
          deps.config.privateUpstreamCidrs,
        );
        activeDispatchTiming = timing;
      } catch (error) {
        if (error instanceof UpstreamPreDispatchError && !linked.signal.aborted) {
          recordPreDispatchFailure(
            candidate.channel.name,
            error.code,
            channelFailureKind(error.code),
            timing,
            undefined,
            startedAt,
          );
        }
        throw error;
      } finally {
        linked.dispose();
      }
      pluginSignal.throwIfAborted();
      if (c.req.raw.signal.aborted || drainAborted || projectPermitRevoked) {
        throw admissionError(projectPermitRevoked);
      }
      if (!deps.snapshot.isFresh() || deps.snapshot.get().epoch !== snap.epoch) {
        throw new GatewayError(
          503,
          "config_unavailable",
          "Gateway configuration changed before dispatch",
        );
      }
      const currentAdmission = deps.checkConsoleAdmission(project, key);
      if (currentAdmission) await currentAdmission;
      pluginSignal.throwIfAborted();
    };
    const releaseDispatch = () => {
      activeDispatch?.release();
      activeDispatch = undefined;
      activeDispatchTiming = undefined;
    };
    const preDispatchGatewayError = (error: UpstreamPreDispatchError) => {
      if (error.code === "upstream_capacity") return error;
      if (error.code === "upstream_not_dispatched") {
        if (
          c.req.raw.signal.aborted ||
          drainAborted ||
          projectPermitRevoked ||
          channelPermitRevoked
        ) {
          return admissionError(projectPermitRevoked || channelPermitRevoked);
        }
        return new GatewayError(504, "upstream_timeout", error.message);
      }
      return new GatewayError(503, "upstream_unreachable", error.message);
    };

    let activeIndex = -1;
    let activeAttemptId = "";
    let holdCustomerAtoms = "0";
    let lastHoldErr: GatewayError | null = null;

    for (let idx = 0; idx < selected.length; idx++) {
      const entry = selected[idx]!;
      const cand = entry.route;
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseProbeFor(cand.channel_id);
        continue;
      }
      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(idx);
        throw new GatewayError(
          504,
          "upstream_timeout",
          "Request exceeded first-byte timeout budget",
        );
      }
      const preparedInput = idx === 0 ? firstInput : await prepareRouteInput(cand);
      if (preparedInput instanceof Response) return preparedInput;
      const unavailablePolicy = admitOutputPolicy(cand);
      if (unavailablePolicy) {
        if (idempotencyKey !== null) {
          const prior = await deps.ledger.findIdempotentHold(project.id, operation, idempotencyKey);
          if (prior) {
            if (prior.fingerprint !== fingerprint)
              throw mapHoldError(
                "idempotency_conflict",
                "Idempotency key reused with different request fingerprint",
              );
            return replayedResponse(prior.request_id, prior.state);
          }
        }
        return await rejectOutputPolicy(unavailablePolicy);
      }
      const effectiveBilling = resolveEffectiveCustomerPrice(
        snap,
        project.id,
        model.name,
        operation,
        cand.billingPrice,
      );
      const prepared = prepareForReservation(cand, preparedInput.body, effectiveBilling.price);
      const measurement = {
        ...preparedInput.measurement,
        estimatedInputTokens: prepared?.estimatedInputTokens,
      };
      let customerAtoms: string;
      let supplierAtoms: string;
      try {
        customerAtoms = state.quoteForPrice(
          snap.epoch,
          effectiveBilling.price,
          measurement,
          outputBound,
          effectiveBilling.cacheKey,
        );
        supplierAtoms = state.quoteForPrice(snap.epoch, cand.costPrice, measurement, outputBound);
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
      preparedInput.pluginSignal.throwIfAborted();
      if (c.req.raw.signal.aborted || drainAborted || projectPermitRevoked) {
        throw admissionError(projectPermitRevoked);
      }
      try {
        if (idempotencyKey === null)
          await reserveDispatch(cand, prepared, preparedInput.pluginSignal);
      } catch (error) {
        releaseDispatch();
        preparedInput.pluginSignal.throwIfAborted();
        if (!(error instanceof UpstreamPreDispatchError)) throw error;
        lastHoldErr = preDispatchGatewayError(error);
        releaseProbeFor(cand.channel.id);
        continue;
      }
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseDispatch();
        releaseProbeFor(cand.channel.id);
        continue;
      }
      if (c.req.raw.signal.aborted || drainAborted || projectPermitRevoked) {
        throw admissionError(projectPermitRevoked);
      }
      const attemptId = fastUuid();
      const holdRes = await deps.ledger.acquireHold({
        request_id: requestId,
        attempt_id: attemptId,
        project_id: project.id,
        api_key_id: key.id,
        enforce_key_spend_limit: key.spend_limit_atoms !== null,
        pricing_override_id: effectiveBilling.overrideId,
        discount_numerator: effectiveBilling.discountNumerator,
        discount_denominator: effectiveBilling.discountDenominator,
        currency: project.currency,
        operation,
        model: model.name,
        idempotency_key: idempotencyKey,
        fingerprint,
        customer_atoms: customerAtoms,
        supplier_account_id: cand.account.id,
        supplier_cost_version_id: cand.costPrice.id,
        customer_price_version_id: effectiveBilling.price.id,
        supplier_channel_id: cand.channel.id,
        supplier_currency: cand.costPrice.currency,
        supplier_atoms: supplierAtoms,
        expires_at: state.cachedExpiresAt(
          deps.config.holdTtlMs + (isStream ? maxStreamDurationMs : 0),
        ),
      });
      if (holdRes.ok) {
        if (holdRes.replayed) {
          releaseUnusedProbes(idx);
          return replayedResponse(holdRes.request_id, holdRes.existing_state);
        }
        holdAcquired = true;
        activeIndex = idx;
        activeInput = preparedInput;
        activePrepared = prepared;
        activeAttemptId = attemptId;
        holdCustomerAtoms = holdRes.held_atoms;
        break;
      }
      const lackedSupplierFunding =
        holdRes.code === "supplier_unavailable" || holdRes.code === "supplier_insufficient_balance";
      if (lackedSupplierFunding)
        recordPreDispatchFailure(
          cand.channel.name,
          holdRes.code,
          "supplier_funding",
          activeDispatchTiming,
          attemptId,
        );
      releaseDispatch();
      releaseProbeFor(cand.channel_id);
      if (lackedSupplierFunding) {
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
      if (cursor !== activeIndex) releaseDispatch();
      const entry = selected[cursor]!;
      const cand = entry.route;
      const providerIdHeader =
        cand.route.adapter_overrides.reconciliation_response_id_header ??
        cand.channel.adapter_config.reconciliation_response_id_header;
      const requiresEvidence =
        cand.route.adapter_overrides.reconciliation_evidence_required ??
        cand.channel.adapter_config.reconciliation_evidence_required ??
        Boolean(providerIdHeader);
      const rejectedOutcome = (notExecuted = false): "FAILED" | "UNKNOWN" => {
        if (!requiresEvidence || notExecuted) return "FAILED";
        anyUnknownAttempt = true;
        return "UNKNOWN";
      };
      const mayRetryAmbiguous = () => {
        const limit = deps.config.maxAmbiguousRetriesPerMinute;
        return (
          Boolean(cand.route.retry_policy.retry_on_ambiguous) &&
          (limit === undefined || deps.metrics.tryConsumeAmbiguousRetry(cand.channel.id, limit))
        );
      };
      if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
        releaseProbeFor(cand.channel_id);
        if (cursor === activeIndex && !pendingSwitchFrom) {
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        }
        if (attemptsMade === 0 && lastErrorCode === "upstream_failed") {
          lastErrorStatus = 503;
          lastErrorCode = "no_route";
          lastErrorMessage = "No healthy channel available for this model";
        }
        continue;
      }
      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(cursor);
        if (attemptsMade === 0) {
          lastErrorStatus = 504;
          lastErrorCode = "upstream_timeout";
          lastErrorMessage = "Request exceeded first-byte timeout budget";
        }
        break;
      }
      const candBilling = resolveEffectiveCustomerPrice(
        snap,
        project.id,
        model.name,
        operation,
        cand.billingPrice,
      );
      logChannel = cand.channel.name;
      currentAttemptDispatched = false;
      const attemptInput = cursor === activeIndex ? activeInput : await prepareRouteInput(cand);
      if (attemptInput instanceof Response) {
        await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
        return attemptInput;
      }
      if (cursor !== activeIndex) {
        const unavailablePolicy = admitOutputPolicy(cand);
        if (unavailablePolicy) return await rejectOutputPolicy(unavailablePolicy);
      }
      const {
        body: attemptBody,
        measurement: inputMeasurement,
        chars,
        pluginSignal,
        audit: preRequestAudit,
      } = attemptInput;
      activePluginSignal = pluginSignal;
      pluginSignal.throwIfAborted();
      const preparedAttempt =
        cursor === activeIndex
          ? activePrepared
          : prepareForReservation(cand, attemptBody, candBilling.price);

      if (pendingSwitchFrom) {
        try {
          await reserveDispatch(cand, preparedAttempt, pluginSignal);
        } catch (error) {
          releaseDispatch();
          pluginSignal.throwIfAborted();
          if (!(error instanceof UpstreamPreDispatchError)) throw error;
          const rejection = preDispatchGatewayError(error);
          lastErrorStatus = rejection.status;
          lastErrorCode = rejection.code;
          lastErrorMessage = rejection.message;
          releaseProbeFor(cand.channel.id);
          continue;
        }
        if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
          releaseDispatch();
          releaseProbeFor(cand.channel.id);
          continue;
        }
        const measurement = {
          ...inputMeasurement,
          estimatedInputTokens: preparedAttempt?.estimatedInputTokens,
        };
        let nextCustomerAtoms: string;
        let nextSupplierAtoms: string;
        try {
          nextCustomerAtoms = state.quoteForPrice(
            snap.epoch,
            candBilling.price,
            measurement,
            outputBound,
            candBilling.cacheKey,
          );
          nextSupplierAtoms = state.quoteForPrice(
            snap.epoch,
            cand.costPrice,
            measurement,
            outputBound,
          );
        } catch (err) {
          releaseProbeFor(cand.channel_id);
          lastErrorStatus = 422;
          lastErrorCode = "billing_ceiling";
          lastErrorMessage = err instanceof Error ? err.message : "Fallback quote error";
          continue;
        }
        const nextAttemptId = fastUuid();
        const sw = await deps.ledger.switchSupplierAttempt({
          request_id: requestId,
          failed_attempt_id: pendingSwitchFrom.attemptId,
          failed_outcome: pendingSwitchFrom.outcome,
          next_attempt_id: nextAttemptId,
          next_account_id: cand.account.id,
          next_channel_id: cand.channel.id,
          next_currency: cand.costPrice.currency,
          next_supplier_atoms: nextSupplierAtoms,
          next_supplier_cost_version_id: cand.costPrice.id,
          next_customer_price_version_id: candBilling.price.id,
          next_customer_price_adjustment: {
            override_id: candBilling.overrideId,
            discount_numerator: candBilling.discountNumerator,
            discount_denominator: candBilling.discountDenominator,
          },
          ...(providerRequestIds?.get(pendingSwitchFrom.attemptId)
            ? { failed_provider_request_id: providerRequestIds.get(pendingSwitchFrom.attemptId)! }
            : {}),
          ...(BigInt(nextCustomerAtoms) > BigInt(holdCustomerAtoms)
            ? { required_customer_atoms: nextCustomerAtoms }
            : {}),
        });
        if (!sw.ok) {
          releaseProbeFor(cand.channel_id);
          if (sw.code === "supplier_unavailable" || sw.code === "supplier_insufficient_balance") {
            recordPreDispatchFailure(
              cand.channel.name,
              sw.code,
              "supplier_funding",
              activeDispatchTiming,
              nextAttemptId,
            );
            continue;
          }
          releaseUnusedProbes(cursor + 1);
          await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
          throw mapHoldError(sw.code, sw.message);
        }
        activeAttemptId = nextAttemptId;
        holdCustomerAtoms = sw.held_atoms;
        pendingSwitchFrom = null;
        logFailover = true;
        deps.metrics.failoversTotal++;
      }

      if (firstByteDeadlineMs !== null && performance.now() >= firstByteDeadlineMs) {
        releaseUnusedProbes(cursor);
        if (attemptsMade === 0) {
          lastErrorStatus = 504;
          lastErrorCode = "upstream_timeout";
          lastErrorMessage = "Request exceeded first-byte timeout budget";
        }
        break;
      }
      channelPermitRevoked = false;
      const chLimit = await deps.cluster.acquireRateLimit(
        `channel:${cand.channel.id}`,
        {
          rpm: cand.channel.rpm,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: cand.channel.concurrency,
        },
        1,
        deps.config.holdTtlMs,
        () => {
          channelPermitRevoked = true;
          activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
        },
      );
      if (!chLimit.ok) {
        if (chLimit.reason === "unavailable") {
          await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
          throw new GatewayError(
            503,
            "rate_limit_unavailable",
            "Shared channel quota is unavailable",
          );
        }
        recordPreDispatchFailure(
          cand.channel.name,
          "channel_saturated",
          "capacity",
          activeDispatchTiming,
          activeAttemptId,
        );
        releaseProbeFor(cand.channel_id);
        pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
        lastErrorStatus = 429;
        lastErrorCode = "channel_saturated";
        lastErrorMessage = "Upstream channel concurrency or RPM limit reached";
        continue;
      }
      releaseChannelLimit = chLimit.release;
      const baseAttemptTimeoutMs =
        cand.route.retry_policy.attempt_timeout_ms ?? deps.config.attemptTimeoutMs;
      let attemptTimeoutMs = baseAttemptTimeoutMs;
      if (firstByteDeadlineMs !== null) {
        const remainingTotalMs = Math.floor(firstByteDeadlineMs - performance.now());
        if (remainingTotalMs <= 0) {
          releaseChannelLimit();
          releaseChannelLimit = null;
          releaseUnusedProbes(cursor);
          if (attemptsMade === 0) {
            lastErrorStatus = 504;
            lastErrorCode = "upstream_timeout";
            lastErrorMessage = "Request exceeded first-byte timeout budget";
          }
          break;
        }
        let healthyBackupsLeft = 0;
        const maxFutureAttempts = maxAttempts - attemptsMade - 1;
        for (
          let j = cursor + 1;
          j < selected.length && healthyBackupsLeft < maxFutureAttempts;
          j++
        ) {
          const nextEntry = selected[j]!;
          if (deps.router.peekAvailable(nextEntry.route.channel.id, nextEntry.probe)) {
            healthyBackupsLeft++;
          }
        }
        attemptTimeoutMs =
          healthyBackupsLeft > 0
            ? Math.min(
                baseAttemptTimeoutMs,
                Math.max(
                  Math.min(100, remainingTotalMs),
                  Math.floor(remainingTotalMs / (healthyBackupsLeft + 1)),
                ),
              )
            : Math.min(baseAttemptTimeoutMs, remainingTotalMs);
      }
      attemptsMade++;
      logAttempts = attemptsMade;
      let attemptAborted = false;
      let attemptAbortReason: unknown;
      let attemptCancelCb: ((reason: unknown) => void) | null = null;
      let lazyAttemptController: AbortController | null = null;
      let attemptDisposed = false;
      const triggerAttemptAbort = (reason: unknown) => {
        if (attemptAborted) return;
        attemptAborted = true;
        attemptAbortReason = reason;
        const cb = attemptCancelCb;
        attemptCancelCb = null;
        if (cb) cb(reason);
        lazyAttemptController?.abort(reason);
      };
      if (drainAborted) {
        triggerAttemptAbort(new Error("Gateway shutting down"));
      } else if (projectPermitRevoked || channelPermitRevoked) {
        triggerAttemptAbort(new Error("Shared concurrency permit could not be renewed"));
      } else if (c.req.raw.signal.aborted) {
        triggerAttemptAbort(c.req.raw.signal.reason);
      } else if (pluginSignal.aborted) {
        triggerAttemptAbort(pluginSignal.reason);
      }
      const onReqAbort = () => triggerAttemptAbort(c.req.raw.signal.reason);
      const watchPlugin = !deps.snapshot.isNoopPluginSignal(pluginSignal);
      const onPluginAbort = () => triggerAttemptAbort(pluginSignal.reason);
      if (!attemptAborted) {
        c.req.raw.signal.addEventListener("abort", onReqAbort, { once: true });
        if (watchPlugin) pluginSignal.addEventListener("abort", onPluginAbort, { once: true });
        activeAbortNotify = triggerAttemptAbort;
      }
      const ensureAttemptSignal = (): AbortSignal => {
        if (!lazyAttemptController) {
          lazyAttemptController = new AbortController();
          if (attemptAborted) lazyAttemptController.abort(attemptAbortReason);
        }
        return lazyAttemptController.signal;
      };
      const attemptAbort = { abort: triggerAttemptAbort };
      const attemptLinked = {
        trigger: triggerAttemptAbort,
        dispose() {
          if (attemptDisposed) return;
          attemptDisposed = true;
          attemptCancelCb = null;
          c.req.raw.signal.removeEventListener("abort", onReqAbort);
          if (watchPlugin) pluginSignal.removeEventListener("abort", onPluginAbort);
          if (activeAbortNotify === triggerAttemptAbort) activeAbortNotify = null;
        },
      };
      activeLinkedDispose = attemptLinked.dispose;
      const cancelTimeout = state.deadlines.schedule(attemptTimeoutMs, () =>
        triggerAttemptAbort(new Error("attempt_timeout")),
      );
      const onAttemptCancelRegister: CancelRegistrar = (fn) => {
        if (attemptAborted) {
          fn(attemptAbortReason);
          return () => {};
        }
        attemptCancelCb = fn;
        return () => {
          if (attemptCancelCb === fn) attemptCancelCb = null;
        };
      };

      const startedAt = performance.now();
      traceChannel = cand.channel.name;
      failureRecorded = false;
      channelStage = "request_prepare";
      requestTrace?.beginAttempt(
        startedAt,
        {
          id: activeAttemptId,
          number: attemptsMade,
          channel: cand.channel.name,
        },
        activeDispatchTiming,
      );
      let routingAttempt = deps.router.beginAttempt(cand.channel.id, entry.probe);
      let upstreamSuccessObserved = false;
      const observeAttemptFailure = (
        kind: "retryable" | "rate_limit" | "auth_error",
        retryAfterMs?: number,
        latencyMs?: number,
      ) =>
        deps.router.recordFailure(cand.channel.id, kind, retryAfterMs, latencyMs, routingAttempt);
      const observeAttemptSuccess = (latencyMs: number) => {
        if (upstreamSuccessObserved) return;
        upstreamSuccessObserved = deps.router.recordSuccess(
          cand.channel.id,
          latencyMs,
          routingAttempt,
        );
        if (upstreamSuccessObserved && routingAttempt.probe) {
          routingAttempt = deps.router.beginAttempt(cand.channel.id);
        }
      };
      let fastResult: FastUpstreamResult;
      let prepared: Awaited<ReturnType<typeof prepareUpstreamRequest>>;
      try {
        pluginSignal.throwIfAborted();
        const quotaLostNow = projectPermitRevoked || channelPermitRevoked;
        if (c.req.raw.signal.aborted || drainAborted || quotaLostNow) {
          throw admissionError(quotaLostNow);
        }
        if (preparedAttempt) {
          prepared = preparedAttempt;
        } else {
          const canMutateClientBody =
            selected.length === 1 &&
            cand.route.plugin_pipeline.length === 0 &&
            idempotencyKey === null &&
            !isMultipartOp;
          const options = upstreamOptions(cand, attemptBody, canMutateClientBody);
          prepared =
            tryPrepareUpstreamRequestSync(options) ??
            (await prepareUpstreamRequest({ ...options, signal: ensureAttemptSignal() }));
        }
      } catch (err) {
        if (err instanceof GatewayError && /^(?:conversion_loss$|provider_)/.test(err.code)) {
          recordAttemptFailure(err.code);
        }
        cancelTimeout();
        attemptLinked.dispose();
        activeLinkedDispose = null;
        releaseChannelLimit();
        releaseChannelLimit = null;
        pluginSignal.throwIfAborted();
        if (
          err instanceof GatewayError &&
          err.code === "conversion_loss" &&
          cursor + 1 < selected.length
        ) {
          releaseProbeFor(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
          lastErrorStatus = err.status;
          lastErrorCode = err.code;
          lastErrorMessage = err.message;
          continue;
        }
        if (
          err instanceof GatewayError &&
          (err.code === "provider_capacity_unavailable" || err.code === "provider_plugin_fault")
        ) {
          releaseProbeFor(cand.channel.id);
          releaseUnusedProbes(cursor + 1);
          await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
          throw err;
        }
        if (
          err instanceof GatewayError &&
          (err.code === "provider_plugin_unavailable" || err.code === "provider_adapter_failed")
        ) {
          deps.metrics.recordUpstreamError(cand.channel.name, err.code);
          observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
          markProbeCompleted(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
          lastErrorStatus = err.status;
          lastErrorCode = err.code;
          lastErrorMessage = err.message;
          continue;
        }
        releaseProbeFor(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
        throw err;
      }

      try {
        channelStage = "transport";
        pluginSignal.throwIfAborted();
        if (!deps.router.peekAvailable(cand.channel.id, entry.probe)) {
          throw new GatewayError(503, "no_route", "No healthy channel available for this model");
        }
        const unavailablePolicy = admitOutputPolicy(cand);
        if (unavailablePolicy) {
          cancelTimeout();
          attemptLinked.dispose();
          activeLinkedDispose = null;
          return await rejectOutputPolicy(unavailablePolicy);
        }
        const request = {
          method: "POST",
          headers: prepared.headers,
          ...(prepared.headersValidated ? { headersValidated: true } : {}),
          body: prepared.body as BodyInit,
          onCancelRegister: onAttemptCancelRegister,
          ...(requestTrace?.transportObserver ? { onTiming: requestTrace.transportObserver } : {}),
          dispatchDeadline: performance.now() + attemptTimeoutMs,
        };
        currentAttemptDispatched = true;
        fastResult = activeDispatch
          ? await activeDispatch.fetchFast(request)
          : await deps.transport.fetchFast(
              prepared.url,
              request,
              deps.config.allowPrivateUpstreams,
              deps.config.privateUpstreamCidrs,
            );
      } catch (err) {
        cancelTimeout();
        attemptLinked.dispose();
        activeLinkedDispose = null;
        releaseChannelLimit();
        releaseChannelLimit = null;
        const isPreDispatch =
          err instanceof UpstreamPreDispatchError ||
          (err instanceof Error &&
            /ECONNREFUSED|ENOTFOUND|Unable to connect|ConnectionRefused/i.test(err.message));
        if (isPreDispatch) currentAttemptDispatched = false;
        if (pluginSignal.aborted) throw pluginSignal.reason;
        if (err instanceof GatewayError && err.code === "no_route") {
          releaseProbeFor(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "FAILED" };
          lastErrorStatus = err.status;
          lastErrorCode = err.code;
          lastErrorMessage = err.message;
          continue;
        }
        const quotaLost = projectPermitRevoked || channelPermitRevoked;
        if (c.req.raw.signal.aborted || drainAborted || quotaLost) {
          releaseProbeFor(cand.channel_id);
          releaseUnusedProbes(cursor + 1);
          await finalizeHold(isPreDispatch && !anyUnknownAttempt ? "release" : "unknown");
          throw admissionError(quotaLost);
        }
        const classified =
          err instanceof UpstreamPreDispatchError ? preDispatchGatewayError(err) : null;
        const diagnosticCode =
          err instanceof GatewayError
            ? err.code
            : isPreDispatch
              ? "upstream_connect"
              : attemptAborted
                ? "upstream_timeout"
                : "transport_interrupted";
        recordAttemptFailure(
          diagnosticCode,
          channelFailureKind(diagnosticCode),
          undefined,
          isPreDispatch ? "pre_dispatch" : channelStage,
        );
        if (isPreDispatch) {
          const kind = err instanceof UpstreamPreDispatchError ? err.code : "upstream_connect";
          deps.metrics.recordUpstreamError(cand.channel.name, kind);
          if (kind === "upstream_connect" || kind === "upstream_tls" || kind === "upstream_dns") {
            deps.router.recordDialFailure(cand.channel.id, routingAttempt);
          }
          releaseProbeFor(cand.channel.id);
        } else {
          deps.metrics.recordUpstreamError(cand.channel.name, "timeout");
          deps.router.recordDialFailure(cand.channel.id, routingAttempt);
          releaseProbeFor(cand.channel.id);
        }
        const failedOutcome: "FAILED" | "UNKNOWN" = isPreDispatch ? "FAILED" : "UNKNOWN";
        if (failedOutcome === "UNKNOWN") anyUnknownAttempt = true;
        const allowAmbiguousRetry = !isPreDispatch && mayRetryAmbiguous();
        lastErrorStatus = classified?.status ?? (isPreDispatch ? 503 : 504);
        lastErrorCode =
          classified?.code ?? (isPreDispatch ? "upstream_unreachable" : "upstream_timeout");
        lastErrorMessage =
          classified?.message ?? (err instanceof Error ? err.message : "Upstream request failed");
        if (isPreDispatch || allowAmbiguousRetry) {
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome: failedOutcome };
          continue;
        }
        releaseUnusedProbes(cursor + 1);
        await finalizeHold("unknown");
        throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
      }

      const upstreamResp = fastResult.fast ? null : fastResult.response;
      const upstreamStatus = fastResult.fast ? fastResult.status : fastResult.response.status;
      const upstreamHeaders = fastResult.fast ? fastResult.headers : fastResult.response.headers;
      const diagnostics = deps.traceQueue ? prepared.mergedOverrides.error_diagnostics : undefined;
      const describeFailure = diagnostics?.summaries
        ? (body?: unknown) => {
            try {
              return describeProviderError(
                diagnostics,
                diagnostics.code_header
                  ? getHeaderValue(upstreamHeaders, diagnostics.code_header.toLowerCase())
                  : null,
                body,
              );
            } catch {
              deps.metrics.traceCaptureFailures++;
              return undefined;
            }
          }
        : undefined;
      const diagnosticBodyLimit = deps.config.traceExport?.errorBodyMaxBytes ?? 8192;
      const describeEventFailure =
        describeFailure && diagnostics?.code_path
          ? (data: string) => {
              if (
                data.length > diagnosticBodyLimit ||
                Buffer.byteLength(data) > diagnosticBodyLimit
              )
                return describeFailure();
              try {
                return describeFailure(JSON.parse(data));
              } catch {
                return describeFailure();
              }
            }
          : undefined;
      requestTrace?.observeHeaders(upstreamStatus);
      const diagnosticIdHeader = diagnostics?.request_id_header ?? providerIdHeader;
      if (diagnosticIdHeader && deps.traceQueue) {
        const providerId = getHeaderValue(upstreamHeaders, diagnosticIdHeader.toLowerCase());
        if (
          providerId &&
          /^[A-Za-z0-9._:-]{1,128}$/.test(providerId) &&
          !/^(?:sk[_-]|eyJ)/.test(providerId)
        ) {
          requestTrace?.bindProviderRequestId(providerId);
        }
      }
      const upstreamOk = upstreamStatus >= 200 && upstreamStatus < 300;
      if (providerIdHeader) {
        const providerId = getHeaderValue(upstreamHeaders, providerIdHeader.toLowerCase());
        if (providerId && /^[A-Za-z0-9._:/-]{1,256}$/.test(providerId)) {
          (providerRequestIds ??= new Map()).set(activeAttemptId, providerId);
          await deps.ledger
            .rememberProviderRequestId(requestId, activeAttemptId, providerId)
            .catch(() => undefined);
        }
      }

      if (pluginSignal.aborted) {
        cancelTimeout();
        attemptLinked.dispose();
        activeLinkedDispose = null;
        void upstreamResp?.body?.cancel(pluginSignal.reason).catch(() => undefined);
        throw pluginSignal.reason;
      }
      const latencyMs = performance.now() - startedAt;
      logTtftMs = latencyMs;
      if (!upstreamOk) {
        channelStage = "http_response";
        let detail = describeFailure?.();
        if (!detail && describeFailure && diagnostics?.code_path && diagnosticBodyLimit > 0) {
          const bytes = fastResult.fast
            ? fastResult.body
            : deps.transport.bufferedBody(fastResult.response);
          const contentType = getHeaderValue(upstreamHeaders, "content-type") ?? "";
          if (
            bytes?.byteLength &&
            bytes.byteLength <= diagnosticBodyLimit &&
            /^application\/(?:json|[^;\s]+\+json)(?:\s*;|$)/i.test(contentType)
          ) {
            try {
              detail = describeFailure(JSON.parse(utf8FatalDecoder.decode(bytes)));
            } catch {
              detail = undefined;
            }
          }
        }
        const retryKind = isRetryableStatus(upstreamStatus);
        recordAttemptFailure(
          "upstream_http_error",
          retryKind ?? (upstreamStatus >= 400 && upstreamStatus < 500 ? "client_error" : "other"),
          detail,
        );
        cancelTimeout();
        attemptLinked.dispose();
        activeLinkedDispose = null;
        void upstreamResp?.body?.cancel().catch(() => undefined);
        releaseChannelLimit();
        releaseChannelLimit = null;
        const outcome = rejectedOutcome();
        if (retryKind) {
          const retryAfterHdr = fastResult.fast
            ? getHeaderValue(fastResult.headers, "retry-after")
            : fastResult.response.headers.get("retry-after");
          const retryAfterMs = parseRetryAfterMs(retryAfterHdr);
          deps.metrics.recordUpstreamError(cand.channel.name, `http_${upstreamStatus}`);
          observeAttemptFailure(retryKind, retryAfterMs, latencyMs);
          markProbeCompleted(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome };
          lastErrorStatus = upstreamStatus === 429 ? 429 : 502;
          lastErrorCode =
            retryKind === "rate_limit"
              ? "upstream_rate_limited"
              : retryKind === "auth_error"
                ? "upstream_auth_error"
                : "upstream_error";
          lastErrorMessage = `Upstream returned HTTP ${upstreamStatus}`;
          if (outcome === "UNKNOWN" && !mayRetryAmbiguous()) break;
          continue;
        }
        releaseProbeFor(cand.channel_id);
        releaseUnusedProbes(cursor + 1);
        await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
        emitAccessLog(upstreamStatus, "upstream_client_error");
        return c.json(
          formatProtocolError(
            operation,
            upstreamStatus,
            "upstream_client_error",
            `Upstream rejected request with HTTP ${upstreamStatus}`,
            requestId,
          ),
          upstreamStatus as 400,
          responseHeaders(requestId, trace),
        );
      }

      const upstreamContentType =
        (fastResult.fast
          ? fastResult.contentType || getHeaderValue(fastResult.headers, "content-type")
          : fastResult.response.headers.get("content-type")) ?? "application/json";
      const isUpstreamSse = upstreamContentType.toLowerCase().includes("text/event-stream");
      const isUpstreamJson = upstreamContentType.toLowerCase().includes("application/json");
      const isFakeJsonOnStream =
        isStream && !isUpstreamSse && isUpstreamJson && !prepared.isBedrockStream;

      if ((!isStream && !isUpstreamSse) || isFakeJsonOnStream) {
        channelStage = "response_body";
        let retryFromLogicalError = false;
        const postResponse: { linked: ReturnType<typeof createLinkedController> | null } = {
          linked: null,
        };
        const getPostResponseSignal = () => {
          if (!postResponse.linked) {
            postResponse.linked = createLinkedController([c.req.raw.signal, pluginSignal], false);
          }
          return postResponse.linked.signal;
        };
        try {
          let respBytes: Uint8Array;
          if (fastResult.fast) {
            if (attemptAborted) throw attemptAbortReason ?? new Error("Upstream request aborted");
            if (fastResult.body.byteLength > deps.config.maxBodyBytes) {
              throw new GatewayError(
                413,
                "body_too_large",
                "Payload exceeds the configured size limit",
              );
            }
            respBytes = fastResult.body;
          } else {
            respBytes = await readResponseBytesFast(
              upstreamResp!,
              deps.config.maxBodyBytes,
              ensureAttemptSignal(),
              deps.transport.bufferedBody(upstreamResp!),
            );
          }
          cancelTimeout();
          attemptLinked.dispose();
          activeLinkedDispose = null;
          releaseChannelLimit?.();
          releaseChannelLimit = null;
          pluginSignal.throwIfAborted();
          const droppedHeader: Record<string, string> =
            prepared.droppedFields.length > 0
              ? { "x-gateway-dropped-fields": prepared.droppedFields.join(",") }
              : {};

          if (!isUpstreamJson) {
            releaseUnusedProbes(cursor + 1);
            const usage = buildUsageForOperation(
              operation,
              candBilling.price,
              cand.costPrice,
              undefined,
            );
            if (!usage) {
              await finalizeHold("unknown");
              throw new GatewayError(
                502,
                "invalid_upstream_usage",
                "Non-JSON response requires character or request pricing basis",
              );
            }
            await commitMeasured({
              request_id: requestId,
              project_id: project.id,
              selected_attempt_id: activeAttemptId,
              channel_id: cand.channel.id,
              billing_price_id: candBilling.price.id,
              cost_version_id: cand.costPrice.id,
              api_key_id: key.id,
              pricing_override_id: candBilling.overrideId,
              discount_numerator: candBilling.discountNumerator,
              discount_denominator: candBilling.discountDenominator,
              usage,
              billing_pricing: state.pricingOf(candBilling.price),
              billing_basis: candBilling.price.basis,
              ...(chars !== undefined ? { input_characters: String(chars) } : {}),
              supplier_basis: cand.costPrice.basis,
              supplier_pricing: state.pricingOf(cand.costPrice),
              supplier_currency: cand.costPrice.currency,
              plugin_audit: preRequestAudit,
              charge_policy: "CHARGE_CUSTOMER",
              moderation_blocked: false,
            });
            pluginSignal.throwIfAborted();
            observeAttemptSuccess(latencyMs);
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

          let rawJson: Record<string, unknown>;
          try {
            rawJson = object(parseProviderJson(utf8FatalDecoder.decode(respBytes)));
          } catch (error) {
            recordAttemptFailure("invalid_upstream");
            throw error;
          }
          let rawUsage: Usage | undefined;
          let clientJson: Record<string, unknown>;

          if (
            prepared.resolvedProvider.kind === "WASM" &&
            prepared.resolvedProvider.config.hooks.includes("adapt_response")
          ) {
            const respHeadersObj: Record<string, string> = {};
            const srcHeaders = fastResult.fast ? fastResult.headers : upstreamResp!.headers;
            if (srcHeaders instanceof Headers) {
              srcHeaders.forEach((v, k) => {
                respHeadersObj[k] = v;
              });
            } else {
              for (const k in srcHeaders) respHeadersObj[k] = srcHeaders[k]!;
            }
            let wasmResp: Awaited<ReturnType<typeof invokeWasmProviderResponse>>;
            try {
              wasmResp = await invokeWasmProviderResponse({
                executor: deps.wasmExecutor,
                plugin: prepared.resolvedProvider.plugin,
                config: prepared.resolvedProvider.config,
                tenantId: project.id,
                signal: getPostResponseSignal(),
                ...(deps.config.pluginSigningPublicKeyPem
                  ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
                  : {}),
                input: {
                  operation,
                  upstream_model: cand.route.upstream_model,
                  client_protocol: isConversationalOperation(operation) ? operation : null,
                  status: upstreamStatus,
                  headers: respHeadersObj,
                  body: rawJson,
                },
              });
            } catch (err) {
              pluginSignal.throwIfAborted();
              releaseUnusedProbes(cursor + 1);
              await finalizeHold("unknown");
              if (c.req.raw.signal.aborted || drainAborted || projectPermitRevoked) {
                releaseProbeFor(cand.channel.id);
                throw admissionError(projectPermitRevoked);
              }
              if (err instanceof WasmCapacityError) {
                releaseProbeFor(cand.channel.id);
                throw new GatewayError(503, "provider_capacity_unavailable", err.message);
              }
              if (err instanceof WasmPluginFaultError) {
                releaseProbeFor(cand.channel.id);
                throw new GatewayError(502, "provider_plugin_fault", err.message);
              }
              observeAttemptFailure("retryable", undefined, latencyMs);
              markProbeCompleted(cand.channel.id);
              throw new GatewayError(
                502,
                "provider_adapter_failed",
                err instanceof Error ? err.message : "WASM provider adapt_response failed",
              );
            }

            if (wasmResp.retry_kind) {
              recordAttemptFailure(
                `logical_${wasmResp.retry_kind}`,
                wasmResp.retry_kind,
                describeFailure?.(rawJson),
              );
              const outcome = rejectedOutcome();
              if (wasmResp.retry_kind !== "client_error") {
                deps.metrics.recordUpstreamError(
                  cand.channel.name,
                  `logical_${wasmResp.retry_kind}`,
                );
                observeAttemptFailure(wasmResp.retry_kind, wasmResp.retry_after_ms, latencyMs);
                markProbeCompleted(cand.channel.id);
                pendingSwitchFrom = { attemptId: activeAttemptId, outcome };
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
                await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
                const errStatus = sanitizeHttpErrorStatus(wasmResp.status, 400);
                emitAccessLog(errStatus, "upstream_client_error");
                return c.json(
                  formatProtocolError(
                    operation,
                    errStatus,
                    "upstream_client_error",
                    wasmResp.error_message ?? "Upstream rejected request",
                    requestId,
                  ),
                  errStatus as 400,
                  responseHeaders(requestId, trace),
                );
              }
            }

            if (!retryFromLogicalError) {
              releaseUnusedProbes(cursor + 1);
              if (isFakeJsonOnStream) {
                await finalizeHold("unknown");
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
                  rawUsage = extractUsage(rawJson, prepared.usageDialect);
                }
              } catch (err) {
                await finalizeHold("unknown");
                observeAttemptFailure("retryable", undefined, latencyMs);
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
            const matchedErr =
              prepared.mergedOverrides.error_rules.length > 0
                ? evaluateErrorRules(upstreamStatus, rawJson, prepared.mergedOverrides.error_rules)
                : null;
            if (matchedErr) {
              recordAttemptFailure(
                `logical_${matchedErr.retry_kind}`,
                matchedErr.retry_kind,
                describeFailure?.(rawJson),
              );
              const outcome = rejectedOutcome(matchedErr.execution === "NOT_EXECUTED");
              if (matchedErr.retry_kind !== "client_error") {
                deps.metrics.recordUpstreamError(
                  cand.channel.name,
                  `logical_${matchedErr.retry_kind}`,
                );
                observeAttemptFailure(matchedErr.retry_kind, matchedErr.retry_after_ms, latencyMs);
                markProbeCompleted(cand.channel.id);
                pendingSwitchFrom = { attemptId: activeAttemptId, outcome };
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
                await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
                const errStatus = matchedErr.http_status ?? 400;
                emitAccessLog(errStatus, "upstream_client_error");
                return c.json(
                  formatProtocolError(
                    operation,
                    errStatus,
                    "upstream_client_error",
                    matchedErr.message ?? "Upstream rejected request",
                    requestId,
                  ),
                  errStatus as 400,
                  responseHeaders(requestId, trace),
                );
              }
            } else {
              releaseUnusedProbes(cursor + 1);
              if (isFakeJsonOnStream) {
                await finalizeHold("unknown");
                throw new GatewayError(
                  502,
                  "invalid_upstream",
                  "Upstream returned JSON body instead of event stream",
                );
              }
              const adaptedJson =
                prepared.mergedOverrides.response_transforms.length > 0
                  ? applyResponseTransforms(rawJson, prepared.mergedOverrides.response_transforms)
                  : rawJson;
              try {
                const baseUsage =
                  prepared.usageDialect === "custom"
                    ? undefined
                    : extractUsage(adaptedJson, prepared.usageDialect);
                rawUsage =
                  prepared.mergedOverrides.usage_aliases.length > 0
                    ? mergeUsageWithAliases(
                        baseUsage,
                        adaptedJson,
                        prepared.mergedOverrides.usage_aliases,
                      )
                    : baseUsage;
              } catch (err) {
                await finalizeHold("unknown");
                observeAttemptFailure("retryable", undefined, latencyMs);
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
                clientJson = adaptedJson;
                if (typeof clientJson.model === "string") clientJson.model = model.name;
              }
            }
          }

          if (!retryFromLogicalError) {
            const usage = buildUsageForOperation(
              operation,
              candBilling.price,
              cand.costPrice,
              rawUsage,
              (operation === "images" || operation === "image_edits") &&
                Array.isArray(clientJson.data)
                ? countImageArtifacts(clientJson.data)
                : undefined,
              inputMeasurement.images,
            );
            if (!usage) {
              await finalizeHold("unknown");
              observeAttemptFailure("retryable", undefined, latencyMs);
              markProbeCompleted(cand.channel.id);
              throw new GatewayError(
                502,
                "invalid_upstream_usage",
                "Upstream omitted usage metadata",
              );
            }

            let responseBlocked = false;
            let isContentDecision = true;
            let chargePolicy: "CHARGE_CUSTOMER" | "PLATFORM_ABSORB" = "CHARGE_CUSTOMER";
            let blockRule = "";
            let blockCode = "content_blocked";
            let blockStatus = 400;
            let blockMessage = "Content blocked";
            let blockExtraHeaders: Record<string, string> = {};
            const allAudit: PluginAuditEntry[] =
              preRequestAudit.length > 0 ? [...preRequestAudit] : [];

            if (cand.route.plugin_pipeline.length > 0 && isConversationalOperation(operation)) {
              const respText = extractResponseText(clientJson, operation);
              const postRes = await deps.pluginEngine.runUnary(
                "on_response",
                cand.route.plugin_pipeline,
                snap.plugins,
                { request_id: requestId, project_id: project.id, model: model.name, operation },
                respText,
                getPostResponseSignal(),
              );
              allAudit.push(...postRes.audit);
              if (postRes.blocked) {
                responseBlocked = true;
                const classified = classifyPluginBlock(
                  postRes.outcome_kind,
                  postRes.http_status,
                  postRes.safe_message,
                );
                isContentDecision = classified.isContentDecision;
                chargePolicy = isContentDecision ? postRes.charge_policy : "PLATFORM_ABSORB";
                blockRule = postRes.rule_code ?? classified.code;
                blockCode = classified.code;
                blockStatus = classified.status;
                blockMessage = classified.message;
                blockExtraHeaders = classified.extraHeaders;
                if (isContentDecision) {
                  logModBlocked = true;
                  deps.metrics.moderationBlocksTotal++;
                }
              } else if (postRes.text !== respText) {
                clientJson = replaceResponseText(clientJson, operation, postRes.text);
              }
            }

            const commitRes = await commitMeasured({
              request_id: requestId,
              project_id: project.id,
              selected_attempt_id: activeAttemptId,
              channel_id: cand.channel.id,
              billing_price_id: candBilling.price.id,
              cost_version_id: cand.costPrice.id,
              api_key_id: key.id,
              pricing_override_id: candBilling.overrideId,
              discount_numerator: candBilling.discountNumerator,
              discount_denominator: candBilling.discountDenominator,
              usage,
              billing_pricing: state.pricingOf(candBilling.price),
              billing_basis: candBilling.price.basis,
              ...(chars !== undefined ? { input_characters: String(chars) } : {}),
              supplier_basis: cand.costPrice.basis,
              supplier_pricing: state.pricingOf(cand.costPrice),
              supplier_currency: cand.costPrice.currency,
              plugin_audit: allAudit,
              charge_policy: chargePolicy,
              moderation_blocked: responseBlocked,
            });
            if (responseBlocked && !isContentDecision) {
              deps.metrics.recordPlatformAbsorb(project.id, commitRes.supplier_cost_atoms);
            }

            if (operation === "videos" && deps.asyncTasks && !responseBlocked) {
              const asyncCfg = getAsyncTaskConfig(snap, cand.channel);
              const upstreamTaskId = extractUpstreamTaskId(clientJson, asyncCfg);
              if (upstreamTaskId) {
                const taskId = `vid_${fastUuid().replaceAll("-", "")}`;
                const taskRec = await deps.asyncTasks
                  .createTask({
                    id: taskId,
                    request_id: requestId,
                    charge_id: commitRes.charge_id,
                    project_id: project.id,
                    api_key_id: key.id,
                    channel_id: cand.channel.id,
                    model: model.name,
                    operation,
                    upstream_task_id: upstreamTaskId,
                    status: "QUEUED",
                    result_json: clientJson,
                  })
                  .catch(() => null);
                if (taskRec) {
                  clientJson = {
                    ...clientJson,
                    gateway_task_id: taskRec.id,
                    ...(typeof clientJson.id !== "string" ? { id: taskRec.id } : {}),
                  };
                }
              }
            }

            pluginSignal.throwIfAborted();
            observeAttemptSuccess(latencyMs);
            markProbeCompleted(cand.channel.id);
            if (responseBlocked) {
              emitAccessLog(blockStatus, blockRule);
              return c.json(
                formatProtocolError(
                  operation,
                  blockStatus,
                  blockCode,
                  blockMessage,
                  requestId,
                  blockRule ? { rule_code: blockRule } : undefined,
                ),
                blockStatus as 400,
                responseHeaders(requestId, trace, {
                  "x-gateway-channel": cand.channel.name,
                  ...blockExtraHeaders,
                  ...droppedHeader,
                }),
              );
            }
            emitAccessLog(200, null);
            const okHeaders: Record<string, string> = {
              "x-request-id": requestId,
              "x-trace-id": trace.traceId,
              traceparent: trace.traceparent,
              "content-type": "application/json",
              "x-gateway-channel": cand.channel.name,
            };
            if (prepared.droppedFields.length > 0) {
              okHeaders["x-gateway-dropped-fields"] = prepared.droppedFields.join(",");
            }
            return new Response(stringifyExact(clientJson), {
              status: 200,
              headers: okHeaders,
            });
          }
        } finally {
          cancelTimeout();
          postResponse.linked?.dispose();
          attemptLinked.dispose();
          activeLinkedDispose = null;
        }
        if (retryFromLogicalError) {
          if (pendingSwitchFrom?.outcome === "UNKNOWN" && !mayRetryAmbiguous()) break;
          continue;
        }
      }

      const isWasmStreamProvider =
        prepared.resolvedProvider.kind === "WASM" &&
        prepared.resolvedProvider.config.hooks.includes("adapt_stream_event") &&
        isConversationalOperation(operation);
      const tracedUpstreamProtocol =
        prepared.upstreamProtocol ??
        (!isWasmStreamProvider && isConversationalOperation(operation) ? operation : null);
      const { error_rules: errorRules, media_stream_terminal: mediaTerminal } =
        prepared.mergedOverrides;
      const configuredTerminalEvent =
        mediaTerminal && "event" in mediaTerminal ? mediaTerminal.event : undefined;
      let upstreamBody: ReadableStream<Uint8Array> | null = fastResult.fast
        ? new Response(fastResult.body as BodyInit).body
        : upstreamResp!.body;
      let wasmPrelude:
        | {
            event: string;
            data: string;
            result: Awaited<ReturnType<typeof invokeWasmProviderStreamEvent>>;
          }
        | undefined;
      if (
        upstreamBody &&
        (isWasmStreamProvider || prepared.mergedOverrides.error_rules.length > 0) &&
        (isUpstreamSse || prepared.isBedrockStream)
      ) {
        channelStage = "stream_prelude";
        const parser = new SSEParser();
        const bedrock = prepared.isBedrockStream ? new BedrockEventStreamDecoder() : null;
        let logicalError: MatchedProviderError | null = null;
        let providerError: TraceAttempt["providerError"];
        try {
          const peeked = await peekStream(
            upstreamBody,
            (chunk): SSEEvent | undefined => {
              const events = bedrock ? bedrock.feed(chunk) : parser.feed(chunk);
              requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
              return events.find((frame) => frame.data !== "");
            },
            ensureAttemptSignal(),
          );
          upstreamBody = peeked.body;
          const first = peeked.found;
          if (first && isWasmStreamProvider && prepared.resolvedProvider.kind === "WASM") {
            const result = await invokeWasmProviderStreamEvent({
              executor: deps.wasmExecutor,
              plugin: prepared.resolvedProvider.plugin,
              config: prepared.resolvedProvider.config,
              tenantId: project.id,
              signal: ensureAttemptSignal(),
              ...(deps.config.pluginSigningPublicKeyPem
                ? { signingPublicKeyPem: deps.config.pluginSigningPublicKeyPem }
                : {}),
              input: {
                operation,
                upstream_model: cand.route.upstream_model,
                client_protocol: isConversationalOperation(operation) ? operation : null,
                event: first.event,
                data: first.data,
                state: "",
              },
            });
            wasmPrelude = { event: first.event, data: first.data, result };
            if (result.retry_kind) {
              providerError = describeEventFailure?.(first.data) ?? describeFailure?.();
              logicalError = {
                retry_kind: result.retry_kind,
                ...(result.retry_after_ms === undefined
                  ? {}
                  : { retry_after_ms: result.retry_after_ms }),
                ...(result.error_message === undefined ? {} : { message: result.error_message }),
              };
            }
          } else if (
            first &&
            first.data !== "[DONE]" &&
            !(first.event === configuredTerminalEvent && !first.data.trimStart().startsWith("{"))
          ) {
            const data = parseProviderJson(first.data);
            logicalError = evaluateErrorRules(
              upstreamStatus,
              data,
              prepared.mergedOverrides.error_rules,
            );
            if (logicalError) providerError = describeFailure?.(data);
          }
        } catch (err) {
          cancelTimeout();
          attemptLinked.dispose();
          activeLinkedDispose = null;
          await upstreamBody?.cancel().catch(() => undefined);
          releaseChannelLimit?.();
          releaseChannelLimit = null;
          pluginSignal.throwIfAborted();
          const quotaLost = projectPermitRevoked || channelPermitRevoked;
          if (c.req.raw.signal.aborted || drainAborted || quotaLost) {
            releaseProbeFor(cand.channel_id);
            releaseUnusedProbes(cursor + 1);
            await finalizeHold("unknown");
            throw admissionError(quotaLost);
          }
          if (err instanceof WasmCapacityError) {
            releaseProbeFor(cand.channel.id);
            throw new GatewayError(503, "provider_capacity_unavailable", err.message);
          }
          if (err instanceof WasmPluginFaultError) {
            releaseProbeFor(cand.channel.id);
            throw new GatewayError(502, "provider_plugin_fault", err.message);
          }
          if (attemptAborted) {
            recordAttemptFailure("upstream_timeout");
            deps.metrics.recordUpstreamError(cand.channel.name, "timeout");
            observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
            markProbeCompleted(cand.channel.id);
            anyUnknownAttempt = true;
            const allowAmbiguousRetry = mayRetryAmbiguous();
            lastErrorStatus = 504;
            lastErrorCode = "upstream_timeout";
            lastErrorMessage =
              attemptAbortReason instanceof Error
                ? attemptAbortReason.message
                : "Upstream stream prelude timed out";
            if (allowAmbiguousRetry) {
              pendingSwitchFrom = { attemptId: activeAttemptId, outcome: "UNKNOWN" };
              continue;
            }
            releaseUnusedProbes(cursor + 1);
            await finalizeHold("unknown");
            throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
          }
          throw new GatewayError(
            502,
            "upstream_stream_error",
            "Upstream stream prelude could not be read",
          );
        }
        if (logicalError) {
          recordAttemptFailure(
            `logical_${logicalError.retry_kind}`,
            logicalError.retry_kind,
            providerError,
          );
          const outcome = rejectedOutcome(logicalError.execution === "NOT_EXECUTED");
          cancelTimeout();
          attemptLinked.dispose();
          activeLinkedDispose = null;
          await upstreamBody?.cancel().catch(() => undefined);
          releaseChannelLimit?.();
          releaseChannelLimit = null;
          if (logicalError.retry_kind === "client_error") {
            await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
            throw new GatewayError(
              logicalError.http_status ?? 400,
              "upstream_client_error",
              logicalError.message ?? "Upstream rejected the request",
            );
          }
          deps.metrics.recordUpstreamError(cand.channel.name, `logical_${logicalError.retry_kind}`);
          observeAttemptFailure(
            logicalError.retry_kind,
            logicalError.retry_after_ms,
            performance.now() - startedAt,
          );
          markProbeCompleted(cand.channel.id);
          pendingSwitchFrom = { attemptId: activeAttemptId, outcome };
          lastErrorStatus =
            logicalError.http_status ?? (logicalError.retry_kind === "rate_limit" ? 429 : 502);
          lastErrorCode =
            logicalError.retry_kind === "rate_limit" ? "upstream_rate_limited" : "upstream_error";
          lastErrorMessage = logicalError.message ?? "Upstream stream rejected the request";
          if (outcome === "UNKNOWN" && !mayRetryAmbiguous()) break;
          continue;
        }
      }

      channelStage = "stream";
      releaseUnusedProbes(cursor + 1);
      cancelTimeout();
      const streamProbe = acquiredProbes.get(cand.channel.id);
      acquiredProbes.delete(cand.channel.id);
      const streamLinked = createLinkedController([c.req.raw.signal, pluginSignal], true, false);
      const streamSignal = streamLinked.signal;
      activeAbortNotify = (reason) => {
        attemptLinked.trigger(reason);
        streamLinked.trigger(reason);
      };
      const durationTimer = setTimeout(() => {
        const reason = new Error("Upstream stream exceeded its maximum duration");
        streamLinked.trigger(reason);
        attemptAbort.abort(reason);
      }, maxStreamDurationMs);
      durationTimer.unref();
      const abortedFrame = () => {
        if (c.req.raw.signal.aborted) return undefined;
        if (pluginSignal.aborted) {
          emitAccessLog(503, "plugin_revoked");
          return formatProtocolSseError(
            operation,
            503,
            "plugin_revoked",
            pluginSignal.reason.message,
            requestId,
          );
        }
        const quotaLost = projectPermitRevoked || channelPermitRevoked;
        return formatProtocolSseError(
          operation,
          503,
          quotaLost ? "rate_limit_unavailable" : "stream_interrupted",
          quotaLost ? "Shared concurrency permit was revoked" : "Gateway interrupted the stream",
          requestId,
        );
      };
      const openStreamReader = (reader: ReadableStreamDefaultReader<Uint8Array>) =>
        createStreamChunkReader(
          reader,
          deps.config.streamIdleTimeoutMs ?? baseAttemptTimeoutMs,
          () => {
            if (!streamSignal.aborted)
              recordAttemptFailure("upstream_stream_idle_timeout", "timeout");
            attemptAbort.abort(new Error("Upstream stream exceeded its idle timeout"));
          },
          streamSignal,
        );
      const capturedProjectLimit = releaseProjectLimit;
      const capturedChannelLimit = releaseChannelLimit;
      const capturedLocalProjectSlot = releaseLocalProjectSlot;

      let streamCleanupDone = false;
      let streamCounted = false;
      let streamProbeDone = false;
      const confirmStreamProbe = () => {
        if (streamProbe && !streamProbeDone) {
          streamProbeDone = true;
          observeAttemptSuccess(latencyMs);
        }
      };
      const finishStreamProbe = () => {
        if (!streamProbeDone && streamProbe) {
          streamProbeDone = true;
          deps.router.releaseProbe(cand.channel.id, streamProbe);
        }
      };
      const cleanupStream = () => {
        if (streamCleanupDone) return;
        streamCleanupDone = true;
        releaseOutputAdmission?.();
        releaseOutputAdmission = undefined;
        if (streamCounted) deps.metrics.activeStreams--;
        clearTimeout(durationTimer);
        streamLinked.dispose();
        attemptLinked.dispose();
        activeAbortNotify = null;
        activeLinkedDispose = null;
        finishStreamProbe();
        capturedChannelLimit?.();
        capturedProjectLimit?.();
        capturedLocalProjectSlot?.();
        releaseBodyBuffer?.();
        guard.release();
      };

      const respondWithStream = (stream: ReadableStream<Uint8Array>) => {
        const response = new Response(stream, {
          status: 200,
          headers: responseHeaders(requestId, trace, {
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache",
            "x-gateway-channel": cand.channel.name,
            ...(prepared.droppedFields.length > 0
              ? { "x-gateway-dropped-fields": prepared.droppedFields.join(",") }
              : {}),
          }),
        });
        releaseProjectLimit = null;
        releaseChannelLimit = null;
        releaseLocalProjectSlot = null;
        streamOwnsGuard = true;
        if (!streamCleanupDone) {
          streamCounted = true;
          deps.metrics.activeStreams++;
        }
        return response;
      };

      if (
        !isConversationalOperation(operation) ||
        (!prepared.upstreamProtocol && !isWasmStreamProvider)
      ) {
        const usageDialect = prepared.usageDialect;
        const terminalByType = (type: string) =>
          type === "done" || type === "completed" || type === mediaCompletionEvents[operation];
        let clientCancelled = false;
        const mediaStream = createStreamRelay(
          async (controller) => {
            const reader = upstreamBody?.getReader();
            const chunkReader = reader ? openStreamReader(reader) : null;
            const parser = new SSEParser();
            let observedUsage: Usage | undefined;
            const heldChunks = createTerminalBuffer();
            let terminal = false;
            let sawDone = false;
            const imageStream = operation === "images" || operation === "image_edits";
            let deliveredImages: Set<string> | undefined;
            const failure: { provider?: MatchedProviderError } = {};
            const processEvent = (event: SSEEvent) => {
              if (sawDone && event.data) {
                throw new GatewayError(
                  502,
                  "unexpected_trailing_event",
                  "Upstream sent media data after [DONE]",
                );
              }
              if (event.event === "error" && !event.data.trimStart().startsWith("{")) {
                throw new GatewayError(
                  502,
                  "upstream_stream_error",
                  "Upstream reported a media stream error",
                );
              }
              const namedTerminal = configuredTerminalEvent === event.event;
              if (event.data === "[DONE]") {
                terminal = true;
                sawDone = true;
              } else if (event.data && (!namedTerminal || event.data.trimStart().startsWith("{"))) {
                const parsed = object(parseProviderJson(event.data));
                const providerError = evaluateErrorRules(upstreamStatus, parsed, errorRules);
                if (
                  providerError ||
                  event.event === "error" ||
                  parsed.type === "error" ||
                  parsed.error
                ) {
                  recordAttemptFailure(
                    "upstream_stream_error",
                    providerError?.retry_kind ?? "protocol",
                    describeFailure?.(parsed),
                  );
                  if (providerError) failure.provider = providerError;
                  throw new GatewayError(
                    providerError?.http_status ??
                      (providerError?.retry_kind === "client_error"
                        ? 400
                        : providerError?.retry_kind === "rate_limit"
                          ? 429
                          : 502),
                    "upstream_stream_error",
                    providerError?.message ?? "Upstream reported a media stream error",
                  );
                }
                const baseUsage =
                  usageDialect === "custom"
                    ? observedUsage
                    : (extractUsage(parsed, usageDialect, observedUsage) ?? observedUsage);
                observedUsage =
                  mergeUsageWithAliases(
                    baseUsage,
                    parsed,
                    prepared.mergedOverrides.usage_aliases,
                  ) ?? baseUsage;
                if (
                  imageStream &&
                  (event.event === mediaCompletionEvents[operation] ||
                    parsed.type === mediaCompletionEvents[operation])
                ) {
                  deliveredImages ??= new Set();
                  recordImageArtifact(deliveredImages, parsed);
                }
                terminal ||= mediaTerminal
                  ? "parts" in mediaTerminal &&
                    getAtPath(parsed, mediaTerminal.parts) === mediaTerminal.equals
                  : typeof parsed.type === "string" && terminalByType(parsed.type);
              }
              terminal ||= namedTerminal || (!mediaTerminal && terminalByType(event.event));
              const bytes = utf8Encoder.encode(event.wire);
              if (!terminal) return controller.enqueue(bytes);
              heldChunks.push(bytes);
            };
            try {
              if (!chunkReader) {
                throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
              }
              while (true) {
                const step = await chunkReader.read();
                if (step.done) break;
                const events = parser.feed(step.value);
                requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
                for (const event of events) {
                  const enqueue = processEvent(event);
                  if (enqueue) await enqueue;
                }
              }
              if (clientCancelled || c.req.raw.signal.aborted) {
                await finalizeHold("unknown");
                emitAccessLog(499, "client_aborted");
                return;
              }
              const events = parser.feed(new Uint8Array(), true);
              requestTrace?.observeUpstream(events, tracedUpstreamProtocol);
              for (const event of events) {
                const enqueue = processEvent(event);
                if (enqueue) await enqueue;
              }
              if (!terminal || parser.incomplete) {
                throw new GatewayError(
                  502,
                  "stream_interrupted",
                  "Media stream ended without a complete terminal event",
                );
              }
              const usage = buildUsageForOperation(
                operation,
                candBilling.price,
                cand.costPrice,
                observedUsage,
                deliveredImages?.size,
                inputMeasurement.images,
              );
              if (!usage) {
                await finalizeHold("unknown");
                emitAccessLog(502, "invalid_upstream_usage");
                await controller.enqueue(
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
              await commitMeasured({
                request_id: requestId,
                project_id: project.id,
                selected_attempt_id: activeAttemptId,
                channel_id: cand.channel.id,
                billing_price_id: candBilling.price.id,
                cost_version_id: cand.costPrice.id,
                api_key_id: key.id,
                pricing_override_id: candBilling.overrideId,
                discount_numerator: candBilling.discountNumerator,
                discount_denominator: candBilling.discountDenominator,
                usage,
                billing_pricing: state.pricingOf(candBilling.price),
                billing_basis: candBilling.price.basis,
                ...(chars !== undefined ? { input_characters: String(chars) } : {}),
                supplier_basis: cand.costPrice.basis,
                supplier_pricing: state.pricingOf(cand.costPrice),
                supplier_currency: cand.costPrice.currency,
                plugin_audit: preRequestAudit,
                charge_policy: "CHARGE_CUSTOMER",
                moderation_blocked: false,
              });
              streamProbeDone = true;
              observeAttemptSuccess(latencyMs);
              for (const ch of heldChunks) await controller.enqueue(ch);
              emitAccessLog(200, null);
              controller.close();
            } catch (err) {
              const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
              await finalizeHold("unknown");
              if (abortedByClient) {
                emitAccessLog(499, "client_aborted");
                return;
              }
              const code = err instanceof GatewayError ? err.code : "stream_interrupted";
              const kind = failure.provider?.retry_kind ?? "retryable";
              if (
                code !== "ledger_commit_failed" &&
                !streamSignal.aborted &&
                kind !== "client_error"
              ) {
                recordAttemptFailure(code);
                deps.metrics.recordUpstreamError(cand.channel.name, "media_stream_error");
                streamProbeDone = true;
                observeAttemptFailure(
                  kind,
                  failure.provider?.retry_after_ms,
                  performance.now() - startedAt,
                );
              }
              emitAccessLog(err instanceof GatewayError ? err.status : 502, code);
              try {
                await controller.enqueue(
                  encodeSSE(
                    {
                      error: {
                        code,
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
              chunkReader?.dispose();
              if (reader) {
                await reader.cancel().catch(() => undefined);
                reader.releaseLock();
              }
            }
          },
          {
            signal: streamSignal,
            cancel() {
              clientCancelled = true;
              attemptAbort.abort(new Error("client_cancelled"));
            },
            fail: () => finalizeHold("unknown"),
            abortedFrame,
            finish: cleanupStream,
            ...(requestTrace && traceProtocol ? { onEnqueued: requestTrace.observeEnqueued } : {}),
          },
        );
        return respondWithStream(mediaStream);
      }

      const clientProtocol: Protocol = operation;
      const streamId = `gen_${requestId.replaceAll("-", "")}`;
      const hasPlugins = cand.route.plugin_pipeline.length > 0;
      const openContentSession = () =>
        deps.pluginEngine.createStreamSession(
          cand.route.plugin_pipeline,
          snap.plugins,
          { request_id: requestId, project_id: project.id, model: model.name, operation },
          () => attemptAbort.abort(new Error("moderation_blocked")),
          streamSignal,
        );
      let clientCancelled = false;

      if (isWasmStreamProvider && prepared.resolvedProvider.kind === "WASM") {
        const wasmProvider = prepared.resolvedProvider;
        const wasmStream = createStreamRelay(
          async (controller) => {
            const reader = upstreamBody?.getReader();
            const chunkReader = reader ? openStreamReader(reader) : null;
            const parser = new SSEParser();
            const bedrockDecoder = prepared.isBedrockStream
              ? new BedrockEventStreamDecoder()
              : null;
            const session =
              hasPlugins && clientProtocol !== "responses" ? openContentSession() : null;

            const responsesModeration =
              hasPlugins && clientProtocol === "responses"
                ? createResponsesModeration(openContentSession)
                : null;
            const heldTerminalChunks = createTerminalBuffer();
            const streamPluginAudit: PluginAuditEntry[] = [];
            let observedOutputChars = 0;
            let wasmState = "";
            let wasmUsage: Usage | undefined;
            let wasmTerminal = false;
            let streamFailed = false;
            let blockedEarly = false;

            const processWasmEvent = async (ev: { event: string; data: string }) => {
              if (!ev.data) return;
              const cached =
                wasmPrelude?.event === ev.event && wasmPrelude.data === ev.data
                  ? wasmPrelude.result
                  : undefined;
              wasmPrelude = undefined;
              const out =
                cached ??
                (await invokeWasmProviderStreamEvent({
                  executor: deps.wasmExecutor,
                  plugin: wasmProvider.plugin,
                  config: wasmProvider.config,
                  tenantId: project.id,
                  signal: streamSignal,
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
                }));
              if (out.state !== undefined) wasmState = out.state;
              if (out.retry_kind) {
                recordAttemptFailure(
                  "upstream_stream_error",
                  out.retry_kind,
                  describeEventFailure?.(ev.data) ?? describeFailure?.(),
                );
                throw new GatewayError(
                  out.retry_kind === "rate_limit" ? 429 : 502,
                  "upstream_stream_error",
                  out.error_message ?? "Upstream stream reported error",
                );
              }
              if (out.chunks?.length) confirmStreamProbe();
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
                if (
                  (clientProtocol === "responses" &&
                    ["response.completed", "response.incomplete"].includes(
                      String(chunkObj.type),
                    )) ||
                  (clientProtocol === "messages" && chunkObj.type === "message_stop") ||
                  responsesModeration?.defer(encoded)
                ) {
                  heldTerminalChunks.push(encoded);
                  continue;
                }
                if (responsesModeration) {
                  const result = await responsesModeration.push(encoded);
                  if (!result) await controller.enqueue(responsesModeration.rewrite(encoded));
                  else {
                    observedOutputChars += result.chars;
                    if (result.blocked) {
                      blockedEarly = true;
                      break;
                    }
                    if (result.rendered) await controller.enqueue(result.rendered);
                  }
                  continue;
                }
                if (!session) {
                  await controller.enqueue(encoded);
                  continue;
                }
                const deltaText = extractChunkTextFromEncodedSse(encoded, clientProtocol);
                if (deltaText === null) {
                  await controller.enqueue(encoded);
                  continue;
                }
                observedOutputChars += deltaText.length;
                const mod = await session.push(deltaText);
                if (mod.blocked) {
                  blockedEarly = true;
                  break;
                }
                if (mod.emit.length > 0) {
                  await controller.enqueue(
                    buildTextDeltaSse(clientProtocol, model.name, mod.emit, streamId),
                  );
                }
              }
              wasmTerminal ||= out.done === true;
            };

            try {
              if (!reader || !chunkReader) {
                throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
              }
              while (true) {
                let step: Awaited<ReturnType<typeof chunkReader.read>>;
                try {
                  step = await chunkReader.read();
                } catch (readErr) {
                  if (session?.isBlocked() || responsesModeration?.isBlocked()) {
                    blockedEarly = true;
                    break;
                  }
                  throw readErr;
                }
                if (step.done) break;
                const events = bedrockDecoder
                  ? bedrockDecoder.feed(step.value)
                  : parser.feed(step.value, false);
                requestTrace?.observeUpstream(events, prepared.upstreamProtocol);
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
                await finalizeHold("unknown");
                emitAccessLog(499, "client_aborted");
                return;
              }

              if (!blockedEarly && !bedrockDecoder) {
                const events = parser.feed(new Uint8Array(), true);
                requestTrace?.observeUpstream(events, prepared.upstreamProtocol);
                for (const ev of events) {
                  await processWasmEvent(ev);
                  if (blockedEarly) break;
                }
              }

              if (session || responsesModeration) {
                const finalMod = responsesModeration
                  ? await responsesModeration.finalize()
                  : { ...(await session!.finalize()), frames: [] };
                streamPluginAudit.push(...finalMod.audit);
                if (finalMod.blocked) {
                  const classified = classifyPluginBlock(
                    finalMod.outcome_kind,
                    finalMod.http_status,
                    finalMod.safe_message,
                  );
                  const effectiveChargePolicy = classified.isContentDecision
                    ? finalMod.charge_policy
                    : "PLATFORM_ABSORB";
                  if (classified.isContentDecision) {
                    deps.metrics.moderationBlocksTotal++;
                    logModBlocked = true;
                  }
                  const modUsage = buildModerationUsageFallback(
                    wasmUsage,
                    chars ?? 0,
                    observedOutputChars,
                  );
                  const commitRes = await commitMeasured({
                    request_id: requestId,
                    project_id: project.id,
                    selected_attempt_id: activeAttemptId,
                    channel_id: cand.channel.id,
                    billing_price_id: candBilling.price.id,
                    cost_version_id: cand.costPrice.id,
                    api_key_id: key.id,
                    pricing_override_id: candBilling.overrideId,
                    discount_numerator: candBilling.discountNumerator,
                    discount_denominator: candBilling.discountDenominator,
                    usage: modUsage,
                    billing_pricing: state.pricingOf(candBilling.price),
                    billing_basis: candBilling.price.basis,
                    ...(chars !== undefined ? { input_characters: String(chars) } : {}),
                    supplier_basis: cand.costPrice.basis,
                    supplier_pricing: state.pricingOf(cand.costPrice),
                    supplier_currency: cand.costPrice.currency,
                    plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                    charge_policy: effectiveChargePolicy,
                    moderation_blocked: true,
                  });
                  if (!classified.isContentDecision) {
                    deps.metrics.recordPlatformAbsorb(project.id, commitRes.supplier_cost_atoms);
                  }
                  streamProbeDone = true;
                  observeAttemptSuccess(latencyMs);
                  emitAccessLog(classified.status, finalMod.rule_code ?? classified.code);
                  await controller.enqueue(
                    formatProtocolSseError(
                      operation,
                      classified.status,
                      classified.code,
                      classified.message,
                      requestId,
                      finalMod.rule_code ? { rule_code: finalMod.rule_code } : undefined,
                    ),
                  );
                  controller.close();
                  return;
                }
                for (const frame of finalMod.frames) await controller.enqueue(frame);
                if (finalMod.emit.length > 0) {
                  await controller.enqueue(
                    buildTextDeltaSse(clientProtocol, model.name, finalMod.emit, streamId),
                  );
                }
              }

              if (!wasmTerminal) {
                throw new GatewayError(
                  502,
                  "stream_interrupted",
                  "Provider stream ended without a terminal event",
                );
              }
              if (clientProtocol === "chat") {
                heldTerminalChunks.push(utf8Encoder.encode("data: [DONE]\n\n"));
              }

              const usage = buildUsageForOperation(
                operation,
                candBilling.price,
                cand.costPrice,
                wasmUsage,
              );
              if (!usage) {
                streamFailed = true;
                deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
                streamProbeDone = true;
                observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
                await finalizeHold("unknown");
                emitAccessLog(502, "invalid_upstream_usage");
                await controller.enqueue(
                  formatProtocolSseError(
                    operation,
                    502,
                    "invalid_upstream_usage",
                    "Stream finished without usage metadata",
                    requestId,
                  ),
                );
                controller.close();
                return;
              }

              await commitMeasured({
                request_id: requestId,
                project_id: project.id,
                selected_attempt_id: activeAttemptId,
                channel_id: cand.channel.id,
                billing_price_id: candBilling.price.id,
                cost_version_id: cand.costPrice.id,
                api_key_id: key.id,
                pricing_override_id: candBilling.overrideId,
                discount_numerator: candBilling.discountNumerator,
                discount_denominator: candBilling.discountDenominator,
                usage,
                billing_pricing: state.pricingOf(candBilling.price),
                billing_basis: candBilling.price.basis,
                ...(chars !== undefined ? { input_characters: String(chars) } : {}),
                supplier_basis: cand.costPrice.basis,
                supplier_pricing: state.pricingOf(cand.costPrice),
                supplier_currency: cand.costPrice.currency,
                plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                charge_policy: "CHARGE_CUSTOMER",
                moderation_blocked: false,
              });

              streamProbeDone = true;
              observeAttemptSuccess(latencyMs);
              for (const chunk of heldTerminalChunks) {
                await controller.enqueue(responsesModeration?.rewrite(chunk, true) ?? chunk);
              }
              emitAccessLog(200, null);
              controller.close();
            } catch (err) {
              const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
              const isLocalWasmFault =
                err instanceof WasmCapacityError || err instanceof WasmPluginFaultError;
              if (!streamFailed) {
                streamFailed = true;
                await finalizeHold("unknown");
                if (
                  !abortedByClient &&
                  !streamSignal.aborted &&
                  !isLocalWasmFault &&
                  !(err instanceof GatewayError && err.code === "ledger_commit_failed")
                ) {
                  deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
                  streamProbeDone = true;
                  observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
                }
              }
              if (abortedByClient) {
                emitAccessLog(499, "client_aborted");
                return;
              }
              const errStatus =
                err instanceof WasmCapacityError
                  ? 503
                  : err instanceof GatewayError
                    ? err.status
                    : 502;
              const code =
                err instanceof WasmCapacityError
                  ? "provider_capacity_unavailable"
                  : err instanceof WasmPluginFaultError
                    ? "provider_plugin_fault"
                    : err instanceof GatewayError
                      ? err.code
                      : "stream_interrupted";
              if (!streamSignal.aborted && code !== "ledger_commit_failed")
                recordAttemptFailure(code);
              emitAccessLog(errStatus, code);
              try {
                const msg = err instanceof Error ? err.message : "Stream failed";
                await controller.enqueue(
                  formatProtocolSseError(operation, errStatus, code, msg, requestId),
                );
                controller.close();
              } catch {
                void 0;
              }
            } finally {
              chunkReader?.dispose();
              if (reader) {
                await reader.cancel().catch(() => undefined);
                reader.releaseLock();
              }
            }
          },
          {
            signal: streamSignal,
            cancel() {
              clientCancelled = true;
              attemptAbort.abort(new Error("client_cancelled"));
            },
            fail: () => finalizeHold("unknown"),
            abortedFrame,
            finish: cleanupStream,
            ...(requestTrace && traceProtocol ? { onEnqueued: requestTrace.observeEnqueued } : {}),
          },
        );
        return respondWithStream(wasmStream);
      }

      const upstreamProtocol: Protocol = prepared.upstreamProtocol!;

      const bodyStream = createStreamRelay(
        async (controller) => {
          const reader = upstreamBody?.getReader();
          const chunkReader = reader ? openStreamReader(reader) : null;
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
          let translating: SSEEvent | undefined;
          const session =
            hasPlugins && clientProtocol !== "responses" ? openContentSession() : null;

          const responsesModeration =
            hasPlugins && clientProtocol === "responses"
              ? createResponsesModeration(openContentSession)
              : null;
          const heldApprovedChunks = createTerminalBuffer();
          const heldTerminalChunks = createTerminalBuffer();
          const streamPluginAudit: PluginAuditEntry[] = [];
          let observedOutputChars = 0;
          let activeTextBlockIndex = -1;
          let streamFailed = false;
          let blockedEarly = false;

          const handleStreamChunk = async (chunk: Uint8Array, isTerminal: boolean) => {
            if (responsesModeration) {
              if (responsesModeration.defer(chunk)) {
                heldTerminalChunks.push(chunk);
                return;
              }
              const result = await responsesModeration.push(chunk);
              if (!result) {
                if (isTerminal) heldTerminalChunks.push(chunk);
                else await controller.enqueue(responsesModeration.rewrite(chunk));
              } else {
                observedOutputChars += result.chars;
                if (result.blocked) {
                  blockedEarly = true;
                  return;
                }
                if (result.rendered) {
                  if (isTerminal) heldApprovedChunks.push(result.rendered);
                  else await controller.enqueue(result.rendered);
                }
              }
              return;
            }
            if (!session) {
              if (isTerminal) heldTerminalChunks.push(chunk);
              else await controller.enqueue(chunk);
              return;
            }
            if (clientProtocol === "messages") {
              const blk = inspectAnthropicBlockEvent(chunk);
              if (blk?.kind === "text_start") {
                activeTextBlockIndex = blk.index;
              } else if (blk?.kind === "block_stop" && blk.index === activeTextBlockIndex) {
                heldTerminalChunks.push(chunk);
                return;
              }
            }
            const deltaText = extractChunkTextFromEncodedSse(chunk, clientProtocol);
            if (deltaText === null) {
              if (isTerminal) heldTerminalChunks.push(chunk);
              else await controller.enqueue(chunk);
              return;
            }
            observedOutputChars += deltaText.length;
            const mod = await session.push(deltaText);
            if (mod.blocked) {
              blockedEarly = true;
              return;
            }
            if (mod.emit.length > 0) {
              const rendered = buildTextDeltaSse(
                clientProtocol,
                model.name,
                mod.emit,
                streamId,
                activeTextBlockIndex >= 0 ? activeTextBlockIndex : 0,
              );
              if (isTerminal) heldApprovedChunks.push(rendered);
              else await controller.enqueue(rendered);
            }
            const remainder = stripTextFromSseChunk(chunk, clientProtocol);
            if (remainder) {
              if (isTerminal) heldTerminalChunks.push(remainder);
              else await controller.enqueue(remainder);
            }
          };

          try {
            if (!reader || !chunkReader) {
              throw new GatewayError(502, "empty_stream", "Upstream returned empty body");
            }
            while (true) {
              let step: Awaited<ReturnType<typeof chunkReader.read>>;
              try {
                step = await chunkReader.read();
              } catch (readErr) {
                if (session?.isBlocked() || responsesModeration?.isBlocked()) {
                  blockedEarly = true;
                  break;
                }
                throw readErr;
              }
              if (step.done) break;
              const events = bedrockDecoder
                ? bedrockDecoder.feed(step.value)
                : parser.feed(step.value, false);
              requestTrace?.observeUpstream(events, upstreamProtocol);
              for (const ev of events) {
                if (hasStreamRules && ev.data && ev.data !== "[DONE]") {
                  const parsedEv = object(parseProviderJson(ev.data));
                  const matchedErr = evaluateErrorRules(
                    200,
                    parsedEv,
                    prepared.mergedOverrides.error_rules,
                  );
                  if (matchedErr) {
                    recordAttemptFailure(
                      "upstream_stream_error",
                      matchedErr.retry_kind,
                      describeFailure?.(parsedEv),
                    );
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
                if (describeFailure) translating = ev;
                const translated = translator.consume(ev);
                translating = undefined;
                if (translated.chunks.length && ev.event !== "error") confirmStreamProbe();
                const isTerminalChunk = translated.terminal || translator.holdTerminal;
                for (const chunk of translated.chunks) {
                  await handleStreamChunk(chunk, isTerminalChunk);
                  if (blockedEarly) break;
                }
                if (blockedEarly) break;
              }
              if (blockedEarly) {
                await reader.cancel().catch(() => undefined);
                break;
              }
            }

            if (clientCancelled || c.req.raw.signal.aborted) {
              await finalizeHold("unknown");
              emitAccessLog(499, "client_aborted");
              return;
            }

            if (!blockedEarly && !bedrockDecoder) {
              const tailEvents = parser.feed(new Uint8Array(), true);
              requestTrace?.observeUpstream(tailEvents, upstreamProtocol);
              for (const ev of tailEvents) {
                if (describeFailure) translating = ev;
                const translated = translator.consume(ev);
                translating = undefined;
                for (const chunk of translated.chunks) {
                  await handleStreamChunk(chunk, true);
                  if (blockedEarly) break;
                }
                if (blockedEarly) break;
              }
            }

            const combinedStreamUsage = mergeUsageSources(translator.usage, streamAliasUsage);

            if (session || responsesModeration) {
              const finalMod = responsesModeration
                ? await responsesModeration.finalize()
                : { ...(await session!.finalize()), frames: [] };
              streamPluginAudit.push(...finalMod.audit);
              if (finalMod.blocked) {
                const classified = classifyPluginBlock(
                  finalMod.outcome_kind,
                  finalMod.http_status,
                  finalMod.safe_message,
                );
                const effectiveChargePolicy = classified.isContentDecision
                  ? finalMod.charge_policy
                  : "PLATFORM_ABSORB";
                if (classified.isContentDecision) {
                  deps.metrics.moderationBlocksTotal++;
                  logModBlocked = true;
                }
                const modUsage = buildModerationUsageFallback(
                  combinedStreamUsage,
                  chars ?? 0,
                  observedOutputChars,
                );
                const commitRes = await commitMeasured({
                  request_id: requestId,
                  project_id: project.id,
                  selected_attempt_id: activeAttemptId,
                  channel_id: cand.channel.id,
                  billing_price_id: candBilling.price.id,
                  cost_version_id: cand.costPrice.id,
                  api_key_id: key.id,
                  pricing_override_id: candBilling.overrideId,
                  discount_numerator: candBilling.discountNumerator,
                  discount_denominator: candBilling.discountDenominator,
                  usage: modUsage,
                  billing_pricing: state.pricingOf(candBilling.price),
                  billing_basis: candBilling.price.basis,
                  ...(chars !== undefined ? { input_characters: String(chars) } : {}),
                  supplier_basis: cand.costPrice.basis,
                  supplier_pricing: state.pricingOf(cand.costPrice),
                  supplier_currency: cand.costPrice.currency,
                  plugin_audit: [...preRequestAudit, ...streamPluginAudit],
                  charge_policy: effectiveChargePolicy,
                  moderation_blocked: true,
                });
                if (!classified.isContentDecision) {
                  deps.metrics.recordPlatformAbsorb(project.id, commitRes.supplier_cost_atoms);
                }
                streamProbeDone = true;
                observeAttemptSuccess(latencyMs);
                emitAccessLog(classified.status, finalMod.rule_code ?? classified.code);
                await controller.enqueue(
                  formatProtocolSseError(
                    operation,
                    classified.status,
                    classified.code,
                    classified.message,
                    requestId,
                    finalMod.rule_code ? { rule_code: finalMod.rule_code } : undefined,
                  ),
                );
                controller.close();
                return;
              }
              for (const frame of finalMod.frames) heldApprovedChunks.push(frame);
              if (finalMod.emit.length > 0) {
                heldApprovedChunks.push(
                  buildTextDeltaSse(
                    clientProtocol,
                    model.name,
                    finalMod.emit,
                    streamId,
                    activeTextBlockIndex >= 0 ? activeTextBlockIndex : 0,
                  ),
                );
              }
            }

            const finalTranslatorChunks = translator.finalize();
            heldTerminalChunks.push(...finalTranslatorChunks);

            const usage = buildUsageForOperation(
              operation,
              candBilling.price,
              cand.costPrice,
              combinedStreamUsage,
            );
            if (!usage) {
              streamFailed = true;
              deps.metrics.recordUpstreamError(cand.channel.name, "missing_usage");
              streamProbeDone = true;
              observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
              await finalizeHold("unknown");
              emitAccessLog(502, "invalid_upstream_usage");
              await controller.enqueue(
                formatProtocolSseError(
                  operation,
                  502,
                  "invalid_upstream_usage",
                  "Stream finished without usage metadata",
                  requestId,
                ),
              );
              controller.close();
              return;
            }

            await commitMeasured({
              request_id: requestId,
              project_id: project.id,
              selected_attempt_id: activeAttemptId,
              channel_id: cand.channel.id,
              billing_price_id: candBilling.price.id,
              cost_version_id: cand.costPrice.id,
              api_key_id: key.id,
              pricing_override_id: candBilling.overrideId,
              discount_numerator: candBilling.discountNumerator,
              discount_denominator: candBilling.discountDenominator,
              usage,
              billing_pricing: state.pricingOf(candBilling.price),
              billing_basis: candBilling.price.basis,
              ...(chars !== undefined ? { input_characters: String(chars) } : {}),
              supplier_basis: cand.costPrice.basis,
              supplier_pricing: state.pricingOf(cand.costPrice),
              supplier_currency: cand.costPrice.currency,
              plugin_audit: [...preRequestAudit, ...streamPluginAudit],
              charge_policy: "CHARGE_CUSTOMER",
              moderation_blocked: false,
            });

            streamProbeDone = true;
            observeAttemptSuccess(latencyMs);
            for (const chunk of heldApprovedChunks) {
              await controller.enqueue(chunk);
            }
            for (const chunk of heldTerminalChunks) {
              await controller.enqueue(responsesModeration?.rewrite(chunk, true) ?? chunk);
            }
            emitAccessLog(200, null);
            controller.close();
          } catch (err) {
            const abortedByClient = clientCancelled || c.req.raw.signal.aborted;
            const code = err instanceof GatewayError ? err.code : "stream_interrupted";
            if (!streamFailed) {
              streamFailed = true;
              await finalizeHold("unknown");
              if (!abortedByClient && !streamSignal.aborted && code !== "ledger_commit_failed") {
                const diagnosticsForFrame =
                  code === "provider_stream_error" && translating
                    ? (describeEventFailure?.(translating.data) ?? describeFailure?.())
                    : undefined;
                recordAttemptFailure(code, channelFailureKind(code), diagnosticsForFrame);
                deps.metrics.recordUpstreamError(cand.channel.name, "stream_interrupted");
                streamProbeDone = true;
                observeAttemptFailure("retryable", undefined, performance.now() - startedAt);
              }
            }
            if (abortedByClient) {
              emitAccessLog(499, "client_aborted");
              return;
            }
            const errStatus = err instanceof GatewayError ? err.status : 502;
            emitAccessLog(errStatus, code);
            try {
              const msg = err instanceof Error ? err.message : "Stream failed";
              await controller.enqueue(
                formatProtocolSseError(operation, errStatus, code, msg, requestId),
              );
              controller.close();
            } catch {
              void 0;
            }
          } finally {
            chunkReader?.dispose();
            if (reader) {
              await reader.cancel().catch(() => undefined);
              reader.releaseLock();
            }
          }
        },
        {
          signal: streamSignal,
          cancel() {
            clientCancelled = true;
            attemptAbort.abort(new Error("client_cancelled"));
          },
          fail: () => finalizeHold("unknown"),
          abortedFrame,
          finish: cleanupStream,
          ...(requestTrace && traceProtocol ? { onEnqueued: requestTrace.observeEnqueued } : {}),
        },
      );

      return respondWithStream(bodyStream);
    }

    releaseAllPendingProbes();
    await finalizeHold(anyUnknownAttempt ? "unknown" : "release");
    throw new GatewayError(lastErrorStatus, lastErrorCode, lastErrorMessage);
  } catch (err) {
    releaseAllPendingProbes();
    const revocation = activePluginSignal?.aborted ? activePluginSignal.reason : undefined;
    if (holdAcquired && !holdFinalized && !holdFinalizeAttempted && !streamOwnsGuard) {
      await finalizeHold(!currentAttemptDispatched && !anyUnknownAttempt ? "release" : "unknown");
    }
    const failure = revocation ?? err;
    const status = failure instanceof GatewayError ? failure.status : 500;
    const code = failure instanceof GatewayError ? failure.code : "internal_error";
    emitAccessLog(status, code);
    return errorJson(c, operation, failure, requestId, trace);
  } finally {
    activeDispatch?.release();
    releaseAllPendingProbes();
    releaseLimiters();
    if (!streamOwnsGuard) {
      releaseOutputAdmission?.();
      releaseOutputAdmission = undefined;
      activeLinkedDispose?.();
      activeLinkedDispose = null;
      releaseLocalProjectSlot?.();
      releaseLocalProjectSlot = null;
      releaseBodyBuffer?.();
      releaseGuard?.();
    }
  }
}
