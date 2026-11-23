import { GatewayError, object } from "@gateway/contracts";
import type {
  AsyncTaskFailureCategory,
  AsyncTaskProviderConfig,
  AsyncTaskRecord,
  AsyncTaskRefundEligibility,
  ChannelRecord,
  ConfigSnapshot,
  createAsyncTasksRepository,
  createLedgerEngine,
  createLedgerQueries,
} from "@gateway/ledger";
import {
  resolvePinnedProviderPlugin,
  resolveProviderPlugin,
  sanitizeProviderHeaders,
} from "@gateway/plugin-engine";
import { formatProtocolError, parseProviderJson } from "@gateway/protocol";
import {
  applyAuthToRequest,
  readBounded,
  type ClusterCoordinator,
  type createTransport,
  type VertexTokens,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import { authenticateRequest, type InferenceContext } from "./proxy.ts";
import type { ProxyRuntimeState } from "./proxy-state.ts";
import type { SnapshotManager } from "./snapshot.ts";

export type AsyncTaskHandlerDeps = {
  config: GatewayConfig;
  snapshot: SnapshotManager;
  asyncTasks: ReturnType<typeof createAsyncTasksRepository>;
  controlLedger: ReturnType<typeof createLedgerEngine>;
  queries: ReturnType<typeof createLedgerQueries>;
  cluster?: ClusterCoordinator;
  transport: ReturnType<typeof createTransport>;
  vertexTokens: VertexTokens;
  state?: ProxyRuntimeState;
};

const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });
const FORBIDDEN_SEGMENTS = new Set(["__proto__", "constructor", "prototype"]);
const inFlightPollsByRepo = new WeakMap<object, Map<string, Promise<AsyncTaskRecord>>>();

function getPollCoalescer(repo: object): Map<string, Promise<AsyncTaskRecord>> {
  let map = inFlightPollsByRepo.get(repo);
  if (!map) {
    map = new Map();
    inFlightPollsByRepo.set(repo, map);
  }
  return map;
}

export function extractByDotPath(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split(".");
  let cur: unknown = obj;
  for (const part of parts) {
    if (!part || FORBIDDEN_SEGMENTS.has(part)) return undefined;
    if (cur === null || cur === undefined || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[part];
  }
  return cur;
}

export function getAsyncTaskConfig(
  snap: ConfigSnapshot,
  channel: ChannelRecord,
): AsyncTaskProviderConfig | null {
  const resolved = channel.provider_plugin_id
    ? resolvePinnedProviderPlugin(channel.provider_plugin_id, snap)
    : resolveProviderPlugin(channel.kind, null, snap);
  if (resolved?.kind === "DECLARATIVE" && resolved.config.async_task_config) {
    return resolved.config.async_task_config;
  }
  return null;
}

export function extractUpstreamTaskId(
  body: Record<string, unknown>,
  config: AsyncTaskProviderConfig | null,
): string | null {
  const raw = config?.task_id_path
    ? extractByDotPath(body, config.task_id_path)
    : (body.task_id ?? body.id ?? extractByDotPath(body, "data.task_id"));
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    return trimmed.length > 0 && trimmed.length <= 256 ? trimmed : null;
  }
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return String(raw);
  }
  return null;
}

function buildAsyncQueryUrl(
  baseUrl: string,
  queryPath: string,
  upstreamTaskId: string,
  queryParam?: string,
): URL {
  const base = new URL(baseUrl);
  const normalizedBase = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
  const interpolated = queryPath.replaceAll("${task_id}", encodeURIComponent(upstreamTaskId));
  const suffix = interpolated.startsWith("/") ? interpolated : `/${interpolated}`;
  const url = new URL(`${normalizedBase}${suffix}`);
  if (url.origin !== base.origin) {
    throw new GatewayError(502, "invalid_provider_config", "Async query URL escaped base origin");
  }
  if (queryParam) {
    url.searchParams.set(queryParam, upstreamTaskId);
  }
  return url;
}

function formatTaskPayload(task: AsyncTaskRecord) {
  return {
    id: task.id,
    object: "video.task",
    request_id: task.request_id,
    charge_id: task.charge_id,
    model: task.model,
    operation: task.operation,
    upstream_task_id: task.upstream_task_id,
    status: task.status,
    user_cancelled: task.user_cancelled,
    upstream_billed: task.upstream_billed,
    failure_category: task.failure_category,
    refund_eligibility: task.refund_eligibility,
    refund_id: task.refund_id,
    result: task.result_json,
    created_at: task.created_at,
    updated_at: task.updated_at,
    completed_at: task.completed_at,
  };
}

function isTruthyBilled(value: unknown): boolean {
  if (value === true) return true;
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    return trimmed !== "" && trimmed !== "0" && trimmed !== "false" && trimmed !== "no";
  }
  return false;
}

function taskErrorResponse(c: InferenceContext, err: unknown, requestId: string): Response {
  const gw =
    err instanceof GatewayError
      ? err
      : new GatewayError(500, "internal_error", "Internal gateway error");
  return c.json(
    formatProtocolError("videos", gw.status, gw.code, gw.message, requestId),
    gw.status as 400,
    { "x-request-id": requestId },
  );
}

async function pollUpstreamTaskOnce(
  deps: AsyncTaskHandlerDeps,
  snap: ConfigSnapshot,
  task: AsyncTaskRecord,
): Promise<AsyncTaskRecord> {
  const channel = snap.channels.get(task.channel_id);
  if (!channel || !channel.enabled) return task;

  const resolved = channel.provider_plugin_id
    ? resolvePinnedProviderPlugin(channel.provider_plugin_id, snap)
    : resolveProviderPlugin(channel.kind, null, snap);
  if (!resolved) return task;
  if (resolved.source !== "builtin" && snap.revokedPlugins.has(resolved.plugin_id)) {
    throw new GatewayError(503, "plugin_revoked", "Provider plugin has been revoked");
  }
  if (resolved.kind !== "DECLARATIVE" || !resolved.config.async_task_config) {
    return task;
  }

  const asyncCfg = resolved.config.async_task_config;
  const url = buildAsyncQueryUrl(
    channel.base_url,
    asyncCfg.query_path,
    task.upstream_task_id,
    asyncCfg.query_param,
  );
  const headers = new Headers({ accept: "application/json" });
  const extraHeaders = sanitizeProviderHeaders({
    ...resolved.config.extra_headers,
    ...channel.adapter_config.extra_headers,
  });
  for (const [k, v] of Object.entries(extraHeaders)) {
    headers.set(k, v);
  }
  const secret = deps.snapshot.decryptChannelSecret(channel);
  const signal = AbortSignal.timeout(deps.config.attemptTimeoutMs);
  await applyAuthToRequest({
    auth: resolved.config.auth,
    secret,
    url,
    headers,
    body: new Uint8Array(0),
    vertexTokens: deps.vertexTokens,
    signal,
  });

  const resp = await deps.transport.fetch(
    url,
    { method: "GET", headers, signal },
    deps.config.allowPrivateUpstreams,
    deps.config.privateUpstreamCidrs,
  );
  if (!resp.ok) {
    await resp.body?.cancel().catch(() => undefined);
    throw new GatewayError(
      502,
      "upstream_poll_failed",
      `Upstream task status poll returned HTTP ${resp.status}`,
    );
  }
  const rawBytes = await readBounded(resp.body, deps.config.maxBodyBytes, signal);
  const upstreamJson = object(parseProviderJson(utf8FatalDecoder.decode(rawBytes)));

  const rawStatus = extractByDotPath(upstreamJson, asyncCfg.status_path ?? "status");
  const statusLower = typeof rawStatus === "string" ? rawStatus.trim().toLowerCase() : "";
  const succeededSet = new Set(
    (asyncCfg.succeeded_values ?? ["Success", "SUCCEEDED", "succeeded", "completed"]).map((v) =>
      v.toLowerCase(),
    ),
  );
  const failedSet = new Set(
    (asyncCfg.failed_values ?? ["Fail", "FAILED", "failed", "error"]).map((v) => v.toLowerCase()),
  );

  if (succeededSet.has(statusLower)) {
    return (
      (await deps.asyncTasks.updateFromPoll({
        id: task.id,
        status: "SUCCEEDED",
        refund_eligibility: "NOT_APPLICABLE",
        result_json: upstreamJson,
      })) ?? task
    );
  }

  if (failedSet.has(statusLower)) {
    if (task.user_cancelled) {
      return (
        (await deps.asyncTasks.updateFromPoll({
          id: task.id,
          status: "CANCELLED",
          failure_category: "USER_CANCELLED",
          refund_eligibility: "DENIED_USER_CANCELLED",
          result_json: upstreamJson,
        })) ?? task
      );
    }

    const billedVal = asyncCfg.billed_indicator_path
      ? extractByDotPath(upstreamJson, asyncCfg.billed_indicator_path)
      : undefined;
    if (isTruthyBilled(billedVal)) {
      return (
        (await deps.asyncTasks.updateFromPoll({
          id: task.id,
          status: "FAILED",
          upstream_billed: true,
          failure_category: "UPSTREAM_BILLED",
          refund_eligibility: "DENIED_UPSTREAM_BILLED",
          result_json: upstreamJson,
        })) ?? task
      );
    }

    const errorCodeVal = asyncCfg.error_code_path
      ? extractByDotPath(upstreamJson, asyncCfg.error_code_path)
      : (upstreamJson.error_code ??
        extractByDotPath(upstreamJson, "error.code") ??
        extractByDotPath(upstreamJson, "base_resp.status_code"));
    const codeStr =
      errorCodeVal !== undefined && errorCodeVal !== null ? String(errorCodeVal) : null;

    const nonRefundableSet = new Set(
      (asyncCfg.non_refundable_error_codes ?? []).map((v) => String(v)),
    );
    const refundableSet = new Set(
      (asyncCfg.refundable_system_error_codes ?? []).map((v) => String(v)),
    );

    if (codeStr !== null && nonRefundableSet.has(codeStr)) {
      return (
        (await deps.asyncTasks.updateFromPoll({
          id: task.id,
          status: "FAILED",
          failure_category: "CONTENT_POLICY",
          refund_eligibility: "DENIED_POLICY_OR_CLIENT",
          result_json: upstreamJson,
        })) ?? task
      );
    }

    if (codeStr !== null && refundableSet.has(codeStr) && task.charge_id) {
      const maxPer24h = deps.config.maxAutoRefundsPerProject24h ?? 5;
      const claim = await deps.asyncTasks.claimAutoRefund({
        id: task.id,
        max_per_24h: maxPer24h,
        result_json: upstreamJson,
      });
      if (claim.outcome === "not_found") {
        throw new GatewayError(404, "not_found", "Async task not found");
      }
      if (claim.outcome !== "claimed") return claim.task;

      let refundOpId: string | null = null;
      let zeroCharge = false;
      try {
        const billing = await deps.queries.getRequestBilling(task.request_id);
        const ch = billing.charge;
        if (!ch) {
          return (await deps.asyncTasks.finalizeAutoRefund(task.id, null, true)) ?? claim.task;
        }
        const refundableAtoms = BigInt(ch.customerAtoms) - BigInt(ch.refundedAtoms);
        if (refundableAtoms <= 0n) {
          zeroCharge = true;
        } else {
          const refundRes = await deps.controlLedger.refundCharge({
            charge_id: ch.id,
            external_ref: `auto-refund:async-task:${task.id}`,
            amount_atoms: refundableAtoms.toString(),
            reason: `Verified upstream system failure (${codeStr})`,
          });
          refundOpId = refundRes.operation_id;
        }
      } catch {
        return (await deps.asyncTasks.finalizeAutoRefund(task.id, null, true)) ?? claim.task;
      }

      if (zeroCharge) {
        return (await deps.asyncTasks.markZeroChargeNotApplicable(task.id)) ?? claim.task;
      }
      return (
        (await deps.asyncTasks
          .finalizeAutoRefund(task.id, refundOpId, false)
          .catch(() => claim.task)) ?? claim.task
      );
    }

    const category: AsyncTaskFailureCategory = "UNKNOWN";
    const eligibility: AsyncTaskRefundEligibility = "MANUAL_REVIEW";
    return (
      (await deps.asyncTasks.updateFromPoll({
        id: task.id,
        status: "FAILED",
        failure_category: category,
        refund_eligibility: eligibility,
        result_json: upstreamJson,
      })) ?? task
    );
  }

  return (
    (await deps.asyncTasks.updateFromPoll({
      id: task.id,
      status: "PROCESSING",
      result_json: upstreamJson,
    })) ?? task
  );
}

export async function handleGetAsyncTask(
  c: InferenceContext,
  deps: AsyncTaskHandlerDeps,
  taskId: string,
  peerAddress?: string | (() => string | undefined),
): Promise<Response> {
  const requestId = crypto.randomUUID();
  let releaseLimit: (() => void) | null = null;
  try {
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
      "videos",
      peerAddress,
      deps.state,
    );
    const task = await deps.asyncTasks.getTask(taskId);
    if (!task || task.project_id !== project.id) {
      throw new GatewayError(404, "not_found", "Async task not found");
    }
    if (key.models.length > 0 && !key.models.includes(task.model)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    if (task.status === "SUCCEEDED" || task.status === "FAILED" || task.status === "CANCELLED") {
      return c.json(formatTaskPayload(task), 200, { "x-request-id": requestId });
    }

    if (deps.cluster) {
      const acq = await deps.cluster.acquireRateLimit(
        `project:${project.id}`,
        { rpm: project.rpm, tpm: project.tpm, concurrency: project.concurrency },
        1,
        deps.config.attemptTimeoutMs,
      );
      if (!acq.ok) {
        if (acq.reason === "unavailable") {
          throw new GatewayError(503, "rate_limit_unavailable", "Shared request quota unavailable");
        }
        throw new GatewayError(429, `rate_limit_${acq.reason}`, "Project rate limit exceeded");
      }
      releaseLimit = acq.release;
    }

    const coalescer = getPollCoalescer(deps.asyncTasks);
    let inflight = coalescer.get(task.id);
    if (!inflight) {
      inflight = pollUpstreamTaskOnce(deps, snap, task).finally(() => {
        if (coalescer.get(task.id) === inflight) coalescer.delete(task.id);
      });
      coalescer.set(task.id, inflight);
    }
    const result = await inflight;
    return c.json(formatTaskPayload(result), 200, { "x-request-id": requestId });
  } catch (err) {
    return taskErrorResponse(c, err, requestId);
  } finally {
    releaseLimit?.();
  }
}

export async function handleCancelAsyncTask(
  c: InferenceContext,
  deps: AsyncTaskHandlerDeps,
  taskId: string,
  peerAddress?: string | (() => string | undefined),
): Promise<Response> {
  const requestId = crypto.randomUUID();
  try {
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
      "videos",
      peerAddress,
      deps.state,
    );
    const existing = await deps.asyncTasks.getTask(taskId);
    if (!existing || existing.project_id !== project.id) {
      throw new GatewayError(404, "not_found", "Async task not found");
    }
    if (key.models.length > 0 && !key.models.includes(existing.model)) {
      throw new GatewayError(403, "model_not_allowed", "API key cannot access this model");
    }
    const updated = await deps.asyncTasks.markUserCancelled(taskId, project.id);
    if (!updated) {
      throw new GatewayError(404, "not_found", "Async task not found");
    }
    return c.json(formatTaskPayload(updated), 200, { "x-request-id": requestId });
  } catch (err) {
    return taskErrorResponse(c, err, requestId);
  }
}
