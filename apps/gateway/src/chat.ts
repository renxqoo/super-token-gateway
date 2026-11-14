import {
  GatewayError,
  dispatchProjectionV1Schema,
  executionRejectionStatus,
  prepBindingV1,
  type ExecutionContextResponse,
  type ExecutionReply,
  type SignedCoreEvidenceEventV1,
} from "@gateway/contracts";
import { CoreRpcError, type createCoreRpcClient } from "@gateway/core-client";
import { canonical } from "@gateway/kernel";
import {
  observeAttempt,
  type AbiResult,
  type AttemptOutcome,
  type createPluginAttempts,
  type PreparedRequest,
} from "@gateway/plugin-host";
import {
  UpstreamPreDispatchError,
  readBounded,
  sha256,
  validateBaseUrl,
  type createTransport,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { createPackageCache } from "./packages.ts";

export type ChatDependencies = {
  config: GatewayConfig;
  core: ReturnType<typeof createCoreRpcClient>;
  packages: ReturnType<typeof createPackageCache>;
  host: ReturnType<typeof createPluginAttempts>;
  transport: ReturnType<typeof createTransport>;
  publish: (events: SignedCoreEvidenceEventV1[]) => Promise<void>;
  grants: ReturnType<typeof createSendRegistry>;
};

export type ChatRequest = {
  requestId: string;
  body: Uint8Array;
  token: string;
  ip: string;
  idempotencyKey: string | undefined;
  signal: AbortSignal;
  drain: AbortSignal;
};

type Attempt = ReturnType<ReturnType<typeof createPluginAttempts>["create"]>;
type Bridge = ReturnType<typeof observeAttempt>;
type Facts = {
  model: string;
  stream: boolean;
  fingerprint: string;
  deadlineAt: string;
  requestId: string;
};
type Candidate = ExecutionContextResponse["candidates"][number];
type Plan = {
  attempt: Attempt;
  attemptId: string;
  prepared: PreparedRequest;
  outbound: Uint8Array;
  url: URL;
  headers: Record<string, string>;
  dispatch: Record<string, unknown> & { dispatched_digest: string; prep_session_id: string };
};
type Permit = { attemptId: string; permitEpoch: string };
type Admitted = Plan & Permit & { begin: Record<string, string> };
type CloseReason = "COMPLETED" | "FAILED" | "CANCELLED" | "DRAINED";
type Ending =
  | { kind: "final"; outcome: AttemptOutcome; reason: CloseReason }
  | { kind: "retry"; outcome: Omit<AttemptOutcome, "request_state"> };
type Outcome = { response: Response } | { retry: GatewayError };

const retryableStatuses = new Set([401, 403, 404, 408, 409, 429, 500, 502, 503, 504, 529]);
const maxOrdinal = 5;

export function createSendRegistry() {
  const live = new Map<string, { consumed: boolean; abort: AbortController }>();
  let closed = false;
  return {
    open(attemptId: string, abort: AbortController) {
      if (closed) throw new GatewayError(503, "draining", "Gateway is draining");
      live.set(attemptId, { consumed: false, abort });
    },
    consume(attemptId: string) {
      const entry = live.get(attemptId);
      if (closed || !entry || entry.consumed || entry.abort.signal.aborted) return false;
      entry.consumed = true;
      return true;
    },
    release(attemptId: string) {
      live.delete(attemptId);
    },
    close() {
      closed = true;
      for (const entry of live.values()) entry.abort.abort();
    },
    size: () => live.size,
  };
}

function coreFailure(error: unknown): GatewayError {
  if (error instanceof GatewayError) return error;
  if (error instanceof CoreRpcError) {
    const code = error.detail.code;
    if (code === "customer_forbidden" || code === "context_invalid")
      return new GatewayError(403, "permission_denied", "The API key may not use this model");
    if (error.detail.command_state === "REJECTED")
      return new GatewayError(
        executionRejectionStatus(code),
        code,
        "The request was rejected by admission policy",
      );
    if (code === "quota_unavailable" || code === "internal_capacity")
      return new GatewayError(429, "rate_limited", "Admission capacity is exhausted");
    if (code === "no_route")
      return new GatewayError(
        503,
        "no_route",
        "No healthy provider supports this model and operation",
      );
  }
  return new GatewayError(503, "core_unavailable", "Execution authority is unavailable");
}

function failure(result: AbiResult): GatewayError | undefined {
  if (result.kind !== "failure") return undefined;
  return result.status === 502
    ? new GatewayError(502, "upstream_invalid_response", "The upstream response was invalid")
    : new GatewayError(result.status, result.code, "The request cannot be represented upstream");
}

function requestFacts(request: ChatRequest, totalTimeoutMs: number): Facts {
  const body = request.body;
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
  } catch {
    throw new GatewayError(400, "invalid_json", "Request body must be JSON");
  }
  const value = parsed as { model?: unknown; stream?: unknown };
  if (typeof value !== "object" || value === null || typeof value.model !== "string")
    throw new GatewayError(400, "invalid_request", "A model is required");
  return {
    model: value.model,
    stream: value.stream === true,
    fingerprint: sha256(body),
    deadlineAt: new Date(Date.now() + totalTimeoutMs).toISOString(),
    requestId: request.requestId,
  };
}

async function openContext(deps: ChatDependencies, request: ChatRequest, facts: Facts) {
  try {
    return (await deps.core.execution(
      "execution_context_v1",
      {
        customer_token: request.token,
        ip: request.ip,
        operation: "chat",
        model: facts.model,
        fingerprint: facts.fingerprint,
      },
      { deadlineAt: facts.deadlineAt },
    )) as ExecutionContextResponse;
  } catch (error) {
    throw coreFailure(error);
  }
}

async function plan(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  context: ExecutionContextResponse,
  candidate: Candidate,
): Promise<Plan> {
  const pkg = await deps.packages.get(candidate.ref.package_digest, facts.deadlineAt).catch(() => {
    throw new GatewayError(503, "plugin_unavailable", "The route's plugin is unavailable");
  });
  const attemptId = crypto.randomUUID();
  const attempt = deps.host.create(pkg, {
    tenant: context.project_id,
    attempt: attemptId,
    incarnation: context.owner.incarnation_id,
    config: candidate.config,
    deadlineAt: Date.parse(facts.deadlineAt),
  });
  try {
    const transformed = await attempt.transformJSON(
      "prepare",
      { protocol: "chat", operation: "chat", model: facts.model },
      request.body,
      request.signal,
    );
    const rejected = failure(transformed.result);
    if (rejected) throw rejected;
    if (transformed.result.kind !== "prepared" || !transformed.body)
      throw new GatewayError(502, "plugin_invalid", "The route's plugin did not prepare a request");
    const prepared = transformed.result.request,
      outbound = transformed.body;
    let url: URL;
    try {
      const origin = validateBaseUrl(
        candidate.origin,
        deps.config.allow_private_upstream,
        deps.config.private_upstream_cidrs,
      );
      url = new URL(prepared.path, origin);
      if (url.origin !== origin.origin) throw new Error("origin");
    } catch {
      throw new GatewayError(502, "unsafe_upstream", "The upstream address is not permitted");
    }
    const headers = { ...prepared.headers, "content-type": "application/json" };
    const manifest = {
      version: 1,
      operation: prepared.manifest.operation,
      model: prepared.manifest.model,
      upstream_model: prepared.manifest.upstream_model,
      stream: prepared.manifest.stream,
      input_bytes: prepared.manifest.input_bytes,
      quantities: {},
      resource_ids: [],
      ceilings: prepared.manifest.ceilings,
    };
    const projection = dispatchProjectionV1Schema.parse({
      version: 1,
      proposed_request_id: facts.requestId,
      proposed_attempt_id: attemptId,
      origin: candidate.origin,
      method: prepared.method,
      operation: prepared.manifest.operation,
      request_target_sha256: sha256(url.pathname + url.search),
      final_headers_digest: sha256(canonical(headers)),
      final_body_sha256: sha256(outbound),
      candidate_ref: candidate.ref,
      auth: prepared.auth,
      credential_ciphertext_sha256: candidate.credential_ciphertext_sha256,
      billable_manifest_sha256: sha256(
        canonical(["stg.internal.billable-manifest", "1", manifest]),
      ),
      deadline_at: facts.deadlineAt,
    });
    const prepSessionId = crypto.randomUUID();
    return {
      attempt,
      attemptId,
      prepared,
      outbound,
      url,
      headers,
      dispatch: {
        context_id: context.context_id,
        dispatch_projection: projection,
        dispatched_digest: sha256(canonical(["stg.internal.dispatch-projection", "1", projection])),
        manifest,
        prep_session_id: prepSessionId,
        prep_binding_digest: sha256(
          canonical([
            "stg.internal.prep-binding",
            "1",
            prepBindingV1({
              owner: context.owner,
              project_id: context.project_id,
              projection,
              prep_session_id: prepSessionId,
              host_config_sha256: sha256(canonical(candidate.config)),
            }),
          ]),
        ),
      },
    };
  } catch (error) {
    attempt.dispose();
    throw error;
  }
}

async function reserve(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  context: ExecutionContextResponse,
  planned: Plan,
  previous: { permit: Permit; ordinal: number } | undefined,
): Promise<Admitted> {
  let reply: ExecutionReply;
  const commandId = crypto.randomUUID();
  try {
    reply = (await (previous
      ? deps.core.execution(
          "execution_next_v1",
          {
            ...planned.dispatch,
            request_id: facts.requestId,
            previous_attempt_id: previous.permit.attemptId,
            expected_ordinal: previous.ordinal + 1,
            visible_output: false,
          },
          { commandId, deadlineAt: facts.deadlineAt },
        )
      : deps.core.execution(
          "execution_admit_v1",
          {
            ...planned.dispatch,
            project_id: context.project_id,
            key_id: context.key_id,
            intent_digest: request.idempotencyKey
              ? sha256(canonical(["stg.public.intent", "1", request.idempotencyKey]))
              : null,
            fingerprint: facts.fingerprint,
            customer_currency: context.customer_currency,
          },
          { commandId, deadlineAt: facts.deadlineAt },
        ))) as ExecutionReply;
  } catch (error) {
    planned.attempt.dispose();
    if (
      !(error instanceof CoreRpcError) ||
      (error.detail.command_state === "UNKNOWN" && error.detail.retryability !== "NONE")
    ) {
      const receipt = await queryCommand(
        deps,
        previous ? "execution_next_v1" : "execution_admit_v1",
        commandId,
      );
      if (receipt?.command_state === "COMMITTED" && receipt.state === "RESERVED")
        await abortBeforeSend(
          deps,
          { attemptId: planned.attemptId, permitEpoch: receipt.permit_epoch ?? "1" },
          "PREPARE_DISPOSED",
        );
    }
    throw coreFailure(error);
  }
  if (reply.receipt.state !== "RESERVED" || !reply.receipt.authorization_plan_digest) {
    planned.attempt.dispose();
    throw new GatewayError(503, "core_unavailable", "Execution authority is unavailable");
  }
  const permitEpoch = reply.receipt.permit_epoch ?? "1";
  return {
    ...planned,
    permitEpoch,
    begin: {
      attempt_id: planned.attemptId,
      permit_epoch: permitEpoch,
      authorization_plan_digest: reply.receipt.authorization_plan_digest,
      dispatched_digest: planned.dispatch.dispatched_digest,
      prep_session_id: planned.dispatch.prep_session_id,
      prep_binding_digest: planned.dispatch.prep_binding_digest as string,
    },
  };
}

function shortDeadline() {
  return { deadlineAt: new Date(Date.now() + 5000).toISOString() };
}

async function finalCommand(
  deps: ChatDependencies,
  kind: "execution_abort_v1" | "execution_quota_v1",
  data: Record<string, unknown>,
) {
  const options = { commandId: crypto.randomUUID(), ...shortDeadline() },
    end = Date.parse(options.deadlineAt) - 250;
  for (let delay = 25; ; delay = Math.min(delay * 2, 400)) {
    try {
      return await deps.core.execution(kind, data, options);
    } catch (error) {
      if (error instanceof CoreRpcError && error.detail.retryability === "NONE") return;
      if (Date.now() + delay >= end) return;
      await Bun.sleep(delay);
    }
  }
}

async function abortBeforeSend(
  deps: ChatDependencies,
  permit: Permit,
  reason: "CANCELLED" | "DEADLINE" | "PREPARE_DISPOSED",
) {
  await finalCommand(deps, "execution_abort_v1", {
    attempt_id: permit.attemptId,
    permit_epoch: permit.permitEpoch,
    reason,
    close_request: true,
  });
}

async function closeRequest(deps: ChatDependencies, permit: Permit, reason: CloseReason) {
  await finalCommand(deps, "execution_quota_v1", {
    attempt_id: permit.attemptId,
    permit_epoch: permit.permitEpoch,
    action: "CLOSE",
    reason,
  });
}

function authorize(headers: Record<string, string>, reply: ExecutionReply) {
  const material = reply.transient_send!.auth_material;
  if (material.type === "bearer") return { ...headers, authorization: `Bearer ${material.value}` };
  if (material.type === "api-key") return { ...headers, [material.header]: material.value };
  return headers;
}

function upstreamRejection(status: number) {
  return new GatewayError(
    status === 429 ? 429 : status >= 500 ? 502 : 400,
    "upstream_rejected",
    "Provider rejected the request",
  );
}

function halt(request: ChatRequest) {
  const drained = request.drain.aborted,
    cancelled = !drained && request.signal.aborted;
  return {
    halted: drained || cancelled,
    requestState: cancelled ? ("CANCELLED" as const) : ("FAILED" as const),
    reason: drained
      ? ("DRAINED" as const)
      : cancelled
        ? ("CANCELLED" as const)
        : ("FAILED" as const),
    error(fallback: GatewayError) {
      if (drained) return new GatewayError(503, "draining", "Gateway is draining");
      if (cancelled) return new GatewayError(499, "cancelled", "Request cancelled");
      return fallback;
    },
  };
}

async function queryCommand(
  deps: ChatDependencies,
  kind: "execution_admit_v1" | "execution_next_v1" | "execution_begin_v1",
  commandId: string,
) {
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      return (
        (await deps.core.execution(
          "execution_query_v1",
          { kind, command_id: commandId },
          shortDeadline(),
        )) as ExecutionReply
      ).receipt;
    } catch {
      await Bun.sleep(200 * (attempt + 1));
    }
  }
  return undefined;
}

async function resolveUnknownBegin(deps: ChatDependencies, commandId: string) {
  const receipt = await queryCommand(deps, "execution_begin_v1", commandId);
  if (!receipt) return "unknown" as const;
  if (receipt.state === "SEND_GRANTED") return "granted" as const;
  if (
    receipt.command_state === "NOT_FOUND" ||
    receipt.command_state === "REJECTED" ||
    receipt.state === "RESERVED"
  )
    return "absent" as const;
  return "unknown" as const;
}

function renewQuota(deps: ChatDependencies, permit: Permit) {
  const timer = setInterval(() => {
    void deps.core
      .execution(
        "execution_quota_v1",
        { attempt_id: permit.attemptId, permit_epoch: permit.permitEpoch, action: "RENEW" },
        shortDeadline(),
      )
      .catch(() => clearInterval(timer));
  }, deps.config.quota_renew_ms);
  return () => clearInterval(timer);
}

async function dispatch(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  admitted: Admitted,
): Promise<Outcome> {
  const local = new AbortController();
  const signal = AbortSignal.any([
    request.signal,
    request.drain,
    local.signal,
    AbortSignal.timeout(Math.max(1, Date.parse(facts.deadlineAt) - Date.now())),
  ]);
  try {
    deps.grants.open(admitted.attemptId, local);
  } catch (error) {
    admitted.attempt.dispose();
    await abortBeforeSend(deps, admitted, "PREPARE_DISPOSED");
    throw error;
  }
  const unavailable = new GatewayError(
    503,
    "core_unavailable",
    "Execution authority is unavailable",
  );
  const bridge = observeAttempt(admitted.attempt, {
    attemptId: admitted.attemptId,
    permitEpoch: admitted.permitEpoch,
    record: (evidence) => deps.publish([deps.core.signEvidence(evidence)]),
  });
  const neverSent = async (outcome: Parameters<Bridge["notExecuted"]>[0]) => {
    deps.grants.release(admitted.attemptId);
    await bridge.notExecuted(outcome).catch(() => undefined);
    bridge.dispose();
  };
  const beginCommand = crypto.randomUUID();
  let reply: ExecutionReply;
  try {
    if (signal.aborted) throw halt(request).error(unavailable);
    reply = (await deps.core.execution("execution_begin_v1", admitted.begin, {
      commandId: beginCommand,
      deadlineAt: facts.deadlineAt,
    })) as ExecutionReply;
  } catch (error) {
    if (error instanceof CoreRpcError && error.detail.command_state === "UNKNOWN") {
      const resolved = await resolveUnknownBegin(deps, beginCommand);
      if (resolved === "granted") {
        await neverSent({ request_state: "FAILED", error_code: "begin_outcome_unknown" });
        await closeRequest(deps, admitted, "FAILED");
      } else {
        deps.grants.release(admitted.attemptId);
        bridge.dispose();
        if (resolved === "absent") await abortBeforeSend(deps, admitted, "PREPARE_DISPOSED");
      }
      throw unavailable;
    }
    deps.grants.release(admitted.attemptId);
    bridge.dispose();
    const stop = halt(request);
    await abortBeforeSend(
      deps,
      admitted,
      stop.requestState === "CANCELLED"
        ? "CANCELLED"
        : Date.now() >= Date.parse(facts.deadlineAt)
          ? "DEADLINE"
          : "PREPARE_DISPOSED",
    );
    throw error instanceof GatewayError ? error : coreFailure(error);
  }
  if (!reply.transient_send || !deps.grants.consume(admitted.attemptId)) {
    await neverSent({ request_state: "FAILED", error_code: "send_not_authorized" });
    await closeRequest(deps, admitted, halt(request).reason);
    throw halt(request).error(unavailable);
  }
  const stopRenewal = renewQuota(deps, admitted);
  const end = async (ending: Ending) => {
    stopRenewal();
    deps.grants.release(admitted.attemptId);
    try {
      await bridge.finish(ending.outcome);
    } finally {
      bridge.dispose();
      if (ending.kind === "final") await closeRequest(deps, admitted, ending.reason);
    }
  };
  const finalFailure = async (
    outcome: Omit<AttemptOutcome, "request_state">,
    error: GatewayError,
  ): Promise<never> => {
    const stop = halt(request);
    await end({
      kind: "final",
      outcome: { ...outcome, request_state: stop.requestState },
      reason: stop.reason,
    }).catch(() => undefined);
    throw stop.error(error);
  };
  let response: Response;
  try {
    response = await deps.transport.fetch(
      admitted.url,
      {
        method: admitted.prepared.method,
        headers: authorize(admitted.headers, reply),
        body: admitted.outbound.slice(),
        signal,
      },
      deps.config.allow_private_upstream,
      deps.config.private_upstream_cidrs,
    );
  } catch (error) {
    const unreachable = new GatewayError(
      502,
      "upstream_unavailable",
      "The upstream could not be reached",
    );
    if (error instanceof UpstreamPreDispatchError) {
      stopRenewal();
      const stop = halt(request),
        final = stop.halted || signal.aborted || error.code === "unsafe_upstream";
      await neverSent({
        ...(final ? { request_state: stop.requestState } : {}),
        error_code:
          error.code === "unsafe_upstream" ? "unsafe_upstream" : "upstream_not_dispatched",
      });
      if (!final) return { retry: unreachable };
      await closeRequest(deps, admitted, stop.reason);
      throw stop.error(
        error.code === "unsafe_upstream"
          ? new GatewayError(502, "unsafe_upstream", "The upstream address is not permitted")
          : unreachable,
      );
    }
    const interrupted = { attempt_state: "UNKNOWN" as const, error_code: "upstream_interrupted" };
    if (signal.aborted) return finalFailure(interrupted, unreachable);
    await end({ kind: "retry", outcome: interrupted }).catch(() => undefined);
    return { retry: unreachable };
  }
  if (!response.ok) {
    await response.body?.cancel().catch(() => undefined);
    const rejected = {
      attempt_state: [400, 401, 403, 404, 422].includes(response.status)
        ? ("FAILED" as const)
        : ("UNKNOWN" as const),
      error_code: `upstream_http_${response.status}`,
    };
    if (!retryableStatuses.has(response.status) || signal.aborted)
      return finalFailure(rejected, upstreamRejection(response.status));
    await end({ kind: "retry", outcome: rejected }).catch(() => undefined);
    return { retry: upstreamRejection(response.status) };
  }
  return {
    response: facts.stream
      ? relaySSE(facts, admitted, bridge, response, signal, request, end)
      : await relayJSON(deps, facts, bridge, response, signal, finalFailure, end),
  };
}

export async function executeChat(deps: ChatDependencies, request: ChatRequest) {
  const facts = requestFacts(request, deps.config.total_timeout_ms);
  const external = { ...request, signal: AbortSignal.any([request.signal, request.drain]) };
  const tried = new Set<string>();
  let previous: { permit: Permit; ordinal: number } | undefined;
  let lastError: GatewayError | undefined;
  for (let ordinal = 1; ordinal <= maxOrdinal; ordinal++) {
    let admitted: Admitted;
    try {
      if (external.signal.aborted)
        throw halt(request).error(new GatewayError(499, "cancelled", "Request cancelled"));
      const context = await openContext(deps, request, facts);
      const candidate = context.candidates.find((c) => !tried.has(c.ref.channel_id));
      if (!candidate) break;
      tried.add(candidate.ref.channel_id);
      const planned = await plan(deps, external, facts, context, candidate);
      admitted = await reserve(deps, request, facts, context, planned, previous);
    } catch (error) {
      if (!previous) throw error;
      await closeRequest(deps, previous.permit, halt(request).reason);
      throw halt(request).error(lastError ?? coreFailure(error));
    }
    const outcome = await dispatch(deps, request, facts, admitted);
    if ("response" in outcome) return outcome.response;
    lastError = outcome.retry;
    previous = { permit: admitted, ordinal };
  }
  if (!previous) throw new GatewayError(404, "model_not_found", "No route serves this model");
  await closeRequest(deps, previous.permit, "FAILED");
  throw lastError!;
}

async function relayJSON(
  deps: ChatDependencies,
  facts: Facts,
  bridge: Bridge,
  response: Response,
  signal: AbortSignal,
  finalFailure: (
    outcome: Omit<AttemptOutcome, "request_state">,
    error: GatewayError,
  ) => Promise<never>,
  end: (ending: Ending) => Promise<void>,
) {
  const invalid = new GatewayError(
    502,
    "upstream_invalid_response",
    "The upstream response was invalid",
  );
  let transformed: Awaited<ReturnType<Bridge["transformJSON"]>>;
  try {
    const bytes = await readBounded(response.body, deps.config.max_response_bytes, signal);
    transformed = await bridge.transformJSON("response", { model: facts.model }, bytes, signal);
  } catch (error) {
    if (error instanceof GatewayError && error.code === "usage_queue_unavailable")
      return finalFailure({ attempt_state: "UNKNOWN", error_code: "evidence_unavailable" }, error);
    return finalFailure({ attempt_state: "UNKNOWN", error_code: "upstream_read_failed" }, invalid);
  }
  const result = transformed.result;
  if (result.kind !== "output" || !result.terminal || !transformed.body)
    return finalFailure(
      { attempt_state: "UNKNOWN", error_code: "upstream_invalid" },
      failure(result) ?? invalid,
    );
  try {
    await end({
      kind: "final",
      outcome: { attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" },
      reason: "COMPLETED",
    });
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError(503, "evidence_unavailable", "Usage evidence could not be recorded");
  }
  return new Response(transformed.body.slice(), {
    status: 200,
    headers: { "content-type": "application/json", "x-request-id": facts.requestId },
  });
}

function relaySSE(
  facts: Facts,
  admitted: Admitted,
  bridge: Bridge,
  response: Response,
  signal: AbortSignal,
  request: ChatRequest,
  end: (ending: Ending) => Promise<void>,
) {
  const encoder = new TextEncoder();
  async function* frames(): AsyncGenerator<Uint8Array> {
    let terminal: { bytes: string } | undefined;
    let ended = false,
      delivered = false,
      failed = false;
    const reader = response.body!.getReader();
    const cancel = () => void reader.cancel().catch(() => undefined);
    signal.addEventListener("abort", cancel, { once: true });
    const settle = function* (result: AbiResult) {
      if (result.kind === "failure") throw new Error(result.code);
      if (result.kind !== "output") throw new Error("plugin_output");
      for (const chunk of result.chunks) {
        const bytes = Buffer.from(chunk, "base64");
        if (bytes.length) {
          delivered = true;
          yield bytes;
        }
      }
      const id = (result as { output_id?: string }).output_id;
      if (id) admitted.attempt.releaseOutput(id);
      if (result.terminal) terminal = result.terminal;
      return result.needs_input || Boolean(result.terminal);
    };
    try {
      await bridge.invoke("stream_open", encoder.encode(JSON.stringify({ model: facts.model })));
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        for (let offset = 0; offset < next.value.length; offset += 64 << 10) {
          let waiting = yield* settle(
            await bridge.invoke("stream_feed", next.value.subarray(offset, offset + (64 << 10))),
          );
          while (!waiting) waiting = yield* settle(await bridge.invoke("stream_drain"));
        }
      }
      await bridge.invoke("stream_finish");
      if (!terminal) throw new Error("stream_truncated");
      ended = true;
      await end({
        kind: "final",
        outcome: { attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" },
        reason: "COMPLETED",
      });
      const held = Buffer.from(terminal.bytes, "base64");
      if (held.length) yield held;
    } catch {
      failed = true;
    } finally {
      signal.removeEventListener("abort", cancel);
      cancel();
      if (!ended) {
        const stop = halt(request);
        await end({
          kind: "final",
          outcome: {
            attempt_state: delivered ? "PARTIAL" : "UNKNOWN",
            request_state: stop.requestState,
            error_code:
              stop.reason === "CANCELLED"
                ? "client_cancelled"
                : stop.reason === "DRAINED"
                  ? "gateway_draining"
                  : "stream_interrupted",
          },
          reason: stop.reason,
        }).catch(() => undefined);
      }
    }
    if (failed && halt(request).requestState !== "CANCELLED")
      yield encoder.encode(
        `data: ${JSON.stringify({
          error: {
            type: "gateway_stream_error",
            message: "Stream interrupted; usage may be partial",
            request_id: facts.requestId,
          },
        })}\n\n`,
      );
  }
  const iterator = frames();
  return new Response(
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        const next = await iterator.next();
        if (next.done) controller.close();
        else controller.enqueue(next.value);
      },
      async cancel() {
        await iterator.return(undefined);
      },
    }),
    {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        "x-request-id": facts.requestId,
      },
    },
  );
}
