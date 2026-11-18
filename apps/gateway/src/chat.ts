import type { createTerminalBudget } from "./terminal-budget.ts";
import { verifiedPackageRuntime } from "@gateway/plugin-sdk";
import {
  GatewayError,
  dispatchProjectionV2Schema,
  executionRejectionStatus,
  prepBindingV2,
  requestPolicySchema,
  type ExecutionContextResponse,
  type ExecutionReply,
  type TransientSend,
  type SignedCoreEvidenceEventV1,
  type DispatchProjectionV2,
} from "@gateway/contracts";
import { CoreRpcError, type createCoreRpcClient } from "@gateway/core-client";
import {
  parseClientJson,
  requestCharacters,
  validateRequestPolicy,
} from "@gateway/adapters/protocol";
import { canonical, protocols, type Operation, type Protocol } from "@gateway/kernel";
import {
  observeAttempt,
  PluginHostError,
  rewriteJSON,
  type AbiResult,
  type AttemptOutcome,
  type createPluginAttempts,
  type PreparedRequest,
} from "@gateway/plugin-host";
import {
  UpstreamPreDispatchError,
  signAwsRequest,
  readBounded,
  sha256,
  validateBaseUrl,
  type createTransport,
} from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { createPackageCache } from "./packages.ts";
import type { createGatewayPluginRuntime } from "./plugin-runtime.ts";
import { collectReferences, type Reference } from "./references.ts";
import { rejectUnmeteredTools } from "./tool-policy.ts";
import { idleGuard, stallGuard } from "./stream-deadlines.ts";
import { encodeForm, readForm, type FormFiles } from "./form.ts";
import {
  exposeBatchFiles,
  parseBatchInput,
  prepareBatchRows,
  type BatchInput,
  type BatchRecord,
} from "./batches.ts";
import { requestAsChat, responseFromChat, streamFromChat } from "./protocols.ts";

export type ChatDependencies = {
  config: GatewayConfig;
  core: ReturnType<typeof createCoreRpcClient>;
  packages: ReturnType<typeof createPackageCache>;
  host: Awaited<ReturnType<typeof createGatewayPluginRuntime>>;
  transport: ReturnType<typeof createTransport>;
  publish: (events: SignedCoreEvidenceEventV1[]) => Promise<void>;
  grants: ReturnType<typeof createSendRegistry>;
  terminalBudget: ReturnType<typeof createTerminalBudget>;
};

export type ChatRequest = {
  requestId: string;
  operation: Operation;
  path?: { model: string; stream: boolean };
  body: Uint8Array;
  contentType: string | undefined;
  token: string;
  ip: string;
  idempotencyKey: string | undefined;
  signal: AbortSignal;
  drain: AbortSignal;
};

type Attempt = ReturnType<ReturnType<typeof createPluginAttempts>["create"]>;
type Bridge = ReturnType<typeof observeAttempt>;
type Facts = {
  operation: Operation;
  protocol: Protocol | undefined;
  inputCharacters: number | undefined;
  document: Uint8Array;
  files: FormFiles | undefined;
  model: string;
  stream: boolean;
  fingerprint: string;
  deadlineAt: string;
  requestId: string;
  references: Reference[];
  background: boolean;
  batch: BatchInput | undefined;
  batchEndpoint: string | undefined;
};
type Candidate = ExecutionContextResponse["candidates"][number];
type Plan = {
  converted: Protocol | undefined;
  attempt: Attempt;
  attemptId: string;
  channelId: string;
  prepared: PreparedRequest;
  outbound: Uint8Array;
  url: URL;
  headers: Record<string, string>;
  dispatch: Record<string, unknown> & {
    dispatched_digest: string;
    prep_session_id: string;
    dispatch_projection: DispatchProjectionV2;
  };
  commit: { batch: BatchRecord } | { input_file_id: string } | undefined;
  files: Map<string, string>;
};
type Permit = { attemptId: string; permitEpoch: string };
type HealthOutcome = "SUCCESS" | "FAILURE" | "NOT_SENT";
type Admitted = Plan & Permit & { begin: Record<string, string> };
type CloseReason = "COMPLETED" | "FAILED" | "CANCELLED" | "DRAINED";
type Ending = { outcome: AttemptOutcome; reason: CloseReason; outputBytes?: number };
type TerminalReservation = ReturnType<ChatDependencies["terminalBudget"]["reserve"]>;
type Outcome =
  { response: Response } | { retry: GatewayError; conclude: (last: boolean) => Promise<void> };
type Deadline = "upstream_header_timeout" | "upstream_idle_timeout" | "downstream_stalled";
type Failed = Omit<AttemptOutcome, "request_state">;
type Control = {
  signal: AbortSignal;
  expire: (cause: Deadline) => void;
  cause: () => Deadline | undefined;
  timeoutError: (fallback: GatewayError) => GatewayError;
  end: (ending: Ending) => Promise<void>;
  fail: (outcome: Failed, error: GatewayError) => Promise<never>;
  abandon: (outcome: Failed, error: GatewayError) => Promise<Outcome>;
};

const retryableStatuses = new Set([401, 403, 404, 408, 409, 429, 500, 502, 503, 504, 529]);
const unhealthyStatuses = new Set([401, 403, 408, 409, 429, 500, 502, 503, 504, 529]);
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

export function coreFailure(error: unknown): GatewayError {
  if (error instanceof GatewayError) return error;
  if (error instanceof CoreRpcError) {
    const code = error.detail.code;
    if (code === "resource_not_found")
      return new GatewayError(404, "not_found", "Resource not found");
    if (code === "resource_provider_mismatch")
      return new GatewayError(
        422,
        "resource_provider_mismatch",
        "Resources belong to different providers",
      );
    if (code === "resource_rate_limited")
      return new GatewayError(429, "rate_limited", "Resource request rate limit reached");
    if (code === "customer_forbidden" || code === "context_invalid")
      return new GatewayError(403, "permission_denied", "The API key may not use this model");
    if (error.detail.command_state === "REJECTED")
      return new GatewayError(
        executionRejectionStatus(code),
        code,
        "The request was rejected by admission policy",
      );
    if (
      code === "quota_unavailable" ||
      code === "quota_limited" ||
      code === "channel_capacity" ||
      code === "internal_capacity"
    )
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

async function requestFacts(request: ChatRequest, totalTimeoutMs: number): Promise<Facts> {
  const body = request.body;
  let value: { model?: unknown; stream?: unknown },
    document = body,
    files: FormFiles | undefined;
  if (/^multipart\/form-data\s*;/i.test(request.contentType ?? "")) {
    const form = await readForm(body, request.contentType!);
    if (
      form.view.stream !== undefined &&
      form.view.stream !== "true" &&
      form.view.stream !== "false"
    )
      throw new GatewayError(400, "invalid_request", "stream must be boolean");
    value = { ...form.view, stream: form.view.stream === "true" };
    document = new TextEncoder().encode(JSON.stringify(form.view));
    files = form.files;
  } else {
    let parsed: unknown;
    try {
      parsed = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
    } catch {
      throw new GatewayError(400, "invalid_json", "Request body must be JSON");
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
      throw new GatewayError(400, "invalid_request", "Request body must be a JSON object");
    value = parsed;
  }
  const fields = value as Record<string, unknown>;
  let references: Reference[],
    batch: BatchInput | undefined,
    batchEndpoint: string | undefined,
    model = request.path ? request.path.model : value.model;
  if (request.operation === "batches") {
    const input = fields.input_file_id;
    if (typeof input !== "string" || !/^file_[0-9a-f]{32}$/.test(input))
      throw new GatewayError(400, "invalid_request", "input_file_id must name an uploaded file");
    if (typeof fields.endpoint !== "string")
      throw new GatewayError(400, "invalid_request", "endpoint is required");
    if (model !== undefined)
      throw new GatewayError(400, "invalid_request", "model is taken from the input file");
    collectReferences({ ...fields, input_file_id: undefined }, false);
    references = [{ pointer: "/input_file_id", resourceId: input }];
    batchEndpoint = fields.endpoint;
  } else {
    references = collectReferences(
      value,
      (request.operation === "responses" || request.operation === "chat") && !files,
    );
    rejectUnmeteredTools(fields, request.operation);
  }
  if (request.operation === "files") {
    const purpose = fields.purpose;
    if (!files || typeof purpose !== "string")
      throw new GatewayError(400, "invalid_request", "A multipart file and purpose are required");
    if (model !== undefined && (typeof model !== "string" || !model))
      throw new GatewayError(400, "invalid_request", "model must be a non-empty string");
    if (purpose === "batch") {
      const descriptor = fields.file;
      const data =
        typeof descriptor === "object" && descriptor !== null && !Array.isArray(descriptor)
          ? files.get(canonical(descriptor as Parameters<typeof canonical>[0]))?.[0]
          : undefined;
      if (!data) throw new GatewayError(400, "invalid_request", "A batch upload requires one file");
      batch = parseBatchInput(data);
      if (model !== undefined && model !== batch.model)
        throw new GatewayError(400, "invalid_batch", "model must match the batch requests");
      model = batch.model;
    }
  } else if (request.operation !== "batches" && (typeof model !== "string" || !model))
    throw new GatewayError(400, "invalid_request", "A model is required");
  const protocol = (protocols as readonly string[]).includes(request.operation)
    ? (request.operation as Protocol)
    : undefined;
  return {
    operation: request.operation,
    protocol,
    inputCharacters: requestCharacters(request.operation, value as Record<string, unknown>),
    document,
    files,
    model: typeof model === "string" ? model : "",
    stream: request.path ? request.path.stream : value.stream === true,
    fingerprint: sha256(body),
    deadlineAt: new Date(Date.now() + totalTimeoutMs).toISOString(),
    requestId: request.requestId,
    references,
    background: request.operation === "responses" && !files && fields.background === true,
    batch,
    batchEndpoint,
  };
}

async function whileBusy<T>(request: ChatRequest, facts: Facts, run: () => Promise<T>) {
  for (let delay = 25; ; delay = Math.min(delay * 2, 200)) {
    try {
      return await run();
    } catch (error) {
      if (
        !(error instanceof CoreRpcError) ||
        (error.detail.code !== "identity_busy" && error.detail.code !== "internal_capacity") ||
        request.signal.aborted ||
        Date.now() + delay >= Date.parse(facts.deadlineAt)
      )
        throw error;
      await Bun.sleep(delay);
    }
  }
}

async function openContext(deps: ChatDependencies, request: ChatRequest, facts: Facts) {
  try {
    return (await whileBusy(request, facts, () =>
      deps.core.execution(
        "execution_context_v1",
        {
          customer_token: request.token,
          ...(facts.operation === "chat" &&
          !facts.stream &&
          !facts.background &&
          !facts.references.length
            ? { route_projection: true }
            : {}),
          ip: request.ip,
          operation: facts.operation,
          ...(facts.model ? { model: facts.model } : {}),
          fingerprint: facts.fingerprint,
          ...(facts.references.length
            ? { resource_ids: [...new Set(facts.references.map((r) => r.resourceId))] }
            : {}),
        },
        { deadlineAt: facts.deadlineAt },
      ),
    )) as ExecutionContextResponse;
  } catch (error) {
    throw coreFailure(error);
  }
}

class CandidateRefusal extends GatewayError {}

function enforceRoutePolicy(body: Uint8Array, policy: unknown) {
  const parsed = requestPolicySchema.parse(policy);
  if (
    !parsed.allowed_parameters &&
    !parsed.blocked_parameters?.length &&
    !Object.keys(parsed.parameter_ranges ?? {}).length
  )
    return;
  let document: Record<string, unknown>;
  try {
    document = parseClientJson(new TextDecoder().decode(body)) as Record<string, unknown>;
  } catch {
    throw new GatewayError(502, "plugin_invalid", "The route's plugin did not prepare a request");
  }
  try {
    validateRequestPolicy(document, parsed);
  } catch (error) {
    if (error instanceof GatewayError)
      throw new CandidateRefusal(error.status, error.code, error.message);
    throw error;
  }
}

async function plan(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  context: ExecutionContextResponse,
  candidate: Candidate,
  releasePreparation?: () => void,
): Promise<Plan> {
  const pkg = await deps.packages.get(candidate.ref.package_digest, facts.deadlineAt).catch(() => {
    throw new GatewayError(503, "plugin_unavailable", "The route's plugin is unavailable");
  });
  const attemptId = crypto.randomUUID();
  let bound: ReturnType<ChatDependencies["host"]["bind"]>;
  try {
    releasePreparation?.();
    bound = deps.host.bind(
      pkg,
      {
        tenant: context.project_id,
        attempt: attemptId,
        incarnation: context.owner.incarnation_id,
        config: candidate.config,
        deadlineAt: Date.parse(facts.deadlineAt),
      },
      context.execution_runtimes[candidate.ref.package_digest] ?? [],
    );
  } catch (error) {
    if (!(error instanceof PluginHostError) || error.code !== "EXECUTION_UNAVAILABLE") throw error;
    throw new GatewayError(
      503,
      "plugin_unavailable",
      "The route's execution runtime is unavailable",
    );
  }
  const attempt = bound.attempt;
  try {
    const source = facts.protocol;
    const converted =
      source !== undefined &&
      !verifiedPackageRuntime(pkg).manifest.capabilities.operations.includes(facts.operation);
    if (converted && facts.references.length)
      throw new GatewayError(
        422,
        "unsupported_semantics",
        "Stored responses cannot be continued through this route",
      );
    if (converted && facts.background)
      throw new GatewayError(
        422,
        "unsupported_semantics",
        "Background responses are not available through this route",
      );
    let document = converted ? undefined : resolvedDocument(facts, context),
      files = facts.files,
      commit: Plan["commit"];
    if (context.batch) {
      if (facts.batchEndpoint !== context.batch.endpoint)
        throw new GatewayError(
          400,
          "invalid_batch",
          "endpoint must match the requests in the input file",
        );
      commit = { input_file_id: facts.references[0]!.resourceId };
    }
    if (facts.batch && files && document) {
      const rows = await prepareBatchRows(
        facts.batch,
        () => bound.createSibling(crypto.randomUUID()),
        request.signal,
        (body) => enforceRoutePolicy(body, candidate.request_policy),
      );
      const view = JSON.parse(new TextDecoder().decode(document)) as {
        file: { file: Record<string, unknown> };
      };
      const previous = canonical(view.file as Parameters<typeof canonical>[0]);
      view.file = {
        file: { ...view.file.file, bytes: rows.bytes.length, sha256: sha256(rows.bytes) },
      };
      files = new Map(files);
      files.delete(previous);
      files.set(canonical(view.file as Parameters<typeof canonical>[0]), [rows.bytes]);
      document = new TextEncoder().encode(JSON.stringify(view));
      commit = { batch: rows.record };
    }
    const transformed = await attempt.transformJSON(
      "prepare",
      converted
        ? { protocol: "chat", operation: "chat", model: facts.model }
        : {
            ...(source ? { protocol: source } : {}),
            operation: facts.operation,
            model: facts.model,
            ...(request.path ? { stream: request.path.stream } : {}),
            ...(facts.files ? { content: "multipart" } : {}),
            ...(facts.inputCharacters === undefined
              ? {}
              : { input_characters: String(facts.inputCharacters) }),
          },
      document ?? requestAsChat(request.body, source as Protocol, facts.model, request.path),
      request.signal,
    );
    const rejected = failure(transformed.result);
    if (rejected) throw rejected;
    if (
      transformed.result.kind !== "prepared" ||
      !transformed.body ||
      transformed.result.request.manifest.operation !== (converted ? "chat" : facts.operation) ||
      (transformed.result.request.body_format === "multipart") !== Boolean(files)
    )
      throw new GatewayError(502, "plugin_invalid", "The route's plugin did not prepare a request");
    enforceRoutePolicy(transformed.body, candidate.request_policy);
    const prepared = transformed.result.request,
      boundary = `stg-${attemptId.replaceAll("-", "")}`;
    let outbound = transformed.body;
    if (files)
      try {
        outbound = encodeForm(
          JSON.parse(new TextDecoder().decode(transformed.body)),
          files,
          boundary,
        );
      } catch {
        throw new GatewayError(502, "plugin_invalid", "The route's plugin rewrote form files");
      }
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
    const headers = {
      ...prepared.headers,
      "content-type": files ? `multipart/form-data; boundary=${boundary}` : "application/json",
    };
    const manifest = {
      version: 1,
      operation: facts.operation,
      model: prepared.manifest.model,
      upstream_model: prepared.manifest.upstream_model,
      stream: prepared.manifest.stream,
      input_bytes: prepared.manifest.input_bytes,
      quantities: prepared.manifest.quantities ?? {},
      resource_ids: [],
      ceilings: context.batch ? context.batch.ceilings : prepared.manifest.ceilings,
      ...(prepared.manifest.images === undefined ? {} : { images: prepared.manifest.images }),
      ...(facts.inputCharacters === undefined
        ? {}
        : { input_characters: String(facts.inputCharacters) }),
    };
    const projection = dispatchProjectionV2Schema.parse({
      version: 2,
      execution: bound.execution,
      proposed_request_id: facts.requestId,
      proposed_attempt_id: attemptId,
      origin: candidate.origin,
      method: prepared.method,
      operation: facts.operation,
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
      commit,
      files: new Map(
        (context.resources ?? []).map((resource) => [resource.remote_id, resource.resource_id]),
      ),
      converted: converted ? source : undefined,
      attempt,
      attemptId,
      channelId: candidate.ref.channel_id,
      prepared,
      outbound,
      url,
      headers,
      dispatch: {
        context_id: context.context_id,
        dispatch_projection: projection,
        dispatched_digest: sha256(canonical(["stg.internal.dispatch-projection", "2", projection])),
        manifest,
        prep_session_id: prepSessionId,
        prep_binding_digest: sha256(
          canonical([
            "stg.internal.prep-binding",
            "2",
            prepBindingV2({
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
    reply = (await whileBusy(request, facts, () =>
      previous
        ? deps.core.execution(
            "execution_next_v1",
            {
              ...planned.dispatch,
              request_id: facts.requestId,
              previous_attempt_id: previous.permit.attemptId,
              expected_ordinal: previous.ordinal + 1,
              visible_output: false,
            },
            {
              commandId,
              deadlineAt: facts.deadlineAt,
              ...(context.route_ticket ? { routeTicket: context.route_ticket } : {}),
            },
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
            {
              commandId,
              deadlineAt: facts.deadlineAt,
              ...(context.route_ticket ? { routeTicket: context.route_ticket } : {}),
            },
          ),
    )) as ExecutionReply;
  } catch (error) {
    planned.attempt.dispose();
    const abandoned = {
      attemptId: planned.attemptId,
      permitEpoch: "1",
      channelId: planned.channelId,
    };
    if (
      !(error instanceof CoreRpcError) ||
      (error.detail.command_state === "UNKNOWN" && error.detail.retryability !== "NONE")
    ) {
      const receipt = await queryCommand(
        deps,
        previous ? "execution_next_v1" : "execution_admit_v1",
        commandId,
      );
      if (receipt?.command_state === "COMMITTED" && receipt.state === "RESERVED") {
        await abortBeforeSend(
          deps,
          { ...abandoned, permitEpoch: receipt.permit_epoch ?? "1" },
          "PREPARE_DISPOSED",
          !previous,
        );
        throw coreFailure(error);
      }
    }
    await reportHealth(deps, abandoned, "NOT_SENT");
    const failure = coreFailure(error);
    if (
      error instanceof CoreRpcError &&
      error.detail.command_state === "NOT_ATTEMPTED" &&
      (error.detail.code === "channel_capacity" || error.detail.code === "no_route")
    )
      throw new CandidateRefusal(failure.status, failure.code, failure.message);
    throw failure;
  }
  const permitEpoch = reply.receipt.permit_epoch ?? "1";
  const expectedPlan = {
    version: 2,
    owner: context.owner,
    attempt_id: planned.attemptId,
    permit_epoch: permitEpoch,
    dispatched_digest: planned.dispatch.dispatched_digest,
    dispatch_projection: planned.dispatch.dispatch_projection,
  };
  if (
    reply.receipt.state !== "RESERVED" ||
    reply.receipt.request_id !== facts.requestId ||
    reply.receipt.attempt_id !== planned.attemptId ||
    canonical(reply.receipt.authorization_plan ?? null) !== canonical(expectedPlan) ||
    reply.receipt.authorization_plan_digest !==
      sha256(canonical(["stg.internal.host-authorization-plan", "2", expectedPlan]))
  ) {
    planned.attempt.dispose();
    if (reply.receipt.state === "RESERVED")
      await abortBeforeSend(
        deps,
        { attemptId: planned.attemptId, permitEpoch, channelId: planned.channelId },
        "PREPARE_DISPOSED",
        !previous,
      );
    await reportHealth(
      deps,
      { attemptId: planned.attemptId, permitEpoch: "1", channelId: planned.channelId },
      "NOT_SENT",
    );
    throw new GatewayError(503, "core_unavailable", "Execution authority is unavailable");
  }
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

function resolvedDocument(facts: Facts, context: ExecutionContextResponse) {
  if (!facts.references.length) return facts.document;
  const remote = new Map((context.resources ?? []).map((r) => [r.resource_id, r.remote_id]));
  const edits = facts.references.map((reference) => {
    const value = remote.get(reference.resourceId);
    if (!value) throw new GatewayError(503, "core_unavailable", "Resource resolution failed");
    return { op: "set" as const, pointer: reference.pointer, value: JSON.stringify(value) };
  });
  return rewriteJSON(facts.document, edits);
}

export function publicResourceId(kind: string, requestId: string) {
  return `${kind}_${requestId.replaceAll("-", "")}`;
}

function createdResource(facts: Facts, admitted: Admitted) {
  if (admitted.converted) return undefined;
  if (facts.operation === "responses") return publicResourceId("response", facts.requestId);
  if (facts.operation === "files") return publicResourceId("file", facts.requestId);
  if (facts.operation === "batches") return publicResourceId("batch", facts.requestId);
  if (facts.operation === "videos") return publicResourceId("video", facts.requestId);
  return undefined;
}

async function commitResource(
  deps: ChatDependencies,
  admitted: Admitted,
  expected: string | undefined,
  declared: { kind: string; remote_id: string } | undefined,
  state: "ACTIVE" | "COMPLETED" = "COMPLETED",
) {
  if (!expected && !declared) return;
  if (!expected || !declared || !expected.startsWith(`${declared.kind}_`))
    throw new GatewayError(502, "plugin_invalid", "The route's plugin misreported a resource");
  for (let attempt = 0; ; attempt++) {
    try {
      const committed = (await deps.core.execution(
        "execution_resource_commit_v1",
        {
          attempt_id: admitted.attemptId,
          permit_epoch: admitted.permitEpoch,
          kind: declared.kind,
          remote_id: declared.remote_id,
          state,
          ...admitted.commit,
        },
        shortDeadline(),
      )) as { resource_id: string };
      if (committed.resource_id !== expected)
        throw new GatewayError(
          502,
          "upstream_invalid_response",
          "The upstream reused a resource identity",
        );
      return;
    } catch (error) {
      if (error instanceof GatewayError || attempt >= 2)
        throw error instanceof GatewayError
          ? error
          : new GatewayError(503, "resource_unavailable", "The resource could not be recorded");
      await Bun.sleep(100 * (attempt + 1));
    }
  }
}

function shortDeadline() {
  return { deadlineAt: new Date(Date.now() + 5000).toISOString() };
}

async function sameCommand(
  deps: ChatDependencies,
  kind: "execution_abort_v1" | "execution_quota_v1",
  data: Record<string, unknown>,
) {
  const options = { commandId: crypto.randomUUID(), ...shortDeadline() },
    end = Date.parse(options.deadlineAt) - 250;
  for (let delay = 25; ; delay = Math.min(delay * 2, 400)) {
    try {
      await deps.core.execution(kind, data, options);
      return "done" as const;
    } catch (error) {
      if (error instanceof CoreRpcError && error.detail.retryability === "NONE")
        return "refused" as const;
      if (Date.now() + delay >= end) return "unknown" as const;
      await Bun.sleep(delay);
    }
  }
}

async function abortBeforeSend(
  deps: ChatDependencies,
  permit: Permit & { channelId: string },
  reason: "CANCELLED" | "DEADLINE" | "PREPARE_DISPOSED",
  closeRequest: boolean,
) {
  await sameCommand(deps, "execution_abort_v1", {
    attempt_id: permit.attemptId,
    permit_epoch: permit.permitEpoch,
    reason,
    close_request: closeRequest,
  });
  await reportHealth(deps, permit, "NOT_SENT");
}

async function reportHealth(
  deps: ChatDependencies,
  permit: Permit & { channelId: string },
  outcome: HealthOutcome,
  latency = 0,
) {
  await deps.core
    .execution(
      "execution_quota_v1",
      {
        attempt_id: permit.attemptId,
        permit_epoch: permit.permitEpoch,
        action: "OBSERVE",
        observation: {
          channel_id: permit.channelId,
          outcome,
          latency_ms: Math.min(3_600_000, Math.max(0, Math.round(latency))),
        },
      },
      shortDeadline(),
    )
    .catch(() => undefined);
}

function healthOutcome(
  outcome: Omit<AttemptOutcome, "request_state">,
  halted: boolean,
): HealthOutcome {
  if (outcome.attempt_state === "SUCCEEDED") return "SUCCESS";
  const status = /^upstream_http_(\d+)$/.exec(outcome.error_code ?? "")?.[1];
  if (status) return unhealthyStatuses.has(Number(status)) ? "FAILURE" : "SUCCESS";
  if (
    halted ||
    outcome.error_code?.startsWith("plugin_host_") ||
    outcome.error_code === "evidence_unavailable" ||
    outcome.error_code === "downstream_stalled"
  )
    return "NOT_SENT";
  return "FAILURE";
}

function responseFailureCode(error: unknown, fallback: string): string {
  if (!(error instanceof PluginHostError)) return fallback;
  return `plugin_host_${/^[A-Z][A-Z0-9_]{0,63}$/.test(error.code) ? error.code.toLowerCase() : "unknown"}`;
}

async function closeRequest(deps: ChatDependencies, permit: Permit, reason: CloseReason) {
  await sameCommand(deps, "execution_quota_v1", {
    attempt_id: permit.attemptId,
    permit_epoch: permit.permitEpoch,
    action: "CLOSE",
    reason,
  });
}

export function authorize(
  headers: Record<string, string>,
  material: TransientSend["auth_material"],
  url: URL,
  body: Uint8Array,
): Record<string, string> {
  if (material.type === "bearer") return { ...headers, authorization: `Bearer ${material.value}` };
  if (material.type === "api-key") return { ...headers, [material.header]: material.value };
  if (material.type === "aws-sigv4") {
    const signed = new Headers(headers);
    signAwsRequest(url, body, signed, {
      accessKeyId: material.access_key_id,
      sessionToken: material.session_token,
      region: material.region,
      service: material.service,
      amzDate: material.amz_date,
      signingKey: Buffer.from(material.signing_key, "hex"),
    });
    return Object.fromEntries(signed);
  }
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
  let renewing = false;
  const timer = setInterval(() => {
    if (renewing) return;
    renewing = true;
    void sameCommand(deps, "execution_quota_v1", {
      attempt_id: permit.attemptId,
      permit_epoch: permit.permitEpoch,
      action: "RENEW",
    }).then((outcome) => {
      renewing = false;
      if (outcome === "refused") clearInterval(timer);
    });
  }, deps.config.quota_renew_ms);
  return () => clearInterval(timer);
}

async function dispatch(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  admitted: Admitted,
  previous: { conclude: (last: boolean) => Promise<void> } | undefined,
  reservation: TerminalReservation | undefined,
): Promise<Outcome> {
  const notSent = async (reason: "CANCELLED" | "DEADLINE" | "PREPARE_DISPOSED") => {
    await abortBeforeSend(deps, admitted, reason, !previous);
    await previous?.conclude(true);
  };
  const local = new AbortController(),
    expiry = new AbortController(),
    total = AbortSignal.timeout(Math.max(1, Date.parse(facts.deadlineAt) - Date.now()));
  const signal = AbortSignal.any([
    request.signal,
    request.drain,
    local.signal,
    total,
    expiry.signal,
  ]);
  try {
    deps.grants.open(admitted.attemptId, local);
  } catch (error) {
    admitted.attempt.dispose();
    await notSent("PREPARE_DISPOSED");
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
    delivery:
      facts.operation === "chat" && !admitted.prepared.manifest.stream && !facts.background
        ? "terminal-group"
        : "progressive",
    seal: (evidence) => deps.core.signEvidence(evidence),
    record: (evidence) => deps.publish(evidence),
  });
  const neverSent = async (outcome: Parameters<Bridge["notExecuted"]>[0]) => {
    deps.grants.release(admitted.attemptId);
    await bridge.notExecuted(outcome).catch(() => undefined);
    bridge.dispose();
    await reportHealth(deps, admitted, "NOT_SENT");
  };
  const beginCommand = crypto.randomUUID();
  let reply: ExecutionReply;
  try {
    if (signal.aborted) throw halt(request).error(unavailable);
    reply = (await whileBusy(request, facts, () =>
      deps.core.execution("execution_begin_v1", admitted.begin, {
        commandId: beginCommand,
        deadlineAt: facts.deadlineAt,
      }),
    )) as ExecutionReply;
  } catch (error) {
    if (error instanceof CoreRpcError && error.detail.command_state === "UNKNOWN") {
      const resolved = await resolveUnknownBegin(deps, beginCommand);
      if (resolved === "granted") {
        await previous?.conclude(false);
        await neverSent({ request_state: "FAILED", error_code: "begin_outcome_unknown" });
        await closeRequest(deps, admitted, "FAILED");
      } else {
        deps.grants.release(admitted.attemptId);
        bridge.dispose();
        if (resolved === "absent") await notSent("PREPARE_DISPOSED");
        else {
          await previous?.conclude(false);
          await reportHealth(deps, admitted, "NOT_SENT");
        }
      }
      throw unavailable;
    }
    deps.grants.release(admitted.attemptId);
    bridge.dispose();
    const stop = halt(request);
    await notSent(
      stop.requestState === "CANCELLED"
        ? "CANCELLED"
        : Date.now() >= Date.parse(facts.deadlineAt)
          ? "DEADLINE"
          : "PREPARE_DISPOSED",
    );
    throw error instanceof GatewayError ? error : coreFailure(error);
  }
  await previous?.conclude(false);
  if (!reply.transient_send || !deps.grants.consume(admitted.attemptId)) {
    await neverSent({ request_state: "FAILED", error_code: "send_not_authorized" });
    await closeRequest(deps, admitted, halt(request).reason);
    throw halt(request).error(unavailable);
  }
  const stopRenewal = renewQuota(deps, admitted);
  let started = performance.now();
  const end = async (ending: Ending) => {
    stopRenewal();
    deps.grants.release(admitted.attemptId);
    const latency = performance.now() - started;
    let completed = false;
    try {
      const canRelease =
        ending.outputBytes !== undefined &&
        reservation?.retainOutput(
          request.body.buffer.byteLength + admitted.outbound.buffer.byteLength + ending.outputBytes,
        );
      await bridge.finish(ending.outcome, canRelease);
      completed =
        ending.outcome.attempt_state === "SUCCEEDED" &&
        ending.outcome.request_state === "SUCCEEDED";
    } finally {
      bridge.dispose();
      await reportHealth(
        deps,
        admitted,
        healthOutcome(ending.outcome, halt(request).halted),
        latency,
      );
      if (!completed) await closeRequest(deps, admitted, ending.reason);
    }
  };
  const finalFailure = async (
    outcome: Omit<AttemptOutcome, "request_state">,
    error: GatewayError,
  ): Promise<never> => {
    const stop = halt(request);
    await end({
      outcome: { ...outcome, request_state: stop.requestState },
      reason: stop.reason,
    }).catch(() => undefined);
    throw stop.error(error);
  };
  const deferred = (
    error: GatewayError,
    publish: (requestState?: "FAILED" | "CANCELLED") => Promise<unknown>,
  ): Outcome => ({
    retry: error,
    async conclude(last) {
      const stop = halt(request);
      await publish(last ? stop.requestState : undefined).catch(() => undefined);
      bridge.dispose();
      if (last) await closeRequest(deps, admitted, stop.reason);
    },
  });
  const retryLater = async (
    outcome: Omit<AttemptOutcome, "request_state">,
    error: GatewayError,
  ): Promise<Outcome> => {
    stopRenewal();
    deps.grants.release(admitted.attemptId);
    try {
      await bridge.flush();
    } catch (error) {
      return finalFailure(
        { attempt_state: "UNKNOWN", error_code: "evidence_unavailable" },
        error instanceof GatewayError
          ? error
          : new GatewayError(503, "evidence_unavailable", "Usage evidence could not be recorded"),
      );
    }
    await reportHealth(
      deps,
      admitted,
      healthOutcome(outcome, halt(request).halted),
      performance.now() - started,
    );
    return deferred(error, (requestState) =>
      bridge.finish(requestState ? { ...outcome, request_state: requestState } : outcome),
    );
  };
  const deadlineReached = () => total.aborted || Date.now() >= Date.parse(facts.deadlineAt);
  const cause = () => (expiry.signal.aborted ? (expiry.signal.reason as Deadline) : undefined);
  const control: Control = {
    signal,
    expire: (reason) => expiry.abort(reason),
    cause,
    timeoutError: (fallback) =>
      deadlineReached()
        ? new GatewayError(504, "request_deadline", "Request deadline reached")
        : cause() === "upstream_header_timeout" || cause() === "upstream_idle_timeout"
          ? new GatewayError(504, "upstream_timeout", "The upstream did not respond in time")
          : fallback,
    end,
    fail: finalFailure,
    abandon: (outcome, error) =>
      !halt(request).halted && !local.signal.aborted && !deadlineReached()
        ? retryLater(outcome, error)
        : finalFailure(outcome, error),
  };
  let response: Response;
  const headerTimer = admitted.prepared.manifest.stream
    ? setTimeout(
        () => control.expire("upstream_header_timeout"),
        deps.config.upstream_header_timeout_ms,
      )
    : undefined;
  try {
    started = performance.now();
    response = await deps.transport.fetch(
      admitted.url,
      {
        method: admitted.prepared.method,
        headers: authorize(
          admitted.headers,
          reply.transient_send!.auth_material,
          admitted.url,
          admitted.outbound,
        ),
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
        final =
          stop.halted ||
          local.signal.aborted ||
          deadlineReached() ||
          error.code === "unsafe_upstream";
      if (!final) {
        deps.grants.release(admitted.attemptId);
        await bridge.flush();
        await reportHealth(deps, admitted, "NOT_SENT");
        return deferred(unreachable, (requestState) =>
          bridge.notExecuted({
            ...(requestState ? { request_state: requestState } : {}),
            error_code: "upstream_not_dispatched",
          }),
        );
      }
      await neverSent({
        request_state: stop.requestState,
        error_code:
          error.code === "unsafe_upstream" ? "unsafe_upstream" : "upstream_not_dispatched",
      });
      await closeRequest(deps, admitted, stop.reason);
      throw stop.error(
        error.code === "unsafe_upstream"
          ? new GatewayError(502, "unsafe_upstream", "The upstream address is not permitted")
          : control.timeoutError(unreachable),
      );
    }
    return control.abandon(
      { attempt_state: "UNKNOWN", error_code: cause() ?? "upstream_interrupted" },
      control.timeoutError(unreachable),
    );
  } finally {
    clearTimeout(headerTimer);
  }
  if (!response.ok) {
    await response.body?.cancel().catch(() => undefined);
    const rejected = {
      attempt_state: [400, 401, 403, 404, 422].includes(response.status)
        ? ("FAILED" as const)
        : ("UNKNOWN" as const),
      error_code: `upstream_http_${response.status}`,
    };
    if (!retryableStatuses.has(response.status))
      return finalFailure(rejected, upstreamRejection(response.status));
    return control.abandon(rejected, upstreamRejection(response.status));
  }
  const type = response.headers.get("content-type") ?? "";
  const upstream = idleGuard(response.body, deps.config.upstream_idle_timeout_ms, () =>
    control.expire("upstream_idle_timeout"),
  );
  if (admitted.prepared.binary && !/json|^text\/event-stream/i.test(type))
    return {
      response: relayBinary(deps, facts, admitted, bridge, upstream, type, request, control),
    };
  if (admitted.prepared.manifest.stream)
    return relaySSE(deps, facts, admitted, bridge, upstream, request, control);
  return relayJSON(deps, facts, admitted, bridge, upstream, control);
}

export async function executeChat(deps: ChatDependencies, request: ChatRequest) {
  const facts = await requestFacts(request, deps.config.total_timeout_ms);
  const reservation =
    facts.operation === "chat" && !facts.stream && !facts.background
      ? deps.terminalBudget.reserve()
      : undefined;
  try {
    return await executePreparedChat(deps, request, facts, reservation);
  } finally {
    reservation?.release();
  }
}
async function executePreparedChat(
  deps: ChatDependencies,
  request: ChatRequest,
  facts: Facts,
  reservation: TerminalReservation | undefined,
) {
  const external = { ...request, signal: AbortSignal.any([request.signal, request.drain]) };
  const tried = new Set<string>();
  let previous:
    { permit: Permit; ordinal: number; conclude: (last: boolean) => Promise<void> } | undefined;
  let lastError: GatewayError | undefined;
  for (let ordinal = 1; ordinal <= maxOrdinal; ordinal++) {
    let admitted: Admitted;
    let releasePreparation: (() => void) | undefined;
    try {
      if (external.signal.aborted)
        throw halt(request).error(new GatewayError(499, "cancelled", "Request cancelled"));
      if (reservation) releasePreparation = deps.host.reservePreparation();
      const context = await openContext(deps, request, facts);
      if (!facts.model) facts = { ...facts, model: context.model };
      const untried = context.candidates.filter((c) => !tried.has(c.ref.channel_id));
      if (!untried.length) break;
      let reserved: Admitted | undefined, refused: CandidateRefusal | undefined;
      for (const candidate of untried) {
        tried.add(candidate.ref.channel_id);
        try {
          const planned = await plan(deps, external, facts, context, candidate, releasePreparation);
          reserved = await reserve(deps, request, facts, context, planned, previous);
          break;
        } catch (error) {
          if (!(error instanceof CandidateRefusal)) throw error;
          refused ??= error;
        }
      }
      if (!reserved) throw refused!;
      admitted = reserved;
    } catch (error) {
      if (!previous) throw error;
      await previous.conclude(true);
      throw halt(request).error(lastError ?? coreFailure(error));
    } finally {
      releasePreparation?.();
    }
    const outcome = await dispatch(deps, request, facts, admitted, previous, reservation);
    if ("response" in outcome) return outcome.response;
    lastError = outcome.retry;
    previous = { permit: admitted, ordinal, conclude: outcome.conclude };
  }
  if (!previous) throw new GatewayError(404, "model_not_found", "No route serves this model");
  await previous.conclude(true);
  throw halt(request).error(lastError!);
}

async function relayJSON(
  deps: ChatDependencies,
  facts: Facts,
  admitted: Admitted,
  bridge: Bridge,
  upstream: ReadableStream<Uint8Array>,
  control: Control,
): Promise<Outcome> {
  const invalid = new GatewayError(
    502,
    "upstream_invalid_response",
    "The upstream response was invalid",
  );
  let transformed: Awaited<ReturnType<Bridge["transformJSON"]>>;
  try {
    const bytes = await readBounded(upstream, deps.config.max_response_bytes, control.signal);
    transformed = await bridge.transformJSON(
      "response",
      admitted.converted
        ? { protocol: "chat", model: facts.model }
        : facts.protocol
          ? {
              protocol: facts.protocol,
              model: facts.model,
              ...(createdResource(facts, admitted)
                ? { resource_id: createdResource(facts, admitted) }
                : {}),
            }
          : {
              operation: facts.operation,
              model: facts.model,
              ...(createdResource(facts, admitted)
                ? { resource_id: createdResource(facts, admitted) }
                : {}),
            },
      bytes,
      control.signal,
    );
  } catch (error) {
    if (error instanceof GatewayError && error.code === "usage_queue_unavailable")
      return control.fail({ attempt_state: "UNKNOWN", error_code: "evidence_unavailable" }, error);
    return control.abandon(
      {
        attempt_state: "UNKNOWN",
        error_code: control.cause() ?? responseFailureCode(error, "upstream_read_failed"),
      },
      control.timeoutError(invalid),
    );
  }
  const result = transformed.result;
  const pending = result.kind === "output" && result.terminal?.kind === "pending";
  if (
    result.kind !== "output" ||
    !result.terminal ||
    !transformed.body ||
    (pending &&
      (!(facts.background || facts.operation === "batches" || facts.operation === "videos") ||
        !createdResource(facts, admitted)))
  )
    return control.abandon(
      { attempt_state: "UNKNOWN", error_code: "upstream_invalid" },
      failure(result) ?? invalid,
    );
  try {
    await commitResource(
      deps,
      admitted,
      createdResource(facts, admitted),
      result.resource,
      pending ? "ACTIVE" : "COMPLETED",
    );
  } catch (error) {
    return control.fail(
      { attempt_state: "UNKNOWN", error_code: "resource_unrecorded" },
      error instanceof GatewayError ? error : invalid,
    );
  }
  try {
    await control.end({
      outcome: pending
        ? { attempt_state: "PARTIAL" }
        : { attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" },
      outputBytes: transformed.body.buffer.byteLength,
      reason: "COMPLETED",
    });
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError(503, "evidence_unavailable", "Usage evidence could not be recorded");
  }
  const body = admitted.converted
    ? responseFromChat(transformed.body, admitted.converted, facts.model)
    : facts.operation === "batches"
      ? exposeBatchFiles(transformed.body, admitted.files)
      : transformed.body;
  return {
    response: new Response(body.slice(), {
      status: 200,
      headers: { "content-type": "application/json", "x-request-id": facts.requestId },
    }),
  };
}

async function relaySSE(
  deps: ChatDependencies,
  facts: Facts,
  admitted: Admitted,
  bridge: Bridge,
  upstream: ReadableStream<Uint8Array>,
  request: ChatRequest,
  control: Control,
): Promise<Outcome> {
  const encoder = new TextEncoder();
  const translated = admitted.converted
    ? streamFromChat(admitted.converted, facts.model)
    : undefined;
  const emit = function* (bytes: Uint8Array) {
    if (translated) yield* translated.feed(bytes);
    else yield bytes;
  };
  let undelivered: { outcome: Failed; fault: unknown } | undefined;
  async function* frames(): AsyncGenerator<Uint8Array> {
    let terminal: { bytes: string } | undefined;
    let ended = false,
      delivered = false,
      failed = false,
      recorded = false,
      fault: unknown;
    const expected = createdResource(facts, admitted);
    const reader = upstream.getReader();
    const cancel = () => void reader.cancel().catch(() => undefined);
    control.signal.addEventListener("abort", cancel, { once: true });
    const record = async (result: AbiResult) => {
      if (result.kind !== "output" || !result.resource) return result;
      if (recorded) throw new Error("resource_redeclared");
      await commitResource(deps, admitted, expected, result.resource);
      recorded = true;
      return result;
    };
    const settle = function* (result: AbiResult) {
      if (result.kind === "failure") throw new Error(result.code);
      if (result.kind !== "output") throw new Error("plugin_output");
      if (expected && !recorded && (result.chunks.length || result.terminal))
        throw new Error("resource_unrecorded");
      for (const chunk of result.chunks) {
        const bytes = Buffer.from(chunk, "base64");
        if (bytes.length) {
          delivered = true;
          yield* emit(bytes);
        }
      }
      const id = (result as { output_id?: string }).output_id;
      if (id) admitted.attempt.releaseOutput(id);
      if (result.terminal) terminal = result.terminal;
      return result.needs_input || Boolean(result.terminal);
    };
    try {
      const opened = await bridge.invoke(
        "stream_open",
        encoder.encode(
          JSON.stringify(
            admitted.converted || facts.protocol
              ? {
                  model: facts.model,
                  protocol: admitted.converted ? "chat" : facts.protocol,
                  ...(expected ? { resource_id: expected } : {}),
                }
              : { model: facts.model, operation: facts.operation },
          ),
        ),
      );
      if (opened.kind !== "output") throw new Error("stream_open");
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        for (let offset = 0; offset < next.value.length && !terminal;) {
          const fed = await record(
            await bridge.invoke("stream_feed", next.value.subarray(offset, offset + (64 << 10))),
          );
          let waiting = yield* settle(fed);
          while (!waiting)
            waiting = yield* settle(await record(await bridge.invoke("stream_drain")));
          if (fed.kind !== "output" || fed.consumed < 1) throw new Error("stream_feed_stalled");
          offset += fed.consumed;
        }
      }
      await bridge.invoke("stream_finish");
      if (!terminal) throw new Error("stream_truncated");
      ended = true;
      await control.end({
        outcome: { attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" },
        reason: "COMPLETED",
      });
      const held = Buffer.from(terminal.bytes, "base64");
      if (held.length) yield* emit(held);
      if (translated) yield* translated.finish();
    } catch (error) {
      failed = true;
      fault = error;
    } finally {
      control.signal.removeEventListener("abort", cancel);
      cancel();
      if (!ended) {
        const stop = halt(request);
        const outcome: Failed = {
          attempt_state: delivered ? "PARTIAL" : "UNKNOWN",
          error_code:
            stop.reason === "CANCELLED"
              ? "client_cancelled"
              : stop.reason === "DRAINED"
                ? "gateway_draining"
                : (control.cause() ?? responseFailureCode(fault, "stream_interrupted")),
        };
        if (!delivered && !recorded) undelivered = { outcome, fault };
        else
          await control
            .end({ outcome: { ...outcome, request_state: stop.requestState }, reason: stop.reason })
            .catch(() => undefined);
      }
    }
    if (failed && !undelivered && halt(request).requestState !== "CANCELLED")
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
  const first = await iterator.next();
  if (undelivered) {
    const { outcome, fault } = undelivered;
    if (fault instanceof GatewayError && fault.code === "usage_queue_unavailable")
      return control.fail({ ...outcome, error_code: "evidence_unavailable" }, fault);
    if (fault instanceof GatewayError || fault instanceof CoreRpcError)
      return control.fail(outcome, coreFailure(fault));
    return control.abandon(
      outcome,
      control.timeoutError(
        new GatewayError(502, "upstream_invalid_response", "The upstream response was invalid"),
      ),
    );
  }
  return {
    response: new Response(
      stallGuard(
        iterator,
        first.done ? undefined : first.value,
        deps.config.downstream_stall_timeout_ms,
        () => control.expire("downstream_stalled"),
      ),
      {
        status: 200,
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
          "x-request-id": facts.requestId,
        },
      },
    ),
  };
}

const binaryTypes =
  /^(?:audio\/[A-Za-z0-9.+-]{1,64}|text\/plain|text\/vtt|application\/x-subrip|application\/octet-stream)(?:\s*;.*)?$/i;

function relayBinary(
  deps: ChatDependencies,
  facts: Facts,
  admitted: Admitted,
  bridge: Bridge,
  upstream: ReadableStream<Uint8Array>,
  type: string,
  request: ChatRequest,
  control: Control,
) {
  async function* chunks(): AsyncGenerator<Uint8Array> {
    let ended = false,
      failed = false;
    const reader = upstream.getReader();
    const cancel = () => void reader.cancel().catch(() => undefined);
    control.signal.addEventListener("abort", cancel, { once: true });
    try {
      let held: Uint8Array | undefined;
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        if (held) yield held;
        held = next.value;
      }
      await bridge.declare({ ...admitted.prepared.binary!.usage, requests: "1" });
      ended = true;
      await control.end({
        outcome: { attempt_state: "SUCCEEDED", request_state: "SUCCEEDED" },
        reason: "COMPLETED",
      });
      if (held) yield held;
    } catch {
      failed = true;
    } finally {
      control.signal.removeEventListener("abort", cancel);
      cancel();
      if (!ended) {
        const stop = halt(request);
        await control
          .end({
            outcome: {
              attempt_state: "UNKNOWN",
              request_state: stop.requestState,
              error_code:
                stop.reason === "CANCELLED"
                  ? "client_cancelled"
                  : (control.cause() ?? "binary_interrupted"),
            },
            reason: stop.reason,
          })
          .catch(() => undefined);
      }
    }
    if (failed) throw new GatewayError(502, "upstream_interrupted", "Binary response interrupted");
  }
  return new Response(
    stallGuard(chunks(), undefined, deps.config.downstream_stall_timeout_ms, () =>
      control.expire("downstream_stalled"),
    ),
    {
      status: 200,
      headers: {
        "content-type": binaryTypes.test(type) ? type : "application/octet-stream",
        "x-request-id": facts.requestId,
      },
    },
  );
}
