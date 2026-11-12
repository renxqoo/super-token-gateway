import { copyExact, parseClientJson, stringifyExact } from "@gateway/connectors";
import { GatewayError, object, text } from "@gateway/contracts";
import { batchIdentityDigest } from "@gateway/runtime";
import type { BatchManifest, Resource, Route } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import { readBounded } from "@gateway/runtime";
import type { Auth, Dependencies } from "./context.ts";
import { applyOutputBound, rejectUnmeteredTools } from "./input-policy.ts";
import { prepareOperation } from "@gateway/connectors";
import { queryResource, resolveFileReferences } from "./resources.ts";

const endpoints: Record<string, Operation> = {
  "/v1/chat/completions": "chat",
  "/v1/responses": "responses",
  "/v1/embeddings": "embeddings",
  "/v1/completions": "completions",
  "/v1/moderations": "moderations",
  "/v1/images/generations": "images",
  "/v1/images/edits": "image_edits",
};
export interface BatchInput {
  model: string;
  endpoint: string;
  operation: Operation;
  requests: Record<string, unknown>[];
  manifest: BatchManifest;
}

function parseBatchFile(bytes: Uint8Array): BatchInput {
  const source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  const lines = source.split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length || lines.length > 50000)
    throw new GatewayError(400, "invalid_batch", "Batch must contain 1 to 50000 requests");
  const ids = new Set<string>();
  let model = "";
  let endpoint = "";
  let embeddingInputs = 0;
  let images = 0;
  const requests = lines.map((line) => {
    const request = object(parseClientJson(line));
    const body = object(request.body);
    if (
      request.method !== "POST" ||
      typeof request.custom_id !== "string" ||
      !request.custom_id.length ||
      request.custom_id.length > 200 ||
      ids.has(request.custom_id)
    )
      throw new GatewayError(
        400,
        "invalid_batch",
        "Each batch request requires a unique custom_id and POST method",
      );
    ids.add(request.custom_id);
    if (body.stream === true || !endpoints[text(request.url)])
      throw new GatewayError(
        422,
        "invalid_batch",
        "Unsupported batch endpoint or streaming request",
      );
    if ((model && model !== body.model) || (endpoint && endpoint !== request.url))
      throw new GatewayError(
        400,
        "invalid_batch",
        "A batch file must use one logical model and one endpoint",
      );
    model = text(body.model);
    endpoint = text(request.url);
    if (!model)
      throw new GatewayError(
        403,
        "scope_denied",
        "Batch contains an unauthorized model or operation",
      );
    if (endpoint === "/v1/embeddings")
      embeddingInputs +=
        Array.isArray(body.input) && typeof body.input[0] !== "number" ? body.input.length : 1;
    if (endpoints[endpoint] === "images" || endpoints[endpoint] === "image_edits") {
      const count = body.n ?? 1;
      if (
        typeof count !== "number" ||
        !Number.isSafeInteger(count) ||
        count < 1 ||
        !Number.isSafeInteger(images + count)
      )
        throw new GatewayError(
          400,
          "invalid_batch",
          "Batch image counts must be positive integers within the batch budget",
        );
      images += count;
    }
    return copyExact(request, { ...request, body });
  });
  if (embeddingInputs > 50000)
    throw new GatewayError(400, "invalid_batch", "Embedding batch input limit exceeded");
  return {
    model,
    endpoint,
    operation: endpoints[endpoint] as Operation,
    requests,
    manifest: {
      items: requests.length,
      idsSha256: batchIdentityDigest(ids),
      ...(images ? { images } : {}),
    },
  };
}

export async function parseBatchInput(file: File, auth: Auth): Promise<BatchInput> {
  const batch = parseBatchFile(new Uint8Array(await file.arrayBuffer()));
  if (
    (auth.credential.models.length && !auth.credential.models.includes(batch.model)) ||
    !auth.credential.operations.includes(batch.operation)
  )
    throw new GatewayError(
      403,
      "scope_denied",
      "Batch contains an unauthorized model or operation",
    );
  return batch;
}

export async function readBatchManifest(
  deps: Dependencies,
  file: Resource,
  signal: AbortSignal,
  reserveMemory?: (bytes: number) => void,
): Promise<BatchManifest> {
  if (file.batch_manifest) return file.batch_manifest;
  const response = await queryResource(deps, file, "content", signal, reserveMemory);
  const bytes = await readBounded(response.body, deps.config.MAX_BODY_BYTES, signal, reserveMemory);
  const batch = parseBatchFile(bytes);
  if (batch.endpoint !== file.batch_endpoint || batch.model !== file.batch_upstream_model)
    throw new GatewayError(
      422,
      "invalid_batch",
      "Stored batch content does not match its registered endpoint and model",
    );
  return batch.manifest;
}

export interface BatchPreparation {
  route: Route;
  maxOutputTokens: number;
}

async function prepareBatchRequests(
  deps: Dependencies,
  auth: Auth,
  input: BatchInput,
  preparation: BatchPreparation,
  accept?: (request: Record<string, unknown>) => void,
): Promise<void> {
  const { route, maxOutputTokens } = preparation;
  for (const request of input.requests) {
    const original = object(request.body);
    const body = copyExact(original, { ...original });
    rejectUnmeteredTools(body, input.operation);
    applyOutputBound(body, input.operation, maxOutputTokens);
    const references = await resolveFileReferences(
      deps,
      auth.project.id,
      body,
      auth.credential.models,
    );
    if (references.providerId && references.providerId !== route.provider_id)
      throw new GatewayError(
        422,
        "resource_provider_mismatch",
        "Batch file references a different provider",
      );
    const prepared = prepareOperation(
      references.body,
      input.operation,
      route.kind,
      route.upstream_model,
      route.request_policy,
    );
    accept?.(copyExact(request, { ...request, body: prepared.body }));
  }
}

export async function prepareBatchInput(
  deps: Dependencies,
  auth: Auth,
  input: BatchInput,
  selected: Route,
): Promise<BatchPreparation> {
  const candidates = (await deps.store.routes(input.model, input.operation)).filter(
    (route) => route.provider_id === selected.provider_id && route.kind === "openai",
  );
  const policy = await deps.store.model(input.model);
  let failure: unknown = new GatewayError(
    422,
    "resource_provider_mismatch",
    "Batch model has no compatible route on the upload provider",
  );
  for (const route of candidates) {
    const preparation = { route, maxOutputTokens: policy.max_output_tokens };
    try {
      await prepareBatchRequests(deps, auth, input, preparation);
      return preparation;
    } catch (error) {
      if (!(error instanceof GatewayError)) throw error;
      failure = error;
    }
  }
  throw failure;
}

export async function rewriteBatchInput(
  deps: Dependencies,
  auth: Auth,
  input: BatchInput,
  preparation: BatchPreparation,
): Promise<{ file: File; upstreamModel: string }> {
  const rewritten: string[] = [];
  await prepareBatchRequests(deps, auth, input, preparation, (request) => {
    rewritten.push(stringifyExact(request));
  });
  return {
    file: new File([rewritten.join("\n") + "\n"], "batch-input.jsonl", {
      type: "application/jsonl",
    }),
    upstreamModel: preparation.route.upstream_model,
  };
}
