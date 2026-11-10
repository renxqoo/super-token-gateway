import {
  copyExact,
  parseClientJson,
  stringifyExact,
  GatewayError,
  object,
  text,
} from "@gateway/kernel";
import type { Operation, Route } from "@gateway/kernel";
import type { Auth, Dependencies } from "./context.ts";
import { applyOutputBound } from "./input-policy.ts";
import { resolveFileReferences } from "./resources.ts";

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
}

export async function parseBatchInput(file: File, auth: Auth): Promise<BatchInput> {
  const source = new TextDecoder("utf-8", { fatal: true }).decode(await file.arrayBuffer());
  const lines = source.split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length || lines.length > 50000)
    throw new GatewayError(400, "invalid_batch", "Batch must contain 1 to 50000 requests");
  const ids = new Set<string>();
  let model = "";
  let endpoint = "";
  let embeddingInputs = 0;
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
    if (
      !model ||
      (auth.credential.models.length && !auth.credential.models.includes(model)) ||
      !auth.credential.operations.includes(endpoints[endpoint] as Operation)
    )
      throw new GatewayError(
        403,
        "scope_denied",
        "Batch contains an unauthorized model or operation",
      );
    if (endpoint === "/v1/embeddings")
      embeddingInputs +=
        Array.isArray(body.input) && typeof body.input[0] !== "number" ? body.input.length : 1;
    return copyExact(request, { ...request, body });
  });
  if (embeddingInputs > 50000)
    throw new GatewayError(400, "invalid_batch", "Embedding batch input limit exceeded");
  return { model, endpoint, operation: endpoints[endpoint] as Operation, requests };
}

export async function rewriteBatchInput(
  deps: Dependencies,
  auth: Auth,
  input: BatchInput,
  selected: Route,
): Promise<{ file: File; upstreamModel: string }> {
  const candidates = await deps.store.routes(input.model, input.operation);
  const route = candidates.find(
    (route) => route.provider_id === selected.provider_id && route.kind === "openai",
  );
  if (!route)
    throw new GatewayError(
      422,
      "resource_provider_mismatch",
      "Batch model has no compatible route on the upload provider",
    );
  const policy = await deps.store.model(input.model);
  const rewritten: string[] = [];
  for (const request of input.requests) {
    const original = object(request.body);
    const body = copyExact(original, { ...original });
    applyOutputBound(body, input.operation, policy.max_output_tokens);
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
    rewritten.push(
      stringifyExact(
        copyExact(request, {
          ...request,
          body: copyExact(references.body, { ...references.body, model: route.upstream_model }),
        }),
      ),
    );
  }
  return {
    file: new File([rewritten.join("\n") + "\n"], "batch-input.jsonl", {
      type: "application/jsonl",
    }),
    upstreamModel: route.upstream_model,
  };
}
