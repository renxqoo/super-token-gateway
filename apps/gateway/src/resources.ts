import {
  GatewayError,
  extractUsage,
  providerObject,
  object,
  optionalObject,
  text,
  parseProviderJson,
  stringifyExact,
  copyExact,
  sha256,
} from "@gateway/kernel";
import type { BatchResourceMetadata, Operation, Resource, Route } from "@gateway/kernel";
import { readBounded } from "@gateway/runtime";
import type { Auth, Dependencies } from "./context.ts";
import { batchUsage } from "./batch-usage.ts";
import { upstreamHeaders } from "./upstream.ts";

const finalStates = new Set(["completed", "succeeded", "SUCCEEDED", "COMPLETED"]);
const states = new Set([
  "queued",
  "pending",
  "in_progress",
  "running",
  "completed",
  "succeeded",
  "failed",
  "cancelled",
  "canceled",
  "cancelling",
  "validating",
  "finalizing",
  "expired",
]);

export function resourceKind(operation: Operation): string | undefined {
  return operation === "responses"
    ? "response"
    : operation === "videos"
      ? "video"
      : operation === "batches"
        ? "batch"
        : operation === "files"
          ? "file"
          : undefined;
}

export async function rememberResource(
  deps: Dependencies,
  auth: Auth,
  kind: string,
  model: string,
  body: Record<string, unknown>,
  route: Route,
  attemptId: string,
  batch?: BatchResourceMetadata,
): Promise<Record<string, unknown>> {
  const remote = text(body.id ?? body.name);
  if (!remote)
    throw new GatewayError(502, "invalid_upstream", "Provider omitted resource identity");
  const id = await deps.store.saveResource(
    auth.project.id,
    route.provider_id,
    remote,
    kind,
    model,
    attemptId,
    undefined,
    batch,
  );
  if (kind === "batch" && finalStates.has(text(body.status)))
    await deps.store.updateResource(id, "PENDING_USAGE", text(body.status).toLowerCase());
  else if (finalStates.has(text(body.status)))
    await deps.store.updateResource(
      id,
      kind === "file" || extractUsage(body, route.kind) ? "COMPLETED" : "PENDING_USAGE",
      text(body.status).toLowerCase(),
    );
  else if (states.has(text(body.status).toLowerCase()))
    await deps.store.updateResource(id, "ACTIVE", text(body.status).toLowerCase());
  return copyExact(body, { ...body, id, ...(body.name ? { name: id } : {}) });
}

export async function resolveFileReferences(
  deps: Dependencies,
  projectId: string,
  body: Record<string, unknown>,
  allowedModels: string[] = [],
): Promise<{ body: Record<string, unknown>; providerId?: string }> {
  let providerId: string | undefined;
  let nodes = 0;
  const references = new Map<string, Promise<Resource>>();
  const resolve = async (entry: string, kind: string) => {
    const key = `${kind}:${entry}`;
    let pending = references.get(key);
    if (!pending) {
      if (references.size >= 128)
        throw new GatewayError(400, "resource_limit", "Too many resource references");
      pending = deps.store.resource(entry, projectId, kind);
      references.set(key, pending);
    }
    const resource = await pending;
    if (allowedModels.length && (!resource.model || !allowedModels.includes(resource.model)))
      throw new GatewayError(404, "not_found", "Resource not found");
    if (providerId && providerId !== resource.provider_id)
      throw new GatewayError(
        422,
        "resource_provider_mismatch",
        "Resources belong to different providers",
      );
    providerId = resource.provider_id;
    return resource.remote_id;
  };
  const visit = async (value: unknown, depth: number, parent = ""): Promise<unknown> => {
    if (++nodes > 100000 || depth > 64)
      throw new GatewayError(400, "invalid_request", "Request nesting exceeds limit");
    if (Array.isArray(value))
      return copyExact(
        value,
        await Promise.all(value.map((entry) => visit(entry, depth + 1, parent))),
      );
    if (typeof value !== "object" || value === null) return value;
    const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    const node = value as Record<string, unknown>;
    for (const [key, entry] of Object.entries(value)) {
      if (
        ["parameters", "input_schema", "schema", "properties"].includes(key) ||
        (key === "input" && node.type === "tool_use") ||
        (key === "args" && parent === "functionCall") ||
        (key === "response" && parent === "functionResponse")
      ) {
        result[key] = entry;
        continue;
      }
      if (
        [
          "conversation",
          "container_id",
          "vector_store_ids",
          "vector_store_id",
          "thread_id",
          "assistant_id",
          "cachedContent",
          "cached_content",
        ].includes(key) ||
        (key === "container" && typeof entry === "string") ||
        (key === "prompt" && typeof optionalObject(entry).id === "string") ||
        key === "fileData" ||
        (key === "file_data" && typeof entry === "object")
      )
        throw new GatewayError(
          422,
          "unmanaged_resource",
          "Stored provider resources must be accessed through gateway-owned identities",
        );
      if (key === "file_ids") {
        if (!Array.isArray(entry) || entry.some((id) => typeof id !== "string"))
          throw new GatewayError(
            400,
            "invalid_request",
            "file_ids must contain owned file identities",
          );
        result[key] = await Promise.all(entry.map((id) => resolve(String(id), "file")));
      } else if (["file_id", "input_file_id", "previous_response_id"].includes(key)) {
        if (typeof entry !== "string")
          throw new GatewayError(400, "invalid_request", "Resource identity must be a string");
        result[key] = await resolve(entry, key === "previous_response_id" ? "response" : "file");
      } else result[key] = await visit(entry, depth + 1, key);
    }
    return copyExact(value, result);
  };
  const resolved = object(await visit(body, 0));
  return { body: resolved, ...(providerId ? { providerId } : {}) };
}

function resourcePath(resource: Resource, route: Route): string {
  if (route.kind === "gemini" && resource.kind === "video") {
    if (!/^operations\/[A-Za-z0-9_-]+$/.test(resource.remote_id))
      throw new GatewayError(502, "invalid_upstream", "Invalid operation identity");
    return resource.remote_id;
  }
  return `${resource.kind === "file" ? "files" : resource.kind === "batch" ? "batches" : resource.kind === "response" ? "responses" : "videos"}/${encodeURIComponent(resource.remote_id)}`;
}

export async function queryResource(
  deps: Dependencies,
  resource: Resource,
  action: "get" | "content" | "delete" | "cancel" | "input_items",
  signal: AbortSignal,
  reserveMemory?: (bytes: number) => void,
): Promise<Response> {
  const lease = reserveMemory ? undefined : deps.memory.lease();
  reserveMemory ??= lease?.update;
  try {
    const route = await deps.store.provider(resource.provider_id);
    const suffix =
      action === "input_items"
        ? "/input_items"
        : action === "content"
          ? "/content"
          : action === "cancel"
            ? route.kind === "gemini"
              ? ":cancel"
              : "/cancel"
            : "";
    const url = new URL(
      `${route.base_url.replace(/\/$/, "")}/${resourcePath(resource, route)}${suffix}`,
    );
    const method = action === "delete" ? "DELETE" : action === "cancel" ? "POST" : "GET";
    let response = await deps.transport.fetch(
      url,
      { method, headers: upstreamHeaders(route, deps.config), signal },
      deps.config.ALLOW_PRIVATE_UPSTREAM,
      deps.config.PRIVATE_UPSTREAM_CIDRS,
    );
    if (!response.ok) {
      await response.body?.cancel();
      throw new GatewayError(
        response.status === 404 ? 404 : 502,
        "resource_upstream_failed",
        "Original provider could not complete the resource operation",
      );
    }
    if (action === "content") {
      return new Response(response.body, {
        headers: {
          "content-type": response.headers.get("content-type") ?? "application/octet-stream",
          "cache-control": "no-store",
        },
      });
    }
    const data = providerObject(
      parseProviderJson(
        new TextDecoder().decode(
          await readBounded(response.body, deps.config.MAX_RESPONSE_BYTES, signal, reserveMemory),
        ),
      ),
    );
    const status = text(data.status).toLowerCase();
    let metered = resource.kind === "batch" ? undefined : extractUsage(data, route.kind);
    let complete = true;
    const batchVerified =
      resource.kind === "batch" && ["COMPLETED", "CANCELLED", "FAILED"].includes(resource.state);
    if (
      resource.kind === "batch" &&
      !batchVerified &&
      ["completed", "succeeded", "cancelled", "canceled", "failed", "expired"].includes(status)
    ) {
      try {
        const dimensions = resource.attempt_id
          ? await deps.store.usageDimensions(resource.attempt_id)
          : [];
        const evidence = await batchUsage(
          deps,
          route,
          data,
          dimensions,
          resource.batch_endpoint ?? text(data.endpoint),
          resource.batch_manifest,
          signal,
          reserveMemory,
        );
        metered = evidence?.usage;
        complete = evidence?.complete ?? false;
      } catch (error) {
        signal.throwIfAborted();
        if (!(error instanceof GatewayError) || ![502, 503].includes(error.status)) throw error;
        deps.metrics.increment("batch_usage_unavailable_total");
        complete = false;
      }
    }
    if (
      metered &&
      resource.attempt_id &&
      (finalStates.has(status) ||
        data.done === true ||
        ["cancelled", "canceled", "failed", "expired"].includes(status))
    ) {
      await deps.usage.publish({
        attemptId: resource.attempt_id,
        source: complete
          ? resource.kind !== "batch"
            ? "async_final"
            : finalStates.has(status)
              ? "batch_completed"
              : ["cancelled", "canceled"].includes(status)
                ? "batch_cancelled"
                : "batch_failed"
          : `async_partial_${sha256(stringifyExact(metered)).slice(0, 24)}`,
        state:
          finalStates.has(status) || (data.done === true && !data.error) ? "SUCCEEDED" : "FAILED",
        finality: complete ? "FINAL" : "PARTIAL",
        usage: { ...metered, requests: "1" },
      });
    }
    if (states.has(status) && !batchVerified)
      await deps.store.updateResource(
        resource.id,
        finalStates.has(status)
          ? (metered && complete) || resource.kind === "file"
            ? "COMPLETED"
            : "PENDING_USAGE"
          : ["failed", "expired"].includes(status)
            ? metered && complete
              ? "FAILED"
              : "PENDING_USAGE"
            : ["cancelled", "canceled"].includes(status)
              ? metered && complete
                ? "CANCELLED"
                : "PENDING_USAGE"
              : "ACTIVE",
        status,
      );
    if (data.done === true)
      await deps.store.updateResource(
        resource.id,
        data.error ? "FAILED" : metered && complete ? "COMPLETED" : "PENDING_USAGE",
        data.error ? "failed" : "completed",
      );
    if (action === "delete") await deps.store.updateResource(resource.id, "DELETED");
    // Batch 输出文件只登记上游引用，内容始终由上游提供。
    for (const field of ["output_file_id", "error_file_id"])
      if (typeof data[field] === "string") {
        data[field] = await deps.store.saveResource(
          resource.project_id,
          resource.provider_id,
          data[field],
          "file",
          resource.model,
        );
      }
    data.id = resource.id;
    if (data.name) data.name = resource.id;
    return new Response(stringifyExact(data), {
      status: response.status,
      headers: { "content-type": "application/json" },
    });
  } finally {
    lease?.close();
  }
}

export async function videoContent(
  deps: Dependencies,
  resource: Resource,
  signal: AbortSignal,
  reserveMemory?: (bytes: number) => void,
): Promise<Response> {
  const route = await deps.store.provider(resource.provider_id);
  if (route.kind !== "gemini")
    return queryResource(deps, resource, "content", signal, reserveMemory);
  const lease = reserveMemory ? undefined : deps.memory.lease();
  reserveMemory ??= lease?.update;
  try {
    const url = new URL(`${route.base_url.replace(/\/$/, "")}/${resourcePath(resource, route)}`);
    const result = await deps.transport.fetch(
      url,
      { headers: upstreamHeaders(route, deps.config), signal },
      deps.config.ALLOW_PRIVATE_UPSTREAM,
      deps.config.PRIVATE_UPSTREAM_CIDRS,
    );
    if (!result.ok) {
      await result.body?.cancel();
      throw new GatewayError(502, "provider_failed", "Video status lookup failed");
    }
    const body = providerObject(
      parseProviderJson(
        new TextDecoder().decode(
          await readBounded(result.body, deps.config.MAX_RESPONSE_BYTES, signal, reserveMemory),
        ),
      ),
    );
    const output = optionalObject(body.response);
    const generated =
      output.generatedVideos ?? optionalObject(output.generateVideoResponse).generatedSamples;
    const first = Array.isArray(generated) ? optionalObject(generated[0]) : {};
    const uri = text(optionalObject(first.video).uri);
    if (!uri) throw new GatewayError(409, "not_ready", "Video content is not available");
    const location = new URL(uri);
    const headers =
      location.origin === new URL(route.base_url).origin
        ? upstreamHeaders(route, deps.config)
        : new Headers();
    const response = await deps.transport.fetch(
      location,
      { headers, signal },
      deps.config.ALLOW_PRIVATE_UPSTREAM,
      location.origin === new URL(route.base_url).origin ? deps.config.PRIVATE_UPSTREAM_CIDRS : [],
    );
    if (!response.ok) {
      await response.body?.cancel();
      throw new GatewayError(502, "provider_failed", "Video content fetch failed");
    }
    return new Response(response.body, {
      headers: {
        "content-type": response.headers.get("content-type") ?? "video/mp4",
        "cache-control": "no-store",
      },
    });
  } finally {
    lease?.close();
  }
}
