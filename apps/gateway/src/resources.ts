import {
  GatewayError,
  resourceActions,
  resourceKinds,
  type TransientSend,
} from "@gateway/contracts";
import { readBounded, validateBaseUrl } from "@gateway/runtime";
import { authorize, coreFailure, type ChatDependencies } from "./chat.ts";

type ResourceKind = (typeof resourceKinds)[number];
type ResourceAction = (typeof resourceActions)[number];
type Resolution = {
  resource_id: string;
  kind: ResourceKind;
  remote_id: string;
  model: string | null;
  package_digest: string;
  config: Record<string, unknown>;
  origin: string;
  deadline_at: string;
};
type Permit = {
  resolution: Resolution;
  state: "RESOLVED" | "GRANTED" | "REPLAYED" | "DELETED";
  auth_material?: TransientSend["auth_material"];
};
export type ResourceRequest = {
  requestId: string;
  token: string;
  ip: string;
  signal: AbortSignal;
};

function deadline(seconds = 30) {
  return new Date(Date.now() + seconds * 1000).toISOString();
}

async function permit(
  deps: ChatDependencies,
  request: ResourceRequest,
  data: Record<string, unknown>,
  commandId?: string,
) {
  try {
    return (await deps.core.execution(
      "execution_resource_v1",
      { customer_token: request.token, ip: request.ip, ...data },
      { ...(commandId ? { commandId } : {}), deadlineAt: deadline() },
    )) as Permit;
  } catch (error) {
    throw coreFailure(error);
  }
}

export function resourceURL(deps: ChatDependencies, base: string, path: string) {
  try {
    const origin = validateBaseUrl(
      base,
      deps.config.allow_private_upstream,
      deps.config.private_upstream_cidrs,
    );
    const url = new URL(path, origin);
    if (url.origin !== origin.origin) throw new Error("origin");
    return url;
  } catch {
    throw new GatewayError(502, "unsafe_upstream", "The upstream address is not permitted");
  }
}

function upstreamFailure(status: number) {
  return status === 404
    ? new GatewayError(404, "not_found", "Resource not found")
    : new GatewayError(
        502,
        "resource_upstream_failed",
        "Original provider could not complete the resource operation",
      );
}

export async function executeResource(
  deps: ChatDependencies,
  request: ResourceRequest,
  resourceId: string,
  action: ResourceAction,
) {
  if (!/^(response|file|batch|video)_[0-9a-f]{32}$/.test(resourceId))
    throw new GatewayError(404, "not_found", "Resource not found");
  const resolved = await permit(deps, request, { resource_id: resourceId, action });
  const resolution = resolved.resolution,
    deadlineAt = Date.parse(resolution.deadline_at);
  const pkg = await deps.packages
    .get(resolution.package_digest, resolution.deadline_at)
    .catch(() => {
      throw new GatewayError(503, "plugin_unavailable", "The resource's plugin is unavailable");
    });
  const owner = await deps.core.startup();
  const attempt = deps.host.create(pkg, {
    tenant: resolution.resource_id,
    attempt: crypto.randomUUID(),
    incarnation: owner.incarnation_id,
    config: resolution.config,
    deadlineAt,
  });
  try {
    const prepared = await attempt.invoke(
      "resource_prepare",
      new TextEncoder().encode(
        JSON.stringify({ kind: resolution.kind, action, remote_id: resolution.remote_id }),
      ),
      request.signal,
    );
    if (prepared.kind === "failure")
      throw new GatewayError(
        prepared.status === 502 ? 502 : 422,
        prepared.status === 502 ? "plugin_invalid" : "unsupported_capability",
        "The resource's plugin cannot perform this action",
      );
    if (prepared.kind !== "resource_request")
      throw new GatewayError(502, "plugin_invalid", "The resource's plugin returned no request");
    const url = resourceURL(deps, resolution.origin, prepared.request.path);
    const commandId = crypto.randomUUID();
    const granted = await permit(
      deps,
      request,
      { resource_id: resourceId, action, auth: prepared.request.auth },
      commandId,
    );
    if (granted.state !== "GRANTED" || !granted.auth_material)
      throw new GatewayError(503, "resource_unavailable", "The resource action was not granted");
    const signal = AbortSignal.any([
      request.signal,
      AbortSignal.timeout(Math.max(1, deadlineAt - Date.now())),
    ]);
    const empty = new Uint8Array();
    const response = await deps.transport.fetch(
      url,
      {
        method: prepared.request.method,
        headers: authorize(prepared.request.headers, granted.auth_material, url, empty),
        signal,
      },
      deps.config.allow_private_upstream,
      deps.config.private_upstream_cidrs,
    );
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      throw upstreamFailure(response.status);
    }
    const bytes = await readBounded(response.body, deps.config.max_response_bytes, signal);
    const transformed = await attempt.transformJSON(
      "resource_response",
      {
        action,
        resource_id: resolution.resource_id,
        ...(resolution.model ? { model: resolution.model } : {}),
      },
      bytes,
      signal,
    );
    if (transformed.result.kind !== "output" || !transformed.body)
      throw new GatewayError(502, "upstream_invalid_response", "The upstream response was invalid");
    if (action === "delete")
      await permit(deps, request, {
        resource_id: resourceId,
        action,
        granted_command_id: commandId,
      });
    return new Response(transformed.body.slice(), {
      status: 200,
      headers: { "content-type": "application/json", "x-request-id": request.requestId },
    });
  } finally {
    attempt.dispose();
  }
}

export async function listResources(
  deps: ChatDependencies,
  request: ResourceRequest,
  kind: ResourceKind,
  query: { limit?: string | undefined; after?: string | undefined },
) {
  const limit = query.limit === undefined ? 100 : Number(query.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 100)
    throw new GatewayError(400, "invalid_request", "limit must be an integer from 1 to 100");
  if (query.after !== undefined && !new RegExp(`^${kind}_[0-9a-f]{32}$`).test(query.after))
    throw new GatewayError(404, "not_found", "Resource cursor not found");
  let page: { data: { id: string; state: string; created_at: number }[] };
  try {
    page = (await deps.core.execution(
      "execution_resource_list_v1",
      {
        customer_token: request.token,
        ip: request.ip,
        kind,
        limit,
        ...(query.after ? { after: query.after } : {}),
      },
      { deadlineAt: deadline(10) },
    )) as typeof page;
  } catch (error) {
    throw coreFailure(error);
  }
  const rows = page.data.slice(0, limit);
  return Response.json(
    {
      object: "list",
      has_more: page.data.length > limit,
      first_id: rows[0]?.id ?? null,
      last_id: rows.at(-1)?.id ?? null,
      data: rows.map((row) => ({
        id: row.id,
        object: kind,
        status: row.state,
        created_at: row.created_at,
      })),
    },
    { headers: { "x-request-id": request.requestId } },
  );
}
