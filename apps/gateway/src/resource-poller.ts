import type { resourceClaimResponseSchema } from "@gateway/contracts";
import { pricedDimensions } from "@gateway/plugin-host";
import { readBounded } from "@gateway/runtime";
import type { z } from "zod";
import { authorize, type ChatDependencies } from "./chat.ts";
import { resourceURL } from "./resources.ts";

type Lease = z.infer<typeof resourceClaimResponseSchema>["leases"][number];
type Release = { state: "ACTIVE" | "COMPLETED" | "FAILED"; retry: boolean };

const retry: Release = { state: "ACTIVE", retry: true };

function deadline(seconds: number) {
  return new Date(Date.now() + seconds * 1000).toISOString();
}

async function record(
  deps: ChatDependencies,
  lease: Lease,
  outcome: {
    usage: Record<string, string>;
    finality: "FINAL" | "PARTIAL" | "UNKNOWN";
    attempt_state: "SUCCEEDED" | "FAILED";
    request_state: "SUCCEEDED" | "FAILED";
    error_code?: string;
  },
) {
  await deps.publish([
    deps.core.signEvidence({
      event_id: crypto.randomUUID(),
      attempt_id: lease.attempt_id,
      permit_epoch: lease.permit_epoch,
      source: "async_final",
      source_revision: lease.lease_epoch,
      final_dimensions: outcome.finality === "FINAL" ? pricedDimensions(outcome.usage) : [],
      ...outcome,
    }),
  ]);
}

async function observe(deps: ChatDependencies, lease: Lease, stop: AbortSignal): Promise<Release> {
  const resolution = lease.resolution,
    deadlineAt = Date.parse(resolution.deadline_at);
  if (resolution.kind !== "response" || !resolution.model) return retry;
  const pkg = await deps.packages.get(resolution.package_digest, resolution.deadline_at);
  const owner = await deps.core.startup();
  const attempt = deps.host.create(pkg, {
    tenant: resolution.resource_id,
    attempt: crypto.randomUUID(),
    incarnation: owner.incarnation_id,
    config: resolution.config,
    deadlineAt,
  });
  try {
    const signal = AbortSignal.any([
      stop,
      AbortSignal.timeout(Math.max(1, deadlineAt - Date.now())),
    ]);
    const prepared = await attempt.invoke(
      "resource_prepare",
      new TextEncoder().encode(
        JSON.stringify({ kind: resolution.kind, action: "get", remote_id: resolution.remote_id }),
      ),
      signal,
    );
    if (prepared.kind !== "resource_request") return retry;
    const url = resourceURL(deps, resolution.origin, prepared.request.path);
    const granted = (await deps.core.execution(
      "execution_resource_poll_v1",
      {
        resource_id: resolution.resource_id,
        lease_epoch: lease.lease_epoch,
        auth: prepared.request.auth,
      },
      { commandId: crypto.randomUUID(), deadlineAt: resolution.deadline_at },
    )) as { auth_material: Parameters<typeof authorize>[1] };
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
      if (response.status !== 404 || resolution.state !== "DELETED") return retry;
      await record(deps, lease, {
        usage: {},
        finality: "UNKNOWN",
        attempt_state: "FAILED",
        request_state: "FAILED",
        error_code: "resource_deleted",
      });
      return { state: "FAILED", retry: false };
    }
    const bytes = await readBounded(response.body, deps.config.max_response_bytes, signal);
    const { result } = await attempt.transformJSON(
      "response",
      { protocol: "responses", model: resolution.model, resource_id: resolution.resource_id },
      bytes,
      signal,
    );
    if (result.kind === "failure") {
      if (result.code !== "upstream_error") return retry;
      await record(deps, lease, {
        usage: {},
        finality: "UNKNOWN",
        attempt_state: "FAILED",
        request_state: "FAILED",
        error_code: "upstream_error",
      });
      return { state: "FAILED", retry: false };
    }
    if (result.kind !== "output" || !result.terminal) return retry;
    if (result.terminal.kind === "pending") return { state: "ACTIVE", retry: false };
    await record(deps, lease, {
      usage: { ...result.usage } as Record<string, string>,
      finality: result.finality === "ESTIMATED" ? "UNKNOWN" : result.finality,
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    return { state: "COMPLETED", retry: false };
  } finally {
    attempt.dispose();
  }
}

export async function pollResources(
  deps: ChatDependencies,
  stop: AbortSignal = new AbortController().signal,
  limit = 4,
) {
  const claimed = (await deps.core.execution(
    "execution_resource_claim_v1",
    { limit },
    { deadlineAt: deadline(10) },
  )) as { leases: Lease[] };
  await Promise.all(
    claimed.leases.map(async (lease) => {
      const outcome = await observe(deps, lease, stop).catch(() => retry);
      await deps.core
        .execution(
          "execution_resource_release_v1",
          { resource_id: lease.resource_id, lease_epoch: lease.lease_epoch, ...outcome },
          { deadlineAt: deadline(10) },
        )
        .catch(() => undefined);
    }),
  );
  return claimed.leases.length;
}

export function startResourcePolling(
  deps: ChatDependencies,
  intervalMs: number,
  onError: (error: unknown) => void,
) {
  const stop = new AbortController();
  let stopped = false,
    wake: (() => void) | undefined;
  const loop = (async () => {
    while (!stopped) {
      let claimed = 0;
      try {
        claimed = await pollResources(deps, stop.signal);
      } catch (error) {
        onError(error);
      }
      if (stopped || claimed > 0) continue;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, intervalMs);
        wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      wake = undefined;
    }
  })();
  return {
    async close() {
      stopped = true;
      stop.abort();
      wake?.();
      await loop;
    },
  };
}
