import type { resourceClaimResponseSchema } from "@gateway/contracts";
import { pricedDimensions } from "@gateway/plugin-host";
import { batchIdentityDigest, readBounded } from "@gateway/runtime";
import type { z } from "zod";
import { batchFiles, batchMetadata, parseBatchOutput, sumQuantities } from "./batches.ts";
import { authorize, type ChatDependencies } from "./chat.ts";
import { resourceURL } from "./resources.ts";

type Lease = z.infer<typeof resourceClaimResponseSchema>["leases"][number];
type Release = { state: "ACTIVE" | "COMPLETED" | "FAILED" | "CANCELLED"; retry: boolean };
type Source = "async_final" | "batch_completed" | "batch_cancelled" | "batch_failed";

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
    request_state: "SUCCEEDED" | "FAILED" | "CANCELLED";
    error_code?: string;
  },
  source: Source = "async_final",
) {
  await deps.publish([
    deps.core.signEvidence({
      event_id: crypto.randomUUID(),
      attempt_id: lease.attempt_id,
      permit_epoch: lease.permit_epoch,
      source,
      source_revision: lease.lease_epoch,
      final_dimensions: outcome.finality === "FINAL" ? pricedDimensions(outcome.usage) : [],
      ...outcome,
    }),
  ]);
}

type Opener = () => ReturnType<ChatDependencies["host"]["create"]>;

async function upstream(
  deps: ChatDependencies,
  lease: Lease,
  open: Opener,
  target: { kind: string; action: string; remote_id: string },
  signal: AbortSignal,
) {
  const resolution = lease.resolution;
  const attempt = open();
  try {
    const prepared = await attempt.invoke(
      "resource_prepare",
      new TextEncoder().encode(JSON.stringify(target)),
      signal,
    );
    if (prepared.kind !== "resource_request") return undefined;
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
    return await deps.transport.fetch(
      url,
      {
        method: prepared.request.method,
        headers: authorize(prepared.request.headers, granted.auth_material, url, new Uint8Array()),
        signal,
      },
      deps.config.allow_private_upstream,
      deps.config.private_upstream_cidrs,
    );
  } finally {
    attempt.dispose();
  }
}

const batchOutcomes: Record<
  string,
  {
    source: Source;
    release: Release["state"];
    attempt_state: "SUCCEEDED" | "FAILED";
    request_state: "SUCCEEDED" | "FAILED" | "CANCELLED";
  }
> = {
  completed: {
    source: "batch_completed",
    release: "COMPLETED",
    attempt_state: "SUCCEEDED",
    request_state: "SUCCEEDED",
  },
  cancelled: {
    source: "batch_cancelled",
    release: "CANCELLED",
    attempt_state: "SUCCEEDED",
    request_state: "CANCELLED",
  },
  failed: {
    source: "batch_failed",
    release: "FAILED",
    attempt_state: "FAILED",
    request_state: "FAILED",
  },
  expired: {
    source: "batch_failed",
    release: "FAILED",
    attempt_state: "FAILED",
    request_state: "FAILED",
  },
};

async function observeBatch(
  deps: ChatDependencies,
  lease: Lease,
  open: Opener,
  signal: AbortSignal,
): Promise<Release> {
  const resolution = lease.resolution,
    batch = resolution.batch;
  if (!batch || !resolution.model) return retry;
  const response = await upstream(
    deps,
    lease,
    open,
    { kind: "batch", action: "get", remote_id: resolution.remote_id },
    signal,
  );
  if (!response?.ok) {
    await response?.body?.cancel().catch(() => undefined);
    return retry;
  }
  const bytes = await readBounded(response.body, deps.config.max_response_bytes, signal);
  const viewer = open();
  let body: Uint8Array | undefined;
  try {
    const transformed = await viewer.transformJSON(
      "resource_response",
      { action: "get", resource_id: resolution.resource_id, model: resolution.model },
      bytes,
      signal,
    );
    if (transformed.result.kind !== "output") return retry;
    body = transformed.body;
  } finally {
    viewer.dispose();
  }
  if (!body) return retry;
  const observed = batchFiles(body),
    outcome = typeof observed.status === "string" ? batchOutcomes[observed.status] : undefined;
  if (!outcome) return { state: "ACTIVE", retry: false };
  const usage: Record<string, string> = {},
    seen = new Set<string>();
  let complete = true;
  for (const remote of [observed.files.output_file_id, observed.files.error_file_id]) {
    if (!remote) continue;
    const file = await upstream(
      deps,
      lease,
      open,
      { kind: "file", action: "content", remote_id: remote },
      signal,
    );
    if (!file?.ok) {
      await file?.body?.cancel().catch(() => undefined);
      return retry;
    }
    let lines: ReturnType<typeof parseBatchOutput>;
    try {
      lines = parseBatchOutput(
        await readBounded(file.body, deps.config.max_response_bytes, signal),
      );
    } catch {
      return retry;
    }
    for (const line of lines) {
      if (seen.has(line.customId)) complete = false;
      seen.add(line.customId);
    }
    const metadata = batchMetadata(batch.endpoint, resolution.model);
    let next = 0;
    const work = async () => {
      while (next < lines.length) {
        const line = lines[next++]!;
        if (!line.body) continue;
        const reader = open();
        try {
          const { result } = await reader.transformJSON("response", metadata, line.body, signal);
          if (result.kind === "output" && result.usage)
            sumQuantities(usage, result.usage as Record<string, string>);
          if (result.kind !== "output" || result.finality !== "FINAL") complete = false;
        } catch {
          complete = false;
        } finally {
          reader.dispose();
        }
      }
    };
    await Promise.all(Array.from({ length: 4 }, work));
  }
  if (seen.size !== batch.items || batchIdentityDigest(seen) !== batch.ids_sha256) complete = false;
  const priced = Object.keys(usage).length > 0;
  await record(
    deps,
    lease,
    {
      usage,
      finality: priced ? (complete ? "FINAL" : "PARTIAL") : "UNKNOWN",
      attempt_state: outcome.attempt_state,
      request_state: outcome.request_state,
      ...(outcome.attempt_state === "FAILED" ? { error_code: `batch_${observed.status}` } : {}),
    },
    outcome.source,
  );
  return { state: outcome.release, retry: false };
}

async function observe(deps: ChatDependencies, lease: Lease, stop: AbortSignal): Promise<Release> {
  const resolution = lease.resolution,
    deadlineAt = Date.parse(resolution.deadline_at);
  if (!["response", "batch"].includes(resolution.kind) || !resolution.model) return retry;
  const pkg = await deps.packages.get(resolution.package_digest, resolution.deadline_at);
  const owner = await deps.core.startup();
  const open: Opener = () =>
    deps.host.create(pkg, {
      tenant: resolution.resource_id,
      attempt: crypto.randomUUID(),
      incarnation: owner.incarnation_id,
      config: resolution.config,
      deadlineAt,
    });
  const signal = AbortSignal.any([stop, AbortSignal.timeout(Math.max(1, deadlineAt - Date.now()))]);
  if (resolution.kind === "batch") return observeBatch(deps, lease, open, signal);
  const attempt = open();
  try {
    const response = await upstream(
      deps,
      lease,
      open,
      { kind: resolution.kind, action: "get", remote_id: resolution.remote_id },
      signal,
    );
    if (!response) return retry;
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
