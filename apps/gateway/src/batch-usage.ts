import {
  GatewayError,
  batchIdentityDigest,
  extractUsage,
  optionalObject,
  parseProviderJson,
} from "@gateway/kernel";
import type { BatchManifest, Route, Usage } from "@gateway/kernel";
import { imageCount } from "@gateway/connectors";
import type { Dependencies } from "./context.ts";
import { upstreamHeaders } from "./upstream.ts";

function scaledSeconds(value: string): bigint {
  const [seconds, fraction = ""] = value.split(".");
  return BigInt(`${seconds}${fraction.padEnd(18, "0")}`);
}

function addDuration(left: string, right: string): string {
  const sum = (scaledSeconds(left) + scaledSeconds(right)).toString().padStart(19, "0");
  const seconds = sum.slice(0, -18);
  const fraction = sum.slice(-18).replace(/0+$/, "");
  return fraction ? `${seconds}.${fraction}` : seconds;
}

// Scan upstream result streams in bounded memory. Only numeric counters escape this function.
export async function batchUsage(
  deps: Dependencies,
  route: Route,
  body: Record<string, unknown>,
  dimensions: readonly (keyof Usage)[],
  endpoint: string,
  manifest: BatchManifest | null,
  signal: AbortSignal,
  reserveMemory?: (bytes: number) => void,
): Promise<{ usage: Usage; complete: boolean } | undefined> {
  const totals: Usage = {};
  let rows = 0;
  let metered = 0;
  let complete = true;
  const incompleteMilliseconds = new Set<keyof Usage>();
  const files = new Set<string>();
  const identities = new Set<string>();
  const dimensionCounts: Partial<Record<keyof Usage, number>> = {};
  const measureImages =
    endpoint === "/v1/images/generations" ||
    endpoint === "/v1/images/edits" ||
    (!endpoint && dimensions.includes("images"));
  for (const field of ["output_file_id", "error_file_id"]) {
    const id = body[field];
    if (typeof id !== "string" || !id) continue;
    if (!/^[A-Za-z0-9_-]{1,500}$/.test(id))
      throw new GatewayError(502, "invalid_upstream", "Invalid batch result identity");
    if (files.has(id)) {
      complete = false;
      continue;
    }
    files.add(id);
    const url = new URL(
      `${route.base_url.replace(/\/$/, "")}/files/${encodeURIComponent(id)}/content`,
    );
    const response = await deps.transport.fetch(
      url,
      { headers: upstreamHeaders(route, deps.config), signal },
      deps.config.ALLOW_PRIVATE_UPSTREAM,
      deps.config.PRIVATE_UPSTREAM_CIDRS,
    );
    if (!response.ok || !response.body) {
      await response.body?.cancel();
      throw new GatewayError(502, "batch_usage_unavailable", "Batch usage evidence is unavailable");
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const decode = (chunk?: Uint8Array): string => {
      try {
        return decoder.decode(chunk, { stream: chunk !== undefined });
      } catch {
        throw new GatewayError(
          502,
          "batch_usage_unavailable",
          "Batch result contains invalid UTF-8",
        );
      }
    };
    let pending = "";
    let bytes = 0;
    const consume = (line: string) => {
      reserveMemory?.(-new TextEncoder().encode(line + "\n").byteLength);
      if (!line.trim()) return;
      if (++rows > 50000)
        throw new GatewayError(502, "batch_usage_limit", "Batch result count exceeds limit");
      const item = optionalObject(parseProviderJson(line));
      const result = optionalObject(item.response);
      const identity = item.custom_id;
      const successful =
        typeof result.status_code === "number" &&
        result.status_code >= 200 &&
        result.status_code < 300;
      if (
        typeof identity !== "string" ||
        identity.length < 1 ||
        identity.length > 200 ||
        identities.has(identity) ||
        (successful && item.error !== undefined && item.error !== null)
      ) {
        complete = false;
        return;
      }
      identities.add(identity);
      const body = optionalObject(result.body);
      const imageOutput = measureImages && successful ? imageCount(body.data) : 0;
      let usage = extractUsage(body, route.kind);
      if (imageOutput) usage = { ...usage, images: String(imageOutput) };
      if (successful && !dimensions.length) usage ??= {};
      if (!usage) {
        complete = false;
        return;
      }
      metered++;
      if (dimensions.some((dimension) => usage[dimension] === undefined)) complete = false;
      if (usage.audio_seconds !== undefined && usage.audio_ms === undefined)
        incompleteMilliseconds.add("audio_ms");
      if (usage.video_seconds !== undefined && usage.video_ms === undefined)
        incompleteMilliseconds.add("video_ms");
      for (const [dimension, value] of Object.entries(usage)) {
        const key = dimension as keyof Usage;
        dimensionCounts[key] = (dimensionCounts[key] ?? 0) + 1;
        if (incompleteMilliseconds.has(key)) continue;
        const previous = totals[key] ?? "0";
        const total =
          key === "audio_seconds" || key === "video_seconds"
            ? addDuration(previous, value)
            : (BigInt(previous) + BigInt(value)).toString();
        if (total.length > 38 && (total.split(".")[0]?.length ?? 0) > 38) {
          if (key === "audio_ms" || key === "video_ms") {
            incompleteMilliseconds.add(key);
            continue;
          }
          throw new GatewayError(502, "batch_usage_limit", "Batch usage exceeds numeric budget");
        }
        totals[key] = total;
      }
    };
    const abort = () => {
      void reader.cancel().catch(() => undefined);
    };
    signal.addEventListener("abort", abort, { once: true });
    try {
      while (true) {
        signal.throwIfAborted();
        const chunk = await reader.read().catch(() => {
          signal.throwIfAborted();
          throw new GatewayError(
            502,
            "batch_usage_unavailable",
            "Batch result stream was interrupted",
          );
        });
        if (chunk.done) break;
        reserveMemory?.(chunk.value.byteLength);
        bytes += chunk.value.byteLength;
        if (bytes > 268435456)
          throw new GatewayError(502, "batch_usage_limit", "Batch scan exceeds byte budget");
        pending += decode(chunk.value);
        let newline: number;
        while ((newline = pending.indexOf("\n")) >= 0) {
          const line = pending.slice(0, newline);
          pending = pending.slice(newline + 1);
          if (line.length > deps.config.MAX_RESPONSE_BYTES)
            throw new GatewayError(502, "batch_usage_limit", "Batch result line exceeds limit");
          consume(line);
        }
        if (pending.length > deps.config.MAX_RESPONSE_BYTES)
          throw new GatewayError(502, "batch_usage_limit", "Batch result line exceeds limit");
      }
      signal.throwIfAborted();
      pending += decode();
      if (pending) consume(pending);
    } finally {
      signal.removeEventListener("abort", abort);
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }
  const expected = optionalObject(body.request_counts).total;
  if (
    (!manifest || expected !== undefined) &&
    (!Number.isSafeInteger(expected) || expected !== rows)
  )
    complete = false;
  if (
    manifest &&
    (manifest.items !== rows || manifest.idsSha256 !== batchIdentityDigest(identities))
  )
    return metered ? { usage: {}, complete: false } : undefined;
  const coverage = typeof expected === "number" ? Math.max(rows, expected) : rows;
  for (const key of Object.keys(totals) as (keyof Usage)[])
    if (dimensionCounts[key] !== coverage) delete totals[key];
  for (const dimension of incompleteMilliseconds) {
    delete totals[dimension];
    if (dimensions.includes(dimension)) complete = false;
  }
  return metered ? { usage: totals, complete: complete && rows === metered } : undefined;
}
