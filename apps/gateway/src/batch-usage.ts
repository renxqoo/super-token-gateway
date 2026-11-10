import { GatewayError, extractUsage, optionalObject, parseProviderJson } from "@gateway/kernel";
import type { Route, Usage } from "@gateway/kernel";
import type { Dependencies } from "./context.ts";
import { upstreamHeaders } from "./upstream.ts";

// Scan upstream result streams in bounded memory. Only numeric counters escape this function.
export async function batchUsage(
  deps: Dependencies,
  route: Route,
  body: Record<string, unknown>,
  dimensions: readonly (keyof Usage)[],
  signal: AbortSignal,
  reserveMemory?: (bytes: number) => void,
): Promise<{ usage: Usage; complete: boolean } | undefined> {
  const totals: Usage = {};
  let rows = 0;
  let metered = 0;
  let complete = true;
  for (const field of ["output_file_id", "error_file_id"]) {
    const id = body[field];
    if (typeof id !== "string" || !id) continue;
    if (!/^[A-Za-z0-9_-]{1,500}$/.test(id))
      throw new GatewayError(502, "invalid_upstream", "Invalid batch result identity");
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
    let pending = "";
    let bytes = 0;
    const consume = (line: string) => {
      reserveMemory?.(-new TextEncoder().encode(line + "\n").byteLength);
      if (!line.trim()) return;
      if (++rows > 50000)
        throw new GatewayError(502, "batch_usage_limit", "Batch result count exceeds limit");
      const item = optionalObject(parseProviderJson(line));
      const usage = extractUsage(optionalObject(item.response).body, "openai");
      if (!usage) {
        complete = false;
        return;
      }
      metered++;
      if (dimensions.some((dimension) => usage[dimension] === undefined)) complete = false;
      for (const [dimension, value] of Object.entries(usage)) {
        if (!/^(0|[1-9][0-9]{0,37})$/.test(value)) {
          complete = false;
          continue;
        }
        const key = dimension as keyof Usage;
        const total = (BigInt(totals[key] ?? "0") + BigInt(value)).toString();
        if (total.length > 38)
          throw new GatewayError(502, "batch_usage_limit", "Batch usage exceeds numeric budget");
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
        const chunk = await reader.read();
        if (chunk.done) break;
        reserveMemory?.(chunk.value.byteLength);
        bytes += chunk.value.byteLength;
        if (bytes > 268435456)
          throw new GatewayError(502, "batch_usage_limit", "Batch scan exceeds byte budget");
        pending += decoder.decode(chunk.value, { stream: true });
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
      pending += decoder.decode();
      if (pending) consume(pending);
    } finally {
      signal.removeEventListener("abort", abort);
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }
  const expected = optionalObject(body.request_counts).total;
  if (expected !== undefined && (!Number.isSafeInteger(expected) || expected !== rows))
    complete = false;
  return metered ? { usage: totals, complete: complete && rows === metered } : undefined;
}
