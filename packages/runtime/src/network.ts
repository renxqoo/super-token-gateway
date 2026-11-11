import { lookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { inCidrs } from "./ip.ts";
import { GatewayError } from "@gateway/kernel";
import { createHttpPool, UpstreamPreDispatchError } from "./http-pool.ts";
import type { PinnedRequestInit } from "./http-pool.ts";

export { UpstreamPreDispatchError };

export function createTransport(maxConnections = 1024) {
  const pool = createHttpPool(maxConnections);
  return {
    fetch(
      url: URL,
      init: RequestInit,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ) {
      return safeFetch(url, init, allowPrivate, privateCidrs, pool.send);
    },
    close: pool.close,
  };
}

const blocked = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.168.0.0", 16],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const)
  blocked.addSubnet(address, prefix, "ipv4");

const blockedV6 = new BlockList();
for (const [address, prefix] of [
  ["2001::", 23],
  ["2001:2::", 48],
  ["2001:db8::", 32],
  ["2001:10::", 28],
  ["2001:20::", 28],
  ["2002::", 16],
  ["3fff::", 20],
] as const)
  blockedV6.addSubnet(address, prefix, "ipv6");
const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");
export function isPublicAddress(address: string): boolean {
  if (isIP(address) === 4) return !blocked.check(address, "ipv4");
  return (
    isIP(address) === 6 && globalV6.check(address, "ipv6") && !blockedV6.check(address, "ipv6")
  );
}

export function validateBaseUrl(
  value: string,
  allowPrivate: boolean,
  privateCidrs: readonly string[] = [],
): URL {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !["https:", "http:"].includes(url.protocol)
  ) {
    throw new GatewayError(400, "unsafe_upstream", "Invalid upstream URL");
  }
  const literal = url.hostname.replace(/^\[|\]$/g, "");
  const permittedPrivate = isIP(literal) && inCidrs(literal, privateCidrs);
  if (!allowPrivate && !permittedPrivate && url.protocol !== "https:")
    throw new GatewayError(400, "unsafe_upstream", "Upstream requires HTTPS");
  const host = url.hostname.replace(/^\[|\]$/g, "");
  if (
    !allowPrivate &&
    !permittedPrivate &&
    (host === "localhost" || (isIP(host) && !isPublicAddress(host)))
  ) {
    throw new GatewayError(400, "unsafe_upstream", "Private upstream addresses are forbidden");
  }
  return url;
}

export async function safeFetch(
  url: URL,
  init: RequestInit,
  allowPrivate: boolean,
  privateCidrs: readonly string[] = [],
  send: (url: URL, init: PinnedRequestInit) => Promise<Response> = fetch,
): Promise<Response> {
  if (url.username || url.password || !["https:", "http:"].includes(url.protocol))
    throw new UpstreamPreDispatchError("unsafe_upstream", "Unsafe upstream URL");
  const signal = init.signal;
  const cancelled = () =>
    new UpstreamPreDispatchError(
      "upstream_not_dispatched",
      "Upstream request cancelled before dispatch",
    );
  if (signal?.aborted) throw cancelled();
  const hostname = url.hostname.replace(/^\[|\]$/g, "");
  const addresses = isIP(hostname)
    ? [{ address: hostname, family: isIP(hostname) }]
    : await new Promise<{ address: string; family: number }[]>((resolve, reject) => {
        const abort = () => reject(cancelled());
        signal?.addEventListener("abort", abort, { once: true });
        lookup(hostname, { all: true })
          .then(resolve, () =>
            reject(new UpstreamPreDispatchError("upstream_dns", "Upstream address lookup failed")),
          )
          .finally(() => signal?.removeEventListener("abort", abort));
        if (signal?.aborted) abort();
      });
  if (signal?.aborted) throw cancelled();
  const chosen = addresses.find((entry) => entry.family === 4) ?? addresses[0];
  if (!chosen) throw new UpstreamPreDispatchError("upstream_dns", "Upstream address lookup failed");
  if (
    !allowPrivate &&
    (addresses.some(
      (entry) => !isPublicAddress(entry.address) && !inCidrs(entry.address, privateCidrs),
    ) ||
      (url.protocol !== "https:" &&
        addresses.some((entry) => !inCidrs(entry.address, privateCidrs))))
  ) {
    throw new UpstreamPreDispatchError("unsafe_upstream", "Upstream address is not permitted");
  }
  const pinned = new URL(url);
  pinned.hostname = chosen.family === 6 ? `[${chosen.address}]` : chosen.address;
  const headers = new Headers(init.headers);
  headers.set("host", url.host);
  headers.set("accept-encoding", "identity");
  // URL 使用已经校验的 IP；Host、SNI 与证书验证仍绑定原域名，禁用环境代理与重定向。
  return send(pinned, {
    ...init,
    headers,
    redirect: "manual",
    proxy: false,
    ...(url.protocol === "https:"
      ? { tls: { serverName: hostname, rejectUnauthorized: true } }
      : {}),
  });
}

export async function readBounded(
  body: ReadableStream<Uint8Array> | null,
  maxBytes: number,
  signal?: AbortSignal,
  onBytes?: (bytes: number) => void,
): Promise<Uint8Array> {
  if (!body) return new Uint8Array();
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const abort = () => {
    void reader.cancel().catch(() => undefined);
  };
  signal?.addEventListener("abort", abort, { once: true });
  try {
    while (true) {
      signal?.throwIfAborted();
      const result = await reader.read();
      if (result.done) break;
      size += result.value.byteLength;
      if (size > maxBytes)
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      onBytes?.(result.value.byteLength);
      chunks.push(result.value);
    }
    signal?.throwIfAborted();
    if (chunks.length === 1) return chunks[0]!;
    onBytes?.(size);
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    chunks.length = 0;
    onBytes?.(-size);
    return output;
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  } finally {
    signal?.removeEventListener("abort", abort);
    reader.releaseLock();
  }
}
