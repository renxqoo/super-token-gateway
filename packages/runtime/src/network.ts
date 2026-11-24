import { lookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { inCidrs } from "./ip.ts";
import { GatewayError } from "@gateway/contracts";
import type { DialLimits } from "./dial-scheduler.ts";
import { isValidHttpHeaderName, isValidHttpHeaderValue } from "./bun-http1.ts";
import {
  createHttpPool,
  UpstreamPreDispatchError,
  type CancelRegistrar,
  type FastUpstreamResult,
  type PinnedRequestInit,
} from "./http-pool.ts";

export { UpstreamPreDispatchError, isValidHttpHeaderName, isValidHttpHeaderValue };
export type { CancelRegistrar, FastUpstreamResult };

export type DnsAddress = { address: string; family: number };
export type DnsLookupFn = (hostname: string) => Promise<DnsAddress[]>;

export type TransportOptions = {
  dnsCacheTtlMs?: number;
  dnsCacheMaxEntries?: number;
  dnsLookup?: DnsLookupFn;
  dial?: DialLimits;
};

type DispatchInit = Pick<PinnedRequestInit, "signal" | "dispatchDeadline" | "onCancelRegister">;
type FetchInit = RequestInit &
  Pick<PinnedRequestInit, "dispatchDeadline" | "headersValidated" | "onCancelRegister">;
type PinnedTarget = { original: URL; pinned: URL; tls: PinnedRequestInit["tls"] };

export type PreparedDispatch = {
  fetchFast(init: FetchInit): Promise<FastUpstreamResult>;
  release(): void;
};

type CachedDnsEntry = {
  addresses: DnsAddress[];
  expiresAt: number;
};

function awaitDnsWithCancel(
  pending: Promise<DnsAddress[]>,
  signal?: AbortSignal | null,
  onCancelRegister?: CancelRegistrar,
  dispatchDeadline?: number,
): Promise<DnsAddress[]> {
  if (!signal && !onCancelRegister && dispatchDeadline === undefined) {
    return pending.catch(() => {
      throw new UpstreamPreDispatchError("upstream_dns", "Upstream address lookup failed");
    });
  }
  return new Promise<DnsAddress[]>((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let detachCancel: (() => void) | undefined;
    const cleanup = () => {
      if (timer !== undefined) {
        clearTimeout(timer);
        timer = undefined;
      }
      if (signal) signal.removeEventListener("abort", abort);
      if (detachCancel) {
        const detach = detachCancel;
        detachCancel = undefined;
        detach();
      }
    };
    const abort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(
        new UpstreamPreDispatchError(
          "upstream_not_dispatched",
          "Upstream request cancelled before dispatch",
        ),
      );
    };
    if (signal) signal.addEventListener("abort", abort, { once: true });
    if (onCancelRegister) detachCancel = onCancelRegister(abort);
    if (settled) return;
    if (signal?.aborted) {
      abort();
      return;
    }
    if (dispatchDeadline !== undefined) {
      const remaining = dispatchDeadline - performance.now();
      if (!(remaining > 0)) {
        abort();
        return;
      }
      timer = setTimeout(abort, remaining);
    }
    pending.then(
      (addresses) => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(addresses);
      },
      () => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(new UpstreamPreDispatchError("upstream_dns", "Upstream address lookup failed"));
      },
    );
  });
}

function createDnsResolver(options: TransportOptions = {}) {
  const ttlMs = options.dnsCacheTtlMs ?? 5_000;
  const maxEntries = options.dnsCacheMaxEntries ?? 1024;
  const lookupFn: DnsLookupFn =
    options.dnsLookup ??
    ((hostname: string) => lookup(hostname, { all: true }) as Promise<DnsAddress[]>);
  const cache = new Map<string, CachedDnsEntry>();
  const inFlight = new Map<string, Promise<DnsAddress[]>>();

  return {
    resolve(
      hostname: string,
      signal?: AbortSignal | null,
      onCancelRegister?: CancelRegistrar,
      dispatchDeadline?: number,
    ): Promise<DnsAddress[]> {
      if (ttlMs > 0) {
        const now = performance.now();
        const hit = cache.get(hostname);
        if (hit) {
          if (hit.expiresAt > now) {
            cache.delete(hostname);
            cache.set(hostname, hit);
            return Promise.resolve(hit.addresses);
          }
          cache.delete(hostname);
        }
      }
      let pending = inFlight.get(hostname);
      if (!pending) {
        pending = lookupFn(hostname)
          .then((addresses) => {
            if (ttlMs > 0 && addresses.length > 0) {
              if (cache.size >= maxEntries) {
                const oldest = cache.keys().next().value;
                if (oldest !== undefined) cache.delete(oldest);
              }
              cache.set(hostname, { addresses, expiresAt: performance.now() + ttlMs });
            }
            return addresses;
          })
          .finally(() => {
            inFlight.delete(hostname);
          });
        inFlight.set(hostname, pending);
      }
      return awaitDnsWithCancel(pending, signal, onCancelRegister, dispatchDeadline);
    },
    clear() {
      cache.clear();
    },
  };
}

export function createTransport(maxConnections = 1024, options: TransportOptions = {}) {
  const pool = createHttpPool(maxConnections, options.dial);
  const dns = createDnsResolver(options);
  return {
    async prepareDispatch(
      url: URL,
      bytes: number,
      init: DispatchInit,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ): Promise<PreparedDispatch> {
      const cancel = init.onCancelRegister ? new AbortController() : null;
      const detach = cancel && init.onCancelRegister?.((reason) => cancel.abort(reason));
      const admission = cancel
        ? {
            ...(init.dispatchDeadline === undefined
              ? {}
              : { dispatchDeadline: init.dispatchDeadline }),
            signal: init.signal ? AbortSignal.any([init.signal, cancel.signal]) : cancel.signal,
          }
        : init;
      const { target, outbound } = await (async () => {
        try {
          const target = await resolvePinnedTarget(
            url,
            admission,
            allowPrivate,
            privateCidrs,
            dns.resolve,
          );
          const outbound = await pool.reserve(target.pinned, target.tls, bytes, admission);
          if (admission.signal?.aborted) {
            outbound.release();
            throw new UpstreamPreDispatchError(
              "upstream_not_dispatched",
              "Upstream dispatch cancelled",
            );
          }
          return { target, outbound };
        } finally {
          detach?.();
        }
      })();
      return {
        async fetchFast(request: FetchInit) {
          try {
            if (request.body instanceof ReadableStream) {
              throw new TypeError("Prepared dispatch requires a buffered request body");
            }
            return await sendPinned(target, request, (pinned, pinnedInit) =>
              pool.sendFast(pinned, { ...pinnedInit, outbound }),
            );
          } finally {
            outbound.release();
          }
        },
        release: outbound.release,
      };
    },
    fetch(
      url: URL,
      init: RequestInit &
        Pick<PinnedRequestInit, "dispatchDeadline" | "headersValidated" | "onCancelRegister">,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ) {
      return safeFetch(url, init, allowPrivate, privateCidrs, pool.send, dns.resolve);
    },
    fetchFast(
      url: URL,
      init: RequestInit &
        Pick<PinnedRequestInit, "dispatchDeadline" | "headersValidated" | "onCancelRegister">,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ): Promise<FastUpstreamResult> {
      return safeFetchGeneric(url, init, allowPrivate, privateCidrs, pool.sendFast, dns.resolve);
    },
    sseFraming: pool.sseFraming,
    bufferedBody: pool.bufferedBody,
    close() {
      dns.clear();
      pool.close();
    },
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

function defaultDnsResolve(
  hostname: string,
  signal?: AbortSignal | null,
  onCancelRegister?: CancelRegistrar,
  dispatchDeadline?: number,
): Promise<DnsAddress[]> {
  return awaitDnsWithCancel(
    lookup(hostname, { all: true }) as Promise<DnsAddress[]>,
    signal,
    onCancelRegister,
    dispatchDeadline,
  );
}

const IP_FAMILY_CACHE = new Map<string, number>();

function cachedIpFamily(hostname: string): number {
  const hit = IP_FAMILY_CACHE.get(hostname);
  if (hit !== undefined) return hit;
  const fam = isIP(hostname);
  if (IP_FAMILY_CACHE.size >= 512) IP_FAMILY_CACHE.clear();
  IP_FAMILY_CACHE.set(hostname, fam);
  return fam;
}

type ResolveDns = (
  hostname: string,
  signal?: AbortSignal | null,
  onCancelRegister?: CancelRegistrar,
  dispatchDeadline?: number,
) => Promise<DnsAddress[]>;

async function resolvePinnedTarget(
  url: URL,
  init: DispatchInit,
  allowPrivate: boolean,
  privateCidrs: readonly string[],
  resolveDns: ResolveDns,
): Promise<PinnedTarget> {
  url = new URL(url);
  if (url.username || url.password || (url.protocol !== "https:" && url.protocol !== "http:"))
    throw new UpstreamPreDispatchError("unsafe_upstream", "Unsafe upstream URL");
  const signal = init.signal;
  const cancelled = () =>
    new UpstreamPreDispatchError(
      "upstream_not_dispatched",
      "Upstream request cancelled before dispatch",
    );
  const expired = () =>
    init.dispatchDeadline !== undefined && !(performance.now() < init.dispatchDeadline);
  if (signal?.aborted || expired()) throw cancelled();
  const rawHost = url.hostname;
  const hostname = rawHost.charCodeAt(0) === 91 ? rawHost.slice(1, -1) : rawHost;
  const ipFamily = cachedIpFamily(hostname);
  const addresses = ipFamily
    ? [{ address: hostname, family: ipFamily }]
    : await resolveDns(hostname, signal, init.onCancelRegister, init.dispatchDeadline);
  if (signal?.aborted || expired()) throw cancelled();
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
  const pinnedHost = chosen.family === 6 ? `[${chosen.address}]` : chosen.address;
  const pinned = pinnedHost === url.hostname ? url : new URL(url);
  if (pinned !== url) pinned.hostname = pinnedHost;
  const tls =
    url.protocol === "https:"
      ? { rejectUnauthorized: true as const, ...(!ipFamily && { serverName: hostname }) }
      : undefined;
  return { original: url, pinned, tls };
}

function sendPinned<T>(
  target: PinnedTarget,
  init: FetchInit,
  send: (url: URL, init: PinnedRequestInit) => Promise<T>,
): Promise<T> {
  const { original: url, pinned, tls } = target;
  let headers: HeadersInit;
  let headersValidated = false;
  if (init.headers instanceof Headers || Array.isArray(init.headers)) {
    const h = new Headers(init.headers);
    if (pinned !== url || !h.has("host")) h.set("host", url.host);
    h.set("accept-encoding", "identity");
    headers = h;
  } else if (init.headersValidated && init.headers) {
    const plain = init.headers as Record<string, string>;
    if (pinned !== url || !("host" in plain)) plain.host = url.host;
    plain["accept-encoding"] = "identity";
    headers = plain;
    headersValidated = true;
  } else {
    const plain: Record<string, string> = {};
    if (init.headers) {
      for (const k in init.headers) {
        const v = init.headers[k];
        if (v === undefined) continue;
        if (!isValidHttpHeaderName(k) || !isValidHttpHeaderValue(v)) {
          throw new TypeError("Invalid HTTP header");
        }
        plain[k.toLowerCase()] = v;
      }
    }
    if (pinned !== url || !("host" in plain)) plain.host = url.host;
    plain["accept-encoding"] = "identity";
    headers = plain;
    headersValidated = true;
  }
  const sendInit: PinnedRequestInit = {
    headers,
    headersValidated,
    redirect: "manual",
    proxy: false,
  };
  if (init.method !== undefined) sendInit.method = init.method;
  if (init.body !== undefined) sendInit.body = init.body;
  if (init.signal !== undefined) sendInit.signal = init.signal;
  if (init.dispatchDeadline !== undefined) sendInit.dispatchDeadline = init.dispatchDeadline;
  if (init.onCancelRegister !== undefined) sendInit.onCancelRegister = init.onCancelRegister;
  if (tls) sendInit.tls = tls;
  return send(pinned, sendInit);
}

async function safeFetchGeneric<T>(
  url: URL,
  init: FetchInit,
  allowPrivate: boolean,
  privateCidrs: readonly string[],
  send: (url: URL, init: PinnedRequestInit) => Promise<T>,
  resolveDns: ResolveDns,
): Promise<T> {
  return sendPinned(
    await resolvePinnedTarget(url, init, allowPrivate, privateCidrs, resolveDns),
    init,
    send,
  );
}

export function safeFetch(
  url: URL,
  init: RequestInit &
    Pick<PinnedRequestInit, "dispatchDeadline" | "headersValidated" | "onCancelRegister">,
  allowPrivate: boolean,
  privateCidrs: readonly string[] = [],
  send: (url: URL, init: PinnedRequestInit) => Promise<Response> = fetch,
  resolveDns: (
    hostname: string,
    signal?: AbortSignal | null,
    onCancelRegister?: CancelRegistrar,
    dispatchDeadline?: number,
  ) => Promise<DnsAddress[]> = defaultDnsResolve,
): Promise<Response> {
  return safeFetchGeneric(url, init, allowPrivate, privateCidrs, send, resolveDns);
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
