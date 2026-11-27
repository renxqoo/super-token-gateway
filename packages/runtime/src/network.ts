import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import { inCidrs, isPublicAddress } from "./ip.ts";
import { GatewayError } from "@gateway/contracts";
import { DEFAULT_DIAL_QUEUE_WAIT_MS, type DialLimits } from "./dial-scheduler.ts";
import { markUpstream } from "./transport-timing.ts";
import { isValidHttpHeaderName, isValidHttpHeaderValue } from "./bun-http1-codec.ts";
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

type DispatchInit = Pick<
  PinnedRequestInit,
  "signal" | "dispatchDeadline" | "onCancelRegister" | "onTiming"
>;
type FetchInit = RequestInit &
  Pick<
    PinnedRequestInit,
    "dispatchDeadline" | "headersValidated" | "onCancelRegister" | "onTiming"
  >;
type UpstreamOrigin = {
  url: URL;
  host: string;
  addresses: DnsAddress[];
  tls: PinnedRequestInit["tls"];
  ipLiteral: boolean;
};

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
    peekCached(hostname: string): DnsAddress[] | undefined {
      if (ttlMs <= 0) return undefined;
      const hit = cache.get(hostname);
      if (!hit) return undefined;
      if (hit.expiresAt > performance.now()) return hit.addresses;
      cache.delete(hostname);
      return undefined;
    },
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

type CachedUrlMeta = {
  href: string;
  url: URL;
  protocol: string;
  host: string;
  hostname: string;
  rawHostname: string;
  port: number;
  ipFamily: number;
  unsafe: boolean;
  tls: PinnedRequestInit["tls"];
  literalAddresses: DnsAddress[] | null;
  pinnedUrls: Map<string, URL>;
};

const URL_META_CACHE = new WeakMap<URL, CachedUrlMeta>();

function readUrlMeta(input: URL): CachedUrlMeta {
  const cached = URL_META_CACHE.get(input);
  const href = input.href;
  if (cached && cached.href === href) return cached;
  const copy = new URL(href);
  const protocol = copy.protocol;
  const unsafe =
    Boolean(copy.username || copy.password) || (protocol !== "https:" && protocol !== "http:");
  const rawHost = copy.hostname;
  const hostname = rawHost.charCodeAt(0) === 91 ? rawHost.slice(1, -1) : rawHost;
  const ipFamily = cachedIpFamily(hostname);
  const port = copy.port ? Number(copy.port) : protocol === "https:" ? 443 : 80;
  const tls =
    protocol === "https:"
      ? { rejectUnauthorized: true as const, ...(!ipFamily && { serverName: hostname }) }
      : undefined;
  const meta: CachedUrlMeta = {
    href,
    url: copy,
    protocol,
    host: copy.host,
    hostname,
    rawHostname: rawHost,
    port,
    ipFamily,
    unsafe,
    tls,
    literalAddresses: ipFamily ? [{ address: hostname, family: ipFamily }] : null,
    pinnedUrls: new Map(),
  };
  URL_META_CACHE.set(input, meta);
  return meta;
}

export function createTransport(maxConnections = 1024, options: TransportOptions = {}) {
  const pool = createHttpPool(maxConnections, options.dial);
  const dns = createDnsResolver(options);
  const fallbackBudget = options.dial?.maxQueueWaitMs ?? DEFAULT_DIAL_QUEUE_WAIT_MS;
  return {
    tryPrepareDispatchSync(
      url: URL,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ): PreparedDispatch | null {
      const meta = readUrlMeta(url);
      if (meta.unsafe) return null;
      const addresses = meta.literalAddresses ?? dns.peekCached(meta.hostname);
      if (!addresses || addresses.length !== 1) return null;
      const entry = addresses[0]!;
      if (!entry.family || entry.address.includes("%")) return null;
      if (
        !allowPrivate &&
        ((!isPublicAddress(entry.address) && !inCidrs(entry.address, privateCidrs)) ||
          (meta.protocol !== "https:" && !inCidrs(entry.address, privateCidrs)))
      ) {
        return null;
      }
      const secure = meta.protocol === "https:";
      const outbound = pool.tryReserveSync(entry.address, meta.port, meta.tls, secure);
      if (!outbound) return null;
      const origin: UpstreamOrigin = {
        url: meta.url,
        host: meta.host,
        addresses,
        tls: meta.tls,
        ipLiteral: Boolean(meta.ipFamily),
      };
      let pinnedUrl: URL;
      if (meta.ipFamily) {
        pinnedUrl = meta.url;
      } else {
        const host = entry.family === 6 ? `[${entry.address}]` : entry.address;
        if (host === meta.rawHostname) {
          pinnedUrl = meta.url;
        } else {
          let cachedPinned = meta.pinnedUrls.get(host);
          if (!cachedPinned) {
            cachedPinned = new URL(meta.url);
            cachedPinned.hostname = host;
            if (meta.pinnedUrls.size >= 8) meta.pinnedUrls.clear();
            meta.pinnedUrls.set(host, cachedPinned);
          }
          pinnedUrl = cachedPinned;
        }
      }
      let available = true;
      return {
        async fetchFast(request: FetchInit) {
          if (!available)
            throw new UpstreamPreDispatchError("upstream_not_dispatched", "Dispatch unavailable");
          available = false;
          try {
            if (request.body instanceof ReadableStream) {
              throw new TypeError("Prepared dispatch requires a buffered request body");
            }
            const sendInit = pinnedRequest(origin, request);
            if (!origin.tls) {
              sendInit.outbound = outbound;
              return await pool.sendFast(pinnedUrl, sendInit);
            }
            return await runWithTransientTlsRetry(
              sendInit,
              (attemptInit, retry) => {
                if (retry) {
                  delete attemptInit.outbound;
                  return pool.sendFast(pinnedUrl, attemptInit);
                }
                attemptInit.outbound = outbound;
                return pool.sendFast(pinnedUrl, attemptInit);
              },
              () => outbound.release(),
            );
          } finally {
            outbound.release();
          }
        },
        release() {
          available = false;
          outbound.release();
        },
      };
    },
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
            ...(init.onTiming ? { onTiming: init.onTiming } : {}),
          }
        : init;
      const { origin, outbound, selectedIndex } = await (async () => {
        try {
          const origin = await resolveUpstreamOrigin(
            url,
            admission,
            allowPrivate,
            privateCidrs,
            dns.resolve,
          );
          const { outbound, index } = await useAddresses(
            origin,
            admission,
            fallbackBudget,
            async (target, attempt, index) => ({
              outbound: await pool.reserve(target, origin.tls, bytes, attempt),
              index,
            }),
          );
          if (admission.signal?.aborted) {
            outbound.release();
            throw new UpstreamPreDispatchError(
              "upstream_not_dispatched",
              "Upstream dispatch cancelled",
            );
          }
          return { origin, outbound, selectedIndex: index };
        } finally {
          detach?.();
        }
      })();
      let available = true;
      return {
        async fetchFast(request: FetchInit) {
          if (!available)
            throw new UpstreamPreDispatchError("upstream_not_dispatched", "Dispatch unavailable");
          available = false;
          try {
            if (request.body instanceof ReadableStream) {
              throw new TypeError("Prepared dispatch requires a buffered request body");
            }
            return await useAddresses(
              origin,
              pinnedRequest(origin, request),
              fallbackBudget,
              async (pinned, init, index, retry) => {
                if (retry || index !== selectedIndex) return pool.sendFast(pinned, init);
                try {
                  return await pool.sendFast(pinned, { ...init, outbound });
                } finally {
                  outbound.release();
                }
              },
              selectedIndex,
            );
          } finally {
            outbound.release();
          }
        },
        release() {
          available = false;
          outbound.release();
        },
      };
    },
    async fetch(
      url: URL,
      init: FetchInit,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ) {
      const origin = await resolveUpstreamOrigin(
        url,
        init,
        allowPrivate,
        privateCidrs,
        dns.resolve,
      );
      return useAddresses(origin, pinnedRequest(origin, init), fallbackBudget, pool.send);
    },
    async fetchFast(
      url: URL,
      init: FetchInit,
      allowPrivate: boolean,
      privateCidrs: readonly string[] = [],
    ): Promise<FastUpstreamResult> {
      const origin = await resolveUpstreamOrigin(
        url,
        init,
        allowPrivate,
        privateCidrs,
        dns.resolve,
      );
      return useAddresses(origin, pinnedRequest(origin, init), fallbackBudget, pool.sendFast);
    },
    sseFraming: pool.sseFraming,
    bufferedBody: pool.bufferedBody,
    close() {
      dns.clear();
      pool.close();
    },
  };
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

async function resolveUpstreamOrigin(
  url: URL,
  init: DispatchInit,
  allowPrivate: boolean,
  privateCidrs: readonly string[],
  resolveDns: ResolveDns,
): Promise<UpstreamOrigin> {
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
  let addresses: DnsAddress[];
  if (ipFamily) addresses = [{ address: hostname, family: ipFamily }];
  else {
    markUpstream(init.onTiming, "dns_start");
    try {
      addresses = await resolveDns(hostname, signal, init.onCancelRegister, init.dispatchDeadline);
    } finally {
      markUpstream(init.onTiming, "dns_end");
    }
  }
  if (signal?.aborted || expired()) throw cancelled();
  if (
    !addresses.length ||
    addresses.some(
      (entry) =>
        !entry.family || entry.address.includes("%") || isIP(entry.address) !== entry.family,
    )
  )
    throw new UpstreamPreDispatchError("upstream_dns", "Upstream address lookup failed");
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
  const tls =
    url.protocol === "https:"
      ? { rejectUnauthorized: true as const, ...(!ipFamily && { serverName: hostname }) }
      : undefined;
  const ordered =
    addresses.length === 1
      ? addresses
      : [
          ...new Map(
            addresses
              .filter((entry) => entry.family === 4)
              .concat(addresses.filter((entry) => entry.family === 6))
              .map((entry) => [entry.address, entry]),
          ).values(),
        ];
  return { url, host: url.host, addresses: ordered, tls, ipLiteral: Boolean(ipFamily) };
}

function pinAddress(origin: UpstreamOrigin, address: DnsAddress): URL {
  const host = address.family === 6 ? `[${address.address}]` : address.address;
  if (host === origin.url.hostname) return origin.url;
  const pinned = new URL(origin.url);
  pinned.hostname = host;
  return pinned;
}

function pinnedRequest(origin: UpstreamOrigin, init: FetchInit): PinnedRequestInit {
  const { host, tls, ipLiteral } = origin;
  let headers: HeadersInit;
  let headersValidated = false;
  if (init.headers instanceof Headers || Array.isArray(init.headers)) {
    const h = new Headers(init.headers);
    if (!ipLiteral || !h.has("host")) h.set("host", host);
    h.set("accept-encoding", "identity");
    headers = h;
  } else if (init.headersValidated && init.headers) {
    const plain = init.headers as Record<string, string>;
    if (!ipLiteral || !("host" in plain)) plain.host = host;
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
    if (!ipLiteral || !("host" in plain)) plain.host = host;
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
  if (init.onTiming) sendInit.onTiming = init.onTiming;
  if (tls) sendInit.tls = tls;
  return sendInit;
}

function canTryAnotherAddress(error: unknown) {
  if (!(error instanceof UpstreamPreDispatchError)) return false;
  if (error.code === "upstream_connect" || error.code === "upstream_not_dispatched") return true;
  if (error.code !== "upstream_tls") return false;
  const cause = error.cause as { code?: string } | undefined;
  return (
    !cause ||
    /^(ECONNRESET|ECONNREFUSED|ECONNABORTED|EHOSTUNREACH|ENETUNREACH|ETIMEDOUT|EPIPE)$/.test(
      cause.code ?? "",
    )
  );
}

function canRetryTransientTlsHandshake(error: unknown, init: PinnedRequestInit): boolean {
  if (!(error instanceof UpstreamPreDispatchError) || error.code !== "upstream_tls") return false;
  if (init.body instanceof ReadableStream && init.dispatchDeadline === undefined) return false;
  const cause = error.cause as { code?: string } | undefined;
  return !cause || /^(ECONNRESET|ECONNABORTED|EPIPE)$/.test(cause.code ?? "");
}

async function runWithTransientTlsRetry<T>(
  init: PinnedRequestInit,
  run: (attemptInit: PinnedRequestInit, retry: boolean) => Promise<T>,
  onBeforeRetry?: () => void,
): Promise<T> {
  let cancelled = false;
  const register = init.onCancelRegister;
  const attemptInit: PinnedRequestInit = register
    ? {
        ...init,
        onCancelRegister: (cancel) =>
          register((reason) => {
            cancelled = true;
            cancel(reason);
          }),
      }
    : init;
  try {
    return await run(attemptInit, false);
  } catch (error) {
    onBeforeRetry?.();
    if (
      cancelled ||
      init.signal?.aborted ||
      (init.dispatchDeadline !== undefined && performance.now() >= init.dispatchDeadline) ||
      !canRetryTransientTlsHandshake(error, init)
    ) {
      throw error;
    }
    return await run(attemptInit, true);
  }
}

async function useAddresses<T>(
  origin: UpstreamOrigin,
  init: PinnedRequestInit,
  fallbackBudget: number,
  tryAddress: (url: URL, init: PinnedRequestInit, index: number, retry?: boolean) => Promise<T>,
  firstIndex = 0,
): Promise<T> {
  const { addresses } = origin;
  if (addresses.length - firstIndex === 1) {
    const pinned = pinAddress(origin, addresses[firstIndex]!);
    if (!origin.tls) return tryAddress(pinned, init, firstIndex, false);
    return runWithTransientTlsRetry(init, (attemptInit, retry) =>
      tryAddress(pinned, attemptInit, firstIndex, retry),
    );
  }

  const deadline = init.dispatchDeadline ?? performance.now() + fallbackBudget;
  const register = init.onCancelRegister;
  let cancelled = false;
  const onCancelRegister: CancelRegistrar | undefined = register
    ? (cancel) =>
        register((reason) => {
          cancelled = true;
          cancel(reason);
        })
    : undefined;
  for (let index = firstIndex; ; index++) {
    const now = performance.now();
    const remaining = addresses.length - index;
    const addressDeadline =
      remaining === 1
        ? deadline
        : Math.min(
            now + fallbackBudget,
            now + (deadline - now) / (index === firstIndex ? 2 : remaining),
          );
    try {
      return await tryAddress(
        pinAddress(origin, addresses[index]!),
        {
          ...init,
          dispatchDeadline: addressDeadline,
          ...(onCancelRegister ? { onCancelRegister } : {}),
        },
        index,
      );
    } catch (error) {
      if (
        remaining === 1 ||
        cancelled ||
        init.signal?.aborted ||
        performance.now() >= deadline ||
        !canTryAnotherAddress(error)
      )
        throw error;
    }
  }
}

export async function safeFetch(
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
  const origin = await resolveUpstreamOrigin(url, init, allowPrivate, privateCidrs, resolveDns);
  const first = pinAddress(origin, origin.addresses[0]!);
  return send(first, pinnedRequest(origin, init));
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
