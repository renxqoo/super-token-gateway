import { Agent, request as httpRequest } from "node:http";
import type { ClientRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { Readable, pipeline } from "node:stream";
import type { Duplex } from "node:stream";
import type { TLSSocket } from "node:tls";
import {
  createGunzip,
  createInflate,
  createBrotliDecompress,
  createZstdDecompress,
} from "node:zlib";
import { GatewayError } from "@gateway/contracts";
import {
  connectionFailure,
  createBunHttp1Pool,
  type CancelRegistrar,
  type FastUpstreamResult,
  type OutboundReservation,
} from "./bun-http1.ts";
import { createDialScheduler, type DialLimits, type DialPermit } from "./dial-scheduler.ts";

export type { CancelRegistrar, FastUpstreamResult };

export interface PinnedRequestInit extends RequestInit {
  tls?: { serverName?: string; rejectUnauthorized: true };
  proxy?: false;
  dispatchDeadline?: number;
  headersValidated?: boolean;
  onCancelRegister?: CancelRegistrar;
  outbound?: PreparedExchange;
}

type PreparedExchange = {
  connection: OutboundReservation;
  takeSlot(): (() => void) | undefined;
  release(): void;
};

export class UpstreamPreDispatchError extends GatewayError {
  constructor(
    code:
      | "unsafe_upstream"
      | "upstream_dns"
      | "upstream_not_dispatched"
      | "upstream_capacity"
      | "upstream_tls"
      | "upstream_connect",
    message: string,
    cause?: unknown,
  ) {
    super(code === "upstream_capacity" ? 503 : 502, code, message);
    this.cause = cause;
  }
}

function toPlainHeaders(input: HeadersInit | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!input) return out;
  if (input instanceof Headers) {
    input.forEach((value, key) => {
      out[key] = value;
    });
    return out;
  }
  if (Array.isArray(input)) {
    for (const [k, v] of input) out[k.toLowerCase()] = v;
    return out;
  }
  for (const k in input) {
    const v = input[k];
    if (v !== undefined) out[k.toLowerCase()] = v;
  }
  return out;
}

function buildResponseHeaders(rawHeaders: string[]): Headers | Record<string, string> {
  const record: Record<string, string> = {};
  for (let i = 0; i < rawHeaders.length; i += 2) {
    const k = rawHeaders[i]!.toLowerCase();
    const v = rawHeaders[i + 1]!;
    if (k in record) {
      const headers = new Headers();
      for (let j = 0; j < rawHeaders.length; j += 2) {
        headers.append(rawHeaders[j]!, rawHeaders[j + 1]!);
      }
      return headers;
    }
    record[k] = v;
  }
  return record;
}

const IDLE_TTL_MS = 30_000;
const IDLE_SWEEP_MS = 5_000;
type NodeDial = {
  target: string;
  bytes: number;
  signal: AbortSignal;
  deadline: number | undefined;
};

export function createHttpPool(maxConnections: number, dialLimits?: DialLimits) {
  if (!Number.isSafeInteger(maxConnections) || maxConnections < 1 || maxConnections > 65535) {
    throw new RangeError("Invalid upstream connection capacity");
  }
  const maxIdle = Math.min(1024, Math.max(1, Math.floor(maxConnections / 2)));
  const options = {
    keepAlive: true,
    maxSockets: maxConnections,
    maxTotalSockets: maxConnections + maxIdle,
    maxFreeSockets: maxIdle,
    proxyEnv: {},
  };
  const plain = new Agent(options);
  const secure = new HttpsAgent({ ...options, rejectUnauthorized: true });
  const idle = new Map<Duplex, number>();
  const watched = new WeakSet<Duplex>();
  const sseFraming = new WeakMap<Response, "chunked" | number>();
  const bufferedBodies = new WeakMap<Response, Uint8Array>();
  const dialScheduler = createDialScheduler(maxConnections, dialLimits);
  const bunPlain = createBunHttp1Pool(maxIdle, sseFraming, bufferedBodies, dialScheduler);
  const active = new Set<ClientRequest>();
  const httpSlots = { inUse: 0, waiting: new Map<() => void, () => void>() };
  const httpsSlots = { inUse: 0, waiting: new Map<() => void, () => void>() };
  const maxSlotQueueBytes = dialLimits?.maxQueueBytes ?? 64 * 1024 * 1024;
  const maxSlotQueueWait = dialLimits?.maxQueueWaitMs ?? 10_000;
  let slotQueueBytes = 0;
  let idleSweepTimer: ReturnType<typeof setInterval> | undefined;
  let closed = false;

  const notDispatched = () =>
    new UpstreamPreDispatchError(
      "upstream_not_dispatched",
      "Upstream request stopped before obtaining a connection",
    );

  const ensureSweep = () => {
    if (idleSweepTimer) return;
    idleSweepTimer = setInterval(() => {
      if (idle.size === 0) return;
      const cutoff = performance.now() - IDLE_TTL_MS;
      for (const [socket, since] of idle) {
        if (since > cutoff) break;
        idle.delete(socket);
        socket.destroy();
      }
    }, IDLE_SWEEP_MS);
    idleSweepTimer.unref();
  };

  const forget = (socket: Duplex) => {
    idle.delete(socket);
  };

  function limitNodeDials(agent: Agent, isTls: boolean) {
    const connect = agent.createConnection.bind(agent);
    agent.createConnection = (options, complete) => {
      const context = (options as typeof options & { upstreamDial?: NodeDial }).upstreamDial;
      if (!context || !complete) return connect(options, complete);
      const reject = complete as (error: Error) => void;
      const start = (permit: DialPermit): Duplex => {
        if (context.signal.aborted) {
          permit.release();
          throw notDispatched();
        }
        const finish = permit.start();
        if (!finish) {
          permit.release();
          throw notDispatched();
        }
        try {
          const socket = connect(options);
          if (!socket) throw connectionFailure(isTls);
          socket.once(isTls ? "secureConnect" : "connect", finish);
          socket.once("close", finish);
          return socket;
        } catch (error) {
          finish();
          throw error;
        }
      };
      const asError = (error: unknown) =>
        error instanceof Error ? error : connectionFailure(isTls, error);
      try {
        const permit = dialScheduler.acquire(
          context.target,
          context.bytes,
          context.signal,
          context.deadline,
        );
        if (!(permit instanceof Promise)) return start(permit);
        void permit.then(
          (ticket) => {
            let socket: Duplex;
            try {
              socket = start(ticket);
            } catch (error) {
              reject(asError(error));
              return;
            }
            complete(null, socket);
          },
          (error) => reject(asError(error)),
        );
      } catch (error) {
        reject(asError(error));
      }
      return undefined;
    };
  }

  limitNodeDials(plain, false);
  limitNodeDials(secure, true);

  for (const agent of [plain, secure]) {
    const keep = agent.keepSocketAlive.bind(agent) as (socket: Duplex) => boolean;
    const reuse = agent.reuseSocket.bind(agent);
    agent.keepSocketAlive = (socket) => {
      if (closed || maxIdle === 0 || !keep(socket)) return false;
      idle.delete(socket);
      idle.set(socket, performance.now());
      ensureSweep();
      if (!watched.has(socket)) {
        watched.add(socket);
        socket.once("close", () => forget(socket));
      }
      while (idle.size > maxIdle) {
        const oldest = idle.keys().next().value;
        if (!oldest) break;
        idle.delete(oldest);
        oldest.destroy();
      }
      return true;
    };
    agent.reuseSocket = (socket, request) => {
      forget(socket);
      reuse(socket, request);
    };
  }

  const expired = (init: PinnedRequestInit) =>
    init.dispatchDeadline !== undefined && performance.now() >= init.dispatchDeadline;

  async function acquireSlot(secureRequest: boolean, init: PinnedRequestInit, bytes: number) {
    if (closed || init.signal?.aborted || expired(init)) throw notDispatched();
    const slots = secureRequest ? httpsSlots : httpSlots;
    const release = () => {
      const next = slots.waiting.values().next().value;
      if (!closed && next) next();
      else slots.inUse--;
    };

    if (slots.inUse < maxConnections) {
      slots.inUse++;
    } else {
      const capacity = () =>
        new UpstreamPreDispatchError(
          "upstream_capacity",
          "Upstream connection wait queue capacity exceeded",
        );
      if (slots.waiting.size >= maxConnections || bytes > maxSlotQueueBytes - slotQueueBytes) {
        throw capacity();
      }
      await new Promise<void>((resolve, reject) => {
        let deadline: ReturnType<typeof setTimeout> | undefined;
        let detachCancel: (() => void) | undefined;
        const remove = () => {
          if (!slots.waiting.delete(abort)) return false;
          slotQueueBytes -= bytes;
          clearTimeout(deadline);
          init.signal?.removeEventListener("abort", abort);
          detachCancel?.();
          detachCancel = undefined;
          return true;
        };
        const abort = () => {
          if (remove()) reject(notDispatched());
        };
        slots.waiting.set(abort, () => {
          remove();
          resolve();
        });
        slotQueueBytes += bytes;
        init.signal?.addEventListener("abort", abort, { once: true });
        if (init.onCancelRegister) {
          detachCancel = init.onCancelRegister(abort);
          if (!slots.waiting.has(abort)) {
            detachCancel();
            return;
          }
        }
        const remaining = (init.dispatchDeadline ?? Infinity) - performance.now();
        deadline = setTimeout(
          () => {
            if (remaining <= maxSlotQueueWait) abort();
            else if (remove()) reject(capacity());
          },
          Math.min(maxSlotQueueWait, Math.max(0, remaining)),
        );
        deadline.unref();
        if (init.signal?.aborted || expired(init)) abort();
      });
    }

    return release;
  }

  async function sendInternal(
    url: URL,
    init: PinnedRequestInit,
    fastMode: boolean,
  ): Promise<FastUpstreamResult> {
    if (closed) {
      throw new GatewayError(503, "upstream_transport_closed", "Upstream transport is closed");
    }
    if (init.signal?.aborted || expired(init)) throw notDispatched();
    const secureRequest = url.protocol === "https:";
    const body = init.body;
    const bytes =
      typeof body === "string"
        ? Buffer.byteLength(body)
        : body instanceof Blob
          ? body.size
          : body instanceof ArrayBuffer || ArrayBuffer.isView(body)
            ? body.byteLength
            : 0;
    const release = init.outbound
      ? init.outbound.takeSlot()
      : await acquireSlot(secureRequest, init, bytes);
    if (!release) throw notDispatched();

    let releaseOnClose = false;
    try {
      if (closed || init.signal?.aborted || expired(init)) throw notDispatched();
      const method = (init.method ?? "GET").toUpperCase();
      const headersValidated =
        Boolean(init.headersValidated) &&
        !(init.headers instanceof Headers) &&
        !Array.isArray(init.headers) &&
        typeof init.headers === "object" &&
        init.headers !== null;
      const plainHeaders = headersValidated
        ? (init.headers as Record<string, string>)
        : toPlainHeaders(init.headers);
      const input = init.body;
      let bytes: string | Uint8Array | undefined;
      let streamBody: ReadableStream<Uint8Array> | null = null;
      if (typeof input === "string") bytes = input;
      else if (input instanceof URLSearchParams) bytes = input.toString();
      else if (input instanceof ArrayBuffer) bytes = new Uint8Array(input);
      else if (ArrayBuffer.isView(input)) {
        bytes = new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
      } else if (input instanceof Blob) {
        bytes = new Uint8Array(await input.arrayBuffer());
      } else if (input instanceof ReadableStream) {
        streamBody = input;
      }
      if (!streamBody) {
        if ("transfer-encoding" in plainHeaders) delete plainHeaders["transfer-encoding"];
        if (bytes === undefined && "content-length" in plainHeaders) {
          delete plainHeaders["content-length"];
        }
      }

      const tls = init.tls;
      const serverName = typeof tls === "object" && tls ? tls.serverName : undefined;
      const rawHost = url.hostname;
      const hostname = rawHost.charCodeAt(0) === 91 ? rawHost.slice(1, -1) : rawHost;
      const port = url.port ? Number(url.port) : secureRequest ? 443 : 80;
      const path = url.search ? `${url.pathname}${url.search}` : url.pathname;

      if (!streamBody) {
        const tlsOpts = secureRequest
          ? { rejectUnauthorized: true as const, ...(serverName ? { serverName } : {}) }
          : undefined;
        const promise = fastMode
          ? bunPlain.sendFast(
              hostname,
              port,
              path,
              method,
              plainHeaders,
              bytes,
              init.signal ?? undefined,
              init.dispatchDeadline,
              release,
              tlsOpts,
              headersValidated,
              init.onCancelRegister,
              init.outbound?.connection,
            )
          : bunPlain
              .send(
                hostname,
                port,
                path,
                method,
                plainHeaders,
                bytes,
                init.signal ?? undefined,
                init.dispatchDeadline,
                release,
                tlsOpts,
                headersValidated,
                init.onCancelRegister,
                init.outbound?.connection,
              )
              .then((response): FastUpstreamResult => ({ fast: false, response }));
        releaseOnClose = true;
        return await promise;
      }

      if (bytes !== undefined) {
        plainHeaders["content-length"] = String(
          typeof bytes === "string" ? Buffer.byteLength(bytes) : bytes.byteLength,
        );
      }

      const response = await new Promise<Response>((resolve, reject) => {
        const dialAbort = new AbortController();
        let canHaveDispatched = false;
        let assigned: ClientRequest["socket"] | undefined;
        const readyEvent = secureRequest ? "secureConnect" : "connect";
        let dispatchTimer: ReturnType<typeof setTimeout> | undefined;
        let detachCancel: (() => void) | undefined;

        const sendBody = () => {
          if (bytes !== undefined) request.end(bytes);
          else if (streamBody) {
            const body = Readable.fromWeb(
              streamBody as unknown as Parameters<typeof Readable.fromWeb>[0],
            );
            body.once("error", (error) => request.destroy(error));
            request.once("error", () => body.destroy());
            body.pipe(request);
          } else request.end();
        };

        const markConnected = () => {
          if (canHaveDispatched) return;
          if (dispatchTimer) {
            clearTimeout(dispatchTimer);
            dispatchTimer = undefined;
          }
          if (init.dispatchDeadline !== undefined && (expired(init) || init.signal?.aborted)) {
            request.destroy(notDispatched());
            return;
          }
          canHaveDispatched = true;
          if (init.dispatchDeadline !== undefined) sendBody();
        };

        const reqOptions = {
          protocol: url.protocol,
          hostname,
          port,
          path,
          method,
          headers: plainHeaders,
          agent: secureRequest ? secure : plain,
          upstreamDial: {
            target: JSON.stringify([hostname, port]),
            bytes: Math.max(0, Number(plainHeaders["content-length"] ?? 0) || 0),
            signal: dialAbort.signal,
            deadline: init.dispatchDeadline,
          } satisfies NodeDial,
          ...(secureRequest ? { rejectUnauthorized: true as const } : {}),
          ...(secureRequest && serverName ? { servername: serverName } : {}),
        };

        const request = (secureRequest ? httpsRequest : httpRequest)(reqOptions, (response) => {
          markConnected();
          response.once("end", detachAbort);
          try {
            const status = response.statusCode ?? 502;
            const rawHeaders = response.rawHeaders;
            let contentEncoding = "";
            let contentType = "";
            let contentLengthStr: string | null = null;
            let transferEncoding: string | null = null;
            for (let i = 0; i < rawHeaders.length; i += 2) {
              const k = rawHeaders[i]!.toLowerCase();
              const v = rawHeaders[i + 1]!;
              if (k === "content-encoding") {
                contentEncoding = contentEncoding ? `${contentEncoding},${v}` : v;
              } else if (k === "content-type") contentType = v;
              else if (k === "content-length") contentLengthStr = v;
              else if (k === "transfer-encoding") transferEncoding = v;
            }

            if (status === 204 || status === 205 || status === 304 || method === "HEAD") {
              response.resume();
              resolve(new Response(null, { status, headers: buildResponseHeaders(rawHeaders) }));
              return;
            }

            const isSse = /^text\/event-stream(?:\s*;|$)/i.test(contentType);
            const contentLen =
              contentLengthStr !== null && /^\d+$/.test(contentLengthStr)
                ? Number(contentLengthStr)
                : -1;

            if (!contentEncoding && !isSse && contentLen >= 0 && contentLen <= 65536) {
              const chunks: Buffer[] = [];
              let total = 0;
              response.on("data", (chunk: Buffer) => {
                chunks.push(chunk);
                total += chunk.byteLength;
              });
              response.once("end", () => {
                const buf =
                  chunks.length === 1
                    ? chunks[0]!
                    : chunks.length === 0
                      ? new Uint8Array(0)
                      : Buffer.concat(chunks, total);
                const res = new Response(buf as BodyInit, {
                  status,
                  headers: buildResponseHeaders(rawHeaders),
                });
                bufferedBodies.set(res, buf);
                resolve(res);
              });
              response.once("error", (err) => reject(err));
              return;
            }

            const output = new Headers();
            for (let i = 0; i < rawHeaders.length; i += 2) {
              output.append(rawHeaders[i]!, rawHeaders[i + 1]!);
            }
            const decompressors = {
              gzip: createGunzip,
              "x-gzip": createGunzip,
              deflate: createInflate,
              br: createBrotliDecompress,
              zstd: createZstdDecompress,
            };
            const encodings = contentEncoding
              .split(",")
              .map((value) => value.trim().toLowerCase())
              .filter((value) => value && value !== "identity");
            let body: Readable = response;
            if (
              encodings.length &&
              encodings.every((value) => Object.hasOwn(decompressors, value))
            ) {
              const streams = encodings
                .reverse()
                .map((value) => decompressors[value as keyof typeof decompressors]());
              body = streams.at(-1)!;
              pipeline([response, ...streams], () => {});
              output.delete("content-encoding");
              output.delete("content-length");
            }
            const result = new Response(
              Readable.toWeb(body, {
                strategy: {
                  highWaterMark: 16384,
                  size: (chunk: Uint8Array) => chunk.byteLength,
                },
              }) as unknown as ReadableStream<Uint8Array>,
              { status, headers: output },
            );
            if (!encodings.length && isSse) {
              if (contentLengthStr === null && transferEncoding?.toLowerCase() === "chunked") {
                sseFraming.set(result, "chunked");
              } else if (
                transferEncoding === null &&
                contentLengthStr !== null &&
                /^\d+$/.test(contentLengthStr) &&
                Number.isSafeInteger(Number(contentLengthStr))
              ) {
                sseFraming.set(result, Number(contentLengthStr));
              }
            }
            resolve(result);
          } catch (error) {
            response.destroy();
            reject(error);
          }
        });

        const watchSocket = (socket: NonNullable<ClientRequest["socket"]>) => {
          assigned = socket;
          if (
            request.reusedSocket ||
            (secureRequest ? (socket as TLSSocket).authorized : !socket.connecting)
          ) {
            markConnected();
          } else {
            socket.once(readyEvent, markConnected);
            if (init.dispatchDeadline !== undefined && !dispatchTimer) {
              dispatchTimer = setTimeout(
                () => request.destroy(notDispatched()),
                Math.max(0, init.dispatchDeadline - performance.now()),
              );
            }
          }
        };

        const preDispatch = () =>
          !canHaveDispatched &&
          (init.dispatchDeadline !== undefined ||
            (!request.reusedSocket &&
              !(secureRequest && (request.socket as TLSSocket | undefined)?.authorized)));

        const abort = (reason?: unknown) => {
          dialAbort.abort();
          const error = preDispatch()
            ? notDispatched()
            : (reason ?? init.signal?.reason ?? new Error("Upstream request aborted"));
          if (error instanceof UpstreamPreDispatchError) reject(error);
          request.destroy(error instanceof Error ? error : new Error("Upstream request aborted"));
        };
        const detachAbort = () => {
          init.signal?.removeEventListener("abort", abort);
          detachCancel?.();
          detachCancel = undefined;
        };

        active.add(request);
        request.once("close", () => {
          dialAbort.abort();
          if (dispatchTimer) {
            clearTimeout(dispatchTimer);
            dispatchTimer = undefined;
          }
          active.delete(request);
          detachAbort();
          assigned?.removeListener(readyEvent, markConnected);
          release();
        });
        releaseOnClose = true;
        request.once("socket", watchSocket);
        request.once("error", (error) => {
          reject(
            preDispatch() && !(error instanceof UpstreamPreDispatchError)
              ? connectionFailure(secureRequest, error)
              : error,
          );
        });
        init.signal?.addEventListener("abort", abort, { once: true });
        if (init.onCancelRegister) detachCancel = init.onCancelRegister((r) => abort(r));
        if (init.signal?.aborted) abort();
        request.once("upgrade", (_response, socket) => {
          socket.destroy();
          reject(
            new GatewayError(
              502,
              "unsupported_upstream_upgrade",
              "Upstream upgrade is unsupported",
            ),
          );
        });
        if (init.dispatchDeadline === undefined) sendBody();
      });
      return { fast: false, response };
    } finally {
      if (!releaseOnClose) release();
    }
  }

  return {
    async reserve(
      url: URL,
      tls: PinnedRequestInit["tls"],
      bytes: number,
      init: PinnedRequestInit,
    ): Promise<PreparedExchange> {
      const secureRequest = url.protocol === "https:";
      const release = await acquireSlot(secureRequest, init, bytes);
      try {
        const host = url.hostname.replace(/^\[|\]$/g, "");
        const port = url.port ? Number(url.port) : secureRequest ? 443 : 80;
        const connection = await bunPlain.reserve(
          host,
          port,
          tls,
          bytes,
          init.signal,
          init.dispatchDeadline,
        );
        let available = true;
        return {
          connection,
          takeSlot() {
            if (!available) return undefined;
            available = false;
            return release;
          },
          release() {
            connection.release();
            if (!available) return;
            available = false;
            release();
          },
        };
      } catch (error) {
        release();
        throw error;
      }
    },

    sseFraming(response: Response) {
      return sseFraming.get(response);
    },

    bufferedBody(response: Response) {
      return bufferedBodies.get(response);
    },

    async send(url: URL, init: PinnedRequestInit): Promise<Response> {
      const result = await sendInternal(url, init, false);
      if (!result.fast) return result.response;
      const res = new Response(result.body as BodyInit, {
        status: result.status,
        headers: result.headers,
      });
      bufferedBodies.set(res, result.body);
      return res;
    },

    sendFast(url: URL, init: PinnedRequestInit): Promise<FastUpstreamResult> {
      return sendInternal(url, init, true);
    },

    close() {
      if (closed) return;
      closed = true;
      dialScheduler.close();
      for (const slots of [httpSlots, httpsSlots]) {
        for (const abort of slots.waiting.keys()) abort();
      }
      if (idleSweepTimer) {
        clearInterval(idleSweepTimer);
        idleSweepTimer = undefined;
      }
      idle.clear();
      for (const request of active) request.destroy(new Error("Upstream transport closed"));
      active.clear();
      bunPlain.close();
      plain.destroy();
      secure.destroy();
    },
  };
}
