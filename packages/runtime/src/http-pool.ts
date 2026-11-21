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

export interface PinnedRequestInit extends RequestInit {
  tls?: { serverName?: string; rejectUnauthorized: true };
  proxy?: false;
  dispatchDeadline?: number;
}

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
  for (const [k, v] of Object.entries(input)) {
    if (v !== undefined) out[k.toLowerCase()] = v;
  }
  return out;
}

export function createHttpPool(maxConnections: number) {
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
  const idle = new Map<Duplex, ReturnType<typeof setTimeout>>();
  const watched = new WeakSet<Duplex>();
  const sseFraming = new WeakMap<Response, "chunked" | number>();
  const active = new Set<ClientRequest>();
  const httpSlots = { inUse: 0, waiting: new Map<() => void, () => void>() };
  const httpsSlots = { inUse: 0, waiting: new Map<() => void, () => void>() };
  let closed = false;

  const notDispatched = () =>
    new UpstreamPreDispatchError(
      "upstream_not_dispatched",
      "Upstream request stopped before obtaining a connection",
    );

  const forget = (socket: Duplex) => {
    const t = idle.get(socket);
    if (t) {
      clearTimeout(t);
      idle.delete(socket);
    }
  };

  for (const agent of [plain, secure]) {
    const keep = agent.keepSocketAlive.bind(agent) as (socket: Duplex) => boolean;
    const reuse = agent.reuseSocket.bind(agent);
    agent.keepSocketAlive = (socket) => {
      if (closed || maxIdle === 0 || !keep(socket)) return false;
      forget(socket);
      const timer = setTimeout(() => {
        forget(socket);
        socket.destroy();
      }, 30_000);
      timer.unref();
      idle.set(socket, timer);
      if (!watched.has(socket)) {
        watched.add(socket);
        socket.once("close", () => forget(socket));
      }
      while (idle.size > maxIdle) {
        const oldest = idle.keys().next().value;
        if (!oldest) break;
        forget(oldest);
        oldest.destroy();
      }
      return true;
    };
    agent.reuseSocket = (socket, request) => {
      forget(socket);
      reuse(socket, request);
    };
  }

  return {
    sseFraming(response: Response) {
      return sseFraming.get(response);
    },

    async send(url: URL, init: PinnedRequestInit): Promise<Response> {
      if (closed) {
        throw new GatewayError(503, "upstream_transport_closed", "Upstream transport is closed");
      }
      const expired = () =>
        init.dispatchDeadline !== undefined && !(performance.now() < init.dispatchDeadline);
      if (init.signal?.aborted || expired()) throw notDispatched();

      const secureRequest = url.protocol === "https:";
      const slots = secureRequest ? httpsSlots : httpSlots;
      const release = () => {
        const next = slots.waiting.values().next().value;
        if (!closed && next) next();
        else slots.inUse--;
      };

      if (slots.inUse < maxConnections) {
        slots.inUse++;
      } else {
        if (slots.waiting.size >= maxConnections) {
          throw new UpstreamPreDispatchError(
            "upstream_capacity",
            "Upstream connection wait queue is full",
          );
        }
        await new Promise<void>((resolve, reject) => {
          let deadline: ReturnType<typeof setTimeout> | undefined;
          const remove = () => {
            clearTimeout(deadline);
            slots.waiting.delete(abort);
            init.signal?.removeEventListener("abort", abort);
          };
          const abort = () => {
            remove();
            reject(notDispatched());
          };
          slots.waiting.set(abort, () => {
            remove();
            resolve();
          });
          init.signal?.addEventListener("abort", abort, { once: true });
          if (init.dispatchDeadline !== undefined) {
            deadline = setTimeout(abort, Math.max(0, init.dispatchDeadline - performance.now()));
          }
          if (init.signal?.aborted || expired()) abort();
        });
      }

      let releaseOnClose = false;
      try {
        if (closed || init.signal?.aborted || expired()) throw notDispatched();
        const method = (init.method ?? "GET").toUpperCase();
        const plainHeaders = toPlainHeaders(init.headers);
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
        if (bytes !== undefined && plainHeaders["content-length"] === undefined) {
          plainHeaders["content-length"] = String(
            typeof bytes === "string" ? Buffer.byteLength(bytes) : bytes.byteLength,
          );
        }

        const tls = init.tls;
        const serverName = typeof tls === "object" && tls ? tls.serverName : undefined;

        return await new Promise<Response>((resolve, reject) => {
          let canHaveDispatched = false;
          let assigned: ClientRequest["socket"] | undefined;
          const readyEvent = secureRequest ? "secureConnect" : "connect";
          let dispatchTimer: ReturnType<typeof setTimeout> | undefined;

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
            if (init.dispatchDeadline !== undefined && (expired() || init.signal?.aborted)) {
              request.destroy(notDispatched());
              return;
            }
            canHaveDispatched = true;
            if (init.dispatchDeadline !== undefined) sendBody();
          };

          const request = (secureRequest ? httpsRequest : httpRequest)(
            url,
            {
              method,
              headers: plainHeaders,
              agent: secureRequest ? secure : plain,
              ...(secureRequest ? { rejectUnauthorized: true as const } : {}),
              ...(secureRequest && serverName ? { servername: serverName } : {}),
            },
            (response) => {
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
                  const output = new Headers();
                  for (let i = 0; i < rawHeaders.length; i += 2) {
                    output.append(rawHeaders[i]!, rawHeaders[i + 1]!);
                  }
                  response.resume();
                  resolve(new Response(null, { status, headers: output }));
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
                    const output = new Headers();
                    for (let i = 0; i < rawHeaders.length; i += 2) {
                      output.append(rawHeaders[i]!, rawHeaders[i + 1]!);
                    }
                    const buf = chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks, total);
                    resolve(new Response(buf as BodyInit, { status, headers: output }));
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
            },
          );

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

          const abort = () => {
            const error = preDispatch()
              ? notDispatched()
              : (init.signal?.reason ?? new Error("Upstream request aborted"));
            if (error instanceof UpstreamPreDispatchError) reject(error);
            request.destroy(error instanceof Error ? error : new Error("Upstream request aborted"));
          };
          const detachAbort = () => init.signal?.removeEventListener("abort", abort);

          active.add(request);
          request.once("close", () => {
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
            const local = ["EADDRNOTAVAIL", "EMFILE", "ENFILE"].includes(
              String((error as NodeJS.ErrnoException).code),
            );
            reject(
              preDispatch() && !local && !(error instanceof UpstreamPreDispatchError)
                ? new UpstreamPreDispatchError(
                    secureRequest ? "upstream_tls" : "upstream_connect",
                    "Upstream connection failed before dispatch",
                    error,
                  )
                : error,
            );
          });
          init.signal?.addEventListener("abort", abort, { once: true });
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
      } finally {
        if (!releaseOnClose) release();
      }
    },

    close() {
      if (closed) return;
      closed = true;
      for (const slots of [httpSlots, httpsSlots]) {
        for (const abort of slots.waiting.keys()) abort();
      }
      for (const timer of idle.values()) clearTimeout(timer);
      idle.clear();
      for (const request of active) request.destroy(new Error("Upstream transport closed"));
      active.clear();
      plain.destroy();
      secure.destroy();
    },
  };
}
