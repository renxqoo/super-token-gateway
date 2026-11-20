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

export function createHttpPool(maxConnections: number) {
  if (!Number.isSafeInteger(maxConnections) || maxConnections < 1 || maxConnections > 65535)
    throw new RangeError("Invalid upstream connection capacity");
  const maxIdle = Math.min(512, Math.floor(maxConnections / 2));
  const options = {
    keepAlive: true,
    maxSockets: maxConnections,
    maxTotalSockets: maxConnections + maxIdle,
    maxFreeSockets: Math.max(1, maxIdle),
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
    clearTimeout(idle.get(socket));
    idle.delete(socket);
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
      }, 30000);
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
      if (closed)
        throw new GatewayError(503, "upstream_transport_closed", "Upstream transport is closed");
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
      if (slots.inUse < maxConnections) slots.inUse++;
      else {
        if (slots.waiting.size >= maxConnections)
          throw new UpstreamPreDispatchError(
            "upstream_capacity",
            "Upstream connection wait queue is full",
          );
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
          if (init.dispatchDeadline !== undefined)
            deadline = setTimeout(abort, Math.max(0, init.dispatchDeadline - performance.now()));
          if (init.signal?.aborted || expired()) abort();
        });
      }
      let releaseOnClose = false;
      try {
        if (closed || init.signal?.aborted || expired()) throw notDispatched();
        const serialized = new Request(url, init);
        const headers = new Headers(serialized.headers);
        const input = init?.body;
        let bytes: string | Uint8Array | undefined;
        if (typeof input === "string") bytes = input;
        else if (input instanceof URLSearchParams) bytes = input.toString();
        else if (input instanceof ArrayBuffer) bytes = new Uint8Array(input);
        else if (ArrayBuffer.isView(input))
          bytes = new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
        if (bytes !== undefined && !headers.has("content-length"))
          headers.set("content-length", String(Buffer.byteLength(bytes)));
        if (input instanceof Blob && !headers.has("content-length"))
          headers.set("content-length", String(input.size));
        const tls = init?.tls;
        const serverName = typeof tls === "object" && tls ? tls.serverName : undefined;
        return await new Promise<Response>((resolve, reject) => {
          let canHaveDispatched = false;
          let assigned: ClientRequest["socket"] | undefined;
          const readyEvent = secureRequest ? "secureConnect" : "connect";
          let dispatchTimer: ReturnType<typeof setTimeout> | undefined;
          const markConnected = () => {
            if (canHaveDispatched) return;
            clearTimeout(dispatchTimer);
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
              method: serialized.method,
              headers: Object.fromEntries(headers),
              agent: secureRequest ? secure : plain,
              ...(secureRequest ? { rejectUnauthorized: true as const } : {}),
              ...(secureRequest && serverName ? { servername: serverName } : {}),
            },
            (response) => {
              markConnected();
              response.once("end", detachAbort);
              try {
                const output = new Headers();
                for (let i = 0; i < response.rawHeaders.length; i += 2)
                  output.append(response.rawHeaders[i]!, response.rawHeaders[i + 1]!);
                const status = response.statusCode ?? 502;
                if ([204, 205, 304].includes(status) || serialized.method === "HEAD") {
                  response.resume();
                  resolve(new Response(null, { status, headers: output }));
                  return;
                }
                const decompressors = {
                  gzip: createGunzip,
                  "x-gzip": createGunzip,
                  deflate: createInflate,
                  br: createBrotliDecompress,
                  zstd: createZstdDecompress,
                };
                const encodings = (output.get("content-encoding") ?? "")
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
                if (
                  !encodings.length &&
                  /^text\/event-stream(?:\s*;|$)/i.test(output.get("content-type") ?? "")
                ) {
                  const length = output.get("content-length");
                  const transfer = output.get("transfer-encoding");
                  if (length === null && transfer?.toLowerCase() === "chunked")
                    sseFraming.set(result, "chunked");
                  else if (
                    transfer === null &&
                    length !== null &&
                    /^\d+$/.test(length) &&
                    Number.isSafeInteger(Number(length))
                  )
                    sseFraming.set(result, Number(length));
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
            )
              markConnected();
            else socket.once(readyEvent, markConnected);
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
            clearTimeout(dispatchTimer);
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
          function sendBody() {
            if (bytes !== undefined) request.end(bytes);
            else if (serialized.body) {
              const body = Readable.fromWeb(
                serialized.body as unknown as Parameters<typeof Readable.fromWeb>[0],
              );
              body.once("error", (error) => request.destroy(error));
              request.once("error", () => body.destroy());
              body.pipe(request);
            } else request.end();
          }
          if (init.dispatchDeadline === undefined) sendBody();
          else
            dispatchTimer = setTimeout(
              () => request.destroy(notDispatched()),
              Math.max(0, init.dispatchDeadline - performance.now()),
            );
        });
      } finally {
        if (!releaseOnClose) release();
      }
    },
    close() {
      if (closed) return;
      closed = true;
      for (const slots of [httpSlots, httpsSlots])
        for (const abort of slots.waiting.keys()) abort();
      for (const timer of idle.values()) clearTimeout(timer);
      idle.clear();
      for (const request of active) request.destroy(new Error("Upstream transport closed"));
      active.clear();
      plain.destroy();
      secure.destroy();
    },
  };
}
