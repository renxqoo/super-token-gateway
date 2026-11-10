import { Agent, request as httpRequest } from "node:http";
import type { ClientRequest } from "node:http";
import { Agent as HttpsAgent, request as httpsRequest } from "node:https";
import { Readable, pipeline } from "node:stream";
import type { Duplex } from "node:stream";
import {
  createGunzip,
  createInflate,
  createBrotliDecompress,
  createZstdDecompress,
} from "node:zlib";
import { GatewayError } from "@gateway/kernel";

export interface PinnedRequestInit extends RequestInit {
  tls?: { serverName: string; rejectUnauthorized: true };
  proxy?: false;
}

export function createHttpPool(maxConnections: number) {
  if (!Number.isSafeInteger(maxConnections) || maxConnections < 1 || maxConnections > 65535)
    throw new RangeError("Invalid upstream connection capacity");
  const maxIdle = Math.min(512, Math.floor(maxConnections / 2));
  const options = {
    keepAlive: true,
    maxSockets: maxConnections,
    maxTotalSockets: maxConnections,
    maxFreeSockets: Math.max(1, maxIdle),
    proxyEnv: {},
  };
  const plain = new Agent(options);
  const secure = new HttpsAgent({ ...options, rejectUnauthorized: true });
  const idle = new Map<Duplex, ReturnType<typeof setTimeout>>();
  const watched = new WeakSet<Duplex>();
  const active = new Set<ClientRequest>();
  let closed = false;
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
    async send(url: URL, init: PinnedRequestInit): Promise<Response> {
      if (closed)
        throw new GatewayError(503, "upstream_transport_closed", "Upstream transport is closed");
      init?.signal?.throwIfAborted();
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
      return new Promise<Response>((resolve, reject) => {
        const request = (url.protocol === "https:" ? httpsRequest : httpRequest)(
          url,
          {
            method: serialized.method,
            headers: Object.fromEntries(headers),
            agent: url.protocol === "https:" ? secure : plain,
            ...(url.protocol === "https:"
              ? { servername: serverName, rejectUnauthorized: true }
              : {}),
          },
          (response) => {
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
              resolve(
                new Response(
                  Readable.toWeb(body, {
                    strategy: {
                      highWaterMark: 16384,
                      size: (chunk: Uint8Array) => chunk.byteLength,
                    },
                  }) as unknown as ReadableStream<Uint8Array>,
                  { status, headers: output },
                ),
              );
            } catch (error) {
              response.destroy();
              reject(error);
            }
          },
        );
        const abort = () =>
          request.destroy(
            init.signal?.reason instanceof Error
              ? init.signal.reason
              : new Error("Upstream request aborted"),
          );
        const detachAbort = () => init.signal?.removeEventListener("abort", abort);
        active.add(request);
        request.once("close", () => {
          active.delete(request);
          detachAbort();
        });
        init.signal?.addEventListener("abort", abort, { once: true });
        if (init.signal?.aborted) abort();
        request.once("error", reject);
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
        if (bytes !== undefined) request.end(bytes);
        else if (serialized.body) {
          const body = Readable.fromWeb(
            serialized.body as unknown as Parameters<typeof Readable.fromWeb>[0],
          );
          body.once("error", (error) => request.destroy(error));
          request.once("error", () => body.destroy());
          body.pipe(request);
        } else request.end();
      });
    },
    close() {
      if (closed) return;
      closed = true;
      for (const timer of idle.values()) clearTimeout(timer);
      idle.clear();
      for (const request of active) request.destroy(new Error("Upstream transport closed"));
      active.clear();
      plain.destroy();
      secure.destroy();
    },
  };
}
