import {
  createSecureServer,
  connect,
  constants,
  type IncomingHttpHeaders,
  type OutgoingHttpHeaders,
  type ServerHttp2Stream,
  type ServerHttp2Session,
  type ClientHttp2Session,
} from "node:http2";
import type { Socket } from "node:net";
import type { TLSSocket } from "node:tls";
import { constants as cryptoConstants } from "node:crypto";
import { sha256 } from "./crypto.ts";

export type H2PeerObservation = {
  authorized: boolean;
  alpn: string;
  leaf_der: Buffer;
  subject_altname_exact: string;
  not_before: number;
  not_after: number;
};
export function readH2Peer(stream: ServerHttp2Stream): H2PeerObservation {
  const socket = stream.session?.socket as TLSSocket | undefined;
  if (!socket?.encrypted || !socket.authorized || socket.alpnProtocol !== "h2")
    throw new Error("Unverified TLS peer");
  const cert = socket.getPeerCertificate();
  if (!cert.raw?.length || !cert.subjectaltname) throw new Error("Missing peer certificate");
  const before = Date.parse(cert.valid_from),
    after = Date.parse(cert.valid_to);
  if (
    !Number.isFinite(before) ||
    !Number.isFinite(after) ||
    before > Date.now() ||
    after <= Date.now()
  )
    throw new Error("Invalid peer certificate");
  return {
    authorized: true,
    alpn: "h2",
    leaf_der: Buffer.from(cert.raw),
    subject_altname_exact: cert.subjectaltname,
    not_before: before,
    not_after: after,
  };
}
export type H2Limits = {
  bodyBytes: number;
  responseBytes: number;
  headerBytes: number;
  headerCount: number;
  connections: number;
  connectionsPerPeer: number;
  streams: number;
  streamsPerSession: number;
  receiveBytes: number;
  headersMs: number;
  bodyMs: number;
  progressMs: number;
  processingMs: number;
  idleMs: number;
  lifetimeMs: number;
  drainMs: number;
};
export const boundedH2Defaults: Readonly<H2Limits> = Object.freeze({
  bodyBytes: 8192,
  responseBytes: 8192,
  headerBytes: 16384,
  headerCount: 32,
  connections: 128,
  connectionsPerPeer: 4,
  streams: 16,
  streamsPerSession: 32,
  receiveBytes: 16 * 1024 * 1024,
  headersMs: 1000,
  bodyMs: 2000,
  progressMs: 1000,
  processingMs: 5000,
  idleMs: 30000,
  lifetimeMs: 300000,
  drainMs: 1000,
});
function safeReply(stream: ServerHttp2Stream, status: number, value: Uint8Array) {
  if (stream.destroyed || stream.closed || stream.headersSent) return;
  stream.respond({
    ":status": status,
    "content-type": "application/json",
    "content-length": value.byteLength,
  });
  stream.end(value, () => {
    if (!stream.readableEnded && !stream.destroyed) stream.close(constants.NGHTTP2_NO_ERROR);
  });
}
export function h2JsonReply(
  stream: ServerHttp2Stream,
  status: number,
  value: unknown,
  maxBytes = 8192,
) {
  const bytes = Buffer.from(JSON.stringify(value));
  if (bytes.length > maxBytes) throw new Error("Response budget exceeded");
  safeReply(stream, status, bytes);
}
export async function readBoundedData(
  stream: ServerHttp2Stream,
  headers: IncomingHttpHeaders,
  limit: H2Limits,
  signal: AbortSignal,
): Promise<Buffer> {
  const declared = headers["content-length"];
  if (
    declared !== undefined &&
    (typeof declared !== "string" ||
      !/^(0|[1-9][0-9]*)$/.test(declared) ||
      Number(declared) > limit.bodyBytes)
  )
    throw new Error("body_budget");
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let length = 0,
      done = false;
    const timer = setTimeout(() => finish(new Error("body_timeout")), limit.bodyMs);
    let progress = setTimeout(() => finish(new Error("body_timeout")), limit.progressMs);
    function finish(error?: Error) {
      if (done) return;
      done = true;
      clearTimeout(timer);
      clearTimeout(progress);
      stream.off("data", onData);
      stream.off("end", onEnd);
      stream.off("aborted", onAbort);
      stream.off("close", onClose);
      stream.off("error", onError);
      signal.removeEventListener("abort", onAbort);
      if (error) {
        for (const c of chunks) c.fill(0);
        chunks.length = 0;
        reject(error);
      } else {
        const bytes = Buffer.concat(chunks, length);
        for (const c of chunks) c.fill(0);
        chunks.length = 0;
        resolve(bytes);
      }
    }
    function onData(chunk: Buffer) {
      length += chunk.length;
      if (length > limit.bodyBytes) {
        finish(new Error("body_budget"));
        return;
      }
      chunks.push(Buffer.from(chunk));
      clearTimeout(progress);
      progress = setTimeout(() => finish(new Error("body_timeout")), limit.progressMs);
    }
    function onEnd() {
      if (declared !== undefined && Number(declared) !== length) finish(new Error("body_length"));
      else finish();
    }
    function onAbort() {
      finish(new Error("body_cancelled"));
    }
    function onClose() {
      if (!stream.readableEnded) finish(new Error("body_cancelled"));
    }
    function onError() {
      finish(new Error("body_cancelled"));
    }
    stream.on("data", onData);
    stream.once("end", onEnd);
    stream.once("aborted", onAbort);
    stream.once("close", onClose);
    stream.once("error", onError);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}
export async function listenBoundedH2(
  options: {
    key: string | Buffer;
    cert: string | Buffer;
    ca: string | Buffer;
    hostname: string;
    port: number;
    limits?: Partial<H2Limits>;
    rejections?: { capacity: unknown; headers: unknown; failure: unknown };
  },
  onStream: (
    stream: ServerHttp2Stream,
    headers: IncomingHttpHeaders,
    signal: AbortSignal,
    limits: H2Limits,
  ) => Promise<void>,
) {
  const limits = { ...boundedH2Defaults, ...options.limits };
  for (const value of Object.values(limits))
    if (!Number.isSafeInteger(value) || value < 1) throw new Error("Invalid transport budget");
  const sockets = new Set<Socket>();
  const firstHeaders = new Map<object, ReturnType<typeof setTimeout>>();
  const sessions = new Set<ServerHttp2Session>(),
    peers = new Map<string, number>(),
    work = new Set<Promise<void>>(),
    timers = new Set<ReturnType<typeof setTimeout>>();
  let streams = 0,
    reserved = 0,
    draining = false;
  const server = createSecureServer({
    key: options.key,
    cert: options.cert,
    ca: options.ca,
    requestCert: true,
    rejectUnauthorized: true,
    allowHTTP1: false,
    ALPNProtocols: ["h2"],
    minVersion: "TLSv1.3",
    maxVersion: "TLSv1.3",
    secureOptions: cryptoConstants.SSL_OP_NO_TICKET,
    handshakeTimeout: 3000,
    maxSessionMemory: 4,
    maxHeaderListPairs: limits.headerCount,
    settings: {
      maxConcurrentStreams: limits.streamsPerSession,
      maxHeaderListSize: limits.headerBytes,
    },
  });
  server.on("connection", (socket) => {
    socket.on("error", () => {});
    if (draining || sockets.size >= limits.connections) {
      socket.destroy();
      return;
    }
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  server.on("error", () => {});
  server.on("tlsClientError", () => {});
  server.on("sessionError", () => {});
  server.on("unknownProtocol", (socket) => socket.destroy());
  server.on("session", (session) => {
    const socket = session.socket as TLSSocket;
    const cert = socket.getPeerCertificate(),
      peer = cert.raw ? sha256(cert.raw) : "";
    if (
      draining ||
      !peer ||
      sessions.size >= limits.connections ||
      (peers.get(peer) ?? 0) >= limits.connectionsPerPeer
    ) {
      session.destroy();
      return;
    }
    sessions.add(session);
    peers.set(peer, (peers.get(peer) ?? 0) + 1);
    session.on("error", () => {});
    session.setTimeout(limits.idleMs, () => session.destroy());
    const headersTimer = setTimeout(() => {
      timers.delete(headersTimer);
      firstHeaders.delete(session);
      session.destroy();
    }, limits.headersMs);
    timers.add(headersTimer);
    firstHeaders.set(session, headersTimer);
    const life = setTimeout(() => {
      timers.delete(life);
      session.close();
      const reap = setTimeout(() => {
        timers.delete(reap);
        session.destroy();
      }, limits.drainMs);
      timers.add(reap);
      session.once("close", () => {
        clearTimeout(reap);
        timers.delete(reap);
      });
    }, limits.lifetimeMs);
    timers.add(life);
    session.once("close", () => {
      clearTimeout(headersTimer);
      timers.delete(headersTimer);
      firstHeaders.delete(session);
      clearTimeout(life);
      timers.delete(life);
      sessions.delete(session);
      const n = (peers.get(peer) ?? 1) - 1;
      if (n) peers.set(peer, n);
      else peers.delete(peer);
    });
  });
  server.on("stream", (stream: ServerHttp2Stream, headers, _flags, rawHeaders) => {
    stream.on("error", () => {});
    const initial = stream.session && firstHeaders.get(stream.session);
    if (initial) {
      clearTimeout(initial);
      timers.delete(initial);
      firstHeaders.delete(stream.session!);
    }
    const fail = (status: number, type: "capacity" | "headers" | "failure") =>
      h2JsonReply(
        stream,
        status,
        options.rejections?.[type] ?? { error: type },
        limits.responseBytes,
      );
    const headerBytes = rawHeaders.reduce((n, s) => n + Buffer.byteLength(s), 0),
      names = rawHeaders.filter((_s, i) => i % 2 === 0);
    if (
      draining ||
      streams >= limits.streams ||
      reserved + limits.bodyBytes > limits.receiveBytes
    ) {
      fail(429, "capacity");
      stream.resume();
      return;
    }
    if (
      headerBytes > limits.headerBytes ||
      names.length > limits.headerCount ||
      new Set(names).size !== names.length
    ) {
      fail(400, "headers");
      stream.resume();
      return;
    }
    const controller = new AbortController();
    streams++;
    reserved += limits.bodyBytes;
    const timer = setTimeout(() => {
      controller.abort();
      if (!stream.destroyed) stream.close(constants.NGHTTP2_CANCEL);
    }, limits.processingMs);
    timers.add(timer);
    const cancel = () => controller.abort();
    stream.once("aborted", cancel);
    stream.once("close", cancel);
    const task = Promise.resolve()
      .then(() => onStream(stream, headers, controller.signal, limits))
      .catch(() => {
        fail(503, "failure");
      })
      .finally(() => {
        clearTimeout(timer);
        timers.delete(timer);
        stream.off("aborted", cancel);
        stream.off("close", cancel);
        streams--;
        reserved -= limits.bodyBytes;
        work.delete(task);
        if (!stream.readableEnded) stream.resume();
      });
    work.add(task);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port, options.hostname, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing listener address");
  return {
    port: address.port,
    stats: () => ({
      connections: sockets.size,
      sessions: sessions.size,
      peers: peers.size,
      streams,
      reservedBytes: reserved,
      timers: timers.size,
      tasks: work.size,
    }),
    async stop() {
      if (draining) return;
      draining = true;
      const closing = [...sessions, ...sockets].map(
        (s) => new Promise<void>((resolve) => s.once("close", () => resolve())),
      );
      const closed = new Promise<void>((resolve) => server.close(() => resolve()));
      for (const s of sessions) s.close();
      const reap = setTimeout(() => {
        for (const s of sessions) s.destroy();
        for (const s of sockets) s.destroy();
      }, limits.drainMs);
      await Promise.all([closed, ...closing]);
      clearTimeout(reap);
      for (const t of timers) clearTimeout(t);
      timers.clear();
      await Promise.allSettled(work);
    },
  };
}
export async function createBoundedH2Client(options: {
  origin: string;
  servername: string;
  key: string | Buffer;
  cert: string | Buffer;
  ca: string | Buffer;
  expectedPeerSha256?: string;
  expectedSubjectAltname?: string;
  responseBytes?: number;
  timeoutMs?: number;
}) {
  const timeout = options.timeoutMs ?? 3000;
  const session = connect(options.origin, {
    key: options.key,
    cert: options.cert,
    ca: options.ca,
    servername: options.servername,
    rejectUnauthorized: true,
    minVersion: "TLSv1.3",
    maxVersion: "TLSv1.3",
    ALPNProtocols: ["h2"],
    maxHeaderListPairs: 32,
    maxSessionMemory: 4,
    settings: { maxHeaderListSize: 16384 },
  });
  session.on("error", () => {});
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("TLS timeout")), timeout);
      function finish(error?: Error) {
        clearTimeout(timer);
        session.off("error", errorHandler);
        session.off("connect", connected);
        if (error) reject(error);
        else resolve();
      }
      function errorHandler() {
        finish(new Error("TLS connection failed"));
      }
      function connected() {
        const socket = session.socket as TLSSocket;
        const cert = socket.getPeerCertificate();
        if (
          !socket.authorized ||
          socket.alpnProtocol !== "h2" ||
          !cert.raw?.length ||
          (options.expectedPeerSha256 && sha256(cert.raw) !== options.expectedPeerSha256) ||
          (options.expectedSubjectAltname && cert.subjectaltname !== options.expectedSubjectAltname)
        )
          finish(new Error("TLS peer mismatch"));
        else finish();
      }
      session.once("error", errorHandler);
      session.once("connect", connected);
    });
  } catch {
    session.destroy();
    throw new Error("Internal transport unavailable");
  }
  let closed = false,
    sessionEnded = false,
    active = 0;
  const pending = new Set<Promise<{ status: number; body: Buffer }>>();
  session.on("goaway", () => {
    closed = true;
  });
  session.on("close", () => {
    closed = true;
    sessionEnded = true;
  });
  return {
    async request(
      headers: OutgoingHttpHeaders,
      body: Uint8Array,
      signal?: AbortSignal,
    ): Promise<{ status: number; body: Buffer }> {
      if (closed || session.closed || session.destroyed || active >= 32)
        throw new Error("Internal transport unavailable");
      active++;
      const task = sendH2(session, headers, body, options.responseBytes ?? 8192, timeout, signal);
      pending.add(task);
      try {
        return await task;
      } finally {
        active--;
        pending.delete(task);
      }
    },
    async close() {
      closed = true;
      if (!sessionEnded) {
        const ended = new Promise<void>((resolve) => session.once("close", () => resolve()));
        session.destroy();
        await ended;
      }
      await Promise.allSettled(pending);
    },
    stats: () => ({ streams: active, closed }),
  };
}
function sendH2(
  session: ClientHttp2Session,
  headers: OutgoingHttpHeaders,
  body: Uint8Array,
  max: number,
  timeout: number,
  signal?: AbortSignal,
): Promise<{ status: number; body: Buffer }> {
  return new Promise((resolve, reject) => {
    let status = 0,
      size = 0,
      done = false;
    const chunks: Buffer[] = [];
    const stream = session.request(headers);
    const timer = setTimeout(() => finish(new Error("Internal response timeout")), timeout);
    function finish(error?: Error) {
      if (done) return;
      done = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) {
        for (const c of chunks) c.fill(0);
        chunks.length = 0;
        stream.close(constants.NGHTTP2_CANCEL);
        reject(error);
      } else {
        const bytes = Buffer.concat(chunks, size);
        for (const c of chunks) c.fill(0);
        resolve({ status, body: bytes });
        chunks.length = 0;
      }
    }
    function abort() {
      finish(new Error("Internal request cancelled"));
    }
    stream.on("response", (h) => {
      if (typeof h[":status"] !== "number" || h["content-type"] !== "application/json")
        finish(new Error("Invalid internal response"));
      else status = h[":status"];
    });
    stream.on("data", (chunk) => {
      size += chunk.length;
      if (size > max) finish(new Error("Internal response budget exceeded"));
      else chunks.push(Buffer.from(chunk));
    });
    stream.once("end", () => {
      if (!status) finish(new Error("Missing internal response"));
      else finish();
    });
    stream.once("error", () => finish(new Error("Internal response interrupted")));
    stream.once("aborted", () => finish(new Error("Internal response interrupted")));
    stream.once("close", () => {
      if (!done) finish(new Error("Internal response interrupted"));
    });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    else stream.end(body);
  });
}
