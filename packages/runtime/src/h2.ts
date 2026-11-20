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
  streams: 256,
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
function safeReply(
  stream: ServerHttp2Stream,
  status: number,
  value: Uint8Array,
  contentType = "application/json",
) {
  if (stream.destroyed || stream.closed || stream.headersSent) return;
  stream.respond({
    ":status": status,
    "content-type": contentType,
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
export function h2BytesReply(stream: ServerHttp2Stream, bytes: Uint8Array, maxBytes: number) {
  if (bytes.byteLength > maxBytes) throw new Error("Response budget exceeded");
  safeReply(stream, 200, bytes, "application/octet-stream");
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
    extraBodyBytesByPath?: Readonly<Record<string, number>>;
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
  for (const extra of Object.values(options.extraBodyBytesByPath ?? {}))
    if (!Number.isSafeInteger(extra) || extra < 0) throw new Error("Invalid transport budget");
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
    const path = headers[":path"];
    const extra =
      typeof path === "string" && Object.hasOwn(options.extraBodyBytesByPath ?? {}, path)
        ? options.extraBodyBytesByPath![path]!
        : 0;
    const declared = headers["content-length"];
    const pathLimit = limits.bodyBytes + extra;
    const streamLimits = extra
      ? {
          ...limits,
          bodyBytes:
            typeof declared === "string" && /^(0|[1-9][0-9]*)$/.test(declared)
              ? Math.min(Number(declared), pathLimit)
              : pathLimit,
        }
      : limits;
    if (
      draining ||
      streams >= limits.streams ||
      reserved + streamLimits.bodyBytes > limits.receiveBytes
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
    reserved += streamLimits.bodyBytes;
    const timer = setTimeout(() => {
      controller.abort();
      if (!stream.destroyed) stream.close(constants.NGHTTP2_CANCEL);
    }, limits.processingMs);
    timers.add(timer);
    const cancel = () => controller.abort();
    stream.once("aborted", cancel);
    stream.once("close", cancel);
    const task = Promise.resolve()
      .then(() => onStream(stream, headers, controller.signal, streamLimits))
      .catch(() => {
        fail(503, "failure");
      })
      .finally(() => {
        clearTimeout(timer);
        timers.delete(timer);
        stream.off("aborted", cancel);
        stream.off("close", cancel);
        streams--;
        reserved -= streamLimits.bodyBytes;
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
export class InternalTransportBusy extends Error {
  constructor() {
    super("Internal transport busy");
    this.name = "InternalTransportBusy";
  }
}
export class InternalTransportRefused extends Error {
  constructor() {
    super("Internal transport unavailable");
    this.name = "InternalTransportRefused";
  }
}
export class InternalTransportCancelled extends Error {
  constructor() {
    super("Internal request cancelled before dispatch");
    this.name = "InternalTransportCancelled";
  }
}
export class InternalTransportDeadline extends Error {
  constructor() {
    super("Internal dispatch deadline elapsed");
    this.name = "InternalTransportDeadline";
  }
}
export const h2ClientBudget = Object.freeze({ streams: 32, requests: 256 });
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
  const waiting: { resume: () => void; fail: (error: Error) => void }[] = [];
  async function slot(signal?: AbortSignal, dispatchDeadlineMs?: number) {
    if (dispatchDeadlineMs !== undefined && Date.now() >= dispatchDeadlineMs)
      throw new InternalTransportDeadline();
    if (active < h2ClientBudget.streams) {
      active++;
      return;
    }
    if (waiting.length + active >= h2ClientBudget.requests) throw new InternalTransportBusy();
    await new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
      };
      const waiter = {
        resume() {
          cleanup();
          resolve();
        },
        fail(error: Error) {
          cleanup();
          reject(error);
        },
      };
      function remove(error: Error) {
        waiting.splice(waiting.indexOf(waiter), 1);
        waiter.fail(error);
      }
      function abort() {
        remove(new Error("Internal transport unavailable"));
      }
      if (signal?.aborted) return reject(new Error("Internal transport unavailable"));
      signal?.addEventListener("abort", abort, { once: true });
      waiting.push(waiter);
      if (dispatchDeadlineMs !== undefined)
        timer = setTimeout(
          () => remove(new InternalTransportDeadline()),
          Math.max(0, dispatchDeadlineMs - Date.now()),
        );
    });
  }
  function release() {
    const next = waiting.shift();
    if (next) next.resume();
    else active--;
  }
  session.on("goaway", () => {
    closed = true;
  });
  session.on("close", () => {
    closed = true;
    sessionEnded = true;
  });
  return {
    async request(
      prepare: () => { headers: OutgoingHttpHeaders; body: Uint8Array },
      signal?: AbortSignal,
      reply?: {
        maxBytes?: number;
        timeoutMs?: number;
        binary?: boolean;
        dispatchDeadlineMs?: number;
        dispatchSignal?: AbortSignal;
      },
    ): Promise<{ status: number; body: Buffer }> {
      if (closed || session.closed || session.destroyed) throw new InternalTransportRefused();
      const dispatchSignal = reply?.dispatchSignal;
      if (dispatchSignal?.aborted) throw new InternalTransportCancelled();
      const waitingSignal =
        signal && dispatchSignal
          ? AbortSignal.any([signal, dispatchSignal])
          : (signal ?? dispatchSignal);
      try {
        await slot(waitingSignal, reply?.dispatchDeadlineMs);
      } catch (error) {
        if (dispatchSignal?.aborted) throw new InternalTransportCancelled();
        throw error;
      }
      try {
        if (closed || session.closed || session.destroyed) throw new InternalTransportRefused();
        if (reply?.dispatchDeadlineMs !== undefined && Date.now() >= reply.dispatchDeadlineMs)
          throw new InternalTransportDeadline();
        if (dispatchSignal?.aborted) throw new InternalTransportCancelled();
        const { headers, body } = prepare();
        if (dispatchSignal?.aborted) throw new InternalTransportCancelled();
        if (reply?.dispatchDeadlineMs !== undefined && Date.now() >= reply.dispatchDeadlineMs)
          throw new InternalTransportDeadline();
        const task = sendH2(
          session,
          headers,
          body,
          reply?.maxBytes ?? options.responseBytes ?? 8192,
          reply?.timeoutMs ?? timeout,
          signal,
          reply?.binary === true,
        );
        pending.add(task);
        try {
          return await task;
        } finally {
          pending.delete(task);
        }
      } finally {
        release();
      }
    },
    async retire() {
      closed = true;
      await Promise.allSettled(pending);
      for (const waiter of waiting.splice(0)) waiter.fail(new InternalTransportRefused());
      if (!sessionEnded) {
        const ended = new Promise<void>((resolve) => session.once("close", () => resolve()));
        session.destroy();
        await ended;
      }
    },
    async close() {
      closed = true;
      for (const waiter of waiting.splice(0))
        waiter.fail(new Error("Internal transport unavailable"));
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
  binary = false,
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
    let limit = max;
    stream.on("response", (h) => {
      const code = h[":status"];
      const json = h["content-type"] === "application/json";
      if (
        typeof code !== "number" ||
        (binary ? (code === 200 ? h["content-type"] !== "application/octet-stream" : !json) : !json)
      )
        finish(new Error("Invalid internal response"));
      else {
        status = code;
        if (json && binary) limit = 8192;
      }
    });
    stream.on("data", (chunk) => {
      size += chunk.length;
      if (size > limit) finish(new Error("Internal response budget exceeded"));
      else chunks.push(Buffer.from(chunk));
    });
    stream.once("end", () => {
      if (!status) finish(new Error("Missing internal response"));
      else finish();
    });
    const interrupted = () =>
      !status && stream.rstCode === constants.NGHTTP2_REFUSED_STREAM
        ? new InternalTransportRefused()
        : new Error("Internal response interrupted");
    stream.on("error", () => {});
    stream.once("aborted", () => finish(interrupted()));
    stream.once("close", () => {
      if (!done) finish(interrupted());
    });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    else stream.end(body);
  });
}
