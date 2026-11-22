import { PassThrough, Readable, pipeline } from "node:stream";
import {
  createBrotliDecompress,
  createGunzip,
  createInflate,
  createZstdDecompress,
} from "node:zlib";
import { GatewayError } from "@gateway/contracts";
import { UpstreamPreDispatchError } from "./http-pool.ts";

const HEADER_END = Buffer.from("\r\n\r\n");
const CRLF = Buffer.from("\r\n");
const MAX_HEADER_BYTES = 65536;
const MAX_INFORMATIONAL = 5;
const FAST_BUFFER_MAX = 65536;
const IDLE_TTL_MS = 30_000;
const IDLE_SWEEP_MS = 5_000;
const LOCAL_ERRNOS = new Set(["EADDRNOTAVAIL", "EMFILE", "ENFILE"]);
const TCHAR_TABLE = new Uint8Array(128);
for (const ch of "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
  TCHAR_TABLE[ch.charCodeAt(0)] = 1;
}

export function isValidHttpHeaderName(name: string): boolean {
  const len = name.length;
  if (len === 0) return false;
  for (let i = 0; i < len; i++) {
    const c = name.charCodeAt(i);
    if (c >= 128 || TCHAR_TABLE[c] !== 1) return false;
  }
  return true;
}

export function isValidHttpHeaderValue(value: string): boolean {
  const len = value.length;
  for (let i = 0; i < len; i++) {
    const c = value.charCodeAt(i);
    if (c > 255 || (c < 32 && c !== 9) || c === 127) return false;
  }
  return true;
}

function isValidHttpPath(path: string): boolean {
  const len = path.length;
  if (len === 0 || path.charCodeAt(0) !== 47) return false;
  for (let i = 1; i < len; i++) {
    const c = path.charCodeAt(i);
    if (c < 33 || c > 126) return false;
  }
  return true;
}

function trimOws(value: string): string {
  let start = 0;
  let end = value.length;
  while (start < end) {
    const c = value.charCodeAt(start);
    if (c !== 32 && c !== 9) break;
    start++;
  }
  while (end > start) {
    const c = value.charCodeAt(end - 1);
    if (c !== 32 && c !== 9) break;
    end--;
  }
  return start === 0 && end === value.length ? value : value.slice(start, end);
}

function parseAsciiDigits(value: string): number {
  const len = value.length;
  if (len === 0) return -1;
  let n = 0;
  for (let i = 0; i < len; i++) {
    const d = value.charCodeAt(i) - 48;
    if (d < 0 || d > 9) return -1;
    n = n * 10 + d;
    if (!Number.isSafeInteger(n)) return -1;
  }
  return n;
}

type ChunkState =
  | { phase: "size" }
  | { phase: "data"; remaining: number }
  | { phase: "data_crlf" }
  | { phase: "trailers" }
  | { phase: "done" };

type ActiveExchange = {
  method: string;
  signal: AbortSignal | undefined;
  dispatchDeadline: number | undefined;
  canHaveDispatched: boolean;
  settledHeader: boolean;
  finished: boolean;
  informationalCount: number;
  headerBuf: Buffer | null;
  status: number;
  headers: Headers | Record<string, string>;
  connectionClose: boolean;
  isChunked: boolean;
  contentLength: number;
  bodyRemaining: number;
  bufferedChunks: Uint8Array[] | null;
  bufferedTotal: number;
  streamController: ReadableStreamDefaultController<Uint8Array> | null;
  zlibInput: PassThrough | null;
  chunkBuf: Buffer;
  chunkState: ChunkState;
  dispatchTimer: ReturnType<typeof setTimeout> | undefined;
  abortHandler: (() => void) | undefined;
  pendingWrite: Buffer | null;
  resolve: (res: Response) => void;
  reject: (err: unknown) => void;
  releaseSlot: () => void;
  slotReleased: boolean;
};

type PooledConn = {
  origin: string;
  socket: Bun.Socket<PooledConn> | null;
  connected: boolean;
  closed: boolean;
  inUse: boolean;
  exchange: ActiveExchange | null;
  writeBuf: Buffer | null;
  writeOffset: number;
};

function notDispatched(): UpstreamPreDispatchError {
  return new UpstreamPreDispatchError(
    "upstream_not_dispatched",
    "Upstream request stopped before obtaining a connection",
  );
}

function buildRequestBuffer(
  method: string,
  path: string,
  headers: Record<string, string>,
  body: string | Uint8Array | undefined,
): Buffer {
  if (!isValidHttpHeaderName(method)) {
    throw new TypeError("Invalid HTTP method");
  }
  if (!isValidHttpPath(path)) {
    throw new TypeError("Invalid HTTP request target");
  }
  let head = `${method} ${path} HTTP/1.1\r\n`;
  let hasConnection = false;
  for (const key in headers) {
    const val = headers[key];
    if (val === undefined) continue;
    if (!isValidHttpHeaderName(key) || !isValidHttpHeaderValue(val)) {
      throw new TypeError("Invalid HTTP header");
    }
    const lower = key.toLowerCase();
    if (lower === "transfer-encoding") {
      throw new TypeError("Transfer-Encoding is not permitted on buffered requests");
    }
    if (lower === "content-length") {
      continue;
    }
    if (lower === "connection") {
      hasConnection = true;
      head += "connection: keep-alive\r\n";
      continue;
    }
    head += `${key}: ${val}\r\n`;
  }
  const bodyByteLength =
    body === undefined
      ? -1
      : typeof body === "string"
        ? Buffer.byteLength(body, "utf8")
        : body.byteLength;
  if (bodyByteLength >= 0) {
    head += `content-length: ${bodyByteLength}\r\n`;
  } else if (method === "POST" || method === "PUT" || method === "PATCH") {
    head += "content-length: 0\r\n";
  }
  if (!hasConnection) head += "connection: keep-alive\r\n";
  head += "\r\n";
  if (body === undefined) return Buffer.from(head, "latin1");
  if (typeof body === "string") {
    const headLen = Buffer.byteLength(head, "latin1");
    const out = Buffer.allocUnsafe(headLen + bodyByteLength);
    out.write(head, 0, headLen, "latin1");
    out.write(body, headLen, bodyByteLength, "utf8");
    return out;
  }
  const headBuf = Buffer.from(head, "latin1");
  const out = Buffer.allocUnsafe(headBuf.byteLength + body.byteLength);
  headBuf.copy(out, 0);
  out.set(body, headBuf.byteLength);
  return out;
}

function parseHeadersBlock(headStr: string): {
  status: number;
  http10: boolean;
  headers: Headers | Record<string, string>;
  contentEncoding: string;
  contentType: string;
  contentLengthStr: string | null;
  transferEncoding: string | null;
  connectionClose: boolean;
} {
  const firstEnd = headStr.indexOf("\r\n");
  const statusLine = firstEnd === -1 ? headStr : headStr.slice(0, firstEnd);
  if (
    statusLine.length < 12 ||
    !statusLine.startsWith("HTTP/1.") ||
    statusLine.charCodeAt(8) !== 32
  ) {
    throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP status line");
  }
  const minorCode = statusLine.charCodeAt(7);
  if (minorCode !== 48 && minorCode !== 49) {
    throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP status line");
  }
  const http10 = minorCode === 48;
  const d0 = statusLine.charCodeAt(9) - 48;
  const d1 = statusLine.charCodeAt(10) - 48;
  const d2 = statusLine.charCodeAt(11) - 48;
  if (d0 < 1 || d0 > 5 || d1 < 0 || d1 > 9 || d2 < 0 || d2 > 9) {
    throw new GatewayError(502, "invalid_upstream", "Invalid upstream HTTP status code");
  }
  if (statusLine.length > 12) {
    if (statusLine.charCodeAt(12) !== 32) {
      throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP status line");
    }
    for (let i = 13; i < statusLine.length; i++) {
      const c = statusLine.charCodeAt(i);
      if (c > 255 || (c < 32 && c !== 9) || c === 127) {
        throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP status line");
      }
    }
  }
  const status = d0 * 100 + d1 * 10 + d2;

  let record: Record<string, string> | null = {};
  let multiHeaders: Headers | null = null;
  let contentEncoding = "";
  let contentType = "";
  let contentLengthStr: string | null = null;
  let transferEncoding: string | null = null;
  let connectionHeader = "";

  let pos = firstEnd === -1 ? headStr.length : firstEnd + 2;
  while (pos < headStr.length) {
    let next = headStr.indexOf("\r\n", pos);
    if (next === -1) next = headStr.length;
    const colon = headStr.indexOf(":", pos);
    if (colon === -1 || colon >= next) {
      throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP header");
    }
    const rawName = headStr.slice(pos, colon);
    if (!isValidHttpHeaderName(rawName)) {
      throw new GatewayError(502, "invalid_upstream", "Invalid upstream HTTP header name");
    }
    const lower = rawName.toLowerCase();
    const rawVal = headStr.slice(colon + 1, next);
    if (!isValidHttpHeaderValue(rawVal)) {
      throw new GatewayError(502, "invalid_upstream", "Invalid upstream HTTP header value");
    }
    const val = trimOws(rawVal);
    if (lower === "content-encoding") {
      contentEncoding = contentEncoding ? `${contentEncoding},${val}` : val;
    } else if (lower === "content-type") {
      contentType = val;
    } else if (lower === "content-length") {
      if (val.indexOf(",") === -1) {
        const parsedLen = parseAsciiDigits(val);
        if (parsedLen < 0) {
          throw new GatewayError(502, "invalid_upstream", "Invalid upstream content-length");
        }
        const canonicalLen = String(parsedLen);
        if (contentLengthStr !== null && contentLengthStr !== canonicalLen) {
          throw new GatewayError(502, "invalid_upstream", "Conflicting upstream content-length");
        }
        contentLengthStr = canonicalLen;
      } else {
        const parts = val.split(",");
        for (const rawPart of parts) {
          const parsedLen = parseAsciiDigits(rawPart.trim());
          if (parsedLen < 0) {
            throw new GatewayError(502, "invalid_upstream", "Invalid upstream content-length");
          }
          const canonicalLen = String(parsedLen);
          if (contentLengthStr !== null && contentLengthStr !== canonicalLen) {
            throw new GatewayError(502, "invalid_upstream", "Conflicting upstream content-length");
          }
          contentLengthStr = canonicalLen;
        }
      }
    } else if (lower === "transfer-encoding") {
      transferEncoding = transferEncoding ? `${transferEncoding},${val}` : val;
    } else if (lower === "connection") {
      connectionHeader = connectionHeader ? `${connectionHeader},${val}` : val;
    }
    if (multiHeaders) {
      multiHeaders.append(rawName, val);
    } else if (lower in record!) {
      multiHeaders = new Headers();
      for (const k in record!) multiHeaders.append(k, record![k]!);
      multiHeaders.append(rawName, val);
      record = null;
    } else {
      record![lower] = val;
    }
    pos = next + 2;
  }

  if (transferEncoding !== null) {
    if (contentLengthStr !== null) {
      throw new GatewayError(
        502,
        "invalid_upstream",
        "Upstream response contains both Transfer-Encoding and Content-Length",
      );
    }
    const teTokens = transferEncoding
      .split(",")
      .map((t) => t.trim().toLowerCase())
      .filter(Boolean);
    if (teTokens.length !== 1 || teTokens[0] !== "chunked") {
      throw new GatewayError(502, "invalid_upstream", "Unsupported upstream Transfer-Encoding");
    }
  }

  const connectionClose = http10
    ? !/\bkeep-alive\b/i.test(connectionHeader)
    : /\bclose\b/i.test(connectionHeader);

  return {
    status,
    http10,
    headers: multiHeaders ?? record!,
    contentEncoding,
    contentType,
    contentLengthStr,
    transferEncoding,
    connectionClose,
  };
}

export function createBunHttp1Pool(
  maxIdle: number,
  sseFraming: WeakMap<Response, "chunked" | number>,
  bufferedBodies: WeakMap<Response, Uint8Array>,
) {
  const idleByOrigin = new Map<string, PooledConn[]>();
  const idleOrder = new Map<PooledConn, number>();
  const allConns = new Set<PooledConn>();
  let sweepTimer: ReturnType<typeof setInterval> | undefined;
  let closed = false;

  const ensureSweep = () => {
    if (sweepTimer) return;
    sweepTimer = setInterval(() => {
      if (idleOrder.size === 0) return;
      const cutoff = performance.now() - IDLE_TTL_MS;
      for (const [conn, since] of idleOrder) {
        if (since > cutoff) break;
        removeIdle(conn);
        destroyConn(conn);
      }
    }, IDLE_SWEEP_MS);
    sweepTimer.unref();
  };

  const removeIdle = (conn: PooledConn) => {
    if (!idleOrder.delete(conn)) return;
    const list = idleByOrigin.get(conn.origin);
    if (list) {
      const idx = list.indexOf(conn);
      if (idx !== -1) list.splice(idx, 1);
      if (list.length === 0) idleByOrigin.delete(conn.origin);
    }
  };

  const terminateSocket = (sock: Bun.Socket<PooledConn>) => {
    try {
      sock.terminate();
    } catch {
      try {
        sock.end();
      } catch {
        void 0;
      }
    }
  };

  const destroyConn = (conn: PooledConn) => {
    removeIdle(conn);
    allConns.delete(conn);
    conn.closed = true;
    conn.inUse = false;
    conn.writeBuf = null;
    conn.writeOffset = 0;
    const sock = conn.socket;
    conn.socket = null;
    if (sock) terminateSocket(sock);
  };

  const returnToIdle = (conn: PooledConn) => {
    if (closed || conn.closed || !conn.socket || maxIdle === 0) {
      destroyConn(conn);
      return;
    }
    conn.inUse = false;
    conn.exchange = null;
    let list = idleByOrigin.get(conn.origin);
    if (!list) {
      list = [];
      idleByOrigin.set(conn.origin, list);
    }
    list.push(conn);
    idleOrder.delete(conn);
    idleOrder.set(conn, performance.now());
    ensureSweep();
    while (idleOrder.size > maxIdle) {
      const oldest = idleOrder.keys().next().value;
      if (!oldest) break;
      removeIdle(oldest);
      destroyConn(oldest);
    }
  };

  const popIdle = (origin: string): PooledConn | null => {
    const list = idleByOrigin.get(origin);
    if (!list) return null;
    while (list.length > 0) {
      const conn = list.pop()!;
      idleOrder.delete(conn);
      if (!conn.closed && conn.socket) {
        if (list.length === 0) idleByOrigin.delete(origin);
        conn.inUse = true;
        return conn;
      }
    }
    idleByOrigin.delete(origin);
    return null;
  };

  const cleanupExchange = (ex: ActiveExchange) => {
    if (ex.dispatchTimer) {
      clearTimeout(ex.dispatchTimer);
      ex.dispatchTimer = undefined;
    }
    if (ex.abortHandler) {
      ex.signal?.removeEventListener("abort", ex.abortHandler);
      ex.abortHandler = undefined;
    }
    if (!ex.slotReleased) {
      ex.slotReleased = true;
      ex.releaseSlot();
    }
  };

  const finishSuccess = (conn: PooledConn, ex: ActiveExchange, reuse: boolean) => {
    if (ex.finished) return;
    ex.finished = true;
    conn.exchange = null;
    cleanupExchange(ex);
    if (reuse && !ex.connectionClose && !conn.closed) returnToIdle(conn);
    else destroyConn(conn);
  };

  const failExchange = (conn: PooledConn, ex: ActiveExchange, err: unknown) => {
    if (ex.finished) return;
    ex.finished = true;
    conn.exchange = null;
    cleanupExchange(ex);
    destroyConn(conn);
    if (!ex.settledHeader) {
      ex.settledHeader = true;
      ex.reject(err);
      return;
    }
    if (ex.zlibInput) {
      const input = ex.zlibInput;
      ex.zlibInput = null;
      if (!input.destroyed) {
        input.destroy(err instanceof Error ? err : new Error("Upstream stream failed"));
      }
      return;
    }
    if (ex.streamController) {
      const ctrl = ex.streamController;
      ex.streamController = null;
      try {
        ctrl.error(err);
      } catch {
        void 0;
      }
      return;
    }
    ex.reject(err);
  };

  const cancelActiveExchange = (conn: PooledConn, ex: ActiveExchange) => {
    if (ex.finished) return;
    ex.finished = true;
    conn.exchange = null;
    ex.zlibInput = null;
    ex.streamController = null;
    cleanupExchange(ex);
    destroyConn(conn);
  };

  const createResponse = (
    conn: PooledConn,
    ex: ActiveExchange,
    body: BodyInit | null,
    status: number,
    headers: Headers | Record<string, string>,
  ): Response | null => {
    try {
      return new Response(body, { status, headers });
    } catch (err) {
      failExchange(conn, ex, err);
      return null;
    }
  };

  const flushWrite = (conn: PooledConn) => {
    const sock = conn.socket;
    if (!sock || !conn.writeBuf) return;
    while (conn.writeOffset < conn.writeBuf.byteLength) {
      const slice =
        conn.writeOffset === 0 ? conn.writeBuf : conn.writeBuf.subarray(conn.writeOffset);
      let written: number;
      try {
        written = sock.write(slice);
      } catch (err) {
        conn.writeBuf = null;
        conn.writeOffset = 0;
        const ex = conn.exchange;
        if (ex && !ex.finished) failExchange(conn, ex, err);
        else destroyConn(conn);
        return;
      }
      if (written < 0) {
        conn.writeBuf = null;
        conn.writeOffset = 0;
        const ex = conn.exchange;
        const err = new Error("Upstream socket write failed");
        if (ex && !ex.finished) failExchange(conn, ex, err);
        else destroyConn(conn);
        return;
      }
      if (written === 0) return;
      conn.writeOffset += written;
    }
    conn.writeBuf = null;
    conn.writeOffset = 0;
  };

  const emitBodyChunk = (conn: PooledConn, ex: ActiveExchange, chunk: Uint8Array) => {
    if (chunk.byteLength === 0 || ex.finished) return;
    if (ex.zlibInput) {
      if (ex.zlibInput.destroyed) {
        cancelActiveExchange(conn, ex);
        return;
      }
      ex.zlibInput.write(chunk);
    } else if (ex.streamController) {
      try {
        ex.streamController.enqueue(chunk);
      } catch {
        cancelActiveExchange(conn, ex);
      }
    }
  };

  const closeBodySink = (ex: ActiveExchange) => {
    if (ex.zlibInput) {
      const input = ex.zlibInput;
      ex.zlibInput = null;
      if (!input.destroyed) input.end();
    } else if (ex.streamController) {
      const ctrl = ex.streamController;
      ex.streamController = null;
      try {
        ctrl.close();
      } catch {
        void 0;
      }
    }
  };

  const feedChunked = (conn: PooledConn, ex: ActiveExchange, data: Buffer) => {
    let buf = ex.chunkBuf.byteLength > 0 ? Buffer.concat([ex.chunkBuf, data]) : data;
    let offset = 0;
    while (offset < buf.byteLength && !ex.finished) {
      const st = ex.chunkState;
      if (st.phase === "size") {
        const idx = buf.indexOf(CRLF, offset);
        if (idx === -1) {
          if (buf.byteLength - offset > 4096) {
            failExchange(
              conn,
              ex,
              new GatewayError(502, "invalid_upstream", "Chunk size line too long"),
            );
            return;
          }
          break;
        }
        const line = buf.subarray(offset, idx).toString("latin1");
        if (!isValidHttpHeaderValue(line)) {
          failExchange(conn, ex, new GatewayError(502, "invalid_upstream", "Invalid chunk line"));
          return;
        }
        const semi = line.indexOf(";");
        const rawHex = semi === -1 ? line : line.slice(0, semi);
        const hex = trimOws(rawHex);
        if (
          hex.length === 0 ||
          (rawHex.length > 0 && (rawHex.charCodeAt(0) === 32 || rawHex.charCodeAt(0) === 9)) ||
          !/^[0-9a-fA-F]+$/.test(hex)
        ) {
          failExchange(conn, ex, new GatewayError(502, "invalid_upstream", "Invalid chunk size"));
          return;
        }
        const size = Number.parseInt(hex, 16);
        if (!Number.isSafeInteger(size) || size < 0) {
          failExchange(conn, ex, new GatewayError(502, "invalid_upstream", "Chunk size overflow"));
          return;
        }
        offset = idx + 2;
        if (size === 0) {
          ex.chunkState = { phase: "trailers" };
        } else {
          ex.chunkState = { phase: "data", remaining: size };
        }
      } else if (st.phase === "data") {
        const avail = buf.byteLength - offset;
        const take = Math.min(avail, st.remaining);
        if (take > 0) {
          emitBodyChunk(conn, ex, Buffer.from(buf.subarray(offset, offset + take)));
          if (ex.finished) return;
          offset += take;
          st.remaining -= take;
        }
        if (st.remaining === 0) {
          ex.chunkState = { phase: "data_crlf" };
        }
      } else if (st.phase === "data_crlf") {
        if (buf.byteLength - offset < 2) break;
        if (buf[offset] !== 13 || buf[offset + 1] !== 10) {
          failExchange(
            conn,
            ex,
            new GatewayError(502, "invalid_upstream", "Malformed chunk terminator"),
          );
          return;
        }
        offset += 2;
        ex.chunkState = { phase: "size" };
      } else if (st.phase === "trailers") {
        if (buf.byteLength - offset >= 2 && buf[offset] === 13 && buf[offset + 1] === 10) {
          offset += 2;
          ex.chunkState = { phase: "done" };
          ex.chunkBuf = Buffer.alloc(0);
          closeBodySink(ex);
          finishSuccess(conn, ex, offset === buf.byteLength);
          return;
        }
        const end = buf.indexOf(HEADER_END, offset);
        if (end === -1) {
          if (buf.byteLength - offset > MAX_HEADER_BYTES) {
            failExchange(
              conn,
              ex,
              new GatewayError(502, "invalid_upstream", "Upstream chunked trailers too large"),
            );
            return;
          }
          break;
        }
        if (end - offset > MAX_HEADER_BYTES) {
          failExchange(
            conn,
            ex,
            new GatewayError(502, "invalid_upstream", "Upstream chunked trailers too large"),
          );
          return;
        }
        offset = end + 4;
        ex.chunkState = { phase: "done" };
        ex.chunkBuf = Buffer.alloc(0);
        closeBodySink(ex);
        finishSuccess(conn, ex, offset === buf.byteLength);
        return;
      } else {
        break;
      }
    }
    if (ex.finished) {
      ex.chunkBuf = Buffer.alloc(0);
      return;
    }
    ex.chunkBuf = offset < buf.byteLength ? Buffer.from(buf.subarray(offset)) : Buffer.alloc(0);
  };

  const feedBody = (conn: PooledConn, ex: ActiveExchange, data: Buffer) => {
    if (ex.finished || data.byteLength === 0) return;
    if (ex.bufferedChunks !== null) {
      const take = Math.min(data.byteLength, ex.bodyRemaining);
      const copy = Buffer.from(data.subarray(0, take));
      ex.bufferedChunks.push(copy);
      ex.bufferedTotal += take;
      ex.bodyRemaining -= take;
      if (ex.bodyRemaining === 0) {
        const extra = data.byteLength > take;
        const body =
          ex.bufferedChunks.length === 1
            ? ex.bufferedChunks[0]!
            : Buffer.concat(ex.bufferedChunks, ex.bufferedTotal);
        ex.bufferedChunks = null;
        const res = createResponse(conn, ex, body as BodyInit, ex.status, ex.headers);
        if (!res) return;
        ex.settledHeader = true;
        bufferedBodies.set(res, body);
        finishSuccess(conn, ex, !extra);
        ex.resolve(res);
      }
      return;
    }
    if (ex.isChunked) {
      feedChunked(conn, ex, data);
      return;
    }
    if (ex.contentLength >= 0) {
      const take = Math.min(data.byteLength, ex.bodyRemaining);
      if (take > 0) {
        emitBodyChunk(conn, ex, Buffer.from(data.subarray(0, take)));
        if (ex.finished) return;
        ex.bodyRemaining -= take;
      }
      if (ex.bodyRemaining === 0) {
        const extra = data.byteLength > take;
        closeBodySink(ex);
        finishSuccess(conn, ex, !extra);
      }
      return;
    }
    emitBodyChunk(conn, ex, Buffer.from(data));
  };

  const handleHeadersComplete = (
    conn: PooledConn,
    ex: ActiveExchange,
    buf: Buffer,
    sep: number,
  ) => {
    let curBuf = buf;
    let curSep = sep;
    let parsed: ReturnType<typeof parseHeadersBlock>;
    let rest: Buffer;
    while (true) {
      if (curSep > MAX_HEADER_BYTES) {
        failExchange(
          conn,
          ex,
          new GatewayError(502, "invalid_upstream", "Upstream response headers too large"),
        );
        return;
      }
      try {
        parsed = parseHeadersBlock(curBuf.subarray(0, curSep).toString("latin1"));
      } catch (err) {
        failExchange(conn, ex, err);
        return;
      }
      rest = curBuf.subarray(curSep + 4);
      if (parsed.status === 101) {
        failExchange(
          conn,
          ex,
          new GatewayError(502, "unsupported_upstream_upgrade", "Upstream upgrade is unsupported"),
        );
        return;
      }
      if (parsed.status >= 100 && parsed.status < 200) {
        if (++ex.informationalCount > MAX_INFORMATIONAL) {
          failExchange(
            conn,
            ex,
            new GatewayError(502, "invalid_upstream", "Too many upstream 1xx responses"),
          );
          return;
        }
        if (rest.byteLength === 0) return;
        const nextSep = rest.indexOf(HEADER_END);
        if (nextSep === -1) {
          if (rest.byteLength > MAX_HEADER_BYTES) {
            failExchange(
              conn,
              ex,
              new GatewayError(502, "invalid_upstream", "Upstream response headers too large"),
            );
            return;
          }
          ex.headerBuf = Buffer.from(rest);
          return;
        }
        curBuf = rest;
        curSep = nextSep;
        continue;
      }
      break;
    }

    ex.status = parsed.status;
    ex.headers = parsed.headers;
    ex.connectionClose = parsed.connectionClose;
    const isChunked = parsed.transferEncoding !== null;
    const contentLen =
      !isChunked && parsed.contentLengthStr !== null ? Number(parsed.contentLengthStr) : -1;
    ex.isChunked = isChunked;
    ex.contentLength = contentLen;
    ex.bodyRemaining = contentLen;

    if (
      parsed.status === 204 ||
      parsed.status === 205 ||
      parsed.status === 304 ||
      ex.method === "HEAD"
    ) {
      const res = createResponse(conn, ex, null, parsed.status, parsed.headers);
      if (!res) return;
      ex.settledHeader = true;
      finishSuccess(conn, ex, rest.byteLength === 0);
      ex.resolve(res);
      return;
    }

    const encodings = parsed.contentEncoding
      ? parsed.contentEncoding
          .split(",")
          .map((value) => value.trim().toLowerCase())
          .filter((value) => value && value !== "identity")
      : [];

    const isSse = /^text\/event-stream(?:\s*;|$)/i.test(parsed.contentType);
    if (
      encodings.length === 0 &&
      !isSse &&
      !isChunked &&
      contentLen >= 0 &&
      contentLen <= FAST_BUFFER_MAX
    ) {
      if (contentLen === 0) {
        const empty = new Uint8Array(0);
        const res = createResponse(conn, ex, empty as BodyInit, parsed.status, parsed.headers);
        if (!res) return;
        ex.settledHeader = true;
        bufferedBodies.set(res, empty);
        finishSuccess(conn, ex, rest.byteLength === 0);
        ex.resolve(res);
        return;
      }
      if (rest.byteLength >= contentLen) {
        const copy = Buffer.from(rest.subarray(0, contentLen));
        const extra = rest.byteLength > contentLen;
        const res = createResponse(conn, ex, copy as BodyInit, parsed.status, parsed.headers);
        if (!res) return;
        ex.settledHeader = true;
        bufferedBodies.set(res, copy);
        finishSuccess(conn, ex, !extra);
        ex.resolve(res);
        return;
      }
      ex.bufferedChunks = [];
      ex.bufferedTotal = 0;
      if (rest.byteLength > 0) feedBody(conn, ex, rest);
      return;
    }

    const decompressors = {
      gzip: createGunzip,
      "x-gzip": createGunzip,
      deflate: createInflate,
      br: createBrotliDecompress,
      zstd: createZstdDecompress,
    };

    if (encodings.length > 0 && encodings.every((value) => Object.hasOwn(decompressors, value))) {
      let outputHeaders: Headers;
      try {
        outputHeaders =
          parsed.headers instanceof Headers ? parsed.headers : new Headers(parsed.headers);
      } catch (err) {
        failExchange(conn, ex, err);
        return;
      }
      outputHeaders.delete("content-encoding");
      outputHeaders.delete("content-length");
      if (!isChunked && contentLen === 0) {
        const empty = new Uint8Array(0);
        const res = createResponse(conn, ex, empty as BodyInit, parsed.status, outputHeaders);
        if (!res) return;
        ex.settledHeader = true;
        bufferedBodies.set(res, empty);
        finishSuccess(conn, ex, rest.byteLength === 0);
        ex.resolve(res);
        return;
      }
      const input = new PassThrough();
      const streams = encodings
        .reverse()
        .map((value) => decompressors[value as keyof typeof decompressors]());
      const tail = streams.at(-1)!;
      const webBody = Readable.toWeb(tail, {
        strategy: { highWaterMark: 16384, size: (chunk: Uint8Array) => chunk.byteLength },
      }) as unknown as ReadableStream<Uint8Array>;
      const res = createResponse(conn, ex, webBody, parsed.status, outputHeaders);
      if (!res) {
        input.destroy();
        for (const s of streams) s.destroy();
        return;
      }
      ex.zlibInput = input;
      ex.settledHeader = true;
      pipeline([input, ...streams], (err) => {
        if (err && !ex.finished && ex.zlibInput === input) {
          cancelActiveExchange(conn, ex);
        }
      });
      input.once("close", () => {
        if (!ex.finished && ex.zlibInput === input) {
          cancelActiveExchange(conn, ex);
        }
      });
      if (rest.byteLength > 0) {
        feedBody(conn, ex, rest);
      }
      ex.resolve(res);
      return;
    }

    if (!isChunked && contentLen < 0) ex.connectionClose = true;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        ex.streamController = controller;
      },
      cancel() {
        cancelActiveExchange(conn, ex);
      },
    });
    const res = createResponse(conn, ex, stream, parsed.status, parsed.headers);
    if (!res) return;
    ex.settledHeader = true;
    if (!encodings.length && isSse) {
      if (
        parsed.contentLengthStr === null &&
        parsed.transferEncoding?.toLowerCase() === "chunked"
      ) {
        sseFraming.set(res, "chunked");
      } else if (
        parsed.transferEncoding === null &&
        contentLen >= 0 &&
        Number.isSafeInteger(contentLen)
      ) {
        sseFraming.set(res, contentLen);
      }
    }
    if (!isChunked && contentLen === 0) {
      closeBodySink(ex);
      finishSuccess(conn, ex, rest.byteLength === 0);
    } else if (rest.byteLength > 0) {
      feedBody(conn, ex, rest);
    }
    ex.resolve(res);
  };

  const socketHandlers: Bun.SocketHandler<PooledConn> = {
    open(socket) {
      const conn = socket.data;
      if (closed || conn.closed) {
        terminateSocket(socket);
        return;
      }
      conn.socket = socket;
      conn.connected = true;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        returnToIdle(conn);
        return;
      }
      if (ex.dispatchTimer) {
        clearTimeout(ex.dispatchTimer);
        ex.dispatchTimer = undefined;
      }
      const expired =
        ex.dispatchDeadline !== undefined && !(performance.now() < ex.dispatchDeadline);
      if (ex.signal?.aborted || expired) {
        failExchange(conn, ex, notDispatched());
        return;
      }
      ex.canHaveDispatched = true;
      const reqBuf = ex.pendingWrite;
      ex.pendingWrite = null;
      if (reqBuf) {
        conn.writeBuf = reqBuf;
        conn.writeOffset = 0;
        flushWrite(conn);
      }
    },
    data(socket, chunk) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      if (!ex.settledHeader && ex.bufferedChunks === null) {
        const buf = ex.headerBuf ? Buffer.concat([ex.headerBuf, chunk]) : Buffer.from(chunk);
        const sep = buf.indexOf(HEADER_END);
        if (sep === -1) {
          if (buf.byteLength > MAX_HEADER_BYTES) {
            failExchange(
              conn,
              ex,
              new GatewayError(502, "invalid_upstream", "Upstream response headers too large"),
            );
            return;
          }
          ex.headerBuf = buf;
          return;
        }
        ex.headerBuf = null;
        handleHeadersComplete(conn, ex, buf, sep);
        return;
      }
      feedBody(conn, ex, chunk);
    },
    drain(socket) {
      flushWrite(socket.data);
    },
    end(socket) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      if (ex.settledHeader && !ex.isChunked && ex.contentLength < 0) {
        closeBodySink(ex);
        finishSuccess(conn, ex, false);
        return;
      }
      if (ex.settledHeader && ex.contentLength >= 0 && ex.bodyRemaining === 0) {
        closeBodySink(ex);
        finishSuccess(conn, ex, false);
        return;
      }
      failExchange(conn, ex, new Error("Upstream closed connection prematurely"));
    },
    close(socket) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      if (ex.settledHeader && !ex.isChunked && ex.contentLength < 0) {
        closeBodySink(ex);
        finishSuccess(conn, ex, false);
        return;
      }
      const err = !ex.canHaveDispatched
        ? new UpstreamPreDispatchError(
            "upstream_connect",
            "Upstream connection failed before dispatch",
          )
        : new Error("Upstream connection closed");
      failExchange(conn, ex, err);
    },
    error(socket, error) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      const code = String((error as NodeJS.ErrnoException).code ?? "");
      const wrapped =
        !ex.canHaveDispatched && !LOCAL_ERRNOS.has(code)
          ? new UpstreamPreDispatchError(
              "upstream_connect",
              "Upstream connection failed before dispatch",
              error,
            )
          : error;
      failExchange(conn, ex, wrapped);
    },
    connectError(socket, error) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      const code = String((error as NodeJS.ErrnoException).code ?? "");
      const wrapped = !LOCAL_ERRNOS.has(code)
        ? new UpstreamPreDispatchError(
            "upstream_connect",
            "Upstream connection failed before dispatch",
            error,
          )
        : error;
      failExchange(conn, ex, wrapped);
    },
  };

  return {
    send(
      hostname: string,
      port: number,
      path: string,
      method: string,
      headers: Record<string, string>,
      body: string | Uint8Array | undefined,
      signal: AbortSignal | undefined,
      dispatchDeadline: number | undefined,
      releaseSlot: () => void,
    ): Promise<Response> {
      const reqBuf = buildRequestBuffer(method, path, headers, body);
      const origin = `${hostname}:${port}`;
      return new Promise<Response>((resolve, reject) => {
        const ex: ActiveExchange = {
          method,
          signal,
          dispatchDeadline,
          canHaveDispatched: false,
          settledHeader: false,
          finished: false,
          informationalCount: 0,
          headerBuf: null,
          status: 502,
          headers: {},
          connectionClose: false,
          isChunked: false,
          contentLength: -1,
          bodyRemaining: -1,
          bufferedChunks: null,
          bufferedTotal: 0,
          streamController: null,
          zlibInput: null,
          chunkBuf: Buffer.alloc(0),
          chunkState: { phase: "size" },
          dispatchTimer: undefined,
          abortHandler: undefined,
          pendingWrite: null,
          resolve,
          reject,
          releaseSlot,
          slotReleased: false,
        };

        const reused = popIdle(origin);
        const conn: PooledConn = reused ?? {
          origin,
          socket: null,
          connected: false,
          closed: false,
          inUse: true,
          exchange: null,
          writeBuf: null,
          writeOffset: 0,
        };
        conn.exchange = ex;
        if (!reused) allConns.add(conn);

        const onAbort = () => {
          const err = !ex.canHaveDispatched
            ? notDispatched()
            : (signal?.reason ?? new Error("Upstream request aborted"));
          failExchange(conn, ex, err);
        };
        if (signal) {
          ex.abortHandler = onAbort;
          signal.addEventListener("abort", onAbort, { once: true });
          if (signal.aborted) {
            onAbort();
            return;
          }
        }

        const expired = dispatchDeadline !== undefined && !(performance.now() < dispatchDeadline);
        if (closed || expired) {
          if (reused) {
            conn.exchange = null;
            cleanupExchange(ex);
            returnToIdle(conn);
            ex.finished = true;
            ex.settledHeader = true;
            reject(notDispatched());
          } else {
            failExchange(conn, ex, notDispatched());
          }
          return;
        }

        if (reused) {
          ex.canHaveDispatched = true;
          conn.writeBuf = reqBuf;
          conn.writeOffset = 0;
          flushWrite(conn);
          return;
        }

        ex.pendingWrite = reqBuf;
        if (dispatchDeadline !== undefined) {
          ex.dispatchTimer = setTimeout(
            () => failExchange(conn, ex, notDispatched()),
            Math.max(0, dispatchDeadline - performance.now()),
          );
        }

        void Bun.connect<PooledConn>({
          hostname,
          port,
          data: conn,
          socket: socketHandlers,
        }).catch((error) => {
          const code = String((error as NodeJS.ErrnoException).code ?? "");
          const wrapped = !LOCAL_ERRNOS.has(code)
            ? new UpstreamPreDispatchError(
                "upstream_connect",
                "Upstream connection failed before dispatch",
                error,
              )
            : error;
          failExchange(conn, ex, wrapped);
        });
      });
    },

    close() {
      if (closed) return;
      closed = true;
      if (sweepTimer) {
        clearInterval(sweepTimer);
        sweepTimer = undefined;
      }
      for (const conn of allConns) {
        const ex = conn.exchange;
        if (ex && !ex.finished) {
          failExchange(conn, ex, new Error("Upstream transport closed"));
        } else {
          destroyConn(conn);
        }
      }
      idleByOrigin.clear();
      idleOrder.clear();
      allConns.clear();
    },
  };
}
