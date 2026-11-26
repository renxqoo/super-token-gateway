import { PassThrough } from "node:stream";
import { GatewayError } from "@gateway/contracts";
import { UpstreamPreDispatchError } from "./upstream-dispatch-error.ts";
import type { DialPermit } from "./dial-scheduler.ts";
import type { UpstreamTimingObserver } from "./transport-timing.ts";

export const HEADER_END = Buffer.from("\r\n\r\n");

export const CRLF = Buffer.from("\r\n");

export const EMPTY_BUF = Buffer.alloc(0);

export const MAX_HEADER_BYTES = 65536;

export const MAX_INFORMATIONAL = 5;

export const FAST_BUFFER_MAX = 65536;

const TCHAR_TABLE = new Uint8Array(128);

for (const ch of "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
  TCHAR_TABLE[ch.charCodeAt(0)] = 1;
}

export type FastUpstreamResult =
  | {
      fast: true;
      status: number;
      contentType: string;
      headers: Headers | Record<string, string>;
      body: Uint8Array;
    }
  | { fast: false; response: Response };

export type CancelRegistrar = (onCancel: (reason: unknown) => void) => () => void;

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

export function trimOws(value: string): string {
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

export type ActiveExchange = {
  method: string;
  timing: UpstreamTimingObserver | undefined;
  dialWaiting?: boolean;
  signal: AbortSignal | undefined;
  dispatchDeadline: number | undefined;
  canHaveDispatched: boolean;
  settledHeader: boolean;
  finished: boolean;
  informationalCount: number;
  headerBuf: Buffer | null;
  status: number;
  contentType: string;
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
  cancelDetach: (() => void) | undefined;
  dialAbort?: AbortController;
  pendingWrite: string | Buffer | null;
  pendingWriteBytes: number;
  fastMode: boolean;
  resolve: (res: Response) => void;
  resolveFast: ((res: FastUpstreamResult) => void) | undefined;
  reject: (err: unknown) => void;
  releaseSlot: () => void;
  slotReleased: boolean;
};

export type PooledConn = {
  origin: string;
  isTls: boolean;
  verifyHost: string;
  socket: Bun.Socket<PooledConn> | null;
  connected: boolean;
  closed: boolean;
  inUse: boolean;
  exchange: ActiveExchange | null;
  writeBuf: string | Buffer | null;
  writeBytes: number;
  writeOffset: number;
  finishDial?: (() => void) | undefined;
};

export type OutboundReservation = {
  take(): { idle: PooledConn | null; dial: DialPermit | null } | undefined;
  release(): void;
};

const URL_PARTS_CACHE = new WeakMap<
  URL,
  { href: string; secure: boolean; hostname: string; port: number; path: string }
>();

export function readUrlParts(url: URL) {
  const href = url.href;
  const cached = URL_PARTS_CACHE.get(url);
  if (cached && cached.href === href) return cached;
  const secure = url.protocol === "https:";
  const rawHost = url.hostname;
  const hostname = rawHost.charCodeAt(0) === 91 ? rawHost.slice(1, -1) : rawHost;
  const port = url.port ? Number(url.port) : secure ? 443 : 80;
  const path = url.search ? `${url.pathname}${url.search}` : url.pathname;
  const parts = { href, secure, hostname, port, path };
  URL_PARTS_CACHE.set(url, parts);
  return parts;
}

export function poolOrigin(host: string, port: number, tls?: { serverName?: string }) {
  return tls ? `tls:${host}:${port}:${tls.serverName ?? ""}` : `${host}:${port}`;
}

export function notDispatched(): UpstreamPreDispatchError {
  return new UpstreamPreDispatchError(
    "upstream_not_dispatched",
    "Upstream request stopped before obtaining a connection",
  );
}

export function buildRequestBuffer(
  method: string,
  path: string,
  headers: Record<string, string>,
  body: string | Uint8Array | undefined,
  headersValidated = false,
): { payload: string | Buffer; byteLength: number } {
  if (method !== "POST" && method !== "GET" && !isValidHttpHeaderName(method)) {
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
    if (!headersValidated && (!isValidHttpHeaderName(key) || !isValidHttpHeaderValue(val))) {
      throw new TypeError("Invalid HTTP header");
    }
    const lower = headersValidated ? key : key.toLowerCase();
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
  if (body === undefined) {
    const buf = Buffer.from(head, "latin1");
    return { payload: buf, byteLength: buf.byteLength };
  }
  if (typeof body === "string") {
    if (headersValidated && Buffer.byteLength(head, "utf8") === head.length) {
      return { payload: `${head}${body}`, byteLength: head.length + bodyByteLength };
    }
    const headLen = Buffer.byteLength(head, "latin1");
    const out = Buffer.allocUnsafe(headLen + bodyByteLength);
    out.write(head, 0, headLen, "latin1");
    out.write(body, headLen, bodyByteLength, "utf8");
    return { payload: out, byteLength: out.byteLength };
  }
  const headBuf = Buffer.from(head, "latin1");
  const out = Buffer.allocUnsafe(headBuf.byteLength + body.byteLength);
  headBuf.copy(out, 0);
  out.set(body, headBuf.byteLength);
  return { payload: out, byteLength: out.byteLength };
}

export function parseHeadersBlock(headStr: string): {
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

  let record: Record<string, string> | null = Object.create(null) as Record<string, string>;
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
    if (colon === -1 || colon >= next || colon === pos) {
      throw new GatewayError(502, "invalid_upstream", "Malformed upstream HTTP header");
    }
    let hasUpper = false;
    for (let i = pos; i < colon; i++) {
      const c = headStr.charCodeAt(i);
      if (c >= 128 || TCHAR_TABLE[c] !== 1) {
        throw new GatewayError(502, "invalid_upstream", "Invalid upstream HTTP header name");
      }
      if (c >= 65 && c <= 90) hasUpper = true;
    }
    const rawName = headStr.slice(pos, colon);
    const lower = hasUpper ? rawName.toLowerCase() : rawName;
    let valStart = colon + 1;
    let valEnd = next;
    for (let i = valStart; i < valEnd; i++) {
      const c = headStr.charCodeAt(i);
      if (c > 255 || (c < 32 && c !== 9) || c === 127) {
        throw new GatewayError(502, "invalid_upstream", "Invalid upstream HTTP header value");
      }
    }
    while (valStart < valEnd) {
      const c = headStr.charCodeAt(valStart);
      if (c !== 32 && c !== 9) break;
      valStart++;
    }
    while (valEnd > valStart) {
      const c = headStr.charCodeAt(valEnd - 1);
      if (c !== 32 && c !== 9) break;
      valEnd--;
    }
    const val = headStr.slice(valStart, valEnd);
    const nameLen = lower.length;
    if (nameLen === 12 && lower === "content-type") {
      contentType = val;
    } else if (nameLen === 14 && lower === "content-length") {
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
    } else if (nameLen === 10 && lower === "connection") {
      connectionHeader = connectionHeader ? `${connectionHeader},${val}` : val;
    } else if (nameLen === 16 && lower === "content-encoding") {
      contentEncoding = contentEncoding ? `${contentEncoding},${val}` : val;
    } else if (nameLen === 17 && lower === "transfer-encoding") {
      transferEncoding = transferEncoding ? `${transferEncoding},${val}` : val;
    }
    if (multiHeaders) {
      multiHeaders.append(rawName, val);
    } else if (record![lower] !== undefined) {
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

  const connectionClose =
    connectionHeader.length === 0
      ? http10
      : !http10 && connectionHeader === "keep-alive"
        ? false
        : http10
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
