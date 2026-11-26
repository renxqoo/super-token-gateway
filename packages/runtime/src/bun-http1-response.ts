import {
  type PooledConn,
  type ActiveExchange,
  CRLF,
  isValidHttpHeaderValue,
  trimOws,
  HEADER_END,
  MAX_HEADER_BYTES,
  parseHeadersBlock,
  MAX_INFORMATIONAL,
  FAST_BUFFER_MAX,
} from "./bun-http1-codec.ts";
import { GatewayError } from "@gateway/contracts";
import { markUpstream } from "./transport-timing.ts";
import {
  createGunzip,
  createInflate,
  createBrotliDecompress,
  createZstdDecompress,
} from "node:zlib";
import { PassThrough, Readable, pipeline } from "node:stream";

export function createHttp1ResponseHandler({
  cancelActiveExchange,
  failExchange,
  finishSuccess,
  createResponse,
  bufferedBodies,
  sseFraming,
}: {
  cancelActiveExchange: (conn: PooledConn, ex: ActiveExchange) => void;
  failExchange: (conn: PooledConn, ex: ActiveExchange, err: unknown) => void;
  finishSuccess: (conn: PooledConn, ex: ActiveExchange, reuse: boolean) => void;
  createResponse: (
    conn: PooledConn,
    ex: ActiveExchange,
    body: BodyInit | null,
    status: number,
    headers: Headers | Record<string, string>,
  ) => Response | null;
  bufferedBodies: WeakMap<Response, Uint8Array<ArrayBufferLike>>;
  sseFraming: WeakMap<Response, number | "chunked">;
}) {
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
    let pendingFirst: Buffer | null = null;
    let pendingParts: Buffer[] | null = null;
    let pendingBytes = 0;
    const flushPending = () => {
      if (pendingFirst === null) return;
      if (pendingParts === null) {
        const chunk = pendingFirst;
        pendingFirst = null;
        pendingBytes = 0;
        emitBodyChunk(conn, ex, chunk);
      } else {
        const merged = Buffer.concat(pendingParts, pendingBytes);
        pendingFirst = null;
        pendingParts = null;
        pendingBytes = 0;
        emitBodyChunk(conn, ex, merged);
      }
    };
    while (offset < buf.byteLength && !ex.finished) {
      const st = ex.chunkState;
      if (st.phase === "size") {
        const idx = buf.indexOf(CRLF, offset);
        if (idx === -1) {
          if (buf.byteLength - offset > 4096) {
            flushPending();
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
          flushPending();
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
          flushPending();
          failExchange(conn, ex, new GatewayError(502, "invalid_upstream", "Invalid chunk size"));
          return;
        }
        const size = Number.parseInt(hex, 16);
        if (!Number.isSafeInteger(size) || size < 0) {
          flushPending();
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
          const slice = Buffer.from(buf.subarray(offset, offset + take));
          if (pendingFirst === null) {
            pendingFirst = slice;
            pendingBytes = take;
          } else {
            if (pendingParts === null) pendingParts = [pendingFirst];
            pendingParts.push(slice);
            pendingBytes += take;
          }
          offset += take;
          st.remaining -= take;
        }
        if (st.remaining === 0) {
          ex.chunkState = { phase: "data_crlf" };
        }
      } else if (st.phase === "data_crlf") {
        if (buf.byteLength - offset < 2) break;
        if (buf[offset] !== 13 || buf[offset + 1] !== 10) {
          flushPending();
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
          flushPending();
          if (ex.finished) return;
          closeBodySink(ex);
          finishSuccess(conn, ex, offset === buf.byteLength);
          return;
        }
        const end = buf.indexOf(HEADER_END, offset);
        if (end === -1) {
          if (buf.byteLength - offset > MAX_HEADER_BYTES) {
            flushPending();
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
          flushPending();
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
        flushPending();
        if (ex.finished) return;
        closeBodySink(ex);
        finishSuccess(conn, ex, offset === buf.byteLength);
        return;
      } else {
        break;
      }
    }
    flushPending();
    if (ex.finished) {
      ex.chunkBuf = Buffer.alloc(0);
      return;
    }
    ex.chunkBuf = offset < buf.byteLength ? Buffer.from(buf.subarray(offset)) : Buffer.alloc(0);
  };

  const resolveNonFast = (ex: ActiveExchange, res: Response) => {
    if (ex.fastMode && ex.resolveFast) {
      ex.resolveFast({ fast: false, response: res });
    } else {
      ex.resolve(res);
    }
  };

  const resolveFastBuffered = (
    conn: PooledConn,
    ex: ActiveExchange,
    body: Uint8Array,
    extra: boolean,
  ) => {
    if (ex.fastMode && ex.resolveFast) {
      ex.settledHeader = true;
      finishSuccess(conn, ex, !extra);
      ex.resolveFast({
        fast: true,
        status: ex.status,
        contentType: ex.contentType,
        headers: ex.headers,
        body,
      });
      return;
    }
    const res = createResponse(conn, ex, body as BodyInit, ex.status, ex.headers);
    if (!res) return;
    ex.settledHeader = true;
    bufferedBodies.set(res, body);
    finishSuccess(conn, ex, !extra);
    ex.resolve(res);
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
        resolveFastBuffered(conn, ex, body, extra);
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

    markUpstream(ex.timing, "response_headers");
    ex.status = parsed.status;
    ex.contentType = parsed.contentType;
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
      resolveNonFast(ex, res);
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
        resolveFastBuffered(conn, ex, new Uint8Array(0), rest.byteLength !== 0);
        return;
      }
      if (rest.byteLength >= contentLen) {
        const copy = Buffer.from(rest.subarray(0, contentLen));
        resolveFastBuffered(conn, ex, copy, rest.byteLength > contentLen);
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
        resolveNonFast(ex, res);
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
      resolveNonFast(ex, res);
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
    resolveNonFast(ex, res);
  };

  return { handleHeadersComplete, feedBody, closeBodySink };
}
