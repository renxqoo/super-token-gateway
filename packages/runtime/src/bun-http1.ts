import { checkServerIdentity } from "node:tls";
import { GatewayError } from "@gateway/contracts";
import { UpstreamPreDispatchError } from "./upstream-dispatch-error.ts";
import { dialTargetKey, type DialPermit, type createDialScheduler } from "./dial-scheduler.ts";
import { markUpstream, type UpstreamTimingObserver } from "./transport-timing.ts";
import { createHttp1ResponseHandler } from "./bun-http1-response.ts";
import { createHttp1ExchangeHandlers } from "./bun-http1-exchange.ts";
import {
  type PooledConn,
  type OutboundReservation,
  notDispatched,
  poolOrigin,
  HEADER_END,
  MAX_HEADER_BYTES,
  type CancelRegistrar,
  buildRequestBuffer,
  type ActiveExchange,
  EMPTY_BUF,
  type FastUpstreamResult,
} from "./bun-http1-codec.ts";

const IDLE_TTL_MS = 20_000;

const IDLE_SWEEP_MS = 5_000;

const LOCAL_ERRNOS = new Set(["EADDRNOTAVAIL", "EMFILE", "ENFILE"]);

export function connectionFailure(tls: boolean, cause?: unknown): UpstreamPreDispatchError {
  const code = (cause as NodeJS.ErrnoException | undefined)?.code ?? "";
  return LOCAL_ERRNOS.has(code)
    ? new UpstreamPreDispatchError(
        "upstream_capacity",
        "Local upstream socket capacity exceeded",
        cause,
      )
    : new UpstreamPreDispatchError(
        tls ? "upstream_tls" : "upstream_connect",
        "Upstream connection failed before dispatch",
        cause,
      );
}

export function createBunHttp1Pool(
  maxIdle: number,
  sseFraming: WeakMap<Response, "chunked" | number>,
  bufferedBodies: WeakMap<Response, Uint8Array>,
  dialScheduler: ReturnType<typeof createDialScheduler>,
) {
  const idleByOrigin = new Map<string, PooledConn[]>();
  const idleOrder = new Map<PooledConn, number>();
  const reuseWaiters = new Map<string, Set<(connection: PooledConn) => boolean>>();
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
    conn.writeBytes = 0;
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
    const waiting = reuseWaiters.get(conn.origin);
    if (waiting) {
      for (const offer of waiting) {
        if (offer(conn)) return;
      }
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
    const cutoff = performance.now() - IDLE_TTL_MS;
    while (list.length > 0) {
      const conn = list.pop()!;
      const since = idleOrder.get(conn);
      idleOrder.delete(conn);
      if (!conn.closed && conn.socket) {
        if (since !== undefined && since <= cutoff) {
          destroyConn(conn);
          continue;
        }
        if (list.length === 0) idleByOrigin.delete(origin);
        conn.inUse = true;
        return conn;
      }
    }
    idleByOrigin.delete(origin);
    return null;
  };

  function reservation(idle: PooledConn | null, dial: DialPermit | null): OutboundReservation {
    let available = true;
    return {
      take() {
        if (!available) return undefined;
        available = false;
        return { idle, dial };
      },
      release() {
        if (!available) return;
        available = false;
        if (idle) returnToIdle(idle);
        dial?.release();
      },
    };
  }

  function tryReserveIdle(
    host: string,
    port: number,
    tls: { serverName?: string } | undefined,
  ): OutboundReservation | null {
    if (closed) return null;
    const idle = popIdle(poolOrigin(host, port, tls));
    return idle ? reservation(idle, null) : null;
  }

  function reserve(
    host: string,
    port: number,
    tls: { serverName?: string } | undefined,
    bytes: number,
    signal?: AbortSignal | null,
    deadline?: number,
  ): OutboundReservation | Promise<OutboundReservation> {
    if (closed || signal?.aborted || (deadline !== undefined && performance.now() >= deadline)) {
      throw notDispatched();
    }
    const origin = poolOrigin(host, port, tls);
    const idle = popIdle(origin);
    if (idle) return reservation(idle, null);
    const dialReq = dialScheduler.acquireCancellable(
      dialTargetKey(host, port),
      bytes,
      signal,
      deadline,
    );
    if ("permit" in dialReq) return reservation(null, dialReq.permit);
    return new Promise((resolve, reject) => {
      let queue = reuseWaiters.get(origin);
      if (!queue) {
        queue = new Set();
        reuseWaiters.set(origin, queue);
      }
      const waiting = queue;
      const remove = () => {
        if (!waiting.delete(offer)) return false;
        if (!waiting.size) reuseWaiters.delete(origin);
        return true;
      };
      const offer = (connection: PooledConn) => {
        if (signal?.aborted || (deadline !== undefined && performance.now() >= deadline)) {
          remove();
          dialReq.cancel();
          reject(notDispatched());
          return false;
        }
        if (!remove()) return false;
        connection.inUse = true;
        dialReq.cancel();
        resolve(reservation(connection, null));
        return true;
      };
      waiting.add(offer);
      void dialReq.promise.then(
        (permit) => {
          if (remove()) resolve(reservation(null, permit));
          else permit.release();
        },
        (error) => {
          if (remove()) reject(error);
        },
      );
    });
  }

  const {
    stopDialWait,
    cleanupExchange,
    finishSuccess,
    failExchange,
    cancelActiveExchange,
    createResponse,
    flushWrite,
  } = createHttp1ExchangeHandlers({ returnToIdle, destroyConn });

  const { handleHeadersComplete, feedBody, closeBodySink } = createHttp1ResponseHandler({
    cancelActiveExchange,
    failExchange,
    finishSuccess,
    createResponse,
    bufferedBodies,
    sseFraming,
  });

  const dispatchAfterReady = (conn: PooledConn) => {
    const ex = conn.exchange;
    if (!ex || ex.finished) {
      returnToIdle(conn);
      return;
    }
    if (ex.dispatchTimer) {
      clearTimeout(ex.dispatchTimer);
      ex.dispatchTimer = undefined;
    }
    const expired = ex.dispatchDeadline !== undefined && !(performance.now() < ex.dispatchDeadline);
    if (ex.signal?.aborted || expired) {
      failExchange(conn, ex, notDispatched());
      return;
    }
    ex.canHaveDispatched = true;
    markUpstream(ex.timing, "connection_ready");
    const reqBuf = ex.pendingWrite;
    const reqBytes = ex.pendingWriteBytes;
    ex.pendingWrite = null;
    ex.pendingWriteBytes = 0;
    if (reqBuf) {
      conn.writeBuf = reqBuf;
      conn.writeBytes = reqBytes;
      conn.writeOffset = 0;
      flushWrite(conn);
    }
  };

  const socketHandlers: Bun.SocketHandler<PooledConn> = {
    open(socket) {
      const conn = socket.data;
      if (!conn.isTls) conn.finishDial?.();
      if (closed || conn.closed) {
        terminateSocket(socket);
        return;
      }
      conn.socket = socket;
      markUpstream(conn.exchange?.timing, "tcp_ready");
      if (conn.isTls) return;
      conn.connected = true;
      dispatchAfterReady(conn);
    },
    handshake(socket, authorized, authorizationError) {
      const conn = socket.data;
      conn.finishDial?.();
      if (closed || conn.closed) {
        terminateSocket(socket);
        return;
      }
      conn.socket = socket;
      let tlsErr: unknown = authorized ? null : authorizationError;
      if (!tlsErr && conn.verifyHost) {
        const peerCert = socket.getPeerCertificate?.();
        if (!peerCert) {
          tlsErr = new Error("Upstream peer certificate is missing");
        } else {
          const idErr = checkServerIdentity(
            conn.verifyHost,
            peerCert as Parameters<typeof checkServerIdentity>[1],
          );
          if (idErr) tlsErr = idErr;
        }
      }
      if (tlsErr) {
        const ex = conn.exchange;
        if (ex && !ex.finished) {
          failExchange(
            conn,
            ex,
            new UpstreamPreDispatchError("upstream_tls", "Upstream TLS handshake failed", tlsErr),
          );
        } else {
          destroyConn(conn);
        }
        return;
      }
      markUpstream(conn.exchange?.timing, "tls_ready");
      conn.connected = true;
      dispatchAfterReady(conn);
    },
    data(socket, chunk) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      if (!ex.settledHeader && ex.bufferedChunks === null) {
        const buf = ex.headerBuf ? Buffer.concat([ex.headerBuf, chunk]) : chunk;
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
          ex.headerBuf = ex.headerBuf ? buf : Buffer.from(buf);
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
      const error = ex.canHaveDispatched
        ? new Error("Upstream closed connection prematurely")
        : connectionFailure(conn.isTls);
      failExchange(conn, ex, error);
    },
    close(socket) {
      const conn = socket.data;
      conn.finishDial?.();
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
        ? connectionFailure(conn.isTls)
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
      const wrapped = !ex.canHaveDispatched ? connectionFailure(conn.isTls, error) : error;
      failExchange(conn, ex, wrapped);
    },
    connectError(socket, error) {
      const conn = socket.data;
      conn.finishDial?.();
      const ex = conn.exchange;
      if (!ex || ex.finished) {
        destroyConn(conn);
        return;
      }
      failExchange(conn, ex, ex.canHaveDispatched ? error : connectionFailure(conn.isTls, error));
    },
  };

  function sendExchange<T>(
    hostname: string,
    port: number,
    path: string,
    method: string,
    headers: Record<string, string>,
    body: string | Uint8Array | undefined,
    signal: AbortSignal | undefined,
    dispatchDeadline: number | undefined,
    releaseSlot: () => void,
    fastMode: boolean,
    tls?: { serverName?: string; rejectUnauthorized: true },
    headersValidated = false,
    onCancelRegister?: CancelRegistrar,
    outbound?: OutboundReservation,
    timing?: UpstreamTimingObserver,
  ): Promise<T> {
    const built = buildRequestBuffer(method, path, headers, body, headersValidated);
    const reqBuf = built.payload;
    const reqBytes = built.byteLength;
    const isTls = tls !== undefined;
    const origin = poolOrigin(hostname, port, tls);
    return new Promise<T>((resolve, reject) => {
      const ex: ActiveExchange = {
        method,
        timing,
        signal,
        dispatchDeadline,
        canHaveDispatched: false,
        settledHeader: false,
        finished: false,
        informationalCount: 0,
        headerBuf: null,
        status: 502,
        contentType: "",
        headers: {},
        connectionClose: false,
        isChunked: false,
        contentLength: -1,
        bodyRemaining: -1,
        bufferedChunks: null,
        bufferedTotal: 0,
        streamController: null,
        zlibInput: null,
        chunkBuf: EMPTY_BUF,
        chunkState: { phase: "size" },
        dispatchTimer: undefined,
        abortHandler: undefined,
        cancelDetach: undefined,
        pendingWrite: null,
        pendingWriteBytes: 0,
        fastMode,
        resolve: resolve as (res: Response) => void,
        resolveFast: fastMode ? (resolve as (res: FastUpstreamResult) => void) : undefined,
        reject,
        releaseSlot,
        slotReleased: false,
      };

      const taken = outbound?.take();
      if (outbound && !taken) {
        releaseSlot();
        reject(notDispatched());
        return;
      }
      const reused = taken?.idle && !taken.idle.closed ? taken.idle : popIdle(origin);
      let conn: PooledConn = reused ?? {
        origin,
        isTls,
        verifyHost: isTls ? tls.serverName || hostname : "",
        socket: null,
        connected: false,
        closed: false,
        inUse: true,
        exchange: null,
        writeBuf: null,
        writeBytes: 0,
        writeOffset: 0,
      };
      conn.exchange = ex;
      if (!reused) allConns.add(conn);
      if (reused) taken?.dial?.release();
      const dial = taken?.dial;
      if (dial && !reused) {
        const release = ex.releaseSlot;
        ex.releaseSlot = () => {
          dial.release();
          release();
        };
      }

      const triggerAbort = (reason: unknown) => {
        const err = !ex.canHaveDispatched
          ? notDispatched()
          : (reason ?? new Error("Upstream request aborted"));
        failExchange(conn, ex, err);
      };
      if (onCancelRegister) {
        ex.cancelDetach = onCancelRegister(triggerAbort);
        if (ex.finished) return;
      }
      if (signal) {
        const onAbort = () => triggerAbort(signal.reason);
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
        markUpstream(timing, "connection_reused");
        markUpstream(timing, "connection_ready");
        conn.writeBuf = reqBuf;
        conn.writeBytes = reqBytes;
        conn.writeOffset = 0;
        flushWrite(conn);
        return;
      }

      ex.pendingWrite = reqBuf;
      ex.pendingWriteBytes = reqBytes;
      if (dispatchDeadline !== undefined) {
        ex.dispatchTimer = setTimeout(
          () => failExchange(conn, ex, notDispatched()),
          Math.max(0, dispatchDeadline - performance.now()),
        );
      }

      const startDial = (permit: DialPermit) => {
        delete ex.dialAbort;
        if (ex.finished) {
          permit.release();
          return;
        }
        const finished = permit.start();
        if (!finished) {
          permit.release();
          failExchange(conn, ex, notDispatched());
          return;
        }
        markUpstream(timing, "dial_start");
        conn.finishDial = () => {
          delete conn.finishDial;
          finished();
        };
        const onError = (error: unknown) => {
          conn.finishDial?.();
          failExchange(conn, ex, ex.canHaveDispatched ? error : connectionFailure(isTls, error));
        };
        try {
          void Bun.connect<PooledConn>({
            hostname,
            port,
            ...(isTls
              ? { tls: { rejectUnauthorized: true, serverName: tls.serverName ?? "" } }
              : {}),
            data: conn,
            socket: socketHandlers,
          }).catch(onError);
        } catch (error) {
          onError(error);
        }
      };
      if (dial) {
        startDial(dial);
        return;
      }
      ex.dialAbort = new AbortController();
      const onWaitFailure = (error: unknown) => failExchange(conn, ex, error);
      const useReservation = (outbound: OutboundReservation) => {
        stopDialWait(ex);
        if (ex.finished) {
          outbound.release();
          return;
        }
        const acquired = outbound.take()!;
        if (acquired.dial) {
          startDial(acquired.dial);
        } else if (acquired.idle && !acquired.idle.closed) {
          conn.exchange = null;
          destroyConn(conn);
          conn = acquired.idle;
          conn.exchange = ex;
          markUpstream(timing, "connection_reused");
          dispatchAfterReady(conn);
        } else {
          waitForConnection();
        }
      };
      function waitForConnection() {
        try {
          if (timing) {
            ex.dialWaiting = true;
            markUpstream(timing, "wait_start");
          }
          const ticket = reserve(
            hostname,
            port,
            tls,
            reqBytes,
            ex.dialAbort?.signal,
            dispatchDeadline,
          );
          if (ticket instanceof Promise) void ticket.then(useReservation, onWaitFailure);
          else useReservation(ticket);
        } catch (error) {
          onWaitFailure(error);
        }
      }
      waitForConnection();
    });
  }

  return {
    reserve,
    tryReserveIdle,
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
      tls?: { serverName?: string; rejectUnauthorized: true },
      headersValidated = false,
      onCancelRegister?: CancelRegistrar,
      outbound?: OutboundReservation,
      timing?: UpstreamTimingObserver,
    ): Promise<Response> {
      return sendExchange<Response>(
        hostname,
        port,
        path,
        method,
        headers,
        body,
        signal,
        dispatchDeadline,
        releaseSlot,
        false,
        tls,
        headersValidated,
        onCancelRegister,
        outbound,
        timing,
      );
    },

    sendFast(
      hostname: string,
      port: number,
      path: string,
      method: string,
      headers: Record<string, string>,
      body: string | Uint8Array | undefined,
      signal: AbortSignal | undefined,
      dispatchDeadline: number | undefined,
      releaseSlot: () => void,
      tls?: { serverName?: string; rejectUnauthorized: true },
      headersValidated = false,
      onCancelRegister?: CancelRegistrar,
      outbound?: OutboundReservation,
      timing?: UpstreamTimingObserver,
    ): Promise<FastUpstreamResult> {
      return sendExchange<FastUpstreamResult>(
        hostname,
        port,
        path,
        method,
        headers,
        body,
        signal,
        dispatchDeadline,
        releaseSlot,
        true,
        tls,
        headersValidated,
        onCancelRegister,
        outbound,
        timing,
      );
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
          failExchange(
            conn,
            ex,
            ex.canHaveDispatched ? new Error("Upstream transport closed") : notDispatched(),
          );
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
