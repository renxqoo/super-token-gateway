import type { ActiveExchange, PooledConn } from "./bun-http1-codec.ts";
import { markUpstream } from "./transport-timing.ts";

export function createHttp1ExchangeHandlers({
  returnToIdle,
  destroyConn,
}: {
  returnToIdle: (conn: PooledConn) => void;
  destroyConn: (conn: PooledConn) => void;
}) {
  const stopDialWait = (ex: ActiveExchange) => {
    if (!ex.dialWaiting) return;
    ex.dialWaiting = false;
    markUpstream(ex.timing, "wait_end");
  };

  const cleanupExchange = (ex: ActiveExchange) => {
    stopDialWait(ex);
    ex.dialAbort?.abort();
    delete ex.dialAbort;
    ex.pendingWrite = null;
    ex.pendingWriteBytes = 0;
    if (ex.dispatchTimer) {
      clearTimeout(ex.dispatchTimer);
      ex.dispatchTimer = undefined;
    }
    if (ex.abortHandler) {
      ex.signal?.removeEventListener("abort", ex.abortHandler);
      ex.abortHandler = undefined;
    }
    if (ex.cancelDetach) {
      ex.cancelDetach();
      ex.cancelDetach = undefined;
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
    if (typeof conn.writeBuf === "string") {
      const payload = conn.writeBuf;
      let written: number;
      try {
        written = sock.write(payload);
      } catch (err) {
        conn.writeBuf = null;
        conn.writeBytes = 0;
        conn.writeOffset = 0;
        const ex = conn.exchange;
        if (ex && !ex.finished) failExchange(conn, ex, err);
        else destroyConn(conn);
        return;
      }
      if (written < 0) {
        conn.writeBuf = null;
        conn.writeBytes = 0;
        conn.writeOffset = 0;
        const ex = conn.exchange;
        const err = new Error("Upstream socket write failed");
        if (ex && !ex.finished) failExchange(conn, ex, err);
        else destroyConn(conn);
        return;
      }
      if (written >= conn.writeBytes) {
        markUpstream(conn.exchange?.timing, "request_written");
        conn.writeBuf = null;
        conn.writeBytes = 0;
        conn.writeOffset = 0;
        return;
      }
      conn.writeBuf = Buffer.from(payload, "utf8");
      conn.writeOffset = written;
      if (written === 0) return;
    }
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
    markUpstream(conn.exchange?.timing, "request_written");
    conn.writeBuf = null;
    conn.writeOffset = 0;
  };

  return {
    stopDialWait,
    cleanupExchange,
    finishSuccess,
    failExchange,
    cancelActiveExchange,
    createResponse,
    flushWrite,
  };
}
