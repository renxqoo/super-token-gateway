import { GatewayError } from "@gateway/contracts";
import { readBounded } from "@gateway/runtime";
import { declaredRequestSize } from "../public-ingress.ts";
import type { DeadlineWheel } from "../proxy-state.ts";

export const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });

export async function readRequestBytesBounded(
  req: Request,
  maxBytes: number,
  timeoutMs: number,
  deadlines: DeadlineWheel,
  isDraining: () => boolean,
  setAbortNotify: (fn: ((reason: unknown) => void) | null) => void,
  declaredSize?: number | null,
): Promise<Uint8Array> {
  if (req.signal.aborted) {
    throw new GatewayError(499, "client_aborted", "Client disconnected");
  }
  if (isDraining()) {
    throw new GatewayError(503, "overloaded", "Gateway is draining");
  }
  if (declaredSize === undefined) declaredRequestSize(req, maxBytes);
  const timeoutErr = () => new GatewayError(408, "request_timeout", "Request body read timed out");

  const body = req.body;
  if (!body) return new Uint8Array(0);
  const reader = body.getReader();
  let abortError: unknown = null;
  let watchingSignal = false;
  let cancelSignalWatch: (() => void) | null = null;
  const triggerAbort = (err: unknown) => {
    if (abortError !== null) return;
    abortError = err;
    void reader.cancel().catch(() => undefined);
  };
  const onSignalAbort = () =>
    triggerAbort(new GatewayError(499, "client_aborted", "Client disconnected"));
  const attachSignalWatch = () => {
    cancelSignalWatch = null;
    if (abortError !== null) return;
    if (req.signal.aborted) {
      onSignalAbort();
      return;
    }
    watchingSignal = true;
    req.signal.addEventListener("abort", onSignalAbort, { once: true });
  };
  cancelSignalWatch =
    timeoutMs >= 1000
      ? deadlines.scheduleCoarse(50, attachSignalWatch)
      : deadlines.schedule(Math.min(50, timeoutMs), attachSignalWatch);
  setAbortNotify(() => triggerAbort(new GatewayError(503, "overloaded", "Gateway is draining")));
  const cancelTimer = deadlines.schedule(timeoutMs, () => triggerAbort(timeoutErr()));
  try {
    const first = await reader.read();
    if (abortError !== null) throw abortError;
    if (req.signal.aborted) {
      throw new GatewayError(499, "client_aborted", "Client disconnected");
    }
    if (first.done) return new Uint8Array(0);
    if (first.value.byteLength > maxBytes) {
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    const second = await reader.read();
    if (abortError !== null) throw abortError;
    if (req.signal.aborted) {
      throw new GatewayError(499, "client_aborted", "Client disconnected");
    }
    if (second.done) return first.value;
    let size = first.value.byteLength + second.value.byteLength;
    if (size > maxBytes) {
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    const chunks: Uint8Array[] = [first.value, second.value];
    while (true) {
      const next = await reader.read();
      if (abortError !== null) throw abortError;
      if (req.signal.aborted) {
        throw new GatewayError(499, "client_aborted", "Client disconnected");
      }
      if (next.done) break;
      size += next.value.byteLength;
      if (size > maxBytes) {
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      }
      chunks.push(next.value);
    }
    const output = new Uint8Array(size);
    let offset = 0;
    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i]!;
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
  } catch (err) {
    await reader.cancel().catch(() => undefined);
    if (abortError !== null) throw abortError;
    if (err instanceof GatewayError) throw err;
    if (req.signal.aborted) throw new GatewayError(499, "client_aborted", "Client disconnected");
    if (isDraining()) throw new GatewayError(503, "overloaded", "Gateway is draining");
    throw err;
  } finally {
    cancelTimer();
    if (cancelSignalWatch) {
      cancelSignalWatch();
      cancelSignalWatch = null;
    }
    if (watchingSignal) {
      watchingSignal = false;
      req.signal.removeEventListener("abort", onSignalAbort);
    }
    setAbortNotify(null);
    reader.releaseLock();
  }
}

export async function readResponseBytesFast(
  resp: Response,
  maxBytes: number,
  signal: AbortSignal,
  bufferedBody?: Uint8Array,
): Promise<Uint8Array> {
  if (bufferedBody !== undefined) {
    signal.throwIfAborted();
    if (bufferedBody.byteLength > maxBytes) {
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    return bufferedBody;
  }
  const clHeader = resp.headers.get("content-length");
  if (clHeader !== null && /^\d+$/.test(clHeader)) {
    const cl = Number(clHeader);
    if (cl > maxBytes) {
      void resp.body?.cancel().catch(() => undefined);
      throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
    }
    if (!signal.aborted && !resp.headers.has("content-encoding") && cl <= 262_144) {
      const bytes = await resp.bytes();
      if (bytes.byteLength > maxBytes) {
        throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
      }
      return bytes;
    }
  }
  return readBounded(resp.body, maxBytes, signal);
}
