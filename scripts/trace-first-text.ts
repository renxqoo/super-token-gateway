import { randomBytes } from "node:crypto";
import { parseArgs } from "node:util";
import { optionalObject } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";
import { hasSseText, SSEParser, type SSEEvent } from "@gateway/protocol";
import { createTraceStore, unixNano } from "../apps/gateway/src/trace-storage.ts";

const { values } = parseArgs({
  args: process.argv.slice(2),
  options: {
    url: { type: "string" },
    request: { type: "string" },
    "key-file": { type: "string" },
    protocol: { type: "string", default: "chat" },
    persist: { type: "boolean" },
    "timeout-ms": { type: "string", default: "65000" },
  },
});
if (!values.url || !values.request) {
  throw new Error(
    "Usage: trace-first-text.ts --url URL --request FILE [--key-file FILE] [--protocol chat|messages|responses|gemini]",
  );
}
const url = new URL(values.url);
const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
if (
  url.username ||
  url.password ||
  (url.protocol !== "https:" && !(url.protocol === "http:" && loopback))
) {
  throw new Error("Expected HTTPS or loopback HTTP without URL credentials");
}
function protocolOf(value: string): Protocol {
  if (value === "chat" || value === "messages" || value === "responses" || value === "gemini")
    return value;
  throw new Error("Unsupported streaming protocol");
}
const protocol = protocolOf(values.protocol);
const timeout = Number(values["timeout-ms"]);
if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 2_147_483_647)
  throw new Error("Invalid client timeout");
const keyPath = values["key-file"];
const traceDb = process.env.TRACE_DATABASE_URL;
if (values.persist && !traceDb) throw new Error("TRACE_DATABASE_URL is required when persisting");
const key: unknown = keyPath ? (await Bun.file(keyPath).json()).key : process.env.GATEWAY_API_KEY;
if (typeof key !== "string" || !/^sk_[A-Za-z0-9_-]{43}$/.test(key))
  throw new Error("Expected a Gateway billingAccount API Key");
const input: unknown = await Bun.file(values.request).json();
if (typeof input !== "object" || input === null || Array.isArray(input))
  throw new Error("Expected a request object");
const payload = JSON.stringify(protocol === "gemini" ? input : { ...input, stream: true });
const traceId = randomBytes(16).toString("hex");
const clientSpanId = randomBytes(8).toString("hex");
const signal = AbortSignal.timeout(timeout);
let requestId: string | null = null;
let gatewayTraceId: string | null = null;
let status: number | null = null;
let headersAt: number | null = null;
let firstTextAt: number | null = null;
let streamError = false;
let terminalSeen = false;
const parser = new SSEParser();
const startedAt = performance.now();
let failure: string | undefined;

function observe(events: SSEEvent[], at: number) {
  for (const event of events) {
    if (event.event === "error") streamError = true;
    if (firstTextAt === null && hasSseText(event, protocol)) firstTextAt = at;
    if (!terminalSeen) terminalSeen = isTerminal(event);
  }
}

function isTerminal(event: SSEEvent): boolean {
  if (protocol === "chat") return event.data === "[DONE]";
  if (!event.data) return false;
  let body: Record<string, unknown>;
  try {
    body = optionalObject(JSON.parse(event.data));
  } catch {
    return false;
  }
  const type = body.type ?? event.event;
  if (protocol === "messages") return type === "message_stop";
  if (protocol === "responses")
    return type === "response.completed" || type === "response.incomplete";
  return (
    Array.isArray(body.candidates) &&
    body.candidates.some((candidate) => typeof optionalObject(candidate).finishReason === "string")
  );
}

try {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      authorization: `Bearer ${key}`,
      "content-type": "application/json",
      traceparent: `00-${traceId}-${clientSpanId}-01`,
      ...(protocol === "messages" ? { "anthropic-version": "2023-06-01" } : {}),
    },
    body: payload,
    signal,
    redirect: "error",
  });
  headersAt = performance.now();
  status = response.status;
  requestId = response.headers.get("x-request-id");
  gatewayTraceId = response.headers.get("x-trace-id");
  if (
    !response.ok ||
    !response.body ||
    !response.headers.get("content-type")?.toLowerCase().startsWith("text/event-stream")
  ) {
    await response.body?.cancel();
    throw new Error("no_sse_response");
  }
  const reader = response.body.getReader();
  try {
    while (true) {
      const item = await reader.read();
      const at = performance.now();
      if (item.done) {
        observe(parser.feed(new Uint8Array(), true), at);
        if (parser.incomplete) throw new Error("incomplete_sse_response");
        break;
      }
      observe(parser.feed(item.value), at);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
} catch (error) {
  failure = signal.aborted
    ? "client_timeout"
    : error instanceof Error
      ? error.name
      : "request_failed";
}
const elapsed = (at: number | null) => (at === null ? null : Number((at - startedAt).toFixed(2)));
const endedAt = performance.now();
const ok =
  !failure &&
  !streamError &&
  terminalSeen &&
  requestId !== null &&
  gatewayTraceId === traceId &&
  firstTextAt !== null;
let persisted: boolean | undefined;
if (values.persist && traceDb && requestId && gatewayTraceId === traceId) {
  const dbTimeout = Number(process.env.TRACE_DB_TIMEOUT_MS ?? 2500);
  const retention = Number(process.env.TRACE_RETENTION_SECONDS);
  const joinWait = Number(process.env.TRACE_CLIENT_JOIN_WAIT_MS ?? 5000);
  const limits = [dbTimeout, retention, joinWait];
  if (!limits.every((value) => Number.isSafeInteger(value) && value > 0 && value <= 2_147_483_647))
    throw new Error("Invalid client trace storage limits");
  const store = createTraceStore(traceDb, dbTimeout);
  try {
    const deadline = performance.now() + joinWait;
    do {
      const rows = await store.read(requestId);
      const gateway = rows.find(
        (row) =>
          row.source === "gateway" &&
          row.span.name === "gateway.inference" &&
          row.span.traceId === traceId &&
          row.span.parentSpanId === clientSpanId,
      );
      if (gateway) {
        const unixAt = (at: number) => unixNano(performance.timeOrigin + at);
        await store.insert(
          [
            {
              trace_id: traceId,
              span_id: clientSpanId,
              request_id: requestId,
              billing_account_id: gateway.billing_account_id,
              source: "client",
              start_ms: performance.timeOrigin + startedAt,
              span: {
                traceId,
                spanId: clientSpanId,
                name: "client.inference",
                kind: 3,
                startTimeUnixNano: unixAt(startedAt),
                endTimeUnixNano: unixAt(endedAt),
                attributes: [{ key: "gateway.request.id", value: { stringValue: requestId } }],
                events: [
                  ...(headersAt === null
                    ? []
                    : [{ name: "http.response.headers", timeUnixNano: unixAt(headersAt) }]),
                  ...(firstTextAt === null
                    ? []
                    : [{ name: "client.first_text", timeUnixNano: unixAt(firstTextAt) }]),
                ],
                status: { code: ok ? 0 : 2 },
              },
            },
          ],
          retention,
        );
        persisted = true;
        break;
      }
      await Bun.sleep(50);
    } while (performance.now() < deadline);
  } catch {
    persisted = false;
  } finally {
    persisted ??= false;
    await store.close();
  }
}
console.log(
  JSON.stringify({
    event: "client_first_text_trace",
    request_id: requestId,
    trace_id: gatewayTraceId ?? traceId,
    client_span_id: clientSpanId,
    status,
    client_start_unix_ms: performance.timeOrigin + startedAt,
    client_headers_ms: elapsed(headersAt),
    client_first_text_ms: elapsed(firstTextAt),
    client_duration_ms: elapsed(endedAt),
    terminal_seen: terminalSeen,
    stream_error: streamError,
    ...(failure ? { failure } : {}),
    ...(values.persist ? { trace_persisted: persisted ?? false } : {}),
    ok,
  }),
);
if (!ok || (values.persist && !persisted)) process.exitCode = 1;
