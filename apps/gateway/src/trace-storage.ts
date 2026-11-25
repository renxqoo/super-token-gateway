import { SQL } from "bun";
import type { SpanValue, StoredSpan, TraceRecord } from "./trace-types.ts";

export const unixNano = (ms: number) => BigInt(Math.round(ms * 1000)).toString() + "000";

function errorType(code: string | null | undefined) {
  if (!code) return undefined;
  return /^[a-z0-9_.-]{1,80}$/i.test(code) ? code : "other";
}

function fields(values: Record<string, string | boolean | number | undefined>) {
  return Object.entries(values).flatMap(([key, value]) => {
    if (value === undefined) return [];
    const encoded: SpanValue =
      typeof value === "boolean"
        ? { boolValue: value }
        : typeof value === "number"
          ? { intValue: Math.round(value).toString() }
          : { stringValue: value };
    return [{ key, value: encoded }];
  });
}

export function gatewaySpans(record: TraceRecord): StoredSpan[] {
  const { traceId, spanId, requestId, billingAccountId, origin, start, end, status } = record;
  const make = (
    id: string,
    parent: string | undefined,
    name: string,
    kind: number,
    from: number,
    until: number,
    attributes: ReturnType<typeof fields>,
    events: Record<string, number | undefined>,
    failed: boolean,
  ): StoredSpan => ({
    trace_id: traceId,
    span_id: id,
    request_id: requestId,
    billing_account_id: billingAccountId,
    source: "gateway",
    start_ms: origin + from,
    span: {
      traceId,
      spanId: id,
      ...(parent ? { parentSpanId: parent } : {}),
      name,
      kind,
      startTimeUnixNano: unixNano(origin + from),
      endTimeUnixNano: unixNano(origin + Math.max(from, until)),
      attributes,
      events: Object.entries(events)
        .flatMap(([name, at]) =>
          at === undefined || at < from || at > until ? [] : [{ name, at }],
        )
        .sort((left, right) => left.at - right.at)
        .map(({ name, at }) => ({ name, timeUnixNano: unixNano(origin + at) })),
      status: { code: failed ? 2 : 0 },
    },
  });
  const spans = [
    make(
      spanId,
      record.parentSpanId,
      "gateway.inference",
      2,
      start,
      end,
      fields({
        "gateway.request.id": requestId,
        "gen_ai.request.model": record.model ?? undefined,
        "gateway.operation": record.operation,
        "http.response.status_code": status,
        "error.type": errorType(record.error),
        "gateway.settlement.attempt_id": record.settledAttemptId,
        "gateway.attempts.failed": record.failedAttempts,
        "gateway.attempts.omitted": record.omittedAttempts,
      }),
      {
        "gateway.first_text.enqueued": record.enqueued,
        "gateway.admission.dns.started": record.admission?.dns_start,
        "gateway.admission.dns.finished": record.admission?.dns_end,
        "gateway.admission.wait.started": record.admission?.first_wait,
        "gateway.admission.wait.finished": record.admission?.last_wait,
      },
      status >= 400,
    ),
  ];
  for (const attempt of record.attempts) {
    const timing = attempt.transport;
    const from = Math.min(
      attempt.start,
      timing.dns_start ?? Infinity,
      timing.first_wait ?? Infinity,
    );
    const id = attempt.id.replaceAll("-", "").slice(0, 16);
    spans.push(
      make(
        id,
        spanId,
        "gateway.upstream.attempt",
        3,
        from,
        attempt.end ?? end,
        fields({
          "gateway.request.id": requestId,
          "gateway.attempt.id": attempt.id,
          "gateway.attempt.number": attempt.number,
          "gateway.channel": attempt.channel,
          "http.response.status_code": attempt.status,
          "error.type": errorType(attempt.error),
          "gateway.failure.stage": attempt.failureStage,
          "gateway.failure.kind": attempt.failureKind,
          "gateway.provider.request_id": attempt.providerRequestId,
          "gateway.provider.error_code": attempt.providerError?.code,
          "gateway.provider.error_summary": attempt.providerError?.summary,
          "gateway.connection.reused": timing.connection_reused,
          "gateway.connection.dials": timing.dial_count,
          "gateway.connection.wait_us": timing.wait_ms * 1000,
        }),
        {
          "gateway.attempt.started": attempt.start,
          "dns.started": timing.dns_start,
          "dns.finished": timing.dns_end,
          "connection.wait.started": timing.first_wait,
          "connection.wait.finished": timing.last_wait,
          "connection.dial.started": timing.dial_start,
          "connection.tcp.ready": timing.tcp_ready,
          "connection.tls.ready": timing.tls_ready,
          "connection.ready": timing.connection_ready,
          "http.request.written": timing.request_written,
          "http.response.headers": timing.response_headers,
          "upstream.first_text": attempt.firstText,
        },
        Boolean(attempt.error) || (attempt.status ?? 0) >= 400,
      ),
    );
  }
  return spans;
}

export function createTraceStore(url: string, timeoutMs: number) {
  const db = new SQL({ url, max: 1, connectionTimeout: Math.ceil(timeoutMs / 1000) });
  let verified = false;
  async function ready() {
    if (verified) return;
    const [target] =
      await db`SELECT current_database()= ${decodeURIComponent(new URL(url).pathname.slice(1))} AS target,
      to_regclass('public.billing_holds') IS NULL
        AND to_regclass('public.console_schema_migrations') IS NULL AS isolated,
      to_regclass('telemetry.spans') IS NOT NULL AS initialized`;
    if (!target?.target || !target.isolated || !target.initialized)
      throw new Error("Invalid telemetry database");
    verified = true;
  }
  return {
    async insert(spans: StoredSpan[], retentionSeconds: number) {
      if (!spans.length) return;
      await ready();
      await db.begin(async (tx) => {
        await tx`SELECT set_config('statement_timeout', ${String(timeoutMs)}, true)`;
        await tx`INSERT INTO telemetry.spans (trace_id,span_id,request_id,billing_account_id,source,started_at,expires_at,span)
          SELECT s.trace_id,s.span_id,s.request_id,s.billing_account_id,s.source,to_timestamp(s.start_ms/1000.0),
            now()+${retentionSeconds} * interval '1 second',s.span
          FROM jsonb_to_recordset(${JSON.stringify(spans)}::text::jsonb) AS s(trace_id text,span_id text,request_id uuid,billing_account_id uuid,source text,start_ms double precision,span jsonb)
          ON CONFLICT (trace_id,span_id) DO NOTHING`;
      });
    },
    async expire(limit: number) {
      await ready();
      return db.begin(async (tx) => {
        await tx`SELECT set_config('statement_timeout', ${String(timeoutMs)}, true)`;
        const [result] = await tx<{ removed: number }[]>`WITH deleted AS (
          DELETE FROM telemetry.spans WHERE expires_at < now() AND (trace_id,span_id) IN (
            SELECT trace_id,span_id FROM telemetry.spans WHERE expires_at < now() ORDER BY expires_at LIMIT ${limit}
          ) RETURNING billing_account_id
        ) SELECT count(*)::integer AS removed FROM deleted`;
        return result?.removed ?? 0;
      });
    },
    async read(id: string) {
      await ready();
      type Row = Pick<StoredSpan, "request_id" | "billing_account_id" | "source" | "span">;
      return db.begin(async (tx) => {
        await tx`SELECT set_config('statement_timeout', ${String(timeoutMs)}, true)`;
        return /^[0-9a-f]{32}$/.test(id)
          ? tx<
              Row[]
            >`SELECT request_id,billing_account_id,source,span FROM telemetry.spans WHERE trace_id=${id} AND expires_at > now() ORDER BY started_at,span_id LIMIT 1000`
          : tx<
              Row[]
            >`SELECT request_id,billing_account_id,source,span FROM telemetry.spans WHERE request_id=${id}::uuid AND expires_at > now() ORDER BY started_at,span_id LIMIT 1000`;
      });
    },
    close: () => db.close(),
  };
}
