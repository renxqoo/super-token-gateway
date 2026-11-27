import { expect, test } from "bun:test";
import { createDatabase } from "@gateway/ledger";
import { createGatewayRuntime } from "@gateway/gateway";
import { createTraceStore } from "../../apps/gateway/src/trace-storage.ts";
import {
  testDbName,
  adminUrl,
  adminDb,
  makeConfig,
  db,
  fakeUpstream,
  setup,
  internalCall,
} from "./smart-routing-fixture.ts";

export function registerDiagnosticsCases() {
  test("channel diagnostics retain failures through JSON and SSE retries and classify broken streams", async () => {
    const traceDbName = `trace_${testDbName}`;
    const traceUrl = new URL(adminUrl);
    traceUrl.pathname = `/${traceDbName}`;
    await adminDb.unsafe(`CREATE DATABASE ${traceDbName}`);
    const traceDb = createDatabase(traceUrl.toString(), 1);
    const traces = createTraceStore(traceUrl.toString(), 1000);
    const runtime = createGatewayRuntime({
      config: makeConfig({
        traceExport: {
          databaseUrl: traceUrl.toString(),
          sampleBasisPoints: 0,
          slowFirstTextMs: 60_000,
          maxAttempts: 4,
          errorBodyMaxBytes: 1024,
          maxRecords: 16,
          maxBytes: 65_536,
          maxPerBillingAccount: 8,
          priorityReserve: 4,
          batchSize: 4,
          batchDeadlineMs: 2000,
          flushMs: 20,
          timeoutMs: 1000,
          retryLimit: 0,
          retentionSeconds: 120,
          cleanupIntervalMs: 1000,
          cleanupBatch: 20,
        },
      }),
      db,
    });
    const readTrace = async (requestId: string) => {
      const deadline = performance.now() + 2000;
      let rows = await traces.read(requestId);
      while (!rows.length && performance.now() < deadline) {
        await Bun.sleep(20);
        rows = await traces.read(requestId);
      }
      return rows;
    };
    const attrs = (span: Awaited<ReturnType<typeof readTrace>>[number]["span"]) =>
      Object.fromEntries(span.attributes.map(({ key, value }) => [key, Object.values(value)[0]]));
    const summaries = { quota_exhausted: "Provider account quota exhausted" };
    try {
      await traceDb.unsafe(
        await Bun.file(new URL("../../infra/postgres/trace-schema.sql", import.meta.url)).text(),
      );
      await runtime.start();
      for (const scenario of ["http_body", "http_header", "sse"] as const) {
        const stream = scenario === "sse";
        const fromHeader = scenario === "http_header";
        const body = {
          error: {
            code: fromHeader ? "untrusted-provider-code" : "quota_exhausted",
            message: "private-provider-content".repeat(fromHeader ? 100 : 1),
          },
        };
        const unreachable = fakeUpstream("closed");
        unreachable.server.stop(true);
        const primary = fakeUpstream(
          "primary",
          stream
            ? { streamFrames: `data: ${JSON.stringify(body)}\n\n` }
            : {
                status: 429,
                unaryBody: body,
                headers: {
                  "x-vendor-request-id": "vendor-429",
                  ...(fromHeader ? { "x-vendor-error-code": "quota_exhausted" } : {}),
                },
              },
        );
        const backup = fakeUpstream("backup");
        try {
          const fixture = await setup(runtime, [unreachable, primary, backup], {
            priority: [20, 10, 0],
            strategy: "WEIGHTED",
            adapterOverrides: {
              error_diagnostics: {
                code_path: "error.code",
                code_header: "X-Vendor-Error-Code",
                request_id_header: "x-vendor-request-id",
                summaries,
              },
              error_rules: [
                { path: "error.code", equals: "quota_exhausted", retry_kind: "rate_limit" },
              ],
            },
          });
          const response = await fixture.chat(stream);
          expect(response.status).toBe(200);
          expect(await response.text()).toContain("backup");
          expect(primary.calls()).toBe(1);
          expect(backup.calls()).toBe(1);
          const requestId = response.headers.get("x-request-id")!;
          const rows = await readTrace(requestId);
          expect(rows).toHaveLength(4);
          const request = attrs(rows.find(({ span }) => span.name === "gateway.inference")!.span);
          const attempts = rows
            .filter(({ span }) => span.name === "gateway.upstream.attempt")
            .map(({ span }) => attrs(span))
            .sort(
              (left, right) =>
                Number(left["gateway.attempt.number"]) - Number(right["gateway.attempt.number"]),
            );
          expect(attempts[0]).toMatchObject({
            "gateway.failure.stage": "pre_dispatch",
            "error.type": "upstream_connect",
          });
          expect(attempts[1]).toMatchObject({
            "gateway.failure.stage": stream ? "stream_prelude" : "http_response",
            "http.response.status_code": stream ? "200" : "429",
            "gateway.provider.error_code": "quota_exhausted",
            "gateway.provider.error_summary": "Provider account quota exhausted",
            ...(stream ? {} : { "gateway.provider.request_id": "vendor-429" }),
          });
          expect(JSON.stringify(rows)).not.toContain("private-provider-content");
          expect(JSON.stringify(rows)).not.toContain("untrusted-provider-code");
          expect(attempts[2]?.["gateway.channel"]).toBe(fixture.channels[2]!.name);
          const [charge] =
            await db`SELECT selected_attempt_id, customer_atoms::text, supplier_cost_atoms::text
          FROM public.billing_charges WHERE request_id=${requestId}::uuid`;
          expect(charge).toEqual({
            selected_attempt_id: attempts[2]?.["gateway.attempt.id"],
            customer_atoms: "15",
            supplier_cost_atoms: "15",
          });
          expect(request["gateway.settlement.attempt_id"]).toBe(charge!.selected_attempt_id);
          const metrics = await (await runtime.metricsResponse()).text();
          expect(metrics).toContain(
            `gateway_channel_failures_total{channel="${fixture.channels[1]!.name}",stage="${stream ? "stream_prelude" : "http_response"}",kind="rate_limit"} 1`,
          );
          expect(await fixture.audit()).toMatchObject({
            ok: true,
            open_holds: 0,
            unknown_holds: 0,
          });
        } finally {
          primary.server.stop(true);
          backup.server.stop(true);
        }
      }
      for (const code of ["provider_stream_error", "stream_truncated"]) {
        const textFrame =
          'data: {"id":"x","choices":[{"index":0,"delta":{"content":"first"}}]}\n\n';
        const errorFrame =
          'event: error\ndata: {"error":{"code":"quota_exhausted","message":"private-provider-content"}}\n\n';
        const upstream = fakeUpstream("broken", {
          streamFrames: code === "provider_stream_error" ? textFrame + errorFrame : textFrame,
        });
        try {
          const fixture = await setup(runtime, [upstream], {
            adapterOverrides: {
              error_diagnostics: { code_path: "error.code", summaries },
            },
          });
          const before = await fixture.audit();
          const response = await fixture.chat(true);
          expect(response.status).toBe(200);
          expect(await response.text()).toContain(code);
          const requestId = response.headers.get("x-request-id")!;
          const rows = await readTrace(requestId);
          const attempt = rows.find(({ span }) => span.name === "gateway.upstream.attempt")!.span;
          expect(attempt.status.code).toBe(2);
          expect(attrs(attempt)).toMatchObject({
            "gateway.failure.stage": "stream",
            "gateway.failure.kind": "protocol",
            "error.type": code,
            ...(code === "provider_stream_error"
              ? { "gateway.provider.error_code": "quota_exhausted" }
              : {}),
          });
          expect(JSON.stringify(rows)).not.toContain("private-provider-content");
          if (code === "provider_stream_error") {
            expect(await fixture.charges()).toMatchObject({ n: 0 });
            expect(await fixture.holdsByState()).toEqual({ UNKNOWN: 1 });
            expect(await fixture.audit()).toMatchObject({
              ok: true,
              open_holds: 0,
              unknown_holds: Number(before.unknown_holds) + 1,
            });
            const cleanup = await internalCall(
              runtime.internalApp,
              "POST",
              `/internal/v1/requests/${requestId}/resolve`,
              {
                action: "RELEASE",
                reason: "Isolated test provider confirmed no billable usage",
              },
            );
            expect(cleanup.status).toBe(200);
            expect(await fixture.holdsByState()).toEqual({ RELEASED: 1 });
          } else {
            expect(await fixture.charges()).toMatchObject({ n: 1 });
            expect(await fixture.holdsByState()).toEqual({ SETTLED: 1 });
            expect(await fixture.audit()).toMatchObject({
              ok: true,
              open_holds: 0,
              unknown_holds: Number(before.unknown_holds),
            });
          }
        } finally {
          upstream.server.stop(true);
        }
      }
    } finally {
      await runtime.close();
      await traces.close();
      await traceDb.close();
      await adminDb.unsafe(`DROP DATABASE ${traceDbName} WITH (FORCE)`);
    }
  });
}
