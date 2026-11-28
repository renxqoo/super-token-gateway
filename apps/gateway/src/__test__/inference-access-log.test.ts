import { expect, spyOn, test } from "bun:test";
import { createKey } from "@gateway/runtime";
import { GatewayMetrics } from "../observability.ts";
import { handleInferenceRequest } from "../proxy.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

test("rejected model identifiers do not appear in access logs", async () => {
  const key = createKey();
  const account = makeBillingAccount(crypto.randomUUID(), 2);
  const snapshot = makeSnapshot(1, [account], [makeApiKey(key, account.id)]);
  const deps = makeFakeDeps(snapshot, makeConfig());
  deps.metrics = new GatewayMetrics(true);
  const log = spyOn(console, "log").mockImplementation(() => {});
  try {
    const raw = new Request("http://fixture.test/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "unknown-fixture-model" }),
    });
    const response = await handleInferenceRequest(
      {
        req: { raw, query: () => undefined },
        json: (value, status = 200, headers = {}) => Response.json(value, { status, headers }),
      },
      deps,
      "chat",
    );
    expect(response.status).toBe(404);
    expect(log.mock.calls).toHaveLength(1);
    expect(JSON.parse(String(log.mock.calls[0]![0]))).toMatchObject({
      model: null,
      status: 404,
      error_code: "model_not_found",
    });
  } finally {
    log.mockRestore();
    deps.state?.close();
    deps.transport.close();
    await deps.cluster.close();
  }
});

test("oversized access logs preserve routing diagnostics in a bounded JSON record", () => {
  const metrics = new GatewayMetrics(true);
  const log = spyOn(console, "log").mockImplementation(() => {});
  try {
    metrics.observeRequest({
      trace_id: "trace-fixture",
      request_id: "request-fixture",
      billing_account_id: null,
      model: "catalog-fixture",
      operation: "chat",
      stream: false,
      channel: "x".repeat(20_000),
      status: 200,
      attempts: 1,
      failover: false,
      moderation_blocked: false,
      duration_ms: 20,
      ttft_ms: null,
      error_code: null,
    });
    expect(log.mock.calls).toHaveLength(1);
    const line = String(log.mock.calls[0]![0]);
    expect(Buffer.byteLength(line)).toBeLessThanOrEqual(16 * 1024);
    expect(JSON.parse(line)).toMatchObject({
      request_id: "request-fixture",
      trace_id: "trace-fixture",
      status: 200,
      log_truncated: true,
    });
  } finally {
    log.mockRestore();
  }
});
