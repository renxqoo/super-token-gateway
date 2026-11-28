import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { createGatewayRuntime } from "@gateway/gateway";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import {
  makeConfig,
  db,
  seed,
  wallet,
  internalCall,
  realServer,
  waitFor,
  chatRequest,
} from "./adversarial-billing-fixture.ts";

export function registerRecoveryCases() {
  test("symptom: replaying the same Idempotency-Key returns one charge, not two", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = `adv-idem-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-idem",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      const before = await wallet(runtime, billingAccountId);
      const idem = crypto.randomUUID();
      const send = () =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/chat/completions", {
            method: "POST",
            headers: {
              authorization: `Bearer ${key}`,
              "content-type": "application/json",
              "idempotency-key": idem,
            },
            body: JSON.stringify({
              model,
              stream: false,
              messages: [{ role: "user", content: "replay" }],
              max_tokens: 20,
            }),
          }),
        );
      const responses = await Promise.all([send(), send(), send(), send(), send()]);
      const ok = responses.filter((r) => r.status === 200);
      const rejected = responses.filter((r) => r.status === 409);
      expect(ok.length).toBe(1);
      expect(rejected.length).toBe(4);
      for (const r of rejected) {
        expect(((await r.json()) as { error: { code: string } }).error.code).toBe(
          "idempotency_in_progress",
        );
      }
      await Promise.all(ok.map((r) => r.text()));
      const replay = await send();
      expect(replay.status).toBe(409);
      expect(((await replay.json()) as { error: { code: string } }).error.code).toBe(
        "idempotency_replayed",
      );
      const charges =
        await db`SELECT count(*)::int AS n, count(DISTINCT request_id)::int AS u FROM public.billing_charges WHERE api_key_id = (SELECT id FROM public.api_keys WHERE billing_account_id = ${billingAccountId})`;
      expect(charges[0]!.n).toBe(1);
      expect(charges[0]!.u).toBe(1);
      const after = await wallet(runtime, billingAccountId);
      expect(BigInt(after.reserved_atoms)).toBe(BigInt(before.reserved_atoms));
      const audit = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.data.ok).toBe(true);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test.each([
    ["changing routes", "idempotency_replayed"],
    ["changing the address hidden by redaction", "idempotency_conflict"],
    ["reusing a key held by an obsolete redacted fingerprint", "idempotency_conflict"],
    ["reusing a key held by an obsolete original fingerprint", "idempotency_conflict"],
  ] as const)(
    "symptom: %s returns the wrong idempotency decision",
    async (scenario, expectedCode) => {
      const dispatched: string[] = [];
      const upstream = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        async fetch(req) {
          if (new URL(req.url).pathname === "/redact") {
            const { text } = (await req.json()) as { text: string };
            return Response.json({
              action: "TRANSFORM",
              text: text.replace(/[a-z]+@example\.com/g, "[REDACTED_EMAIL]"),
            });
          }
          const body = (await req.json()) as { messages: { content: string }[] };
          dispatched.push(body.messages[0]!.content);
          return Response.json({
            id: "idem-response",
            object: "chat.completion",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
            usage: { prompt_tokens: 6, completion_tokens: 1, total_tokens: 7 },
          });
        },
      });
      const runtime = createGatewayRuntime({ config: makeConfig(), db });
      try {
        await runtime.start();
        const fixture = await seed(runtime, upstream.url.origin);
        const name = crypto.randomUUID().slice(0, 8);
        const model = `idempotency-${name}`;
        const webhookReply = { action: "TRANSFORM", text: "Contact [REDACTED_EMAIL]" };
        const plugin = await fixture.call("PUT", "/internal/v1/plugins", {
          name: `idem-redactor-${name}`,
          version: "1",
          kind: "WEBHOOK",
          webhook_config: {
            url: new URL("/redact", upstream.url).href,
            failure_policy: "FAIL_CLOSED",
          },
          manifest: { hooks: ["on_request"] },
        });
        await fixture.call("POST", "/internal/v1/plugin-releases", {
          id: crypto.randomUUID(),
          expected_epoch: runtime.snapshot.get().epoch,
          targets: [{ plugin_id: plugin.id }],
          samples: {
            [String(plugin.id)]: {
              text: "Contact sample@example.com",
              webhook_reply: webhookReply,
            },
          },
        });
        await fixture.call("PUT", "/internal/v1/models", { name: model, operations: ["chat"] });
        const backup = await fixture.call("PUT", "/internal/v1/channels", {
          account_id: fixture.channel.account_id,
          name: `idem-backup-${name}`,
          kind: "openai",
          base_url: upstream.url.href,
          api_key: "backup",
        });
        const route = {
          model_name: model,
          operations: ["chat"],
          upstream_model: "idem-model",
          billing_price_id: fixture.customer.id,
          cost_version_id: fixture.supplier.id,
        };
        const primaryConfig = {
          ...route,
          priority: 10,
          channel_id: fixture.channel.id,
          plugin_pipeline: [{ plugin_id: plugin.id, priority: 1 }],
        };
        const primary = await fixture.call("PUT", "/internal/v1/routes", primaryConfig);
        await fixture.call("PUT", "/internal/v1/routes", { ...route, channel_id: backup.id });
        const idem = crypto.randomUUID();
        function send(content: string) {
          const request = chatRequest(fixture.key, model, {
            stream: false,
            messages: [{ role: "user", content }],
          });
          request.headers.set("idempotency-key", idem);
          return runtime.publicApp.fetch(request);
        }
        const first = await send("Contact alice@example.com");
        expect(first.status).toBe(200);
        await first.text();
        expect(dispatched).toEqual(["Contact [REDACTED_EMAIL]"]);

        if (scenario === "changing routes") {
          await fixture.call("PUT", "/internal/v1/routes", {
            ...primaryConfig,
            id: primary.id,
            enabled: false,
          });
        }
        if (scenario === "reusing a key held by an obsolete redacted fingerprint") {
          const historicalFingerprint = createHash("sha256")
            .update(
              JSON.stringify({
                billing_account_id: fixture.billingAccountId,
                body: {
                  max_tokens: 50,
                  messages: [{ content: "Contact [REDACTED_EMAIL]", role: "user" }],
                  model,
                  stream: false,
                },
                model,
                operation: "chat",
              }),
            )
            .digest("hex");
          await db`UPDATE public.billing_holds SET fingerprint = ${historicalFingerprint}
          WHERE billing_account_id = ${fixture.billingAccountId}::uuid AND idempotency_key = ${idem}`;
        }
        if (scenario === "reusing a key held by an obsolete original fingerprint") {
          await db`UPDATE public.billing_holds SET fingerprint = substring(fingerprint FROM 4)
          WHERE billing_account_id = ${fixture.billingAccountId}::uuid AND idempotency_key = ${idem}`;
        }
        const second = await send(
          scenario === "changing the address hidden by redaction"
            ? "Contact bob@example.com"
            : "Contact alice@example.com",
        );
        expect(second.status).toBe(409);
        expect(await second.json()).toMatchObject({ error: { code: expectedCode } });
        if (expectedCode === "idempotency_replayed") {
          expect(second.headers.get("x-request-id")).toBe(first.headers.get("x-request-id"));
        }
        expect(dispatched).toHaveLength(1);
        const [charges] =
          await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id = ${fixture.billingAccountId}::uuid`;
        expect(charges?.n).toBe(1);
      } finally {
        await runtime.close();
        await upstream.stop(true);
      }
    },
  );

  test("symptom: invalid auth, unknown model, oversized body and malformed JSON never create a hold", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig({ maxBodyBytes: 64 * 1024 }), db });
    try {
      await runtime.start();
      const { billingAccountId, key } = await seed(runtime, mockServer.url.origin);
      const before = await wallet(runtime, billingAccountId);
      const badAuth = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: "Bearer sk-forged", "content-type": "application/json" },
          body: JSON.stringify({ model: "x", messages: [] }),
        }),
      );
      expect(badAuth.status).toBe(401);
      const badKey = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: "Bearer ${key}".replace("${key}", key) + "x",
            "content-type": "application/json",
          },
          body: JSON.stringify({ model: "x", messages: [] }),
        }),
      );
      expect(badKey.status).toBe(401);
      const noAuth = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ model: "x", messages: [] }),
        }),
      );
      expect(noAuth.status).toBe(401);
      const unknown = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "does-not-exist",
            messages: [{ role: "user", content: "x" }],
          }),
        }),
      );
      expect(unknown.status).toBe(404);
      const huge = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "x",
            messages: [{ role: "user", content: "A".repeat(100_000) }],
          }),
        }),
      );
      expect(huge.status).toBe(413);
      const malformed = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: "{not json",
        }),
      );
      expect([400, 422]).toContain(malformed.status);
      const emptyModel = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
          body: JSON.stringify({ model: "", messages: [{ role: "user", content: "x" }] }),
        }),
      );
      expect([400, 404, 422]).toContain(emptyModel.status);
      const sqlInjection = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${key}' OR 1=1 --`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ model: "x' DROP TABLE billing_holds; --", messages: [] }),
        }),
      );
      expect(sqlInjection.status).toBe(401);
      const after = await wallet(runtime, billingAccountId);
      expect(after).toEqual(before);
      const holds =
        await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE billing_account_id = ${billingAccountId}`;
      expect(holds[0]!.n).toBe(0);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test("symptom: client disconnects before the upstream answers -> hold becomes UNKNOWN within 2.5s, nothing charged, ops RELEASE restores the wallet exactly", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        mockServer.url.origin,
      );
      const model = `adv-prehdr-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-prehdr",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      mock.scenarios.set("adv-prehdr", { headerDelayMs: 3000 });
      const before = await wallet(runtime, billingAccountId);
      const publicServer = realServer(runtime);
      const t0 = performance.now();
      const ac = new AbortController();
      const pending = fetch(`http://127.0.0.1:${publicServer.port}/v1/chat/completions`, {
        method: "POST",
        headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
        body: JSON.stringify({
          model,
          stream: true,
          messages: [{ role: "user", content: "attack" }],
          max_tokens: 50,
        }),
        signal: ac.signal,
      }).catch(() => undefined);
      await waitFor(async () => {
        const w = await wallet(runtime, billingAccountId);
        return BigInt(w.reserved_atoms) > BigInt(before.reserved_atoms);
      });
      ac.abort();
      await pending;
      const [holdRow] =
        await db`SELECT request_id FROM public.billing_holds WHERE billing_account_id = ${billingAccountId} ORDER BY created_at DESC LIMIT 1`;
      await waitFor(async () => {
        const rows =
          await db`SELECT state FROM public.billing_holds WHERE request_id = ${holdRow!.request_id}`;
        return rows[0]?.state === "UNKNOWN";
      }, 2500);
      const settledWithinMs = performance.now() - t0;
      expect(settledWithinMs).toBeLessThan(2500);
      expect(runtime.lifecycle.size).toBe(0);
      const during = await wallet(runtime, billingAccountId);
      expect(BigInt(during.spent_atoms)).toBe(BigInt(before.spent_atoms));
      expect(BigInt(during.reserved_atoms) - BigInt(before.reserved_atoms)).toBe(100n);
      const charges =
        await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id = ${billingAccountId}`;
      expect(charges[0]!.n).toBe(0);
      const audit = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.data.ok).toBe(true);
      expect(audit.data.open_holds).toBe(0);
      expect(audit.data.unknown_holds).toBe(1);
      const resolve = await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${holdRow!.request_id}/resolve`,
        { action: "RELEASE", reason: "client-gone-before-upstream-headers" },
      );
      expect(resolve.status, JSON.stringify(resolve.data)).toBe(200);
      const after = await wallet(runtime, billingAccountId);
      expect(after).toEqual(before);
      publicServer.stop(true);
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test("symptom: wallet with insufficient funds is rejected before any upstream call and leaves no hold", async () => {
    const mock = createMockApp("adv-ctrl");
    const mockServer = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: mock.app.fetch });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { key, channel, customer, supplier, call } = await seed(runtime, mockServer.url.origin);
      const poor = await call("PUT", "/internal/v1/billing-accounts", {
        name: `adv-poor-${crypto.randomUUID().slice(0, 6)}`,
        currency: "USD",
        rpm: 10,
        tpm: 100,
        concurrency: 1,
      });
      const poorKey = await call("PUT", "/internal/v1/api-keys", {
        billing_account_id: poor.id,
        name: "poor",
      });
      const model = `adv-poor-${crypto.randomUUID().slice(0, 6)}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: channel.id,
        upstream_model: "adv-poor",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
      });
      const requestsBefore = mock.stats.requests;
      const res = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${poorKey.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model,
            stream: false,
            messages: [{ role: "user", content: "x" }],
            max_tokens: 20,
          }),
        }),
      );
      expect([402, 403]).toContain(res.status);
      expect(mock.stats.requests).toBe(requestsBefore);
      const holds =
        await db`SELECT count(*)::int AS n FROM public.billing_holds WHERE billing_account_id = ${poor.id}`;
      expect(holds[0]!.n).toBe(0);
      void key;
    } finally {
      await runtime.close();
      await mockServer.stop(true);
    }
  });

  test("symptom: internal admin API rejects a forged or missing token and never exposes resolve", async () => {
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const forged = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/requests/00000000-0000-0000-0000-000000000000/resolve",
        {
          action: "RELEASE",
          reason: "forged",
        },
        "wrong-token",
      );
      expect(forged.status).toBe(401);
      const missing = await runtime.publicApp.fetch(
        new Request(
          "http://gateway.local/internal/v1/requests/00000000-0000-0000-0000-000000000000/resolve",
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ action: "RELEASE", reason: "x" }),
          },
        ),
      );
      expect([401, 404]).toContain(missing.status);
      const auditForged = await runtime.publicApp.fetch(
        new Request("http://gateway.local/internal/v1/audit/conservation", { method: "GET" }),
      );
      expect([401, 404]).toContain(auditForged.status);
    } finally {
      await runtime.close();
    }
  });

  test("symptom: an upstream business error prematurely releases an evidence-required hold", async () => {
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      fetch: () =>
        Response.json(
          { vendor: { code: "BUSY" } },
          {
            headers: { "x-vendor-request-id": "vendor-throttle-reconciliation" },
          },
        ),
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const { billingAccountId, key, channel, customer, supplier, call } = await seed(
        runtime,
        `http://127.0.0.1:${upstream.port}`,
      );
      const model = "reconciliation-provider-error";
      const routeId = crypto.randomUUID();
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 64,
      });
      const putRoute = (definitive: boolean) =>
        call("PUT", "/internal/v1/routes", {
          id: routeId,
          model_name: model,
          channel_id: channel.id,
          upstream_model: model,
          operations: ["chat"],
          billing_price_id: customer.id,
          cost_version_id: supplier.id,
          adapter_overrides: {
            reconciliation_response_id_header: "x-vendor-request-id",
            error_rules: [
              {
                path: "vendor.code",
                equals: "BUSY",
                retry_kind: "client_error",
                http_status: 429,
                ...(definitive ? { execution: "NOT_EXECUTED" } : {}),
              },
            ],
          },
        });

      await putRoute(false);
      const unknown = await runtime.publicApp.fetch(chatRequest(key, model, { stream: false }));
      expect(unknown.status).toBe(429);
      await unknown.text();
      const requestId = unknown.headers.get("x-request-id")!;
      const [hold] =
        await db`SELECT state, supplier_holds FROM public.billing_holds WHERE request_id=${requestId}`;
      expect(hold.state).toBe("UNKNOWN");
      expect(hold.supplier_holds).toMatchObject([
        { state: "UNKNOWN", provider_request_id: "vendor-throttle-reconciliation" },
      ]);

      const wrongReceipt = {
        request_id: requestId,
        attempt_id: hold.supplier_holds[0].attempt_id,
        provider_request_id: "other-provider-request",
        account_id: hold.supplier_holds[0].account_id,
        channel_id: channel.id,
        currency: "USD",
        evidence_ref: "supplier-proof",
        verdict: "NOT_EXECUTED",
      };
      const rejected = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/reconciliation/receipts",
        { receipts: [wrongReceipt] },
      );
      expect(rejected.data).toEqual({
        results: [{ attempt_id: wrongReceipt.attempt_id, status: "identity_conflict" }],
      });
      expect(BigInt((await wallet(runtime, billingAccountId)).reserved_atoms)).toBeGreaterThan(0n);

      await putRoute(true);
      const certain = await runtime.publicApp.fetch(chatRequest(key, model, { stream: false }));
      expect(certain.status).toBe(429);
      await certain.text();
      const [released] =
        await db`SELECT state FROM public.billing_holds WHERE request_id=${certain.headers.get("x-request-id")}`;
      expect(released.state).toBe("RELEASED");

      const valid = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/reconciliation/receipts",
        {
          receipts: [{ ...wrongReceipt, provider_request_id: "vendor-throttle-reconciliation" }],
        },
      );
      expect(valid.data).toEqual({
        results: [{ attempt_id: wrongReceipt.attempt_id, status: "accepted" }],
      });
      expect(await runtime.ledger.processReconciliation(5)).toEqual({ receipts: 1, waivers: 0 });
      expect(await wallet(runtime, billingAccountId)).toMatchObject({
        reserved_atoms: "0",
        spent_atoms: "0",
      });
      expect((await runtime.ledger.auditConservation()).ok).toBe(true);
    } finally {
      await runtime.close();
      await upstream.stop(true);
    }
  });
}
