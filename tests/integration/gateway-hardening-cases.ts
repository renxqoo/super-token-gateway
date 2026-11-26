import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { makeConfig, db, internalCall, OPERATOR_TOKEN } from "./gateway-fixture.ts";

export function registerGatewayHardeningCases() {
  test("G2.4 Production hardening: webhook SSRF block, /v1/models CIDR check, concurrent Idempotency-Key HELD guard, supplier UNKNOWN reconciliation after customer settlement, active SSE remains counted stalled SSE times out and draining emits an error, and flat customer prices preserve supplier usage", async () => {
    const strictRuntime = createGatewayRuntime({
      config: makeConfig({
        allowPrivateUpstreams: false,
        privateUpstreamCidrs: [],
      }),
      db,
    });
    try {
      await strictRuntime.start();
      const ssrfMeta = await internalCall(
        strictRuntime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "ssrf-meta",
          version: "1.0.0",
          kind: "WEBHOOK",
          webhook_config: {
            url: "http://169.254.169.254/latest/meta-data/",
          },
        },
      );
      expect(ssrfMeta.status).toBe(400);
      expect((ssrfMeta.data.error as { code: string }).code).toBe("unsafe_upstream");

      const ssrfLoopback = await internalCall(
        strictRuntime.internalApp,
        "PUT",
        "/internal/v1/plugins",
        {
          name: "ssrf-loopback",
          version: "1.0.0",
          kind: "WEBHOOK",
          webhook_config: {
            url: "http://127.0.0.1:9999/hook",
          },
        },
      );
      expect(ssrfLoopback.status).toBe(400);
      expect((ssrfLoopback.data.error as { code: string }).code).toBe("unsafe_upstream");
    } finally {
      await strictRuntime.close();
    }

    let primaryMode: "slow_idem" | "hang_timeout" | "sse_hang" = "slow_idem";
    let primaryCalls = 0;
    let backupCalls = 0;
    const openTimers = new Set<ReturnType<typeof setTimeout>>();

    const primaryServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        primaryCalls++;
        if (primaryMode === "slow_idem") {
          await Bun.sleep(150);
          return Response.json({
            id: "chatcmpl_idem_1",
            object: "chat.completion",
            model: "gpt-sec-primary",
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "idem-ok" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
          });
        }
        if (primaryMode === "hang_timeout") {
          await new Promise<void>((resolve) => {
            const t = setTimeout(() => {
              openTimers.delete(t);
              resolve();
            }, 600);
            openTimers.add(t);
            req.signal.addEventListener(
              "abort",
              () => {
                clearTimeout(t);
                openTimers.delete(t);
                resolve();
              },
              { once: true },
            );
          });
          return Response.json({ error: { message: "too slow" } }, { status: 504 });
        }
        const enc = new TextEncoder();
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(
              enc.encode(
                `data: ${JSON.stringify({
                  id: "chatcmpl_sse_abort",
                  object: "chat.completion.chunk",
                  model: "gpt-sec-primary",
                  choices: [{ index: 0, delta: { role: "assistant", content: "partial " } }],
                })}\n\n`,
              ),
            );
            const t = setTimeout(() => {
              openTimers.delete(t);
              try {
                controller.close();
              } catch {
                void 0;
              }
            }, 5000);
            openTimers.add(t);
            req.signal.addEventListener(
              "abort",
              () => {
                clearTimeout(t);
                openTimers.delete(t);
                try {
                  controller.close();
                } catch {
                  void 0;
                }
              },
              { once: true },
            );
          },
        });
        return new Response(stream, {
          status: 200,
          headers: { "content-type": "text/event-stream; charset=utf-8" },
        });
      },
    });

    const backupServer = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch() {
        backupCalls++;
        return Response.json({
          id: "chatcmpl_sec_backup",
          object: "chat.completion",
          model: "gpt-sec-backup",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "from-sec-backup" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
        });
      },
    });

    const runtime = createGatewayRuntime({
      config: makeConfig({ attemptTimeoutMs: 200 }),
      db,
    });

    try {
      await runtime.start();
      const { data: billingAccount } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/billing-accounts",
        {
          name: "hardening-billingAccount",
          currency: "USD",
        },
      );
      const billingAccountId = String(billingAccount.id);

      const { data: unrestrictedKey } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/api-keys",
        {
          billing_account_id: billingAccountId,
          name: "hardening-open",
        },
      );
      const apiKey = String(unrestrictedKey.key);

      const { data: cidrKey } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/api-keys",
        {
          billing_account_id: billingAccountId,
          name: "hardening-cidr",
          allowed_cidrs: ["10.20.30.0/24"],
        },
      );
      const restrictedApiKey = String(cidrKey.key);

      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/wallets/${billingAccountId}/credits`,
        {
          currency: "USD",
          external_ref: "credit-hardening-1",
          amount_atoms: "1000000",
          kind: "CASH",
        },
      );

      const { data: accPrimary } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "hardening-acc-primary" },
      );
      const { data: accBackup } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/upstream-accounts",
        { name: "hardening-acc-backup" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${accPrimary.id}/recharges`,
        { currency: "USD", external_ref: "sup-hp-1", amount_atoms: "1000000" },
      );
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/upstream-accounts/${accBackup.id}/recharges`,
        { currency: "USD", external_ref: "sup-hb-1", amount_atoms: "1000000" },
      );

      const { data: chPrimary } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: accPrimary.id,
          name: "hardening-primary",
          kind: "openai",
          base_url: `http://127.0.0.1:${primaryServer.port}`,
          api_key: "sk-hp",
        },
      );
      const { data: chBackup } = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: accBackup.id,
          name: "hardening-backup",
          kind: "openai",
          base_url: `http://127.0.0.1:${backupServer.port}`,
          api_key: "sk-hb",
        },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "hardening-gpt",
        operations: ["chat"],
        routing_strategy: "WEIGHTED",
        max_output_tokens: 100,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "hardening-sse-only",
        operations: ["chat"],
        routing_strategy: "WEIGHTED",
        max_output_tokens: 100,
      });

      const { data: custPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "10", denominator: "1" },
            output_tokens: { numerator: "20", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );
      const { data: supPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "SUPPLIER",
          currency: "USD",
          rates: {
            input_tokens: { numerator: "2", denominator: "1" },
            output_tokens: { numerator: "4", denominator: "1" },
          },
          ceilings: { input_tokens: "1000", output_tokens: "100" },
        },
      );

      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "hardening-gpt",
        channel_id: chPrimary.id,
        upstream_model: "gpt-sec-primary",
        operations: ["chat"],
        priority: 10,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: { max_attempts: 2, retry_on_ambiguous: true },
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "hardening-gpt",
        channel_id: chBackup.id,
        upstream_model: "gpt-sec-backup",
        operations: ["chat"],
        priority: 5,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: { max_attempts: 2, retry_on_ambiguous: true },
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "hardening-sse-only",
        channel_id: chPrimary.id,
        upstream_model: "gpt-sec-primary",
        operations: ["chat"],
        priority: 10,
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
        retry_policy: { max_attempts: 1 },
      });

      const deniedModels = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/models", {
          headers: { authorization: `Bearer ${restrictedApiKey}` },
        }),
        { peerAddress: "192.168.1.5" },
      );
      expect(deniedModels.status).toBe(403);
      const deniedJson = (await deniedModels.json()) as { error: { code: string } };
      expect(deniedJson.error.code).toBe("ip_not_allowed");

      const allowedModels = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/models", {
          headers: { authorization: `Bearer ${restrictedApiKey}` },
        }),
        { peerAddress: "10.20.30.42" },
      );
      expect(allowedModels.status).toBe(200);
      const allowedJson = (await allowedModels.json()) as { data: { id: string }[] };
      expect(allowedJson.data.some((m) => m.id === "hardening-gpt")).toBe(true);

      const idemKey = "idem-concurrent-held-guard-1";
      const idemResponses = await Promise.all(
        Array.from({ length: 5 }, () =>
          runtime.publicApp.fetch(
            new Request("http://gateway.local/v1/chat/completions", {
              method: "POST",
              headers: {
                authorization: `Bearer ${apiKey}`,
                "content-type": "application/json",
                "idempotency-key": idemKey,
              },
              body: JSON.stringify({
                model: "hardening-gpt",
                messages: [{ role: "user", content: "concurrent idempotency test" }],
                max_tokens: 50,
              }),
            }),
          ),
        ),
      );
      const idemStatuses = idemResponses.map((r) => r.status).sort((a, b) => a - b);
      expect(idemStatuses).toEqual([200, 409, 409, 409, 409]);
      const conflictCodes = await Promise.all(
        idemResponses
          .filter((r) => r.status === 409)
          .map(async (r) => ((await r.json()) as { error: { code: string } }).error.code),
      );
      expect(conflictCodes.every((c) => c === "idempotency_in_progress")).toBe(true);
      expect(primaryCalls).toBe(1);

      const { data: walletAfterIdem } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      const singleCustCost = 10 * 10 + 5 * 20;
      expect(walletAfterIdem.spent_atoms).toBe(String(singleCustCost));
      expect(walletAfterIdem.reserved_atoms).toBe("0");

      primaryMode = "hang_timeout";
      const ambigResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model: "hardening-gpt",
            messages: [{ role: "user", content: "ambiguous timeout failover" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(ambigResp.status).toBe(200);
      expect(ambigResp.headers.get("x-gateway-channel")).toBe("hardening-backup");
      expect(backupCalls).toBe(1);

      const { data: walletAfterAmbig } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/wallets/${billingAccountId}?currency=USD`,
      );
      expect(walletAfterAmbig.spent_atoms).toBe(String(2 * singleCustCost));
      expect(walletAfterAmbig.reserved_atoms).toBe("0");

      const { data: primarySupAfterAmbig } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/upstream-accounts/${accPrimary.id}/balance?currency=USD`,
      );
      expect(BigInt(String(primarySupAfterAmbig.reserved_atoms))).toBeGreaterThan(0n);
      const ambigRequestId = ambigResp.headers.get("x-request-id")!;
      const [attempts] = await db`
      SELECT supplier_holds FROM public.billing_holds WHERE request_id = ${ambigRequestId}
    `;
      const supplierAttempt = (
        attempts!.supplier_holds as { attempt_id: string; state: string }[]
      ).find((attempt) => attempt.state === "UNKNOWN")!;
      const resolveSupplier = () =>
        runtime.internalApp.fetch(
          new Request(
            `http://internal.local/internal/v1/requests/${ambigRequestId}/attempts/${supplierAttempt.attempt_id}/resolve`,
            {
              method: "POST",
              headers: {
                authorization: `Bearer ${OPERATOR_TOKEN}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({
                action: "SETTLE",
                supplier_cost_atoms: "17",
                evidence_ref: "provider-invoice-item-17",
                reason: "supplier confirmed usage after failover",
              }),
            },
          ),
        );
      const supplierResolution = await resolveSupplier();
      expect(supplierResolution.status).toBe(200);
      expect(await supplierResolution.json()).toMatchObject({ state: "SETTLED", replayed: false });
      expect(await (await resolveSupplier()).json()).toMatchObject({
        state: "SETTLED",
        replayed: true,
      });
      expect(await runtime.ledger.getSupplierBalance(String(accPrimary.id), "USD")).toMatchObject({
        reserved_atoms: "0",
        spent_atoms: "57",
      });

      const { data: backupSupAfterAmbig } = await internalCall(
        runtime.internalApp,
        "GET",
        `/internal/v1/upstream-accounts/${accBackup.id}/balance?currency=USD`,
      );
      expect(backupSupAfterAmbig.reserved_atoms).toBe("0");
      expect(backupSupAfterAmbig.spent_atoms).toBe(String(10 * 2 + 5 * 4));

      const preAbortHealth = runtime.router
        .snapshot()
        .find((s) => s.channel_id === String(chPrimary.id));
      const preAbortFailures = preAbortHealth?.consecutive_failures ?? 0;

      primaryMode = "sse_hang";
      const abortController = new AbortController();
      const sseResp = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: {
            authorization: `Bearer ${apiKey}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model: "hardening-sse-only",
            stream: true,
            messages: [{ role: "user", content: "cancel mid stream" }],
            max_tokens: 50,
          }),
          signal: abortController.signal,
        }),
      );
      expect(sseResp.status).toBe(200);
      const sseReqId = sseResp.headers.get("x-request-id")!;
      const reader = sseResp.body!.getReader();
      const firstChunk = await reader.read();
      expect(firstChunk.done).toBe(false);
      expect(new TextDecoder().decode(firstChunk.value)).toContain("partial");
      const liveMetrics = await runtime.publicApp.fetch(
        new Request("http://gateway.local/metrics"),
      );
      expect(await liveMetrics.text()).toMatch(/^gateway_in_flight 1$/m);
      abortController.abort();
      await reader.cancel();

      for (let i = 0; i < 40; i++) {
        if (runtime.lifecycle.size === 0) {
          const rows = await db`
          SELECT state FROM public.billing_holds WHERE request_id = ${sseReqId}
        `;
          if (rows[0]?.state === "UNKNOWN") break;
        }
        await Bun.sleep(20);
      }

      const sseHoldRows = await db`
      SELECT state FROM public.billing_holds WHERE request_id = ${sseReqId}
    `;
      expect(sseHoldRows[0]!.state).toBe("UNKNOWN");
      expect(runtime.lifecycle.size).toBe(0);

      const postAbortHealth = runtime.router
        .snapshot()
        .find((s) => s.channel_id === String(chPrimary.id));
      expect(postAbortHealth?.state).toBe("CLOSED");
      expect(postAbortHealth?.consecutive_failures ?? 0).toBe(preAbortFailures);

      await internalCall(runtime.internalApp, "POST", `/internal/v1/requests/${sseReqId}/resolve`, {
        action: "RELEASE",
        reason: "client_aborted_sse",
      });

      const stalled = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "hardening-sse-only",
            stream: true,
            messages: [{ role: "user", content: "stalled upstream" }],
            max_tokens: 50,
          }),
        }),
      );
      const stalledReader = stalled.body!.getReader();
      try {
        expect(new TextDecoder().decode((await stalledReader.read()).value)).toContain("partial");
        const outcome = await Promise.race([
          stalledReader.read().then((step) => new TextDecoder().decode(step.value)),
          Bun.sleep(1000).then(() => "read timed out"),
        ]);
        expect(outcome).toContain("stream_interrupted");
      } finally {
        await stalledReader.cancel();
      }
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${stalled.headers.get("x-request-id")}/resolve`,
        {
          action: "RELEASE",
          reason: "stalled_upstream_sse",
        },
      );

      primaryMode = "slow_idem";
      const { data: flatPrice } = await internalCall(
        runtime.internalApp,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          basis: "REQUEST_COMPLETION",
          rates: { requests: { numerator: "7", denominator: "1" } },
          ceilings: { requests: "1" },
        },
      );
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
        name: "hardening-mixed-price",
        operations: ["chat"],
        max_output_tokens: 100,
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        model_name: "hardening-mixed-price",
        channel_id: chPrimary.id,
        upstream_model: "gpt-sec-primary",
        operations: ["chat"],
        billing_price_id: flatPrice.id,
        cost_version_id: supPrice.id,
      });
      const mixedPriceResponse = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "hardening-mixed-price",
            messages: [{ role: "user", content: "mixed tariff" }],
            max_tokens: 50,
          }),
        }),
      );
      expect(mixedPriceResponse.status).toBe(200);
      const mixedPriceRequest = mixedPriceResponse.headers.get("x-request-id")!;
      const [mixedPriceCharge] = await db`
      SELECT customer_atoms::text, supplier_cost_atoms::text
      FROM public.billing_charges WHERE request_id = ${mixedPriceRequest}
    `;
      expect(mixedPriceCharge).toMatchObject({ customer_atoms: "7", supplier_cost_atoms: "40" });

      const { data: audit } = await internalCall(
        runtime.internalApp,
        "GET",
        "/internal/v1/audit/conservation",
      );
      expect(audit.ok).toBe(true);
      expect(audit.open_holds).toBe(0);
      expect(audit.unknown_holds).toBe(0);

      primaryMode = "sse_hang";
      const streamFailureMetrics = async () =>
        (await (await runtime.metricsResponse()).text())
          .split("\n")
          .filter(
            (line) =>
              line.startsWith("gateway_channel_failures_total{") && line.includes('stage="stream"'),
          );
      const beforeDrain = await streamFailureMetrics();
      expect(beforeDrain.some((line) => line.includes('kind="timeout"'))).toBe(true);
      const drainingStream = await runtime.publicApp.fetch(
        new Request("http://gateway.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "hardening-sse-only",
            stream: true,
            messages: [{ role: "user", content: "in flight at shutdown" }],
            max_tokens: 50,
          }),
        }),
      );
      const drainingReader = drainingStream.body!.getReader();
      expect(new TextDecoder().decode((await drainingReader.read()).value)).toContain("partial");
      const draining = runtime.lifecycle.drain(0);
      expect(new TextDecoder().decode((await drainingReader.read()).value)).toContain(
        "event: error",
      );
      expect((await drainingReader.read()).done).toBe(true);
      expect(await draining).toBe(true);
      expect(await streamFailureMetrics()).toEqual(beforeDrain);
      const stateAfterDrain = await db`SELECT state FROM public.billing_holds
      WHERE request_id = ${drainingStream.headers.get("x-request-id")}`;
      expect(stateAfterDrain[0]?.state).toBe("UNKNOWN");
      await internalCall(
        runtime.internalApp,
        "POST",
        `/internal/v1/requests/${drainingStream.headers.get("x-request-id")}/resolve`,
        {
          action: "RELEASE",
          reason: "shutdown_recovery",
        },
      );
    } finally {
      for (const t of openTimers) clearTimeout(t);
      openTimers.clear();
      await runtime.close();
      await primaryServer.stop(true);
      await backupServer.stop(true);
    }
  });
}
