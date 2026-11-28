import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createLedgerQueries } from "@gateway/ledger";
import { localInternalCredentials } from "../../scripts/local-internal-auth.ts";
import { db, internalCall, makeConfig, OPERATOR_TOKEN } from "./gateway-fixture.ts";

async function adminCall(
  runtime: ReturnType<typeof createGatewayRuntime>,
  method: string,
  path: string,
  body?: unknown,
  expectedStatus?: number,
  token = OPERATOR_TOKEN,
) {
  const res =
    token === OPERATOR_TOKEN
      ? await internalCall(runtime.internalApp, method, path, body)
      : await (async () => {
          const r = await runtime.internalApp.fetch(
            new Request(`http://internal.local${path}`, {
              method,
              headers: {
                authorization: `Bearer ${token}`,
                "content-type": "application/json",
              },
              ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
            }),
          );
          return { status: r.status, data: (await r.json()) as Record<string, unknown> };
        })();
  const want =
    expectedStatus ??
    (method === "POST" && (path === "/internal/v1/prices" || path === "/internal/v1/fx-rates")
      ? 201
      : 200);
  expect(res.status, `${method} ${path} -> ${JSON.stringify(res.data)}`).toBe(want);
  return res.data;
}

export function registerStandardizedPricingFailoverRbacCases() {
  test("Standardized pricing E2E: failover with cross-currency supplier EXACT_RATE_CARD, finance summary FX normalization, FX conflict, and incomplete non-token receipt REVIEW", async () => {
    let failResponses = false;
    const upstreamA = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        await req.text();
        return new Response("down", { status: 500, headers: { "x-request-id": "fail-route-a" } });
      },
    });
    const upstreamB = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        const path = new URL(req.url).pathname;
        await req.text();
        if (path === "/v1/responses" && failResponses) {
          return new Response("ambiguous", {
            status: 500,
            headers: { "x-request-id": "prov-ws-unknown-1" },
          });
        }
        return Response.json(
          {
            id: "chatcmpl-b",
            object: "chat.completion",
            model: "gpt-fo",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
            usage: { prompt_tokens: 100, completion_tokens: 50 },
          },
          { headers: { "x-request-id": "prov-b-ok" } },
        );
      },
    });

    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    const queries = createLedgerQueries(db);
    try {
      await runtime.start();
      const fxTime = "2024-06-01T00:00:00Z";
      await adminCall(runtime, "POST", "/internal/v1/fx-rates", {
        id: crypto.randomUUID(),
        base_currency: "USD",
        quote_currency: "CNY",
        rate_numerator: "725",
        rate_denominator: "100",
        effective_from: fxTime,
      });
      await adminCall(runtime, "POST", "/internal/v1/fx-rates", {
        id: crypto.randomUUID(),
        base_currency: "EUR",
        quote_currency: "USD",
        rate_numerator: "110",
        rate_denominator: "100",
        effective_from: fxTime,
      });
      await adminCall(
        runtime,
        "POST",
        "/internal/v1/fx-rates",
        {
          id: crypto.randomUUID(),
          base_currency: "USD",
          quote_currency: "CNY",
          rate_numerator: "730",
          rate_denominator: "100",
          effective_from: fxTime,
        },
        409,
      );

      const cnyAcct = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "fo-cny-acct",
        currency: "CNY",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${cnyAcct.id}/credits`, {
        currency: "CNY",
        external_ref: "fo-cny-fund",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: cnyAcct.id,
        name: "fo-cny-key",
      });
      const supA = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `fo-sup-a-${crypto.randomUUID()}`,
      });
      const supB = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `fo-sup-b-${crypto.randomUUID()}`,
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${supA.id}/recharges`, {
        currency: "USD",
        external_ref: "fo-sup-a-usd",
        amount_atoms: "100000000",
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${supB.id}/recharges`, {
        currency: "USD",
        external_ref: "fo-sup-b-usd",
        amount_atoms: "100000000",
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${supB.id}/recharges`, {
        currency: "EUR",
        external_ref: "fo-sup-b-eur",
        amount_atoms: "100000000",
      });

      const chanA = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: supA.id,
        kind: "openai",
        name: "fo-chan-a",
        base_url: upstreamA.url.origin,
        api_key: "sk-a",
      });
      const chanB = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: supB.id,
        kind: "openai",
        name: "fo-chan-b",
        base_url: upstreamB.url.origin,
        api_key: "sk-b",
        adapter_config: {
          reconciliation_evidence_required: true,
          reconciliation_response_id_header: "x-request-id",
        },
      });
      for (const [name, ops] of [
        ["gpt-fo", ["chat"]],
        ["gpt-fx-summary", ["chat"]],
        ["gpt-ws-recon", ["responses"]],
      ] as const) {
        await adminCall(runtime, "PUT", "/internal/v1/models", {
          name,
          operations: [...ops],
          max_output_tokens: 1000,
        });
      }

      const custUsd = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      const supUsd = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "3", denominator: "1" },
          output_tokens: { numerator: "6", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      const supEur = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "EUR",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });

      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-fo",
        channel_id: chanA.id,
        upstream_model: "gpt-fo",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custUsd.id,
        cost_version_id: supUsd.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-fo",
        channel_id: chanB.id,
        upstream_model: "gpt-fo",
        priority: 2,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custUsd.id,
        cost_version_id: supUsd.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        layer: "COMMERCIAL",
        kind: "EXACT_RATE_CARD",
        stacking: "EXCLUSIVE",
        priority: 10,
        match: { side: "SUPPLIER", model: "gpt-fo", channel_id: chanB.id },
        rate_card_price_version_id: supEur.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-fx-summary",
        channel_id: chanB.id,
        upstream_model: "gpt-fx-summary",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custUsd.id,
        cost_version_id: supUsd.id,
      });

      const foRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-fo",
            messages: [{ role: "user", content: "failover" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(foRes.status).toBe(200);
      const foBilling = await queries.getRequestBilling(foRes.headers.get("x-request-id")!);
      expect(foBilling.charge?.supplierCurrency).toBe("USD");
      expect(foBilling.charge?.supplierCostAtoms).toBe("440");
      expect(foBilling.charge?.pricingSnapshot?.supplier?.base_currency).toBe("EUR");
      expect(foBilling.charge?.pricingSnapshot?.supplier?.rate_card_id).toBe(supEur.id as string);
      expect(foBilling.charge?.customerAtoms).toBe("14500");

      const sumRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-fx-summary",
            messages: [{ role: "user", content: "summary" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(sumRes.status).toBe(200);
      const summary = await queries.getFinanceSummary({
        billing_account_id: cnyAcct.id as string,
      });
      const fxModelRow = summary.by_model.find((row) => row.key === "gpt-fx-summary");
      expect(fxModelRow?.currency).toBe("CNY");
      expect(fxModelRow?.customer_billed_atoms).toBe("14500");
      expect(fxModelRow?.supplier_cost_atoms).toBe("4350");
      expect(fxModelRow?.gross_margin_atoms).toBe("10150");

      const wsCustRecon = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
          web_search_calls: { numerator: "500", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "5" },
      });
      const wsSupReconEur = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "EUR",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
          web_search_calls: { numerator: "150", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000", web_search_calls: "5" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-ws-recon",
        channel_id: chanB.id,
        upstream_model: "gpt-ws-recon",
        priority: 1,
        weight: 1,
        operations: ["responses"],
        billing_price_id: wsCustRecon.id,
        cost_version_id: supUsd.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        layer: "COMMERCIAL",
        kind: "EXACT_RATE_CARD",
        stacking: "EXCLUSIVE",
        priority: 10,
        match: { side: "SUPPLIER", model: "gpt-ws-recon", channel_id: chanB.id },
        rate_card_price_version_id: wsSupReconEur.id,
      });

      failResponses = true;
      const unkRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/responses", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-ws-recon",
            input: "search unknown",
            tools: [{ type: "web_search" }],
            max_output_tokens: 100,
          }),
        }),
      );
      expect(unkRes.status).toBe(502);
      const unkReqId = unkRes.headers.get("x-request-id")!;
      const unkBilling = await queries.getRequestBilling(unkReqId);
      const attemptId = unkBilling.hold!.supplierHolds[0]!.attempt_id;

      await runtime.ledger.submitReconciliationReceipt({
        attempt_id: attemptId,
        request_id: unkReqId,
        account_id: supB.id as string,
        channel_id: chanB.id as string,
        currency: "USD",
        provider_request_id: "prov-ws-unknown-1",
        evidence_ref: "ev-ws-incomplete-1",
        actor: "auditor-1",
        verdict: "FINAL",
        supplier_cost_atoms: "800",
        customer_usage: { input_tokens: "100", output_tokens: "20" },
      });
      await runtime.ledger.processReconciliation(10);
      const [reviewRow] = await db<
        { state: string; result: string | null }[]
      >`SELECT state, result FROM public.billing_reconciliation_receipts WHERE attempt_id = ${attemptId}::uuid`;
      expect(reviewRow).toEqual({ state: "REVIEW", result: "incomplete_customer_usage" });

      await runtime.ledger.reviewReconciliationReceipt({
        attempt_id: attemptId,
        action: "RESUME",
        review_evidence_ref: "rev-ws-complete-1",
        reason: "added missing web_search_calls",
        actor: "finance-1",
        customer_usage: { input_tokens: "100", output_tokens: "20", web_search_calls: "1" },
      });
      await runtime.ledger.processReconciliation(10);
      const settledRecon = await queries.getRequestBilling(unkReqId);
      expect(settledRecon.charge?.outcome).toBe("UNKNOWN_RESOLVED");
      expect(settledRecon.charge?.customerAtoms).toBe("13775");

      const matrixCustPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {},
        ceilings: { images: "4" },
        pricing_spec: {
          matrix: [
            {
              match: { size: "1024x1024" },
              rates: { images: { numerator: "400", denominator: "1" } },
            },
          ],
        },
      });
      const legacyNoSnapReqId = crypto.randomUUID();
      const legacyNoSnapAttemptId = crypto.randomUUID();
      await runtime.ledger.acquireHold({
        request_id: legacyNoSnapReqId,
        attempt_id: legacyNoSnapAttemptId,
        billing_account_id: cnyAcct.id as string,
        api_key_id: key.id as string,
        currency: "CNY",
        operation: "images",
        model: "img-matrix",
        idempotency_key: null,
        fingerprint: "",
        customer_atoms: "1600",
        supplier_account_id: supB.id as string,
        supplier_cost_version_id: supUsd.id as string,
        customer_price_version_id: matrixCustPrice.id as string,
        supplier_channel_id: chanB.id as string,
        supplier_currency: "USD",
        supplier_atoms: "400",
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      });
      await runtime.ledger.markUnknown(legacyNoSnapReqId);
      await expect(
        runtime.ledger.resolveRequest({
          request_id: legacyNoSnapReqId,
          action: "SETTLE",
          billing_price_id: matrixCustPrice.id as string,
          usage: { images: "1" },
          supplier_cost_atoms: "100",
          actor: "ops-1",
          reason: "test fail-closed",
        }),
      ).rejects.toThrow("customer_pricing_snapshot");
      await runtime.ledger.resolveRequest({
        request_id: legacyNoSnapReqId,
        action: "RELEASE",
        actor: "ops-1",
        reason: "cleanup",
      });

      const audit = await runtime.ledger.auditConservation();
      expect(audit.ok, JSON.stringify(audit)).toBe(true);
    } finally {
      await runtime.close();
      upstreamA.stop(true);
      upstreamB.stop(true);
    }
  });

  test("Standardized pricing E2E: legacy override vs EXCLUSIVE, flat-route fast-path preservation, RBAC separation, and API validation guards", async () => {
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        await req.text();
        return Response.json({
          id: "chatcmpl-rbac",
          object: "chat.completion",
          model: "gpt-excl",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 100, completion_tokens: 0 },
        });
      },
    });
    const catalogOnlyToken = "e2e-catalog-only-token-0123456789abcdef0000";
    const runtime = createGatewayRuntime({
      config: makeConfig({
        internalCredentials: [
          ...localInternalCredentials(OPERATOR_TOKEN),
          {
            id: "catalog-only",
            token_sha256: new Bun.CryptoHasher("sha256").update(catalogOnlyToken).digest("hex"),
            roles: ["catalog"],
            consumers: ["default"],
          },
        ],
      }),
      db,
    });
    const queries = createLedgerQueries(db);
    try {
      await runtime.start();
      await adminCall(
        runtime,
        "PUT",
        "/internal/v1/billing-accounts",
        { name: "rbac-denied", currency: "USD", customer_group_id: "vip" },
        403,
        catalogOnlyToken,
      );
      await adminCall(
        runtime,
        "PUT",
        "/internal/v1/billing-accounts",
        { name: "rbac-allowed", currency: "USD" },
        200,
        catalogOnlyToken,
      );
      await adminCall(runtime, "GET", "/internal/v1/pricing-modifiers?bogus=1", undefined, 400);
      await adminCall(runtime, "GET", "/internal/v1/pricing-modifiers?layer=NOPE", undefined, 400);
      await adminCall(
        runtime,
        "GET",
        "/internal/v1/pricing-modifiers?enabled=maybe",
        undefined,
        400,
      );
      await adminCall(runtime, "GET", "/internal/v1/fx-rates?base_currency=bad", undefined, 400);
      await adminCall(
        runtime,
        "POST",
        "/internal/v1/prices",
        { side: "CUSTOMER", currency: "USD", rates: {}, pricing_spec: { min_charge_atoms: "100" } },
        400,
      );
      const decPrice = await adminCall(
        runtime,
        "POST",
        "/internal/v1/prices",
        {
          side: "CUSTOMER",
          currency: "USD",
          rates: { video_seconds: { numerator: "100", denominator: "1" } },
          ceilings: { video_seconds: "12.5" },
        },
        201,
      );
      expect(decPrice.ceilings).toEqual({ video_seconds: "12.5" });

      const custPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "10", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      const supPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      });
      await adminCall(
        runtime,
        "PUT",
        "/internal/v1/pricing-modifiers",
        {
          layer: "COMMERCIAL",
          kind: "EXACT_RATE_CARD",
          stacking: "BEST_FOR_CUSTOMER",
          priority: 1,
          match: { side: "CUSTOMER", model: "gpt-excl" },
          rate_card_price_version_id: custPrice.id,
        },
        400,
      );

      const acct = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "excl-acct",
        currency: "USD",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${acct.id}/credits`, {
        currency: "USD",
        external_ref: "excl-fund",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: acct.id,
        name: "excl-key",
      });
      const sup = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `excl-sup-${crypto.randomUUID()}`,
      });
      await adminCall(runtime, "POST", `/internal/v1/upstream-accounts/${sup.id}/recharges`, {
        currency: "USD",
        external_ref: "excl-sup-fund",
        amount_atoms: "100000000",
      });
      const chan = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: sup.id,
        kind: "openai",
        name: "excl-chan",
        base_url: upstream.url.origin,
        api_key: "sk-excl",
      });
      await adminCall(runtime, "PUT", "/internal/v1/models", {
        name: "gpt-excl",
        operations: ["chat"],
        max_output_tokens: 1000,
      });
      await adminCall(runtime, "PUT", "/internal/v1/models", {
        name: "gpt-flat-fast",
        operations: ["chat"],
        max_output_tokens: 1000,
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-excl",
        channel_id: chan.id,
        upstream_model: "gpt-excl",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-flat-fast",
        channel_id: chan.id,
        upstream_model: "gpt-flat-fast",
        priority: 1,
        weight: 1,
        operations: ["chat"],
        billing_price_id: custPrice.id,
        cost_version_id: supPrice.id,
      });

      await adminCall(runtime, "PUT", "/internal/v1/pricing-overrides", {
        billing_account_id: acct.id,
        model_name: "gpt-excl",
        operation: "chat",
        override_kind: "MULTIPLIER",
        multiplier_numerator: "90",
        multiplier_denominator: "100",
        enabled: true,
      });
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "EXCLUSIVE",
        priority: 10,
        match: { side: "CUSTOMER", model: "gpt-excl", billing_account_id: acct.id },
        multiplier_numerator: "70",
        multiplier_denominator: "100",
      });

      const exclRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-excl",
            messages: [{ role: "user", content: "exclusive" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(exclRes.status).toBe(200);
      const exclBilling = await queries.getRequestBilling(exclRes.headers.get("x-request-id")!);
      expect(exclBilling.charge?.customerAtoms).toBe("700");
      expect(exclBilling.charge?.pricingOverrideId).toBeNull();
      expect(exclBilling.charge?.discountNumerator).toBeNull();
      expect(exclBilling.charge?.discountDenominator).toBeNull();

      const flatRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-flat-fast",
            messages: [{ role: "user", content: "fast path" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(flatRes.status).toBe(200);
      const flatBilling = await queries.getRequestBilling(flatRes.headers.get("x-request-id")!);
      expect(flatBilling.charge?.customerAtoms).toBe("1000");
      expect(flatBilling.charge?.pricingSnapshot).toBeNull();
    } finally {
      await runtime.close();
      upstream.stop(true);
    }
  });
}
