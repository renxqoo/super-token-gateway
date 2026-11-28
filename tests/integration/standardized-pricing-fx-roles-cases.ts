import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createDatabase, migrate } from "@gateway/ledger";
import { db, internalCall, makeConfig } from "./gateway-fixture.ts";

async function adminCall(
  runtime: ReturnType<typeof createGatewayRuntime>,
  method: string,
  path: string,
  body?: unknown,
  expectedStatus?: number,
) {
  const res = await internalCall(runtime.internalApp, method, path, body);
  const want =
    expectedStatus ??
    (method === "POST" && (path === "/internal/v1/prices" || path === "/internal/v1/fx-rates")
      ? 201
      : 200);
  expect(res.status, `${method} ${path} -> ${JSON.stringify(res.data)}`).toBe(want);
  return res.data;
}

export function registerStandardizedPricingFxRolesCases() {
  test("Standardized pricing E2E: FX USD->CNY + immutable FX replay/conflict + UNKNOWN reconciliation snapshot freeze", async () => {
    let failUpstream = false;
    const upstream = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      async fetch(req) {
        await req.text();
        if (failUpstream) {
          return new Response("upstream broke", {
            status: 500,
            headers: { "x-request-id": "prov-req-fx-unknown" },
          });
        }
        return Response.json(
          {
            id: "chatcmpl-fx",
            object: "chat.completion",
            model: "gpt-fx",
            choices: [
              { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
            ],
            usage: { prompt_tokens: 100, completion_tokens: 50 },
          },
          { headers: { "x-request-id": "prov-req-fx-ok" } },
        );
      },
    });

    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const cnyAccount = await adminCall(runtime, "PUT", "/internal/v1/billing-accounts", {
        name: "cny-fx-account",
        currency: "CNY",
        rpm: 1000,
        tpm: 10_000_000,
        concurrency: 50,
      });
      await adminCall(runtime, "POST", `/internal/v1/wallets/${cnyAccount.id}/credits`, {
        currency: "CNY",
        external_ref: "cny-fund-1",
        amount_atoms: "100000000",
        kind: "CASH",
      });
      const key = await adminCall(runtime, "PUT", "/internal/v1/api-keys", {
        billing_account_id: cnyAccount.id,
        name: "cny-key",
      });
      const supAccount = await adminCall(runtime, "PUT", "/internal/v1/upstream-accounts", {
        name: `fx-sup-${crypto.randomUUID()}`,
      });
      await adminCall(
        runtime,
        "POST",
        `/internal/v1/upstream-accounts/${supAccount.id}/recharges`,
        {
          currency: "USD",
          external_ref: "fx-sup-fund-1",
          amount_atoms: "100000000",
        },
      );
      const channel = await adminCall(runtime, "PUT", "/internal/v1/channels", {
        account_id: supAccount.id,
        kind: "openai",
        name: "fx-chan",
        base_url: upstream.url.origin,
        api_key: "sk-fx",
        adapter_config: {
          reconciliation_response_id_header: "x-request-id",
        },
      });
      await adminCall(runtime, "PUT", "/internal/v1/models", {
        name: "gpt-fx",
        operations: ["chat"],
        max_output_tokens: 4096,
      });
      const usdCustPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "4096" },
      });
      const usdSupPrice = await adminCall(runtime, "POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "5", denominator: "1" },
          output_tokens: { numerator: "10", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "4096" },
      });
      await adminCall(runtime, "PUT", "/internal/v1/routes", {
        model_name: "gpt-fx",
        channel_id: channel.id,
        upstream_model: "gpt-fx",
        operations: ["chat"],
        billing_price_id: usdCustPrice.id,
        cost_version_id: usdSupPrice.id,
        retry_policy: { max_attempts: 1 },
      });

      const fxId = crypto.randomUUID();
      const fxCreated = await adminCall(runtime, "POST", "/internal/v1/fx-rates", {
        id: fxId,
        base_currency: "USD",
        quote_currency: "CNY",
        rate_numerator: "725",
        rate_denominator: "100",
        effective_from: "2020-01-01T00:00:00.000Z",
      });
      expect(fxCreated.replayed).toBe(false);
      const fxReplay = await adminCall(
        runtime,
        "POST",
        "/internal/v1/fx-rates",
        {
          id: fxId,
          base_currency: "USD",
          quote_currency: "CNY",
          rate_numerator: "725",
          rate_denominator: "100",
          effective_from: "2020-01-01T00:00:00.000Z",
        },
        200,
      );
      expect(fxReplay.replayed).toBe(true);
      await adminCall(
        runtime,
        "POST",
        "/internal/v1/fx-rates",
        {
          id: fxId,
          base_currency: "USD",
          quote_currency: "CNY",
          rate_numerator: "730",
          rate_denominator: "100",
          effective_from: "2020-01-01T00:00:00.000Z",
        },
        409,
      );

      const modId = crypto.randomUUID();
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        id: modId,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 1,
        match: { side: "CUSTOMER", model: "gpt-fx", billing_account_id: cnyAccount.id },
        multiplier_numerator: "80",
        multiplier_denominator: "100",
      });

      const okRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-fx",
            messages: [{ role: "user", content: "fx live test" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(okRes.status).toBe(200);
      const okReqId = okRes.headers.get("x-request-id")!;

      const [okCharge] = await db<
        {
          currency: string;
          customer_atoms: string;
          supplier_cost_atoms: string;
          pricing_snapshot: {
            customer?: { fx_version_id: string | null; currency: string };
          } | null;
        }[]
      >`
        SELECT currency, customer_atoms::text, supplier_cost_atoms::text, pricing_snapshot
        FROM public.billing_charges
        WHERE request_id = ${okReqId}::uuid
      `;
      expect(okCharge!.currency).toBe("CNY");
      expect(okCharge!.customer_atoms).toBe("11600");
      expect(okCharge!.supplier_cost_atoms).toBe("1000");
      expect(okCharge!.pricing_snapshot?.customer?.fx_version_id).toBe(fxId);

      failUpstream = true;
      const unknownRes = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-fx",
            messages: [{ role: "user", content: "fx unknown hold" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(unknownRes.status).toBe(502);
      const unknownReqId = unknownRes.headers.get("x-request-id")!;

      const [unknownHold] = await db<
        { state: string; supplier_holds: { attempt_id: string; provider_request_id?: string }[] }[]
      >`
        SELECT state, supplier_holds FROM public.billing_holds WHERE request_id = ${unknownReqId}::uuid
      `;
      expect(unknownHold!.state).toBe("UNKNOWN");
      const attemptId = unknownHold!.supplier_holds[0]!.attempt_id;

      const unknownRes2 = await runtime.publicApp.fetch(
        new Request("http://public.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "gpt-fx",
            messages: [{ role: "user", content: "fx manual resolve hold" }],
            max_tokens: 100,
          }),
        }),
      );
      expect(unknownRes2.status).toBe(502);
      const unknownReqId2 = unknownRes2.headers.get("x-request-id")!;

      await adminCall(runtime, "POST", "/internal/v1/fx-rates", {
        base_currency: "USD",
        quote_currency: "CNY",
        rate_numerator: "900",
        rate_denominator: "100",
        effective_from: "2021-01-01T00:00:00.000Z",
      });
      await adminCall(runtime, "PUT", "/internal/v1/pricing-modifiers", {
        id: modId,
        layer: "COMMERCIAL",
        kind: "MULTIPLIER",
        stacking: "MULTIPLY",
        priority: 1,
        match: { side: "CUSTOMER", model: "gpt-fx", billing_account_id: cnyAccount.id },
        multiplier_numerator: "50",
        multiplier_denominator: "100",
      });

      const receiptRes = await runtime.ledger.submitReconciliationReceipt({
        request_id: unknownReqId,
        attempt_id: attemptId,
        account_id: supAccount.id as string,
        channel_id: channel.id as string,
        currency: "USD",
        provider_request_id: "prov-req-fx-unknown",
        evidence_ref: "ev-fx-1",
        actor: "reconciler",
        verdict: "FINAL",
        supplier_cost_atoms: "1000",
        customer_usage: { input_tokens: "100", output_tokens: "50" },
      });
      expect(receiptRes.status).toBe("accepted");
      const processed = await runtime.ledger.processReconciliation(10);
      expect(processed.receipts).toBeGreaterThanOrEqual(1);

      const manualRes = await runtime.ledger.resolveRequest({
        request_id: unknownReqId2,
        action: "SETTLE",
        billing_price_id: usdCustPrice.id as string,
        usage: { input_tokens: "100", output_tokens: "50" },
        supplier_cost_atoms: "1000",
        reason: "manual settlement with frozen snapshot",
        actor: "ops",
      });
      expect(manualRes.state).toBe("UNKNOWN_RESOLVED");

      const reconciledCharges = await db<
        {
          request_id: string;
          currency: string;
          customer_atoms: string;
          outcome: string;
          pricing_snapshot: { customer?: { fx_version_id: string | null } } | null;
        }[]
      >`
        SELECT request_id::text, currency, customer_atoms::text, outcome, pricing_snapshot
        FROM public.billing_charges
        WHERE request_id = ANY(${[unknownReqId, unknownReqId2]}::uuid[])
      `;
      const byReq = new Map(reconciledCharges.map((r) => [r.request_id, r]));
      expect(byReq.get(unknownReqId)!.currency).toBe("CNY");
      expect(byReq.get(unknownReqId)!.outcome).toBe("UNKNOWN_RESOLVED");
      expect(byReq.get(unknownReqId)!.customer_atoms).toBe("11600");
      expect(byReq.get(unknownReqId)!.pricing_snapshot?.customer?.fx_version_id).toBe(fxId);
      expect(byReq.get(unknownReqId2)!.currency).toBe("CNY");
      expect(byReq.get(unknownReqId2)!.outcome).toBe("UNKNOWN_RESOLVED");
      expect(byReq.get(unknownReqId2)!.customer_atoms).toBe("11600");
      expect(byReq.get(unknownReqId2)!.pricing_snapshot?.customer?.fx_version_id).toBe(fxId);

      const audit = await runtime.ledger.auditConservation();
      expect(audit.ok, JSON.stringify(audit)).toBe(true);
    } finally {
      await runtime.close();
      upstream.stop(true);
    }
  });

  test("Standardized pricing least-privilege roles.sql enforces read/write/immutability boundaries", async () => {
    const baseDbUrl = process.env.DATABASE_URL!;
    const roleDbName = `gateway_v4_roles_pricing_${crypto.randomUUID().replaceAll("-", "").slice(0, 10)}`;
    const adminUrl = new URL(baseDbUrl);
    adminUrl.pathname = "/postgres";
    const targetUrl = new URL(baseDbUrl);
    targetUrl.pathname = `/${roleDbName}`;

    const owner = createDatabase(adminUrl.toString(), 1);
    await owner.unsafe(`CREATE DATABASE ${roleDbName}`);
    const roleDb = createDatabase(targetUrl.toString(), 1);
    try {
      await migrate(roleDb);
      const rolesSqlRaw = await Bun.file(
        new URL("../../infra/postgres/roles.sql", import.meta.url),
      ).text();
      const cleaned = rolesSqlRaw
        .replace(/^\\set .*$/gm, "")
        .replace(/^SELECT format\(.*\\gexec$/gm, "");
      await roleDb.unsafe(cleaned);
      await roleDb.unsafe(`REVOKE ALL ON DATABASE ${roleDbName} FROM PUBLIC`);
      await roleDb.unsafe(
        `GRANT CONNECT ON DATABASE ${roleDbName} TO stg_data, stg_control, stg_worker, stg_audit`,
      );

      const fxId = crypto.randomUUID();
      await roleDb.begin(async (tx) => {
        await tx.unsafe("SET LOCAL ROLE stg_control");
        await tx`
          INSERT INTO public.fx_rate_versions (
            id, base_currency, quote_currency, rate_numerator, rate_denominator, effective_from, source_digest
          ) VALUES (
            ${fxId}, 'USD', 'EUR', 92, 100, '2025-01-01T00:00:00Z'::timestamptz, 'digest-1'
          )
        `;
      });

      let updateRejected = false;
      try {
        await roleDb.begin(async (tx) => {
          await tx.unsafe("SET LOCAL ROLE stg_control");
          await tx`UPDATE public.fx_rate_versions SET rate_numerator = 95 WHERE id = ${fxId}`;
        });
      } catch {
        updateRejected = true;
      }
      expect(updateRejected).toBe(true);

      let deleteRejected = false;
      try {
        await roleDb.begin(async (tx) => {
          await tx.unsafe("SET LOCAL ROLE stg_control");
          await tx`DELETE FROM public.fx_rate_versions WHERE id = ${fxId}`;
        });
      } catch {
        deleteRejected = true;
      }
      expect(deleteRejected).toBe(true);

      let dataInsertRejected = false;
      try {
        await roleDb.begin(async (tx) => {
          await tx.unsafe("SET LOCAL ROLE stg_data");
          await tx`
            INSERT INTO public.fx_rate_versions (
              id, base_currency, quote_currency, rate_numerator, rate_denominator, effective_from, source_digest
            ) VALUES (
              ${crypto.randomUUID()}, 'USD', 'JPY', 150, 1, '2025-01-01T00:00:00Z'::timestamptz, 'digest-2'
            )
          `;
        });
      } catch {
        dataInsertRejected = true;
      }
      expect(dataInsertRejected).toBe(true);

      let workerInsertRejected = false;
      try {
        await roleDb.begin(async (tx) => {
          await tx.unsafe("SET LOCAL ROLE stg_worker");
          await tx`
            INSERT INTO public.pricing_modifiers (
              id, layer, kind, stacking, priority, enabled, match, multiplier_numerator, multiplier_denominator, source_digest
            ) VALUES (
              ${crypto.randomUUID()}, 'PRODUCT', 'MULTIPLIER', 'MULTIPLY', 1, true, '{}'::jsonb, 1, 1, 'd-w'
            )
          `;
        });
      } catch {
        workerInsertRejected = true;
      }
      expect(workerInsertRejected).toBe(true);

      let auditInsertRejected = false;
      try {
        await roleDb.begin(async (tx) => {
          await tx.unsafe("SET LOCAL ROLE stg_audit");
          await tx`
            INSERT INTO public.pricing_modifiers (
              id, layer, kind, stacking, priority, enabled, match, multiplier_numerator, multiplier_denominator, source_digest
            ) VALUES (
              ${crypto.randomUUID()}, 'PRODUCT', 'MULTIPLIER', 'MULTIPLY', 1, true, '{}'::jsonb, 1, 1, 'd-a'
            )
          `;
        });
      } catch {
        auditInsertRejected = true;
      }
      expect(auditInsertRejected).toBe(true);

      const dataRead = await roleDb.begin(async (tx) => {
        await tx.unsafe("SET LOCAL ROLE stg_data");
        return tx`SELECT count(*)::int AS n FROM public.fx_rate_versions`;
      });
      expect(Number(dataRead[0]!.n)).toBe(1);

      const workerRead = await roleDb.begin(async (tx) => {
        await tx.unsafe("SET LOCAL ROLE stg_worker");
        return tx`SELECT count(*)::int AS n FROM public.fx_rate_versions`;
      });
      expect(Number(workerRead[0]!.n)).toBe(1);

      const auditReads = await roleDb.begin(async (tx) => {
        await tx.unsafe("SET LOCAL ROLE stg_audit");
        const [mods] = await tx`SELECT count(*)::int AS n FROM public.pricing_modifiers`;
        const [prices] = await tx`SELECT count(*)::int AS n FROM public.price_versions`;
        const [fx] = await tx`SELECT count(*)::int AS n FROM public.fx_rate_versions`;
        return { mods: Number(mods!.n), prices: Number(prices!.n), fx: Number(fx!.n) };
      });
      expect(auditReads).toEqual({ mods: 0, prices: 0, fx: 1 });
    } finally {
      await roleDb.close({ timeout: 1 });
      await owner.unsafe(`DROP DATABASE IF EXISTS ${roleDbName} WITH (FORCE)`);
      await owner.close({ timeout: 1 });
    }
  });
}
