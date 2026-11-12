import { financialRejection } from "../support/financial-rejection.ts";
import type { ExecutionReply } from "@gateway/contracts";
import { expect, test } from "bun:test";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";

test(
  "route PATCH stores a new complete ceiling version and overflow admission records a stable rejection without money",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        route = setup.route.id as string,
        before = (await db`SELECT pricing_id FROM routes WHERE id=${route}`)[0].pricing_id;
      const maximum = "99999999999999999999999999999999999999";
      const patch = await f.request(
        `/routes/${route}`,
        {
          pricing: {
            currency: "USD",
            rates: { input_tokens: { numerator: maximum, denominator: "1" } },
          },
          ceilings: { input_tokens: "100" },
        },
        undefined,
        "PATCH",
      );
      expect(patch.status).toBe(200);
      const updated = (await db`SELECT pricing_id FROM routes WHERE id=${route}`)[0].pricing_id;
      expect(updated).not.toBe(before);
      expect((await db`SELECT ceilings FROM cost_versions WHERE id=${before}`)[0].ceilings).toEqual(
        { input_tokens: "100", output_tokens: "50" },
      );
      expect(
        (await db`SELECT ceilings FROM cost_versions WHERE id=${updated}`)[0].ceilings,
      ).toEqual({ input_tokens: "100" });
      const client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client),
        result = await financialRejection(prepared.admit(), 422);
      expect(result.receipt).toEqual({ command_state: "REJECTED", code: "amount_overflow" });
      expect(await financialRejection(prepared.admit(), 422)).toEqual(result);
      expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(0);
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("0");
      const missing = await f.request(
        `/routes/${route}`,
        {
          pricing: {
            currency: "USD",
            rates: { input_tokens: { numerator: "0", denominator: "1" } },
          },
        },
        undefined,
        "PATCH",
      );
      expect(missing.status).toBe(400);
    }),
  30000,
);

test(
  "a manifest over the current model output cap is rejected before either side reserves funds",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap("USD", "1000", 10),
        owner = f.client();
      expect(
        (await db`SELECT max_output_tokens FROM models WHERE name=${setup.model}`)[0]
          .max_output_tokens,
      ).toBe(10);
      await owner.startup();
      const prepared = await prepareFinancialAdmission(setup, owner),
        result = await financialRejection(prepared.admit(), 422);
      expect(result.receipt).toMatchObject({ command_state: "REJECTED", code: "output_bound" });
      expect((await db`SELECT count(*)::integer n FROM attempts`)[0].n).toBe(0);
      expect(
        (await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`)[0]
          .reserved_atoms,
      ).toBe("0");
    }),
  30000,
);

test(
  "character bounds never become exact customer charges and exact preflight quantities narrow the supplier reserve",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      const tariff = await f.request("/billing/tariffs", {
        model: setup.model,
        operation: "chat",
        provider_id: setup.provider.id,
        pricing: { currency: "USD", rates: { characters: { numerator: "2", denominator: "1" } } },
        ceilings: { characters: "100" },
        basis: "INPUT_CHARACTERS",
      });
      expect(tariff.status).toBe(201);
      await client.startup();
      const missing = await prepareFinancialAdmission(setup, client, {
        ceilings: { input_tokens: "60", output_tokens: "30", characters: "60" },
      });
      expect((await financialRejection(missing.admit(), 422)).receipt).toEqual({
        command_state: "REJECTED",
        code: "invalid_request",
      });
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(0);
      const excessive = await prepareFinancialAdmission(setup, client, {
        ceilings: { input_tokens: "60", output_tokens: "30" },
        quantities: { input_tokens: "61" },
        inputCharacters: "3",
      });
      expect((await financialRejection(excessive.admit(), 422)).receipt).toEqual({
        command_state: "REJECTED",
        code: "cost_ceiling",
      });
      const exact = await prepareFinancialAdmission(setup, client, {
          ceilings: { input_tokens: "60", output_tokens: "30", characters: "60" },
          quantities: { input_tokens: "31" },
          inputCharacters: "3",
        }),
        result = await exact.admit();
      expect(result.receipt).toMatchObject({
        command_state: "COMMITTED",
        customer_atoms: "6",
        supplier_atoms: "21",
      });
      expect(
        (
          await db`SELECT billing_quote->>'input_characters' AS input_characters FROM gateway_requests WHERE id=${exact.requestId}`
        )[0].input_characters,
      ).toBe("3");
      expect(
        (
          await db`SELECT estimated_quantities FROM upstream_holds WHERE attempt_id=${exact.attemptId}`
        )[0].estimated_quantities,
      ).toEqual({ input_tokens: "31", output_tokens: "30" });
      const begin = (await client.execution("execution_begin_v1", exact.beginData(result.receipt), {
        deadlineAt: exact.deadline,
      })) as ExecutionReply;
      expect(begin.transient_send?.auth_material.type).toBe("bearer");
      const retry = await prepareFinancialAdmission(setup, client, {
        requestId: exact.requestId,
        deadline: exact.deadline,
        ceilings: { input_tokens: "60", output_tokens: "30", characters: "60" },
        inputCharacters: "1",
      });
      const {
        project_id: _project,
        key_id: _key,
        intent_digest: _intent,
        fingerprint: _fingerprint,
        customer_currency: _currency,
        ...dispatch
      } = retry.data;
      const next = (await client.execution(
        "execution_next_v1",
        {
          ...dispatch,
          request_id: exact.requestId,
          previous_attempt_id: exact.attemptId,
          expected_ordinal: 2,
          visible_output: false,
        },
        { deadlineAt: exact.deadline },
      )) as ExecutionReply;
      expect(next.receipt).toMatchObject({ command_state: "COMMITTED", customer_atoms: "6" });
      expect(
        (
          await db`SELECT billing_quote->>'input_characters' AS n FROM gateway_requests WHERE id=${exact.requestId}`
        )[0].n,
      ).toBe("3");
    }),
  30000,
);
