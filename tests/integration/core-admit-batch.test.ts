import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreVerifiedCallerProjection } from "@gateway/contracts";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { canonical, quoteReservation, type Pricing } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { createQuotaLeases } from "@gateway/runtime/quota-leases";
import {
  withFinancial,
  financialRpcSession,
  prepareFinancialAdmission,
} from "../support/financial-execution.ts";
import { supplier } from "../support/core-settlement.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;

async function admissionBatch(f: Fixture, db: SQL, setup: Setup) {
  const rpc = await financialRpcSession(f);
  const warm = await prepareFinancialAdmission(setup, rpc);
  await warm.admit();
  await rpc.execution("execution_abort_v1", {
    attempt_id: warm.attemptId,
    permit_epoch: "1",
    close_request: true,
    reason: "PREPARE_DISPOSED",
  });
  const store = createInternalIdentityStore(db);
  const session = (await store.readStartupSession(rpc.identity.receipt.session_id))!;
  const peer = (await store.readPeerDirectory(session.certificate_sha256))!;
  const quota = createQuotaLeases({
    urls: [process.env.REDIS_URL!],
    namespace: f.namespace,
    generation: "admit_batch",
  });
  async function prepare(
    options: Parameters<typeof prepareFinancialAdmission>[2] = {},
    selectedSetup = setup,
  ) {
    const request = await prepareFinancialAdmission(selectedSetup, rpc, {
      intent: null,
      ...options,
    });
    async function item(data = request.data, command = request.command) {
      const projection: CoreVerifiedCallerProjection = {
        service: session.service,
        instance_id: session.instance_id,
        certificate_sha256: session.certificate_sha256,
        instance_revision: peer.instance_revision,
        certificate_revision: peer.certificate_revision,
        session_id: session.session_id,
        incarnation_id: session.incarnation_id,
        key_sha256: session.public_key_sha256,
        session_revision: session.session_revision,
        command_type: "execution_admit_v1",
        command_id: command,
        raw_body_sha256: sha256("independent admission batch fixture"),
        method: "POST",
        normalized_path: "/_internal/core/v1/admissions",
        deadline_at: request.deadline,
        trace_id: "admission-batch",
      };
      const envelope = {
        command_id: command,
        caller_incarnation: projection.incarnation_id,
        deadline_at: projection.deadline_at,
        trace_id: projection.trace_id,
        contract_version: 1,
        body_digest: sha256(
          canonical({
            kind: projection.command_type,
            contract_version: 1,
            owner: {
              service: projection.service,
              instance_id: projection.instance_id,
              session_id: projection.session_id,
              incarnation_id: projection.incarnation_id,
            },
            deadline_at: projection.deadline_at,
            data,
          }),
        ),
      };
      const leases = [];
      for (const scope of [
        ...request.context.scopes,
        {
          kind: "channel" as const,
          id: selectedSetup.provider.id,
          rpm: 100,
          tpm: Number.MAX_SAFE_INTEGER,
          concurrency: 10,
        },
      ]) {
        const acquired = await quota.acquire({
          scope,
          kind: projection.command_type,
          caller: `${session.service}:${session.instance_id}`,
          command_id: command,
          body_digest: envelope.body_digest,
          owner_session_id: session.session_id,
          deadline_ms: Date.parse(request.deadline),
          tokens:
            scope.kind === "channel" ? 90 : 90 * request.context.execution_budget.max_attempts,
        });
        leases.push(acquired.descriptor);
      }
      return {
        projection,
        envelope,
        data,
        computed: {
          quote: quoteReservation(
            request.context.candidates.map((candidate) => ({
              provider_id: candidate.ref.channel_id,
              id: candidate.ref.customer_price_id,
              currency: candidate.customer_currency,
              rates: candidate.customer_rates as Pricing["rates"],
              ceilings: candidate.customer_ceilings,
              basis: candidate.customer_basis,
            })),
            "USD",
            {},
            30,
          ),
          estimated_quantities: { input_tokens: "60", output_tokens: "30" },
          supplier_atoms: "30",
          token_count: "90",
          leases,
        },
      };
    }
    return { request, item };
  }
  return { prepare, close: () => quota.close() };
}

test("admission inserts the complete request and bound attempt once while reserving funds exactly once", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const first = await batch.prepare({ attemptId: "10000000-0000-4000-8000-000000000000" });
      const second = await batch.prepare({ attemptId: "20000000-0000-4000-8000-000000000000" });
      const items = [await first.item(), await second.item()];
      await db.unsafe(`
        CREATE TABLE public.admit_batch_trace(name text,operation text,rows integer);
        CREATE FUNCTION public.admit_batch_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
        BEGIN INSERT INTO public.admit_batch_trace SELECT TG_TABLE_NAME,TG_OP,count(*) FROM new_rows;RETURN NULL;END $$;
      `);
      for (const [table, operations] of [
        ["gateway_requests", ["INSERT", "UPDATE"]],
        ["attempts", ["INSERT", "UPDATE"]],
        ["billing_holds", ["INSERT"]],
        ["hold_allocations", ["INSERT"]],
        ["wallet_lots", ["UPDATE"]],
        ["wallets", ["UPDATE"]],
        ["execution_commands", ["INSERT"]],
        ["execution_permits", ["INSERT"]],
        ["upstream_holds", ["INSERT"]],
        ["upstream_journals", ["INSERT"]],
        ["upstream_entries", ["INSERT"]],
        ["execution_quota_leases", ["INSERT"]],
        ["audit_events", ["INSERT"]],
        ["core_funding.balance_cells", ["UPDATE"]],
      ] as const) {
        for (const operation of operations)
          await db.unsafe(
            `CREATE TRIGGER admit_batch_${operation} AFTER ${operation} ON ${table} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.admit_batch_trace()`,
          );
      }
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results).toEqual([
        expect.objectContaining({
          receipt: expect.objectContaining({
            state: "RESERVED",
            attempt_id: first.request.attemptId,
          }),
        }),
        expect.objectContaining({
          receipt: expect.objectContaining({
            state: "RESERVED",
            attempt_id: second.request.attemptId,
          }),
        }),
      ]);
      expect(
        await db`SELECT name,operation,array_agg(rows ORDER BY rows) rows FROM admit_batch_trace WHERE rows>0 GROUP BY name,operation ORDER BY name,operation`,
      ).toEqual([
        { name: "attempts", operation: "INSERT", rows: [2] },
        { name: "audit_events", operation: "INSERT", rows: [2] },
        { name: "balance_cells", operation: "UPDATE", rows: [1] },
        { name: "billing_holds", operation: "INSERT", rows: [2] },
        { name: "execution_commands", operation: "INSERT", rows: [2] },
        { name: "execution_permits", operation: "INSERT", rows: [2] },
        { name: "execution_quota_leases", operation: "INSERT", rows: [6] },
        { name: "gateway_requests", operation: "INSERT", rows: [2] },
        { name: "hold_allocations", operation: "INSERT", rows: [2] },
        { name: "upstream_entries", operation: "INSERT", rows: [4] },
        { name: "upstream_holds", operation: "INSERT", rows: [2] },
        { name: "upstream_journals", operation: "INSERT", rows: [2] },
        { name: "wallet_lots", operation: "UPDATE", rows: [1] },
        { name: "wallets", operation: "UPDATE", rows: [1] },
      ]);
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved,credit_reserved_atoms::text credit FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "800", reserved: "200", credit: "0" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
      expect(
        await db`SELECT q.state, a.binding_id=p.binding_id AS bound,
          q.execution_budget->>'owner_session_id'=p.owner_session_id::text AS owned
          FROM gateway_requests q JOIN attempts a ON a.request_id=q.id
          JOIN execution_permits p ON p.attempt_id=a.id WHERE q.id=ANY(${[first.request.requestId, second.request.requestId]}::uuid[])`,
      ).toEqual([
        { state: "ADMITTED", bound: true, owned: true },
        { state: "ADMITTED", bound: true, owned: true },
      ]);
      const before = await db`SELECT * FROM admit_batch_trace WHERE rows>0`;
      const replay = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(replay.map((result: { receipt: unknown }) => result.receipt)).toEqual(
        results.map((result: { receipt: unknown }) => result.receipt),
      );
      expect(await db`SELECT * FROM admit_batch_trace WHERE rows>0`).toEqual(before);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
    } finally {
      batch.close();
    }
  }));

test("admission with a column-limited allocator preserves request ownership and attempt binding", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    const role = `allocator_${crypto.randomUUID().replaceAll("-", "")}`;
    try {
      const requests = [await batch.prepare(), await batch.prepare()];
      const items = await Promise.all(requests.map((request) => request.item()));
      await db.unsafe(`CREATE ROLE ${role} NOLOGIN`);
      try {
        await db.unsafe(`GRANT USAGE,CREATE ON SCHEMA public TO ${role}`);
        await db.unsafe(`GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO ${role}`);
        await db.unsafe(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO ${role}`);
        await db.unsafe(`REVOKE INSERT ON gateway_requests,attempts FROM ${role}`);
        await db.unsafe(
          `GRANT INSERT(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome) ON gateway_requests TO ${role}`,
        );
        await db.unsafe(
          `GRANT INSERT(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state) ON attempts TO ${role}`,
        );
        await db.unsafe(
          `ALTER FUNCTION public.allocate_customer_rows_v1(jsonb,boolean) OWNER TO ${role}`,
        );
        const [row] = await db`SELECT core_execution_batch_v1(${items}::jsonb) results`;
        expect(row.results).toEqual(
          requests.map((request) =>
            expect.objectContaining({
              receipt: expect.objectContaining({
                state: "RESERVED",
                attempt_id: request.request.attemptId,
              }),
            }),
          ),
        );
        expect(
          await db`SELECT count(*)::int n FROM gateway_requests q JOIN attempts a ON a.request_id=q.id JOIN execution_permits p ON p.attempt_id=a.id WHERE q.id=ANY(${requests.map((r) => r.request.requestId)}::uuid[]) AND q.state='ADMITTED' AND a.binding_id=p.binding_id AND q.execution_budget->>'owner_session_id'=p.owner_session_id::text`,
        ).toEqual([{ n: 2 }]);
        expect(
          await db`SELECT has_column_privilege(${role},'attempts','binding_id','INSERT') binding,has_column_privilege(${role},'gateway_requests','execution_budget','INSERT') budget`,
        ).toEqual([{ binding: false, budget: false }]);
      } finally {
        await db.unsafe(
          "ALTER FUNCTION public.allocate_customer_rows_v1(jsonb,boolean) OWNER TO CURRENT_USER",
        );
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
      }
    } finally {
      batch.close();
    }
  }));

test("a rejected quota descriptor after staging cannot pollute the next admission's wallet, cell or journal", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const prepared = [
        await batch.prepare({ attemptId: "10000000-0000-4000-8000-000000000000" }),
        await batch.prepare({ attemptId: "20000000-0000-4000-8000-000000000000" }),
        await batch.prepare({ attemptId: "30000000-0000-4000-8000-000000000000" }),
      ];
      const items = await Promise.all(prepared.map((request) => request.item()));
      items[1]!.computed.leases[0]!.state = "ADOPTED";
      await db.unsafe(`
        CREATE TABLE public.admit_failure_trace(rows integer);
        CREATE FUNCTION public.admit_failure_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
        BEGIN INSERT INTO public.admit_failure_trace SELECT count(*) FROM new_rows;RETURN NULL;END $$;
        CREATE TRIGGER admit_failure_trace AFTER INSERT ON public.upstream_holds REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.admit_failure_trace();
      `);
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results[0]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(results[1]).toEqual({ error: "identity_store_unavailable" });
      expect(results[2]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(await db`SELECT rows FROM admit_failure_trace`).toEqual([{ rows: 2 }]);
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "800", reserved: "200" }]);
      expect(
        await db`SELECT count(*)::int n FROM execution_commands WHERE command_id=${prepared[1]!.request.command}`,
      ).toEqual([{ n: 0 }]);
      expect(
        await db`SELECT count(*)::int n FROM upstream_holds WHERE attempt_id=${prepared[1]!.request.attemptId}`,
      ).toEqual([{ n: 0 }]);
      expect(
        await db`SELECT count(*)::int n FROM upstream_journals WHERE attempt_id=${prepared[1]!.request.attemptId}`,
      ).toEqual([{ n: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
      expect(
        await db`SELECT reserved_atoms::text FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND cell=0`,
      ).toEqual([{ reserved_atoms: "60" }]);
    } finally {
      batch.close();
    }
  }));

test("admission batch moves the second reservation to another cell when their sum exceeds the first cell's budget", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "640");
    const batch = await admissionBatch(f, db, setup);
    try {
      const first = await batch.prepare({ attemptId: "10000000-0000-4000-8000-000000000000" });
      const second = await batch.prepare({ attemptId: "20000000-0000-4000-8000-000000000000" });
      expect(
        await db`SELECT balance_atoms::text FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND cell=0`,
      ).toEqual([{ balance_atoms: "40" }]);
      await db.unsafe(`
        CREATE TABLE public.admit_budget_trace(rows integer);
        CREATE FUNCTION public.admit_budget_trace() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
        BEGIN INSERT INTO public.admit_budget_trace SELECT count(*) FROM new_rows;RETURN NULL;END $$;
        CREATE TRIGGER admit_budget_trace AFTER INSERT ON public.upstream_holds REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION public.admit_budget_trace();
      `);
      const items = [await first.item(), await second.item()];
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      for (const result of results)
        expect(result).toMatchObject({ receipt: { state: "RESERVED", supplier_atoms: "30" } });
      expect(
        await db`SELECT funding_cell FROM upstream_holds WHERE attempt_id IN (${first.request.attemptId}::uuid,${second.request.attemptId}::uuid) ORDER BY attempt_id`,
      ).toEqual([{ funding_cell: 0 }, { funding_cell: 1 }]);
      expect(await db`SELECT rows FROM admit_budget_trace`).toEqual([{ rows: 2 }]);
      expect(
        await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved FROM core_funding.balance_cells WHERE account_id=${setup.provider.account_id} AND cell IN(0,1) ORDER BY cell`,
      ).toEqual([
        { balance: "40", reserved: "30" },
        { balance: "40", reserved: "30" },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "640",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
    } finally {
      batch.close();
    }
  }));

test("mixed admission query and abort items observe the completed prefix before another admission reserves funds", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const first = await batch.prepare();
      const second = await batch.prepare();
      const firstItem = await first.item();
      function dependent(kind: "execution_query_v1" | "execution_abort_v1", data: unknown) {
        const projection = {
          ...firstItem.projection,
          command_type: kind,
          command_id: crypto.randomUUID(),
          normalized_path: `/_internal/core/v1/${kind === "execution_query_v1" ? "commands" : "abort"}`,
        };
        return {
          projection,
          envelope: {
            ...firstItem.envelope,
            command_id: projection.command_id,
            body_digest: sha256(
              canonical({
                kind,
                contract_version: 1,
                owner: {
                  service: projection.service,
                  instance_id: projection.instance_id,
                  session_id: projection.session_id,
                  incarnation_id: projection.incarnation_id,
                },
                deadline_at: projection.deadline_at,
                data,
              }),
            ),
          },
          data,
          computed: {},
        };
      }
      const items = [
        firstItem,
        dependent("execution_query_v1", {
          kind: "execution_admit_v1",
          command_id: first.request.command,
        }),
        dependent("execution_abort_v1", {
          attempt_id: first.request.attemptId,
          permit_epoch: "1",
          close_request: true,
          reason: "PREPARE_DISPOSED",
        }),
        await second.item(),
      ];
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results[0]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(results[1].receipt).toEqual(results[0].receipt);
      expect(results[2]).toMatchObject({ receipt: { state: "ABORTED" } });
      expect(results[3]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(
        await db`SELECT state FROM execution_permits WHERE attempt_id=${first.request.attemptId}`,
      ).toEqual([{ state: "ABORTED" }]);
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "900", reserved: "100" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "30",
        spent: "0",
        state: "ACTIVE",
      });
    } finally {
      batch.close();
    }
  }));

test("a customer allocation failure rolls back the trial batch without charging or reserving its failed request", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const tariff = await f.request("/tariffs", {
        model: setup.model,
        operation: "chat",
        provider_id: setup.provider.id,
        pricing: { currency: "USD", rates: { requests: { numerator: "600", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "REQUEST_COMPLETION",
      });
      expect(tariff.status).toBe(201);
      const first = await batch.prepare();
      const second = await batch.prepare();
      const items = [await first.item(), await second.item()];
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results[0]).toMatchObject({
        receipt: { state: "RESERVED", customer_atoms: "600", supplier_atoms: "30" },
      });
      expect(results[1]).toMatchObject({
        receipt: { command_state: "REJECTED", code: "insufficient_funds" },
      });
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "400", reserved: "600" }]);
      expect(await db`SELECT attempt_id::text FROM upstream_holds WHERE state='HELD'`).toEqual([
        { attempt_id: first.request.attemptId },
      ]);
      expect(
        await db`SELECT count(*)::int n FROM billing_holds WHERE request_id=${second.request.requestId}`,
      ).toEqual([{ n: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "30",
        spent: "0",
        state: "ACTIVE",
      });
      const replay = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(replay.map((result: { receipt: unknown }) => result.receipt)).toEqual(
        results.map((result: { receipt: unknown }) => result.receipt),
      );
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "30",
        spent: "0",
        state: "ACTIVE",
      });
    } finally {
      batch.close();
    }
  }));

test("an audit failure at admission flush leaves other requests committed and the original failed command can retry without extra money", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const prepared = [await batch.prepare(), await batch.prepare(), await batch.prepare()];
      const items = await Promise.all(prepared.map((request) => request.item()));
      await db.unsafe(`
        CREATE FUNCTION public.admit_batch_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.action='execution_admit_v1' AND NEW.resource_id='${prepared[1]!.request.attemptId}' THEN RAISE EXCEPTION 'independent audit fault';END IF;RETURN NEW;END $$;
        CREATE TRIGGER admit_batch_audit_failure BEFORE INSERT ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.admit_batch_audit_failure();
      `);
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(results[0]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(results[1]).toEqual({ error: "identity_store_unavailable" });
      expect(results[2]).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "800", reserved: "200" }]);
      expect(
        await db`SELECT count(*)::int n FROM execution_commands WHERE command_id=${prepared[1]!.request.command}`,
      ).toEqual([{ n: 0 }]);
      expect(
        await db`SELECT count(*)::int n FROM upstream_holds WHERE attempt_id=${prepared[1]!.request.attemptId}`,
      ).toEqual([{ n: 0 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "60",
        spent: "0",
        state: "ACTIVE",
      });
      await db`DROP TRIGGER admit_batch_audit_failure ON audit_events`;
      const replay = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      for (const result of replay) expect(result).toMatchObject({ receipt: { state: "RESERVED" } });
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "700", reserved: "300" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "90",
        spent: "0",
        state: "ACTIVE",
      });
      expect(
        await db`SELECT count(*)::int n FROM upstream_journals WHERE attempt_id=${prepared[1]!.request.attemptId} AND kind='RESERVE'`,
      ).toEqual([{ n: 1 }]);
    } finally {
      batch.close();
    }
  }));

test("repeated request and attempt UUIDs and intents flush their admission prefix before a later conflict", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const batch = await admissionBatch(f, db, setup);
    try {
      const attempt = "abcdefab-cdef-4abc-8def-abcdefabcdef";
      const request = "bcdefabc-defa-4bcd-8efa-bcdefabcdefa";
      const intent = sha256("one independently identified intent");
      const prepared = [
        await batch.prepare({ attemptId: attempt }),
        await batch.prepare({ attemptId: attempt }),
        await batch.prepare({ requestId: request }),
        await batch.prepare({ requestId: request }),
        await batch.prepare({ intent }),
        await batch.prepare({ intent }),
      ];
      const items = await Promise.all(prepared.map((request) => request.item()));
      const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
      expect(
        results.map(
          (result: { receipt: { state?: string; code?: string } }) =>
            result.receipt.state ?? result.receipt.code,
        ),
      ).toEqual([
        "RESERVED",
        "proposed_id_conflict",
        "RESERVED",
        "proposed_id_conflict",
        "RESERVED",
        "already_accepted",
      ]);
      expect(results[5].receipt.request_id).toBe(prepared[4]!.request.requestId);
      expect(await db`SELECT count(*)::int n FROM upstream_holds WHERE state='HELD'`).toEqual([
        { n: 3 },
      ]);
      expect(
        await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ available: "700", reserved: "300" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "100000",
        reserved: "90",
        spent: "0",
        state: "ACTIVE",
      });
    } finally {
      batch.close();
    }
  }));

test.each(["key", "context"] as const)(
  "admission flush cannot renew a %s that expires during a later item",
  (mode) =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap("USD", "100000");
      const batch = await admissionBatch(f, db, setup);
      try {
        const created = await f.request(`/projects/${setup.project.id}/keys`, {
          name: "independent second key",
          models: [setup.model],
          operations: ["chat"],
        });
        expect(created.status).toBe(201);
        const secondSetup = {
          ...setup,
          key: { id: String(created.value.id), key: String(created.value.key) },
        };
        const first = await batch.prepare();
        const second = await batch.prepare({}, secondSetup);
        const secondItem = await second.item();
        await db`ALTER FUNCTION public.core_digest_v1(jsonb) RENAME TO admit_original_digest_v1`;
        await db.unsafe(`
        CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
        BEGIN IF v->>'kind'='execution_admit_v1' AND v->'data'->'dispatch_projection'->>'proposed_attempt_id'='${second.request.attemptId}' THEN PERFORM pg_sleep(0.3);END IF;RETURN public.admit_original_digest_v1(v);END $$;
      `);
        const contextId = crypto.randomUUID();
        if (mode === "context")
          await db`INSERT INTO execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) SELECT ${contextId}::uuid,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,clock_timestamp()+interval '0.2 seconds' FROM execution_contexts WHERE id=${first.request.data.context_id}`;
        const items = [
          await first.item(
            mode === "context"
              ? { ...first.request.data, context_id: contextId }
              : first.request.data,
          ),
          secondItem,
        ];
        if (mode === "key")
          await db`UPDATE api_keys SET expires_at=clock_timestamp()+interval '0.2 seconds' WHERE id=${setup.key.id}`;
        const results = (await db`SELECT core_execution_batch_v1(${items}::jsonb) value`)[0].value;
        expect(results[0]).toMatchObject(
          mode === "key"
            ? { error: "customer_forbidden" }
            : { receipt: { command_state: "REJECTED", code: "context_stale" } },
        );
        expect(results[1]).toMatchObject({
          receipt: { state: "RESERVED", attempt_id: second.request.attemptId },
        });
        expect(
          await db`SELECT count(*)::int n FROM upstream_holds WHERE attempt_id=${first.request.attemptId}`,
        ).toEqual([{ n: 0 }]);
        expect(
          await db`SELECT available_atoms::text available,reserved_atoms::text reserved FROM wallets WHERE project_id=${setup.project.id}`,
        ).toEqual([{ available: "900", reserved: "100" }]);
        expect(await supplier(db, setup.provider.account_id)).toEqual({
          balance: "100000",
          reserved: "30",
          spent: "0",
          state: "ACTIVE",
        });
      } finally {
        batch.close();
      }
    }),
);
