import { financialRejection } from "../support/financial-rejection.ts";
import { expect, test } from "bun:test";
import {
  withFinancial,
  prepareFinancialAdmission,
  financialBlockedBy,
  financialBalances,
  financialProviderSchema,
} from "../support/financial-execution.ts";
import { syntheticPepper } from "../support/catalog.ts";
import { verifier } from "@gateway/runtime/crypto";
import type { SQL } from "bun";
async function credential(
  db: SQL,
  permissions: (string | null)[] = ["funding:recharge", "funding:read"],
) {
  const administrator = crypto.randomUUID(),
    id = crypto.randomUUID(),
    token = `synthetic-finance-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${administrator},'Synthetic finance administrator',${permissions}::text[])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${id},${administrator},${verifier(token, syntheticPepper)},'synthetic')`;
  return { administrator, id, token };
}

test(
  "platform revocation before funding SHARE denies recharge and money stays unchanged",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        identity = await credential(db);
      let release!: () => void, ready!: (pid: number) => void;
      const gate = new Promise<void>((r) => (release = r)),
        locked = new Promise<number>((r) => (ready = r));
      const revoker = db.begin(async (tx) => {
        await tx`UPDATE administrator_credentials SET enabled=false WHERE id=${identity.id}`;
        ready(Number((await tx`SELECT pg_backend_pid() pid`)[0].pid));
        await gate;
      });
      const pid = await locked,
        body = { ...setup.rechargeInput, external_ref: crypto.randomUUID(), credited_atoms: "17" };
      const request = f.request(
        `/upstream-accounts/${setup.provider.account_id}/recharges`,
        body,
        identity.token,
      );
      try {
        await financialBlockedBy(db, pid);
      } finally {
        release();
        await revoker;
      }
      expect((await request).status).toBe(403);
      expect(
        financialBalances(
          (await f.request(`/upstream-accounts/${setup.provider.account_id}/balances`)).value,
        )[0]!.balance_atoms,
      ).toBe("1000");
      expect(
        (
          await db`SELECT count(*)::integer n FROM upstream_funding_operations WHERE external_ref=${body.external_ref}`
        )[0].n,
      ).toBe(0);
    }),
  30000,
);

test(
  "funding SHARE holds the exact platform credential through recharge receipt journal audit commit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        identity = await credential(db);
      let release!: () => void, ready!: (pid: number) => void;
      const gate = new Promise<void>((r) => (release = r)),
        locked = new Promise<number>((r) => (ready = r));
      const blocker = db.begin(async (tx) => {
        await tx`SELECT account_id FROM upstream_account_balances WHERE account_id=${setup.provider.account_id} AND currency='USD' FOR NO KEY UPDATE`;
        ready(Number((await tx`SELECT pg_backend_pid() pid`)[0].pid));
        await gate;
      });
      const pid = await locked,
        body = { ...setup.rechargeInput, external_ref: crypto.randomUUID(), credited_atoms: "17" },
        pending = f.request(
          `/upstream-accounts/${setup.provider.account_id}/recharges`,
          body,
          identity.token,
        ),
        writer = await financialBlockedBy(db, pid);
      let revoked = false;
      const revoke = db
        .begin(async (tx) => {
          await tx`UPDATE administrator_credentials SET enabled=false WHERE id=${identity.id}`;
        })
        .then(() => (revoked = true));
      try {
        await financialBlockedBy(db, writer);
        expect(revoked).toBe(false);
        expect(
          (
            await db`SELECT count(*)::integer n FROM upstream_funding_operations WHERE external_ref=${body.external_ref}`
          )[0].n,
        ).toBe(0);
      } finally {
        release();
        await blocker;
      }
      const receipt = await pending;
      expect(receipt.status).toBe(201);
      expect(receipt.value.actor).toBe(`technical-admin:${identity.administrator}`);
      expect(receipt.value.balance_atoms).toBe("1017");
      await revoke;
      expect(revoked).toBe(true);
      expect(
        (
          await db`SELECT count(*)::integer n FROM upstream_audit_events WHERE operation_id=${receipt.value.operation_id}`
        )[0].n,
      ).toBe(1);
    }),
  30000,
);

test(
  "NULL permissions project ADMIN and credential mismatch cannot authorize platform funding",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        nullPermission = await credential(db, [null]),
        body = { ...setup.rechargeInput, external_ref: crypto.randomUUID() };
      expect(
        (
          await f.request(
            `/upstream-accounts/${setup.provider.account_id}/recharges`,
            body,
            nullPermission.token,
          )
        ).status,
      ).toBe(403);
      expect(
        (
          await f.request(
            `/upstream-accounts/${setup.provider.account_id}/recharges`,
            body,
            setup.key.key,
          )
        ).status,
      ).toBe(401);
      const identity = await credential(db),
        other = await credential(db);
      let release!: () => void, ready!: (pid: number) => void;
      const gate = new Promise<void>((r) => (release = r)),
        locked = new Promise<number>((r) => (ready = r));
      const changer = db.begin(async (tx) => {
        await tx`UPDATE administrator_credentials SET administrator_id=${other.administrator} WHERE id=${identity.id}`;
        ready(Number((await tx`SELECT pg_backend_pid() pid`)[0].pid));
        await gate;
      });
      const pid = await locked,
        pending = f.request(
          `/upstream-accounts/${setup.provider.account_id}/recharges`,
          body,
          identity.token,
        );
      try {
        await financialBlockedBy(db, pid);
      } finally {
        release();
        await changer;
      }
      expect((await pending).status).toBe(403);
      expect(
        (
          await db`SELECT count(*)::integer n FROM upstream_funding_operations WHERE external_ref=${body.external_ref}`
        )[0].n,
      ).toBe(0);
    }),
  30000,
);

test(
  "shared supplier account gates two channels in its own currency without borrowing other currency balances",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap("HKD", "50"),
        second = await f.request("/providers", {
          name: `shared-${crypto.randomUUID()}`,
          kind: "openai",
          base_url: "https://second.synthetic.test/v1",
          api_key: "second-synthetic-secret",
          concurrency: 10,
          rpm: 100,
          account_id: setup.provider.account_id,
          brand_id: setup.provider.brand_id,
        });
      expect(second.status).toBe(201);
      const secondProvider = financialProviderSchema.parse(second.value);
      const route = await f.request(`/models/${setup.model}/routes`, {
        provider_id: secondProvider.id,
        upstream_model: "synthetic",
        operations: ["chat"],
        pricing: {
          currency: "HKD",
          rates: {
            input_tokens: { numerator: "1", denominator: "3" },
            output_tokens: { numerator: "1", denominator: "3" },
          },
        },
        ceilings: { input_tokens: "100", output_tokens: "50" },
      });
      expect(route.status).toBe(201);
      await f.request("/billing/tariffs", {
        model: setup.model,
        operation: "chat",
        provider_id: secondProvider.id,
        pricing: { currency: "USD", rates: { requests: { numerator: "100", denominator: "1" } } },
        ceilings: { requests: "1" },
        basis: "REQUEST_COMPLETION",
      });
      await f.catalog.application.bind(await f.catalog.actor(), secondProvider.id, {
        ...f.catalog.binding(setup.publication.version_id),
        account_id: setup.provider.account_id,
        brand_id: setup.provider.brand_id,
        secret_reference: `channel:${secondProvider.id}`,
        config: {
          model_map: { [setup.model]: "synthetic" },
          max_output_tokens: 100,
          max_input_tokens: 10000,
          auth: "bearer",
        },
      });
      for (const currency of ["USD", "CNY"])
        expect(
          (
            await f.request(`/upstream-accounts/${setup.provider.account_id}/recharges`, {
              ...setup.rechargeInput,
              external_ref: crypto.randomUUID(),
              credited_currency: currency,
              credited_atoms: "10000",
            })
          ).status,
        ).toBe(201);
      const client = f.client();
      await client.startup();
      const first = await prepareFinancialAdmission(setup, client, {
          channelId: setup.provider.id,
        }),
        next = await prepareFinancialAdmission(setup, client, { channelId: secondProvider.id });
      expect((await first.admit()).receipt.state).toBe("RESERVED");
      expect((await financialRejection(next.admit(), 503)).receipt).toMatchObject({
        command_state: "REJECTED",
        code: "upstream_funds_unavailable",
      });
      const balances = financialBalances(
        (await f.request(`/upstream-accounts/${setup.provider.account_id}/balances`)).value,
      );
      expect(
        balances.map((x) => ({
          currency: x.currency,
          balance: x.balance_atoms,
          reserved: x.reserved_atoms,
        })),
      ).toEqual([
        { currency: "CNY", balance: "10000", reserved: "0" },
        { currency: "HKD", balance: "50", reserved: "30" },
        { currency: "USD", balance: "10000", reserved: "0" },
      ]);
      expect((await db`SELECT count(*)::integer n FROM upstream_holds`)[0].n).toBe(1);
    }),
  30000,
);

test(
  "recharge preserves a reconciliation freeze and failed audit rolls its entire funding group back",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      await db`UPDATE upstream_accounts SET state='RECONCILE_REQUIRED' WHERE id=${setup.provider.account_id}`;
      const body = {
        ...setup.rechargeInput,
        external_ref: crypto.randomUUID(),
        credited_atoms: "9",
      };
      expect(
        (await f.request(`/upstream-accounts/${setup.provider.account_id}/recharges`, body)).status,
      ).toBe(201);
      expect(
        (await db`SELECT state FROM upstream_accounts WHERE id=${setup.provider.account_id}`)[0]
          .state,
      ).toBe("RECONCILE_REQUIRED");
      await db.unsafe(
        `CREATE FUNCTION funding_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic funding audit failure';END $$;CREATE TRIGGER funding_audit_failure BEFORE INSERT ON upstream_audit_events FOR EACH ROW EXECUTE FUNCTION funding_audit_failure()`,
      );
      const fail = { ...body, external_ref: crypto.randomUUID(), credited_atoms: "31" };
      expect(
        (await f.request(`/upstream-accounts/${setup.provider.account_id}/recharges`, fail)).status,
      ).toBe(503);
      expect(
        (
          await db`SELECT balance_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
        )[0].balance_atoms,
      ).toBe("1009");
      expect(
        (
          await db`SELECT count(*)::integer n FROM upstream_funding_operations WHERE external_ref=${fail.external_ref}`
        )[0].n,
      ).toBe(0);
    }),
  30000,
);
