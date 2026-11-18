import { financialRejection } from "../support/financial-rejection.ts";
import { expect, test } from "bun:test";
import {
  withFinancial,
  prepareFinancialAdmission,
  financialBlockedBy,
  financialGate,
  financialBalances,
  financialProviderSchema,
} from "../support/financial-execution.ts";
import { syntheticAdminKey, syntheticPepper } from "../support/catalog.ts";
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
      const setup = await f.bootstrap();
      const identity = await credential(db);
      const gate = await financialGate(db);
      let pending: ReturnType<typeof f.request> | undefined;
      let revoke: Promise<unknown> | undefined;
      try {
        await db.unsafe(`
          CREATE FUNCTION public.funding_credential_audit_gate() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
          BEGIN IF NEW.account_id='${setup.provider.account_id}'::uuid THEN PERFORM pg_advisory_xact_lock(${gate.key}::bigint);END IF;RETURN NEW;END $$;
          CREATE TRIGGER funding_credential_audit_gate BEFORE INSERT ON public.upstream_audit_events FOR EACH ROW EXECUTE FUNCTION public.funding_credential_audit_gate();
        `);
        const body = {
          ...setup.rechargeInput,
          external_ref: crypto.randomUUID(),
          credited_atoms: "17",
        };
        pending = f.request(
          `/upstream-accounts/${setup.provider.account_id}/recharges`,
          body,
          identity.token,
        );
        const writer = await financialBlockedBy(db, gate.pid);
        let revoked = false;
        revoke = db
          .begin(async (tx) => {
            await tx`UPDATE administrator_credentials SET enabled=false WHERE id=${identity.id}`;
          })
          .then(() => (revoked = true));
        await financialBlockedBy(db, writer);
        expect(revoked).toBe(false);
        expect(
          (
            await db`SELECT count(*)::integer n FROM upstream_funding_operations WHERE external_ref=${body.external_ref}`
          )[0].n,
        ).toBe(0);
        await gate.release();
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
      } finally {
        await gate.release();
        await Promise.allSettled([pending, revoke]);
      }
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
          base_url: "https://second.synthetic.test",
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
      await f.request("/tariffs", {
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
      await db.begin(async (tx) => {
        await tx`SELECT core_funding_control_v1(${setup.provider.account_id}::uuid,true)`;
        await tx`UPDATE upstream_accounts SET state='RECONCILE_REQUIRED' WHERE id=${setup.provider.account_id}`;
      });
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
          await db`SELECT balance_atoms::text FROM core_upstream_balances_v1 WHERE account_id=${setup.provider.account_id}`
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

test(
  "a provider base_url with a path prefix is rejected instead of being silently dropped from upstream requests",
  () =>
    withFinancial(async (f, db) => {
      const name = `prefixed-${crypto.randomUUID()}`,
        provider = (base_url: string) =>
          f.request("/providers", {
            name,
            kind: "openai",
            base_url,
            api_key: "prefixed-synthetic-secret",
            concurrency: 1,
            rpm: 1,
          });
      for (const base_url of [
        "https://prefixed.synthetic.test/api/v1",
        "https://prefixed.synthetic.test/v1/",
      ]) {
        const rejected = await provider(base_url);
        expect(rejected.status).toBe(400);
        expect((rejected.value.error as { code: string }).code).toBe("invalid_provider");
      }
      expect((await db`SELECT count(*)::integer n FROM channels WHERE name=${name}`)[0].n).toBe(0);
      expect((await provider("https://prefixed.synthetic.test/")).status).toBe(201);
      expect((await db`SELECT base_url FROM channels WHERE name=${name}`)[0].base_url).toBe(
        "https://prefixed.synthetic.test",
      );
    }),
  30000,
);

test(
  "malformed Azure, Bedrock and Vertex credentials are rejected with 422 when a provider is created or its key replaced instead of being stored and failing every request",
  () =>
    withFinancial(async (f, db) => {
      const name = `cloud-${crypto.randomUUID()}`,
        create = (kind: string, api_key: string) =>
          f.request("/providers", {
            name,
            kind,
            base_url: "https://cloud.synthetic.test/",
            api_key,
            concurrency: 1,
            rpm: 1,
          });
      for (const [kind, api_key] of [
        ["azure", '{"apiKey":"synthetic","apiVersion":"latest"}'],
        ["bedrock", '{"accessKeyId":"AKIDSYNTHETIC"}'],
        ["vertex", '{"access_token":"expired","expires_at":"2000-01-01T00:00:00Z"}'],
        ["vertex", '{"type":"service_account","client_email":"invalid","private_key":"invalid"}'],
      ] as const) {
        const rejected = await create(kind, api_key);
        expect(rejected.status).toBe(422);
        expect((rejected.value.error as { code: string }).code).toBe("invalid_cloud_credentials");
      }
      expect((await db`SELECT count(*)::integer n FROM channels WHERE name=${name}`)[0].n).toBe(0);
      const created = await create(
        "bedrock",
        '{"accessKeyId":"AKIDSYNTHETIC","secretAccessKey":"synthetic","region":"us-east-1"}',
      );
      expect(created.status).toBe(201);
      const id = (created.value as { id: string }).id;
      const before = (await db`SELECT secret FROM channels WHERE id=${id}`)[0].secret;
      const replaced = await f.request(
        `/providers/${id}`,
        { api_key: '{"accessKeyId":"AKIDSYNTHETIC","region":"us-east-1"}' },
        undefined,
        "PATCH",
      );
      expect(replaced.status).toBe(422);
      expect((replaced.value.error as { code: string }).code).toBe("invalid_cloud_credentials");
      expect((await db`SELECT secret FROM channels WHERE id=${id}`)[0].secret).toEqual(before);
    }),
  30000,
);

test(
  "a chunked management body without Content-Length is cut at its byte budget with 413 and at its time budget with 408 body_timeout instead of 503 management_unavailable, releasing the body and creating nothing",
  () =>
    withFinancial(async (f, db) => {
      const name = `chunked-${crypto.randomUUID()}`;
      const post = (body: ReadableStream<Uint8Array>) =>
        f.management.request("http://management.test/projects", {
          method: "POST",
          headers: {
            authorization: `Bearer ${syntheticAdminKey}`,
            "content-type": "application/json",
          },
          body,
          duplex: "half",
        } as RequestInit);
      const mebibyte = new Uint8Array(1 << 20).fill(120);
      let pulled = 0,
        oversizedReleased = false;
      const oversized = await post(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(`{"name":"${name}","padding":"`));
          },
          pull(controller) {
            if (pulled++ < 64) controller.enqueue(mebibyte);
            else controller.close();
          },
          cancel() {
            oversizedReleased = true;
          },
        }),
      );
      expect([oversized.status, (await oversized.json()).error.code]).toEqual([
        413,
        "body_too_large",
      ]);
      expect(pulled).toBeLessThan(16);
      expect(oversizedReleased).toBe(true);

      let stalledReleased = false;
      const started = performance.now();
      const stalled = await post(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(`{"name":"${name}`));
          },
          cancel() {
            stalledReleased = true;
          },
        }),
      );
      expect([stalled.status, (await stalled.json()).error.code]).toEqual([408, "body_timeout"]);
      expect(performance.now() - started).toBeLessThan(15000);
      expect(stalledReleased).toBe(true);
      expect((await db`SELECT count(*)::integer n FROM projects WHERE name=${name}`)[0].n).toBe(0);
    }),
  60000,
);
