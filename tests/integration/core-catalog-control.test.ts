import { expect, test } from "bun:test";
import { SQL, type ReservedSQL, type TransactionSQL } from "bun";
import { verifier } from "@gateway/runtime/crypto";
import { createCoreManagementApplication } from "../../apps/core-api/src/platform/management.ts";
import { financialBlockedBy, financialKey, withFinancial } from "../support/financial-execution.ts";
import { syntheticAdminKey, syntheticPepper } from "../support/catalog.ts";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";

async function hold(db: SQL, lock: (tx: TransactionSQL) => Promise<unknown>) {
  const entered = Promise.withResolvers<number>(),
    released = Promise.withResolvers<void>();
  const done = db.begin(async (tx) => {
    await lock(tx);
    entered.resolve(Number((await tx`SELECT pg_backend_pid() pid`)[0].pid));
    await released.promise;
  });
  void done.catch(entered.reject);
  const pid = await entered.promise;
  return {
    pid,
    async release() {
      released.resolve();
      await done;
    },
  };
}

async function snapshot(db: SQL) {
  return {
    routes: await db`SELECT * FROM routes ORDER BY id`,
    costs: await db`SELECT * FROM cost_versions ORDER BY id`,
    audits: await db`SELECT * FROM audit_events ORDER BY id`,
  };
}

async function credential(db: SQL) {
  const administrator = crypto.randomUUID(),
    id = crypto.randomUUID(),
    token = `synthetic-control-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${administrator},'Route control operator',ARRAY['catalog:manage'])`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${id},${administrator},${verifier(token, syntheticPepper)},'synthetic')`;
  return { id, token };
}

function patch(id: string, signal?: AbortSignal, token = syntheticAdminKey) {
  return new Request(`http://management.test/routes/${id}`, {
    method: "PATCH",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify({ enabled: false }),
    ...(signal ? { signal } : {}),
  });
}

for (const wait of ["pool", "catalog", "route"] as const)
  test(
    `canceling route management during ${wait} wait leaves no late route, cost or audit mutation`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap(),
          before = await snapshot(db),
          controller = new AbortController(),
          reservations: ReservedSQL[] = [];
        let blocker: Awaited<ReturnType<typeof hold>> | undefined;
        let operation: Promise<Response> | undefined;
        try {
          if (wait === "pool") {
            for (let n = 0; n < 4; n++) reservations.push(await f.catalog.appDb.reserve());
          } else {
            blocker = await hold(db, (tx) =>
              wait === "catalog"
                ? tx`SELECT pg_advisory_xact_lock_shared(hashtextextended('stg.catalog.control.v1',0))`
                : tx`SELECT id FROM routes WHERE id=${setup.route.id} FOR UPDATE`,
            );
          }
          operation = Promise.resolve(
            f.management.request(patch(String(setup.route.id), controller.signal)),
          );
          if (blocker) await financialBlockedBy(db, blocker.pid);
          else await Bun.sleep(50);
          controller.abort(new Error("Route request canceled"));
          expect((await operation).status).toBe(503);
          expect(await snapshot(db)).toEqual(before);
        } finally {
          controller.abort();
          await blocker?.release();
          for (const reservation of reservations) await reservation.release();
          await operation;
        }
        expect(await snapshot(db)).toEqual(before);
        expect(
          (await f.request(`/routes/${setup.route.id}`, { enabled: false }, undefined, "PATCH"))
            .status,
        ).toBe(200);
      }),
    30000,
  );

test(
  "a real HTTP disconnect cancels a blocked route mutation before its source row is unlocked",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        before = await snapshot(db),
        blocker = await hold(
          db,
          (tx) => tx`SELECT id FROM routes WHERE id=${setup.route.id} FOR UPDATE`,
        ),
        handled = Promise.withResolvers<Response>(),
        controller = new AbortController();
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        async fetch(request) {
          try {
            const response = await f.management.fetch(request);
            handled.resolve(response);
            return response;
          } catch (error) {
            handled.reject(error);
            throw error;
          }
        },
      });
      const request = patch(String(setup.route.id), controller.signal),
        client = fetch(new URL(`/routes/${setup.route.id}`, server.url), request).catch(
          (error: unknown) => error,
        );
      let watchdog: ReturnType<typeof setTimeout> | undefined;
      try {
        await financialBlockedBy(db, blocker.pid);
        controller.abort(new Error("Disconnected management client"));
        expect(await client).toBeInstanceOf(Error);
        const response = await Promise.race([
          handled.promise,
          new Promise<never>((_, reject) => {
            watchdog = setTimeout(() => reject(new Error("DISCONNECTED_ROUTE_DID_NOT_STOP")), 1000);
          }),
        ]);
        expect(response.status).toBe(503);
        expect(await snapshot(db)).toEqual(before);
      } finally {
        clearTimeout(watchdog);
        controller.abort();
        await blocker.release();
        await client;
        await handled.promise;
        await server.stop(true);
      }
      expect(await snapshot(db)).toEqual(before);
    }),
  30000,
);

test(
  "route authentication uses its reserved connection instead of deadlocking a one-connection pool",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        identity = await credential(db),
        pool = new SQL(f.catalog.appUrl, { max: 1 }),
        app = createCoreManagementApplication({
          db: pool,
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
          encryptionKey: financialKey,
        });
      try {
        expect(
          (await app.request(patch(String(setup.route.id), undefined, identity.token))).status,
        ).toBe(200);
        expect((await db`SELECT enabled FROM routes WHERE id=${setup.route.id}`)[0].enabled).toBe(
          false,
        );
      } finally {
        await pool.close({ timeout: 1 });
      }
    }),
  30000,
);

test(
  "a credential naturally expiring in the catalog writer queue cannot commit a route update",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        identity = await credential(db);
      await db`UPDATE administrator_credentials SET expires_at=clock_timestamp()+interval '600 milliseconds' WHERE id=${identity.id}`;
      const before = await snapshot(db),
        blocker = await hold(
          db,
          (tx) =>
            tx`SELECT pg_advisory_xact_lock_shared(hashtextextended('stg.catalog.control.v1',0))`,
        );
      const operation = f.management.request(
        patch(String(setup.route.id), undefined, identity.token),
      );
      try {
        await financialBlockedBy(db, blocker.pid);
        await Bun.sleep(800);
      } finally {
        await blocker.release();
      }
      expect((await operation).status).toBe(403);
      expect(await snapshot(db)).toEqual(before);
    }),
  30000,
);

test(
  "a queued route writer stops new candidate readers for every model and releases the fence after commit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      await f.request("/models", {
        name: "second-control-model",
        operations: ["chat"],
        max_output_tokens: 100,
      });
      const blocker = await hold(
          db,
          (tx) =>
            tx`SELECT pg_advisory_xact_lock_shared(hashtextextended('stg.catalog.control.v1',0))`,
        ),
        operation = f.management.request(patch(String(setup.route.id)));
      try {
        await financialBlockedBy(db, blocker.pid);
        for (const model of [setup.model, "second-control-model"])
          await expect(db`SELECT core_candidates_v1(${model},'chat','USD')`).rejects.toMatchObject({
            errno: "P0503",
          });
      } finally {
        await blocker.release();
      }
      expect((await operation).status).toBe(200);
      for (const model of [setup.model, "second-control-model"])
        expect(
          (await db`SELECT core_candidates_v1(${model},'chat','USD') candidates`)[0].candidates,
        ).toEqual([]);
    }),
  30000,
);

test(
  "pool and catalog lock waits share the route operation deadline and cannot write after timeout",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        before = await snapshot(db),
        pool = new SQL(f.catalog.appUrl, { max: 1 }),
        reserved = await pool.reserve(),
        blocker = await hold(
          db,
          (tx) =>
            tx`SELECT pg_advisory_xact_lock_shared(hashtextextended('stg.catalog.control.v1',0))`,
        ),
        app = createCoreManagementApplication({
          db: pool,
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
          encryptionKey: financialKey,
        });
      let operation: Promise<Response> | undefined;
      try {
        const start = performance.now();
        operation = Promise.resolve(app.request(patch(String(setup.route.id))));
        await Bun.sleep(1500);
        await reserved.release();
        await financialBlockedBy(db, blocker.pid);
        const response = await operation,
          elapsed = performance.now() - start;
        expect(response.status).toBe(503);
        expect(elapsed).toBeGreaterThan(3500);
        expect(elapsed).toBeLessThan(5000);
      } finally {
        await reserved.release();
        await blocker.release();
        await operation;
        await pool.close({ timeout: 1 });
      }
      expect(await snapshot(db)).toEqual(before);
    }),
  30000,
);

test(
  "only a catalog writer can invoke the control fence while execution readers retain no writer capability",
  () =>
    withFinancial(async (f, db) => {
      const api = new SQL(f.identity.apiUrl, { max: 1 });
      try {
        await expect(api`SELECT core_catalog_write_v1()`).rejects.toMatchObject({ errno: "42501" });
        expect(
          (
            await db`SELECT count(*)::integer n FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a WHERE p.oid='core_catalog_write_v1()'::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE'`
          )[0].n,
        ).toBe(0);
        await f.catalog.appDb.begin(async (tx) => {
          await tx`SELECT core_catalog_write_v1()`;
          await expect(
            db`SELECT core_candidates_v1('unrelated','chat','USD')`,
          ).rejects.toMatchObject({ errno: "P0503" });
        });
        expect(
          (await db`SELECT core_candidates_v1('unrelated','chat','USD') candidates`)[0].candidates,
        ).toEqual([]);
      } finally {
        await api.close({ timeout: 1 });
      }
    }),
  30000,
);

test(
  "a lost route POST COMMIT reply reports an unknown result and never repeats the committed route, cost or audit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        beforeCosts = Number((await db`SELECT count(*) n FROM cost_versions`)[0].n),
        proxy = await createCommitAckProxy(),
        url = new URL(f.catalog.appUrl);
      url.port = String(proxy.port);
      url.searchParams.set("sslmode", "disable");
      const pool = new SQL(url, { max: 1 }),
        controller = new AbortController(),
        app = createCoreManagementApplication({
          db: pool,
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
          encryptionKey: financialKey,
        });
      let operation: Promise<Response> | undefined;
      try {
        await pool`SELECT 1`;
        const committed = proxy.arm([], 3000, "acknowledgement");
        operation = Promise.resolve(
          app.request(
            new Request(`http://management.test/models/${setup.model}/routes`, {
              method: "POST",
              headers: {
                authorization: `Bearer ${syntheticAdminKey}`,
                "content-type": "application/json",
              },
              body: JSON.stringify({
                provider_id: setup.provider.id,
                upstream_model: "unknown-commit-route",
                operations: ["chat"],
                pricing: {
                  currency: "USD",
                  rates: {
                    input_tokens: { numerator: "1", denominator: "3" },
                    output_tokens: { numerator: "1", denominator: "3" },
                  },
                },
                ceilings: { input_tokens: "100", output_tokens: "50" },
              }),
              signal: controller.signal,
            }),
          ),
        );
        await committed;
        controller.abort(new Error("Canceled after actual route commit"));
        const response = await operation;
        expect(response.status).toBe(503);
        expect(await response.json()).toMatchObject({
          error: { code: "management_outcome_unknown" },
        });
        const routes = await db`SELECT id FROM routes WHERE upstream_model='unknown-commit-route'`;
        expect(routes).toHaveLength(1);
        expect(Number((await db`SELECT count(*) n FROM cost_versions`)[0].n)).toBe(beforeCosts + 1);
        expect(
          Number(
            (
              await db`SELECT count(*) n FROM audit_events WHERE action='route.create' AND resource_id=${routes[0].id}`
            )[0].n,
          ),
        ).toBe(1);
      } finally {
        controller.abort();
        await proxy.close();
        await operation;
        await pool.close({ timeout: 1 });
      }
    }),
  30000,
);
