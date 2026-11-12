import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { verifier } from "@gateway/runtime/crypto";
import { createPluginCatalogStore } from "@gateway/storage";
import { withCatalog, syntheticAdminKey, syntheticPepper } from "../support/catalog.ts";
import { bundledReleaseId } from "../../apps/core-api/src/plugins/releases.ts";
import { createPlatformAuthenticator } from "../../apps/core-api/src/platform/authentication.ts";
const permissions = [
  "plugins:read",
  "plugins:install-bundled",
  "plugins:validate",
  "plugins:publish",
  "plugins:bind",
  "plugins:recover",
];
async function credential(db: SQL, granted = permissions) {
  const administrator = crypto.randomUUID(),
    id = crypto.randomUUID(),
    token = `synthetic-auth-${crypto.randomUUID()}`;
  await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${administrator},'Synthetic race operator',${granted})`;
  await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${id},${administrator},${verifier(token, syntheticPepper)},'synthetic')`;
  return { administrator, id, token };
}
type Identity = Awaited<ReturnType<typeof credential>>;
async function revoke(db: SQL, identity: Identity, mode: string) {
  if (mode === "credential")
    await db`UPDATE administrator_credentials SET enabled=false WHERE id=${identity.id}`;
  if (mode === "administrator")
    await db`UPDATE technical_administrators SET enabled=false WHERE id=${identity.administrator}`;
  if (mode === "permission")
    await db`UPDATE technical_administrators SET permissions=${["plugins:read"]} WHERE id=${identity.administrator}`;
  if (mode === "expiry")
    await db`UPDATE administrator_credentials SET expires_at=clock_timestamp()-interval '1 second' WHERE id=${identity.id}`;
  if (mode === "verifier")
    await db`UPDATE administrator_credentials SET verifier=${verifier(`rotated-${identity.token}`, syntheticPepper)} WHERE id=${identity.id}`;
}
async function blockedBy(db: SQL, blocker: number) {
  const deadline = Date.now() + 1200;
  while (Date.now() < deadline) {
    const rows =
      await db`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND ${blocker}=ANY(pg_blocking_pids(pid))`;
    if (rows[0]) return Number(rows[0].pid);
    await Bun.sleep(5);
  }
  throw new Error("Expected PostgreSQL blocking edge was absent");
}
async function snapshot(db: SQL) {
  return {
    versions: await db`SELECT * FROM plugin_versions ORDER BY id`,
    runs: await db`SELECT * FROM plugin_validation_runs ORDER BY id`,
    channels: await db`SELECT * FROM channels ORDER BY id`,
    commands: await db`SELECT * FROM execution_commands ORDER BY id`,
    receipts: await db`SELECT * FROM plugin_catalog_receipts ORDER BY command_id`,
    publications: await db`SELECT * FROM plugin_publication_receipts ORDER BY id`,
    audits: await db`SELECT * FROM audit_events ORDER BY id`,
    bindings: await db`SELECT * FROM channel_plugin_bindings ORDER BY id`,
  };
}
test(
  "revocation committed before authorization lock denies new binding without revision receipt or audit writes",
  () =>
    withCatalog(async (f, db) => {
      const publication = await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Actual signed publication for authorization races",
      });
      for (const mode of ["credential", "administrator", "permission", "expiry", "verifier"]) {
        const identity = await credential(db),
          before = await snapshot(db);
        let release!: () => void, ready!: (pid: number) => void;
        const gate = new Promise<void>((resolve) => (release = resolve)),
          locked = new Promise<number>((resolve) => (ready = resolve));
        const revoker = db.begin(async (tx) => {
          const pid = Number((await tx`SELECT pg_backend_pid() AS pid`)[0].pid);
          await revoke(tx, identity, mode);
          ready(pid);
          await gate;
        });
        const pid = await locked;
        const pending = f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          f.binding(publication.version_id),
          identity.token,
          "PUT",
        );
        try {
          await blockedBy(db, pid);
        } finally {
          release();
          await revoker;
        }
        const response = await pending;
        expect(response.status).toBe(403);
        expect(await snapshot(db)).toEqual(before);
        expect(JSON.stringify(response.value)).not.toContain(identity.token);
      }
    }),
  30000,
);
test(
  "NULL permission elements cannot authorize a binding after the revoker commits before the authorization lock",
  () =>
    withCatalog(async (f, db) => {
      const publication = await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Actual signed publication for NULL permission revocation",
      });
      const identity = await credential(db, ["plugins:bind"]),
        before = await snapshot(db);
      let release!: () => void, ready!: (pid: number) => void;
      const gate = new Promise<void>((resolve) => (release = resolve)),
        locked = new Promise<number>((resolve) => (ready = resolve));
      const revoker = db.begin(async (tx) => {
        await tx`UPDATE technical_administrators SET permissions=ARRAY['plugins:read',NULL]::text[] WHERE id=${identity.administrator}`;
        ready(Number((await tx`SELECT pg_backend_pid() AS pid`)[0].pid));
        await gate;
      });
      const blocker = await locked;
      const pending = f.request(
        `/providers/${f.provider.id}/plugin-binding`,
        f.binding(publication.version_id),
        identity.token,
        "PUT",
      );
      try {
        await blockedBy(db, blocker);
      } finally {
        release();
        await revoker;
      }
      const denied = await pending;
      expect(denied.status).toBe(403);
      expect(await snapshot(db)).toEqual(before);
      expect(JSON.stringify(denied.value)).not.toContain(identity.token);
      await db`UPDATE technical_administrators SET permissions=ARRAY['plugins:bind',NULL]::text[] WHERE id=${identity.administrator}`;
      const input = f.binding(publication.version_id);
      const permitted = await f.request(
        `/providers/${f.provider.id}/plugin-binding`,
        input,
        identity.token,
        "PUT",
      );
      expect(permitted.status).toBe(201);
      expect(permitted.value.config_revision).toBe("1");
      const after = await snapshot(db);
      expect(
        await f.request(`/providers/${f.provider.id}/plugin-binding`, input, identity.token, "PUT"),
      ).toEqual(permitted);
      expect(await snapshot(db)).toEqual(after);
    }),
  30000,
);
test(
  "restricted Core SQL denies empty NULL-only and nonmatching NULL permissions while allowing a matching permission with NULL",
  () =>
    withCatalog(async (f, db) => {
      const publication = await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Actual signed publication for direct permission boundaries",
      });
      const store = createPluginCatalogStore(f.appDb),
        auth = createPlatformAuthenticator(store, {
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
        });
      for (const mode of ["empty", "null-only", "nonmatching-null", "matching-null"]) {
        const identity = await credential(db, ["plugins:bind"]),
          actor = await auth.authenticate(`Bearer ${identity.token}`),
          before = await snapshot(db);
        if (mode === "empty")
          await db`UPDATE technical_administrators SET permissions=ARRAY[]::text[] WHERE id=${identity.administrator}`;
        if (mode === "null-only")
          await db`UPDATE technical_administrators SET permissions=ARRAY[NULL]::text[] WHERE id=${identity.administrator}`;
        if (mode === "nonmatching-null")
          await db`UPDATE technical_administrators SET permissions=ARRAY['plugins:read',NULL]::text[] WHERE id=${identity.administrator}`;
        if (mode === "matching-null")
          await db`UPDATE technical_administrators SET permissions=ARRAY['plugins:bind',NULL]::text[] WHERE id=${identity.administrator}`;
        const input = f.binding(publication.version_id);
        const operation = store.command(
          "plugin_bind_v1",
          auth.authorization(actor, "plugins:bind"),
          input.command_id,
          { ...input, channel_id: f.provider.id },
          undefined,
          f.policy.expires_at,
        );
        if (mode === "matching-null") {
          const bound = await operation;
          expect(bound.config_revision).toBe("1");
          expect((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n).toBe(1);
          expect(
            (await db`SELECT count(*)::int AS n FROM audit_events WHERE action='plugin_bound'`)[0]
              .n,
          ).toBe(1);
        } else {
          await expect(operation).rejects.toMatchObject({ status: 403 });
          expect(await snapshot(db)).toEqual(before);
        }
      }
    }),
  30000,
);
test(
  "authorization shared locks let an admitted mutation commit before a competing revoker",
  () =>
    withCatalog(async (f, db) => {
      const publication = await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Actual publication for command first races",
      });
      for (const [revision, mode] of [
        "credential",
        "administrator",
        "permission",
        "expiry",
        "verifier",
      ].entries()) {
        const identity = await credential(db);
        let release!: () => void, ready!: (pid: number) => void;
        const gate = new Promise<void>((resolve) => (release = resolve)),
          locked = new Promise<number>((resolve) => (ready = resolve));
        const channelLock = db.begin(async (tx) => {
          await tx`SELECT id FROM channels WHERE id=${f.provider.id}::uuid FOR UPDATE`;
          ready(Number((await tx`SELECT pg_backend_pid() AS pid`)[0].pid));
          await gate;
        });
        const blocker = await locked,
          input = f.binding(publication.version_id, { expected_revision: String(revision) });
        const pending = f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          input,
          identity.token,
          "PUT",
        );
        let revoker: Promise<unknown> | undefined;
        try {
          const commandPid = await blockedBy(db, blocker);
          revoker = db.begin(async (tx) => revoke(tx, identity, mode));
          await blockedBy(db, commandPid);
        } finally {
          release();
          await channelLock;
        }
        const response = await pending;
        await revoker;
        expect(response.status).toBe(201);
        expect(response.value.config_revision).toBe(String(revision + 1));
        const before = await snapshot(db);
        const denied = await f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          f.binding(publication.version_id, { expected_revision: String(revision + 1) }),
          identity.token,
          "PUT",
        );
        expect([401, 403]).toContain(denied.status);
        const revokedReplay = await f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          input,
          identity.token,
          "PUT",
        );
        expect([401, 403]).toContain(revokedReplay.status);
        expect(await snapshot(db)).toEqual(before);
      }
      expect((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n).toBe(5);
      expect(
        (await db`SELECT count(*)::int AS n FROM audit_events WHERE action='plugin_bound'`)[0].n,
      ).toBe(5);
    }),
  30000,
);
test(
  "credential naturally expiring during channel lock wait cannot commit an authenticated binding",
  () =>
    withCatalog(async (f, db) => {
      const publication = await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Actual publication for natural credential expiry",
      });
      const identity = await credential(db),
        before = await snapshot(db);
      await db`UPDATE administrator_credentials SET expires_at=clock_timestamp()+interval '600 milliseconds' WHERE id=${identity.id}`;
      let release!: () => void, ready!: (pid: number) => void;
      const gate = new Promise<void>((resolve) => (release = resolve)),
        locked = new Promise<number>((resolve) => (ready = resolve));
      const blocker = db.begin(async (tx) => {
        await tx`SELECT id FROM channels WHERE id=${f.provider.id}::uuid FOR UPDATE`;
        ready(Number((await tx`SELECT pg_backend_pid() AS pid`)[0].pid));
        await gate;
      });
      const pid = await locked;
      const pending = f.request(
        `/providers/${f.provider.id}/plugin-binding`,
        f.binding(publication.version_id),
        identity.token,
        "PUT",
      );
      try {
        await blockedBy(db, pid);
        await Bun.sleep(800);
      } finally {
        release();
        await blocker;
      }
      expect((await pending).status).toBe(403);
      expect(await snapshot(db)).toEqual(before);
    }),
  30000,
);
test(
  "stale authenticated actors cannot register claim validate publish bind or claim recovery after permissions change",
  () =>
    withCatalog(async (f, db) => {
      const uploaded = await f.application.register(
        await f.actor(),
        crypto.randomUUID(),
        "Actual signed package for permission checks",
      );
      for (const action of ["register", "validate", "publish", "bind", "recover"] as const) {
        const identity = await credential(db),
          actor = await f.application.authenticate(`Bearer ${identity.token}`),
          before = await snapshot(db);
        await db`UPDATE technical_administrators SET permissions=${["plugins:read"]} WHERE id=${identity.administrator}`;
        const input = {
          command_id: crypto.randomUUID(),
          expected_revision: "0",
          reason: "Old permission snapshot cannot authorize a new mutation",
        };
        const operation =
          action === "register"
            ? f.application.register(actor, input.command_id, input.reason)
            : action === "bind"
              ? f.application.bind(actor, f.provider.id, f.binding(uploaded.version_id))
              : action === "recover"
                ? f.application.recover(actor, uploaded.version_id, {
                    command_id: input.command_id,
                    run_id: crypto.randomUUID(),
                    lease_epoch: "1",
                    reason: input.reason,
                  })
                : f.application[action](actor, uploaded.version_id, input);
        await expect(operation).rejects.toMatchObject({ status: 403 });
        expect(await snapshot(db)).toEqual(before);
      }
    }),
  30000,
);
test(
  "credential verifier rotation fences old private authentication context but permits the replacement credential",
  () =>
    withCatalog(async (f, db) => {
      const identity = await credential(db),
        actor = await f.application.authenticate(`Bearer ${identity.token}`),
        before = await snapshot(db);
      await revoke(db, identity, "verifier");
      await expect(
        f.application.register(actor, crypto.randomUUID(), "Old verifier must fail"),
      ).rejects.toMatchObject({ status: 403 });
      expect(await snapshot(db)).toEqual(before);
      const replacement = await f.application.authenticate(`Bearer rotated-${identity.token}`);
      const registered = await f.application.register(
        replacement,
        crypto.randomUUID(),
        "Rotated credential can register",
      );
      expect(registered.state).toBe("UPLOADED");
      const receipts = await db`SELECT receipt FROM plugin_catalog_receipts`;
      const audits = await db`SELECT actor,details FROM audit_events WHERE action LIKE 'plugin_%'`;
      expect(audits[0].actor).toBe(`technical-admin:${identity.administrator}`);
      expect(JSON.stringify({ receipts, audits })).not.toContain(
        verifier(`rotated-${identity.token}`, syntheticPepper),
      );
      expect(JSON.stringify({ receipts, audits })).not.toContain(identity.token);
      const store = createPluginCatalogStore(f.appDb);
      expect(
        (await store.authenticate(verifier(`rotated-${identity.token}`, syntheticPepper)))
          ?.credential_id,
      ).toBe(identity.id);
    }),
  30000,
);
test(
  "revoking the validation initiator still permits owned cancellation cleanup reap and a fresh active slot",
  () =>
    withCatalog(async (f, db) => {
      const identity = await credential(db),
        actor = await f.application.authenticate(`Bearer ${identity.token}`);
      const uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Actual package for revoked initiator cleanup",
      );
      const controller = new AbortController();
      const pending = f.application.validate(
        actor,
        uploaded.version_id,
        {
          command_id: crypto.randomUUID(),
          expected_revision: "0",
          reason: "Revoked initiator must not strand helper",
        },
        controller.signal,
      );
      void pending.catch(() => undefined);
      let pid = 0;
      try {
        const deadline = Date.now() + 1000;
        while (Date.now() < deadline) {
          const row = (await db`SELECT resource_pid FROM plugin_validation_runs`)[0];
          if (row?.resource_pid) {
            pid = row.resource_pid;
            break;
          }
          await Bun.sleep(5);
        }
        expect(pid).toBeGreaterThan(1);
        process.kill(-pid, "SIGSTOP");
        await revoke(db, identity, "credential");
      } finally {
        controller.abort();
      }
      const result = await pending;
      expect(result.state).toBe("REJECTED");
      expect(result.result).toMatchObject({
        cleanup_confirmed: true,
        error_code: "CALL_CANCELLED",
      });
      expect(() => process.kill(-pid, 0)).toThrow();
      expect((await db`SELECT state,error_code FROM plugin_validation_runs`)[0]).toEqual({
        state: "FAILED",
        error_code: "CALL_CANCELLED",
      });
      await expect(
        f.application.publish(actor, uploaded.version_id, {
          command_id: crypto.randomUUID(),
          expected_revision: "2",
          reason: "Owned completion grants no publication authority",
        }),
      ).rejects.toMatchObject({ status: 403 });
      const retried = await f.application.validate(await f.actor(), uploaded.version_id, {
        command_id: crypto.randomUUID(),
        expected_revision: "2",
        reason: "New independently authorized validation",
      });
      expect(retried.state).toBe("VALIDATED");
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
      expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(2);
    }),
  30000,
);

test(
  "each technical action succeeds with only its own current permission and recovery cannot publish unknown work",
  () =>
    withCatalog(async (f, db) => {
      const identities = await Promise.all(
        [
          "plugins:install-bundled",
          "plugins:validate",
          "plugins:publish",
          "plugins:bind",
          "plugins:recover",
        ].map((permission) => credential(db, [permission])),
      );
      const [installer, validator, publisher, binder, recoverer] = identities;
      const actors = await Promise.all(
        identities.map((identity) => f.application.authenticate(`Bearer ${identity.token}`)),
      );
      const uploaded = await f.application.register(
        actors[0]!,
        crypto.randomUUID(),
        "Single install permission signed package",
      );
      const store = createPluginCatalogStore(f.appDb),
        auth = createPlatformAuthenticator(store, {
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
        }),
        validationActor = await auth.authenticate(`Bearer ${validator!.token}`),
        owner = crypto.randomUUID(),
        d = f.release.descriptor;
      const started = await store.command(
        "plugin_validate_v1",
        auth.authorization(validationActor, "plugins:validate"),
        crypto.randomUUID(),
        {
          version_id: uploaded.version_id,
          package_digest: d.package_digest,
          fixture_digest: d.fixture_digest,
          policy_digest: d.validation_policy_digest,
          runtime_digest: d.runtime_digest,
          config_schema_digest: d.config_schema_digest,
          expected_revision: "0",
          reason: "Single validate permission abandoned claim",
        },
        owner,
        f.policy.expires_at,
      );
      await db`UPDATE plugin_validation_runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=${started.run_id}::uuid`;
      const recovery = await f.application.recover(actors[4]!, uploaded.version_id, {
        command_id: crypto.randomUUID(),
        run_id: started.run_id,
        lease_epoch: "1",
        reason: "Single recover permission adopts no successful result",
      });
      expect(recovery.state).toBe("CLEANUP_PENDING");
      const run = await store.run(String(started.run_id));
      await store.command(
        "plugin_finish_v1",
        { identity: actors[4]!.identity },
        crypto.randomUUID(),
        {
          version_id: uploaded.version_id,
          run_id: started.run_id,
          lease_epoch: "2",
          result: { cases: 0, steps: 0, cleanup_confirmed: true, error_code: "OWNER_LOST" },
          reason: "Owned recovery confirms no helper was started",
        },
        run.lease_owner,
      );
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
      const validated = await f.application.validate(actors[1]!, uploaded.version_id, {
        command_id: crypto.randomUUID(),
        expected_revision: "3",
        reason: "Single validate permission actual fixture run",
      });
      expect(validated.result).toEqual({
        cases: 15,
        steps: 24,
        cleanup_confirmed: true,
        error_code: null,
      });
      const published = await f.application.publish(actors[2]!, uploaded.version_id, {
        command_id: crypto.randomUUID(),
        expected_revision: "5",
        reason: "Single publish permission",
      });
      expect(published.state).toBe("PUBLISHED");
      const bound = await f.application.bind(
        actors[3]!,
        f.provider.id,
        f.binding(uploaded.version_id),
      );
      expect(bound.config_revision).toBe("1");
      const rows = await db<
        { action: string; actor: string }[]
      >`SELECT action,actor FROM audit_events WHERE action IN ('plugin_uploaded','plugin_validation_started','plugin_validation_recovery','plugin_published','plugin_bound') ORDER BY created_at,id`;
      for (const [action, identity] of [
        ["plugin_uploaded", installer!],
        ["plugin_validation_started", validator!],
        ["plugin_validation_recovery", recoverer!],
        ["plugin_published", publisher!],
        ["plugin_bound", binder!],
      ] as const)
        expect(rows.filter((row) => row.action === action).map((row) => row.actor)).toEqual(
          Array(action === "plugin_validation_started" ? 2 : 1).fill(
            `technical-admin:${identity.administrator}`,
          ),
        );
    }),
  30000,
);
