import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { resolve } from "node:path";
import { createPluginCatalogStore, type CatalogKind } from "@gateway/storage";
import { withCatalog, catalogHelper } from "../support/catalog.ts";
import { bundledReleaseId, verifyRelease } from "../../apps/core-api/src/plugins/releases.ts";
import { validateCatalogPackage } from "../../apps/core-api/src/plugins/validator.ts";
const bootstrap = { identity: "bootstrap:ADMIN_KEY", bootstrap: true } as const;
const cancelled = { cases: 0, steps: 0, cleanup_confirmed: true, error_code: "CALL_CANCELLED" };
async function snapshot(db: SQL) {
  return {
    versions:
      await db`SELECT id,state,revision::text,validation_result FROM plugin_versions ORDER BY id`,
    runs: await db`SELECT * FROM plugin_validation_runs ORDER BY id`,
    commands: await db`SELECT * FROM execution_commands ORDER BY id`,
    receipts: await db`SELECT * FROM plugin_catalog_receipts ORDER BY command_id`,
    audits: await db`SELECT * FROM audit_events ORDER BY id`,
    bindings: await db`SELECT * FROM channel_plugin_bindings ORDER BY id`,
  };
}
test(
  "restricted Core SQL rejects absent null malformed wrong or stale lease identity without partial completion resources or recovery",
  () =>
    withCatalog(async (f, db) => {
      const store = createPluginCatalogStore(f.appDb),
        uploaded = await f.application.register(
          await f.actor(),
          crypto.randomUUID(),
          "Actual signed package for lease guards",
        ),
        owner = crypto.randomUUID(),
        d = f.release.descriptor;
      const started = await store.command(
        "plugin_validate_v1",
        bootstrap,
        crypto.randomUUID(),
        {
          version_id: uploaded.version_id,
          package_digest: d.package_digest,
          fixture_digest: d.fixture_digest,
          policy_digest: d.validation_policy_digest,
          runtime_digest: d.runtime_digest,
          config_schema_digest: d.config_schema_digest,
          expected_revision: "0",
          reason: "New claim deliberately requires no old epoch",
        },
        owner,
        f.policy.expires_at,
      );
      const base = {
        version_id: uploaded.version_id,
        run_id: started.run_id,
        lease_epoch: "1",
        reason: "Malformed lease command must be atomic",
        result: cancelled,
        pid: process.pid,
        connection: "synthetic-unregistered-connection",
      };
      const rejected = async (
        kind: CatalogKind,
        body: Record<string, unknown>,
        claimedOwner: string | undefined,
      ) => {
        const before = await snapshot(db);
        let status: number | undefined;
        try {
          await store.command(kind, bootstrap, crypto.randomUUID(), body, claimedOwner);
        } catch (error) {
          status = (error as { status?: number }).status;
        }
        expect([400, 409].includes(status ?? 0)).toBe(true);
        expect(await snapshot(db)).toEqual(before);
      };
      for (const kind of [
        "plugin_finish_v1",
        "plugin_resources_v1",
        "plugin_recover_v1",
      ] as const) {
        if (kind === "plugin_recover_v1")
          await db`UPDATE plugin_validation_runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=${started.run_id}::uuid`;
        for (const epoch of [
          undefined,
          null,
          "",
          1,
          true,
          ["1"],
          {},
          "01",
          "1.0",
          "1e0",
          "0",
          "-1",
          "1".repeat(39),
          "2",
        ]) {
          const body: Record<string, unknown> = { ...base };
          if (epoch === undefined) delete body.lease_epoch;
          else body.lease_epoch = epoch;
          await rejected(kind, body, owner);
        }
        for (const missingOwner of [undefined, "", "not-a-uuid"])
          await rejected(kind, base, missingOwner);
        for (const invalidOwner of [null, 1, true, {}]) {
          const before = await snapshot(db);
          let rejectedOwner = false;
          try {
            await f.appDb`SELECT plugin_catalog_command_v1(${kind},${bootstrap.identity},${crypto.randomUUID()}::uuid,${base}::jsonb,${invalidOwner}::uuid,NULL,NULL,NULL,NULL,true)`;
          } catch (error) {
            rejectedOwner = ["P0409", "22P02", "42846"].includes(
              (error as { errno: string }).errno,
            );
          }
          expect(rejectedOwner).toBe(true);
          expect(await snapshot(db)).toEqual(before);
        }
        const beforeArrayOwner = await snapshot(db);
        await expect(
          f.appDb`SELECT plugin_catalog_command_v1(${kind},${bootstrap.identity},${crypto.randomUUID()}::uuid,${base}::jsonb,ARRAY[${owner}::uuid]::uuid,NULL,NULL,NULL,NULL,true)`,
        ).rejects.toMatchObject({ errno: "42846" });
        expect(await snapshot(db)).toEqual(beforeArrayOwner);
        for (const field of ["version_id", "run_id"]) {
          const body: Record<string, unknown> = { ...base };
          delete body[field];
          await rejected(kind, body, owner);
          await rejected(kind, { ...base, [field]: null }, owner);
        }
        if (kind !== "plugin_recover_v1") await rejected(kind, base, crypto.randomUUID());
        if (kind === "plugin_finish_v1") {
          for (const field of ["cases", "steps", "cleanup_confirmed"]) {
            const missing: Record<string, unknown> = { ...cancelled };
            delete missing[field];
            await rejected(kind, { ...base, result: missing }, owner);
            for (const value of [null, "0", [], {}])
              await rejected(kind, { ...base, result: { ...cancelled, [field]: value } }, owner);
          }
          await rejected(kind, { ...base, result: null }, owner);
        }
        if (kind === "plugin_resources_v1") {
          for (const field of ["pid", "connection"]) {
            const missing: Record<string, unknown> = { ...base };
            delete missing[field];
            await rejected(kind, missing, owner);
            for (const value of [null, "", [], {}, false])
              await rejected(kind, { ...base, [field]: value }, owner);
          }
          for (const pid of [0, 1, -2, 2.5, 2147483648])
            await rejected(kind, { ...base, pid }, owner);
        }
      }
      const newOwner = crypto.randomUUID();
      const recovered = await store.command(
        "plugin_recover_v1",
        bootstrap,
        crypto.randomUUID(),
        base,
        newOwner,
      );
      expect(recovered.lease_epoch).toBe("2");
      expect(recovered.state).toBe("CLEANUP_PENDING");
      for (const kind of ["plugin_finish_v1", "plugin_resources_v1"] as const) {
        await rejected(kind, { ...base, lease_epoch: "2" }, owner);
        await rejected(kind, base, newOwner);
      }
      const finished = await store.command(
        "plugin_finish_v1",
        { identity: bootstrap.identity },
        crypto.randomUUID(),
        { ...base, lease_epoch: "2", result: { ...cancelled, error_code: "OWNER_LOST" } },
        newOwner,
      );
      expect(finished.state).toBe("REJECTED");
      expect(
        (
          await db`SELECT state,lease_owner,lease_epoch::text,resource_pid,resource_connection FROM plugin_validation_runs`
        )[0],
      ).toEqual({
        state: "FAILED",
        lease_owner: newOwner,
        lease_epoch: "2",
        resource_pid: null,
        resource_connection: null,
      });
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
    }),
  30000,
);
test(
  "restricted Core SQL rejects missing null typed or noncanonical expected revisions before any new lifecycle writes",
  () =>
    withCatalog(async (f, db) => {
      const store = createPluginCatalogStore(f.appDb),
        uploaded = await f.application.register(
          await f.actor(),
          crypto.randomUUID(),
          "Actual signed package for revision guards",
        ),
        d = f.release.descriptor;
      const base: Record<string, unknown> = {
        ...f.binding(uploaded.version_id),
        channel_id: f.provider.id,
        package_digest: d.package_digest,
        fixture_digest: d.fixture_digest,
        policy_digest: d.validation_policy_digest,
        runtime_digest: d.runtime_digest,
        config_schema_digest: d.config_schema_digest,
      };
      for (const kind of ["plugin_validate_v1", "plugin_publish_v1", "plugin_bind_v1"] as const) {
        for (const revision of [
          undefined,
          null,
          "",
          0,
          false,
          {},
          [],
          "00",
          "-1",
          "1.0",
          "1e0",
          "1".repeat(39),
        ]) {
          const body = { ...base };
          if (revision === undefined) delete body.expected_revision;
          else body.expected_revision = revision;
          const before = await snapshot(db);
          await expect(
            store.command(
              kind,
              bootstrap,
              crypto.randomUUID(),
              body,
              kind === "plugin_validate_v1" ? crypto.randomUUID() : undefined,
              f.policy.expires_at,
            ),
          ).rejects.toMatchObject({ status: 400 });
          expect(await snapshot(db)).toEqual(before);
        }
      }
    }),
  30000,
);
test(
  "correct owned resources and finish still record actual helper results and exact replay without a credential grant",
  () =>
    withCatalog(async (f, db) => {
      const store = createPluginCatalogStore(f.appDb),
        actor = await f.actor(),
        uploaded = await f.application.register(
          actor,
          crypto.randomUUID(),
          "Actual owned lease positive path",
        ),
        owner = crypto.randomUUID(),
        d = f.release.descriptor;
      const started = await store.command(
        "plugin_validate_v1",
        bootstrap,
        crypto.randomUUID(),
        {
          version_id: uploaded.version_id,
          package_digest: d.package_digest,
          fixture_digest: d.fixture_digest,
          policy_digest: d.validation_policy_digest,
          runtime_digest: d.runtime_digest,
          config_schema_digest: d.config_schema_digest,
          expected_revision: "0",
          reason: "Authorized new claim with no old epoch",
        },
        owner,
        f.policy.expires_at,
      );
      const pkg = await verifyRelease(
        await Bun.file(resolve(f.directory, bundledReleaseId, "package.json")).bytes(),
        f.release,
        f.policy,
        d.runtime_digest,
      );
      const body = {
        version_id: uploaded.version_id,
        run_id: started.run_id,
        lease_epoch: "1",
        reason: "Exact owned worker cleanup",
      };
      let pid = 0;
      const result = await validateCatalogPackage(pkg.pkg, catalogHelper, {
        onResource: async (resource) => {
          pid = resource.pid;
          await store.command(
            "plugin_resources_v1",
            { identity: actor.identity },
            crypto.randomUUID(),
            { ...body, pid: resource.pid, connection: resource.connection },
            owner,
          );
        },
      });
      expect(result).toEqual({ cases: 15, steps: 24, cleanup_confirmed: true, error_code: null });
      expect(() => process.kill(-pid, 0)).toThrow();
      const command = crypto.randomUUID(),
        finishBody = { ...body, result };
      const finished = await store.command(
        "plugin_finish_v1",
        { identity: actor.identity },
        command,
        finishBody,
        owner,
      );
      expect(finished.state).toBe("VALIDATED");
      const before = await snapshot(db);
      expect(
        await store.command(
          "plugin_finish_v1",
          { identity: actor.identity },
          command,
          finishBody,
          owner,
        ),
      ).toEqual(finished);
      expect(await snapshot(db)).toEqual(before);
    }),
  30000,
);
