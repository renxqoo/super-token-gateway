import { sign } from "node:crypto";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/plugin-sdk";
import { expect, test } from "bun:test";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createPluginCatalogStore, Store } from "@gateway/storage";
import { verifier } from "@gateway/runtime/crypto";
import { withCatalog, syntheticAdminKey, syntheticPepper } from "../support/catalog.ts";
import { bundledReleaseId } from "../../apps/core-api/src/plugins/releases.ts";
import { catalogStageCommand } from "../../apps/core-api/src/plugins/application.ts";
const install = () => ({
  command_id: crypto.randomUUID(),
  release_id: bundledReleaseId,
  reason: "Synthetic accepted builtin installation",
});
test(
  "signed builtin CLI validates 15 cases and 24 steps before atomic publication and revisioned HTTP binding",
  () =>
    withCatalog(async (f, db) => {
      const server = Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch: (request) => f.http.fetch(new Request(request.url.replace("/admin/", "/"), request)),
      });
      const input = install();
      try {
        const cli = Bun.spawn(
          [
            "bun",
            "scripts/install-bundled-release.ts",
            "--admin-url",
            `http://127.0.0.1:${server.port}/admin`,
            "--credential-file",
            f.credentialPath,
            "--command-id",
            input.command_id,
            "--release-id",
            bundledReleaseId,
            "--reason",
            input.reason,
          ],
          { env: { ...process.env, MINIMAX_API_KEY: "" }, stdout: "pipe", stderr: "pipe" },
        );
        const [output, error] = await Promise.all([
          new Response(cli.stdout).text(),
          new Response(cli.stderr).text(),
        ]);
        expect(error).toBe("");
        expect(await cli.exited).toBe(0);
        const publication = JSON.parse(output).publication;
        expect(publication.state).toBe("PUBLISHED");
        expect(publication.revision).toBe("3");
        const id = publication.version_id as string;
        const run = (
          await db`SELECT state,result,resource_pid,lease_epoch::text FROM plugin_validation_runs`
        )[0];
        expect(run.state).toBe("SUCCEEDED");
        expect(run.result).toEqual({
          cases: 15,
          steps: 24,
          cleanup_confirmed: true,
          error_code: null,
        });
        expect(run.lease_epoch).toBe("1");
        expect(run.resource_pid).toBeGreaterThan(1);
        expect(() => process.kill(-run.resource_pid, 0)).toThrow();
        expect((await db`SELECT count(*)::int AS n FROM upstream_account_balances`)[0].n).toBe(0);
        expect(
          (await f.request(`/providers/${f.provider.id}/plugin-binding`)).value.executable,
        ).toBe(false);
        const binding = f.binding(id);
        const bound = await f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          binding,
          syntheticAdminKey,
          "PUT",
        );
        expect(bound.status).toBe(201);
        expect(bound.value.config_revision).toBe("1");
        const replay = await f.request("/plugins/install-bundled-release", input);
        expect(replay.status).toBe(201);
        expect(replay.value).toEqual(publication);
        expect(
          (
            await f.request(
              `/providers/${f.provider.id}/plugin-binding`,
              binding,
              syntheticAdminKey,
              "PUT",
            )
          ).value,
        ).toEqual(bound.value);
        expect(
          (await f.request(`/providers/${f.provider.id}/plugin-binding`)).value.executable,
        ).toBe(true);
        expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(1);
        expect((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n).toBe(1);
        expect(
          (await db`SELECT count(*)::int AS n FROM audit_events WHERE action LIKE 'plugin_%'`)[0].n,
        ).toBe(6);
        expect(
          (
            await f.request("/plugins/install-bundled-release", {
              ...input,
              reason: "Changed body",
            })
          ).status,
        ).toBe(409);
        expect(
          (
            await f.request(
              `/providers/${f.provider.id}/plugin-binding`,
              { ...binding, reason: "Changed body" },
              syntheticAdminKey,
              "PUT",
            )
          ).status,
        ).toBe(409);
        expect(
          (
            await db`SELECT count(*)::int AS n FROM execution_commands WHERE request_id IS NOT NULL OR attempt_id IS NOT NULL`
          )[0].n,
        ).toBe(0);
        await f.appDb`SET gateway.catalog_mutation='on'`;
        await expect(
          f.appDb`UPDATE plugin_versions SET state='PUBLISHED' WHERE id=${id}::uuid`,
        ).rejects.toThrow();
        await expect(
          f.appDb`INSERT INTO plugin_publication_receipts SELECT * FROM plugin_publication_receipts`,
        ).rejects.toThrow();
        await writeFile(f.policyPath, JSON.stringify({ ...f.policy, revoked: true }));
        await f.reload();
        const untrusted = await f.request(`/providers/${f.provider.id}/plugin-binding`);
        expect(untrusted.value.executable).toBe(false);
        expect(untrusted.value.trust_state).toBe("UNTRUSTED");
        const historicalInstall = await f.request("/plugins/install-bundled-release", input);
        expect(historicalInstall.status).toBe(201);
        expect(historicalInstall.value).toEqual(publication);
        const historicalBinding = await f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          binding,
          syntheticAdminKey,
          "PUT",
        );
        expect(historicalBinding.status).toBe(201);
        expect(historicalBinding.value).toEqual(bound.value);
        await expect(
          db`UPDATE plugin_versions SET version='2.0.0' WHERE id=${id}::uuid`,
        ).rejects.toThrow("immutable");
      } finally {
        await server.stop(true);
      }
    }),
  30000,
);

test("public catalog rejects missing project revoked expired and insufficient platform credentials", () =>
  withCatalog(async (f, db) => {
    const input = install();
    expect((await f.request("/plugins/install-bundled-release", input, "")).status).toBe(401);
    const projects = new Store(db),
      project = await projects.createProject({
        name: "Synthetic project",
        rpm: 1,
        tpm: 1,
        concurrency: 1,
      }),
      projectToken = "synthetic-project-api-key-with-admin-role";
    await projects.createCredential(
      project,
      { name: "Synthetic project ADMIN", models: [], operations: ["chat"], role: "ADMIN" },
      verifier(projectToken, syntheticPepper),
      "project",
    );
    expect((await f.request("/plugins/install-bundled-release", input, projectToken)).status).toBe(
      401,
    );
    const admin = crypto.randomUUID(),
      token = "synthetic-technical-operator-key";
    await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${admin},'Synthetic operator',${["plugins:read"]})`;
    const credential = crypto.randomUUID();
    await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${credential},${admin},${verifier(token, syntheticPepper)},'synthetic')`;
    expect((await f.request("/plugins/install-bundled-release", input, token)).status).toBe(403);
    await db`UPDATE technical_administrators SET permissions=${["plugins:install-bundled", "plugins:validate", "plugins:publish"]} WHERE id=${admin}`;
    await db`UPDATE administrator_credentials SET enabled=false WHERE id=${credential}`;
    expect((await f.request("/plugins/install-bundled-release", input, token)).status).toBe(401);
    await db`UPDATE administrator_credentials SET enabled=true,expires_at=clock_timestamp()-interval '1 second' WHERE id=${credential}`;
    expect((await f.request("/plugins/install-bundled-release", input, token)).status).toBe(401);
    await db`UPDATE administrator_credentials SET expires_at=NULL WHERE id=${credential}`;
    await db`UPDATE technical_administrators SET enabled=false WHERE id=${admin}`;
    expect((await f.request("/plugins/install-bundled-release", input, token)).status).toBe(401);
    expect((await db`SELECT count(*)::int AS n FROM plugin_versions`)[0].n).toBe(0);
    await expect(
      f.application.installBundled(
        {
          identity: "bootstrap:forged",
          permissions: ["plugins:install-bundled", "plugins:validate", "plugins:publish"],
        },
        input,
      ),
    ).rejects.toThrow("permission");
  }));

test("external trust denies policy expiry revocation unknown keys tampered detached signatures and changed ABI schema or source", () =>
  withCatalog(async (f, db) => {
    const input = install(),
      releasePath = resolve(f.directory, bundledReleaseId, "release.json"),
      packagePath = resolve(f.directory, bundledReleaseId, "package.json");
    const packageBytes = await Bun.file(packagePath).bytes();
    for (const change of [
      { expires_at: "2020-01-01T00:00:00.000Z" },
      { revoked: true },
      { keys: [{ ...f.policy.keys[0]!, revoked: true }] },
      { keys: [{ ...f.policy.keys[0]!, id: "unknown" }] },
      { keys: [{ ...f.policy.keys[0]!, purposes: ["release"] }] },
      { releases: [{ ...f.policy.releases[0]!, revoked: true }] },
    ]) {
      await writeFile(f.policyPath, JSON.stringify({ ...f.policy, ...change }));
      await f.reload();
      const denied = await f.request("/plugins/install-bundled-release", input);
      expect(denied.status).toBe(400);
    }
    await writeFile(f.policyPath, JSON.stringify(f.policy));
    await f.reload();
    for (const changed of [
      { ...f.release, signature: { ...f.release.signature, value: "A".repeat(86) + "==" } },
      { ...f.release, descriptor: { ...f.release.descriptor, abi_version: 2 } },
      {
        ...f.release,
        descriptor: { ...f.release.descriptor, config_schema_digest: "0".repeat(64) },
      },
      {
        ...f.release,
        descriptor: { ...f.release.descriptor, source: "https://uncontrolled.invalid/module.wasm" },
      },
    ]) {
      await writeFile(releasePath, JSON.stringify(changed));
      expect((await f.request("/plugins/install-bundled-release", input)).status).toBe(400);
    }
    await writeFile(releasePath, JSON.stringify(f.release));
    const envelope = JSON.parse(new TextDecoder().decode(packageBytes));
    delete envelope.signature;
    await writeFile(packagePath, JSON.stringify(envelope));
    expect((await f.request("/plugins/install-bundled-release", input)).status).toBe(400);
    await writeFile(packagePath, Buffer.concat([packageBytes, Buffer.from(" ")]));
    expect((await f.request("/plugins/install-bundled-release", input)).status).toBe(400);
    expect((await db`SELECT count(*)::int AS n FROM plugin_versions`)[0].n).toBe(0);
  }));

test(
  "publication audit failure rolls back VALIDATED state and immutable receipt then exact retry publishes once",
  () =>
    withCatalog(async (f, db) => {
      const actor = await f.actor(),
        uploaded = await f.application.register(
          actor,
          crypto.randomUUID(),
          "Synthetic staged release",
        );
      const validated = await f.application.validate(actor, uploaded.version_id, {
        command_id: crypto.randomUUID(),
        expected_revision: "0",
        reason: "Synthetic actual fixtures",
      });
      expect(validated.state).toBe("VALIDATED");
      await db.unsafe(
        "CREATE FUNCTION reject_test_publication_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='plugin_published' THEN RAISE EXCEPTION 'synthetic audit failure';END IF;RETURN NEW;END $$; CREATE TRIGGER reject_test_publication_audit BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION reject_test_publication_audit()",
      );
      const command = {
        command_id: crypto.randomUUID(),
        expected_revision: "2",
        reason: "Synthetic atomic publish",
      };
      const failed = await f.request(`/plugins/${uploaded.version_id}/publish`, command);
      expect(failed.status).toBe(503);
      expect((await db`SELECT state,revision::text FROM plugin_versions`)[0]).toEqual({
        state: "VALIDATED",
        revision: "2",
      });
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
      await db.unsafe(
        "DROP TRIGGER reject_test_publication_audit ON audit_events; DROP FUNCTION reject_test_publication_audit()",
      );
      const results = await Promise.all([
        f.request(`/plugins/${uploaded.version_id}/publish`, command),
        f.request(`/plugins/${uploaded.version_id}/publish`, command),
      ]);
      expect(results.map((r) => r.status)).toEqual([200, 200]);
      expect(results[0]!.value).toEqual(results[1]!.value);
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(1);
      await writeFile(f.policyPath, JSON.stringify({ ...f.policy, revoked: true }));
      await f.reload();
      expect(
        (
          await f.request(
            `/providers/${f.provider.id}/plugin-binding`,
            f.binding(uploaded.version_id),
            syntheticAdminKey,
            "PUT",
          )
        ).status,
      ).toBe(400);
    }),
  30000,
);

test(
  "binding races allow one expected revision winner and deny foreign references and invalid config without new history",
  () =>
    withCatalog(async (f, db) => {
      const installed = await f.request("/plugins/install-bundled-release", install());
      expect(installed.status).toBe(201);
      const id = String(installed.value.version_id);
      for (const change of [
        { account_id: crypto.randomUUID() },
        { brand_id: crypto.randomUUID() },
        { secret_reference: `channel:${crypto.randomUUID()}` },
        { config: { model_map: {}, max_output_tokens: 0, max_input_tokens: 1, auth: "bearer" } },
        {
          config: {
            model_map: {},
            max_output_tokens: 1,
            max_input_tokens: 1,
            auth: "bearer",
            api_key: "secret",
          },
        },
      ]) {
        const denied = await f.request(
          `/providers/${f.provider.id}/plugin-binding`,
          f.binding(id, change),
          syntheticAdminKey,
          "PUT",
        );
        expect([400, 403]).toContain(denied.status);
      }
      expect((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n).toBe(0);
      const a = f.binding(id),
        b = f.binding(id);
      const results = await Promise.all([
        f.request(`/providers/${f.provider.id}/plugin-binding`, a, syntheticAdminKey, "PUT"),
        f.request(`/providers/${f.provider.id}/plugin-binding`, b, syntheticAdminKey, "PUT"),
      ]);
      expect(results.map((r) => r.status).sort()).toEqual([201, 409]);
      expect((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n).toBe(1);
      const binding = (
        await db`SELECT config_revision::text,account_id,brand_id FROM channel_plugin_bindings`
      )[0];
      expect(binding).toEqual({
        config_revision: "1",
        account_id: f.provider.account_id,
        brand_id: f.provider.brand_id,
      });
    }),
  30000,
);

test("cancelled actual validation is REJECTED with reaped helper and cannot publish", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(actor, crypto.randomUUID(), "Synthetic cancellation");
    const controller = new AbortController();
    controller.abort();
    const result = await f.application.validate(
      actor,
      uploaded.version_id,
      { command_id: crypto.randomUUID(), expected_revision: "0", reason: "Synthetic cancellation" },
      controller.signal,
    );
    expect(result.state).toBe("REJECTED");
    expect(result.result).toEqual({
      cases: 0,
      steps: 0,
      cleanup_confirmed: true,
      error_code: "CALL_CANCELLED",
    });
    expect(
      (
        await f.request(`/plugins/${uploaded.version_id}/publish`, {
          command_id: crypto.randomUUID(),
          expected_revision: "2",
          reason: "Cannot publish cancellation",
        })
      ).status,
    ).toBe(409);
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("expired validation recovery advances epoch without adopting unknown result or auto publishing", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic abandoned run",
      ),
      store = createPluginCatalogStore(db),
      id = uploaded.version_id,
      d = f.release.descriptor;
    const command = crypto.randomUUID(),
      owner = crypto.randomUUID();
    const start = await store.command(
      "plugin_validate_v1",
      { identity: "bootstrap:ADMIN_KEY", bootstrap: true },
      command,
      {
        version_id: id,
        package_digest: d.package_digest,
        fixture_digest: d.fixture_digest,
        policy_digest: d.validation_policy_digest,
        runtime_digest: d.runtime_digest,
        config_schema_digest: d.config_schema_digest,
        expected_revision: "0",
        reason: "Synthetic lost worker",
      },
      owner,
      f.policy.expires_at,
    );
    await db`UPDATE plugin_validation_runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=${start.run_id}::uuid`;
    const recovered = await f.request(`/plugins/${id}/recover`, {
      command_id: crypto.randomUUID(),
      run_id: start.run_id,
      lease_epoch: "1",
      reason: "Synthetic owner lost",
    });
    expect(recovered.status).toBe(200);
    expect(recovered.value.lease_epoch).toBe("2");
    expect(recovered.value.state).toBe("CLEANUP_PENDING");
    await expect(
      store.command(
        "plugin_finish_v1",
        { identity: actor.identity },
        catalogStageCommand(command, "stale"),
        {
          version_id: id,
          run_id: start.run_id,
          lease_epoch: "1",
          result: { cases: 15, steps: 24, cleanup_confirmed: true, error_code: null },
          reason: "Old epoch cannot succeed",
        },
        owner,
      ),
    ).rejects.toThrow("conflicts");
    expect(
      (
        await f.request(`/plugins/${id}/publish`, {
          command_id: crypto.randomUUID(),
          expected_revision: "2",
          reason: "Unknown cannot publish",
        })
      ).status,
    ).toBe(409);
    expect((await db`SELECT state,lease_epoch::text FROM plugin_validation_runs`)[0]).toEqual({
      state: "CLEANUP_PENDING",
      lease_epoch: "2",
    });
  }));

test("current trust revocation after successful fixtures blocks publication without changing VALIDATED state", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic trust recheck",
      );
    await f.application.validate(actor, uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "0",
      reason: "Actual fixture run",
    });
    await writeFile(
      f.policyPath,
      JSON.stringify({ ...f.policy, keys: [{ ...f.policy.keys[0]!, revoked: true }] }),
    );
    await f.reload();
    const rejected = await f.request(`/plugins/${uploaded.version_id}/publish`, {
      command_id: crypto.randomUUID(),
      expected_revision: "2",
      reason: "Trust was revoked",
    });
    expect(rejected.status).toBe(400);
    expect((await db`SELECT state,revision::text FROM plugin_versions`)[0]).toEqual({
      state: "VALIDATED",
      revision: "2",
    });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("receipt insertion failure rolls back publication revision and audit before retrying the same command", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic receipt rollback",
      );
    await f.application.validate(actor, uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "0",
      reason: "Actual fixture run",
    });
    await db.unsafe(
      "CREATE FUNCTION reject_test_catalog_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.receipt->>'state'='PUBLISHED' THEN RAISE EXCEPTION 'synthetic receipt failure';END IF;RETURN NEW;END $$; CREATE TRIGGER reject_test_catalog_receipt BEFORE INSERT ON plugin_catalog_receipts FOR EACH ROW EXECUTE FUNCTION reject_test_catalog_receipt()",
    );
    const command = {
      command_id: crypto.randomUUID(),
      expected_revision: "2",
      reason: "Receipt must commit atomically",
    };
    expect((await f.request(`/plugins/${uploaded.version_id}/publish`, command)).status).toBe(503);
    expect((await db`SELECT state,revision::text FROM plugin_versions`)[0]).toEqual({
      state: "VALIDATED",
      revision: "2",
    });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
    expect(
      (await db`SELECT count(*)::int AS n FROM audit_events WHERE action='plugin_published'`)[0].n,
    ).toBe(0);
    await db.unsafe(
      "DROP TRIGGER reject_test_catalog_receipt ON plugin_catalog_receipts; DROP FUNCTION reject_test_catalog_receipt()",
    );
    expect((await f.request(`/plugins/${uploaded.version_id}/publish`, command)).status).toBe(200);
  }));

test("cleanup confirmation loss persists CLEANUP_PENDING and blocks publication and overlapping validation", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic cleanup confirmation loss",
      ),
      store = createPluginCatalogStore(db),
      d = f.release.descriptor,
      owner = crypto.randomUUID();
    const start = await store.command(
      "plugin_validate_v1",
      { identity: "bootstrap:ADMIN_KEY", bootstrap: true },
      crypto.randomUUID(),
      {
        version_id: uploaded.version_id,
        package_digest: d.package_digest,
        fixture_digest: d.fixture_digest,
        policy_digest: d.validation_policy_digest,
        runtime_digest: d.runtime_digest,
        config_schema_digest: d.config_schema_digest,
        expected_revision: "0",
        reason: "Synthetic worker",
      },
      owner,
      f.policy.expires_at,
    );
    const { verifyRelease } = await import("../../apps/core-api/src/plugins/releases.ts"),
      { validateCatalogPackage } = await import("../../apps/core-api/src/plugins/validator.ts"),
      { catalogHelper } = await import("../support/catalog.ts");
    const bytes = await Bun.file(resolve(f.directory, bundledReleaseId, "package.json")).bytes(),
      verified = await verifyRelease(bytes, f.release, f.policy, d.runtime_digest);
    const actual = await validateCatalogPackage(verified.pkg, catalogHelper);
    expect(actual).toEqual({ cases: 15, steps: 24, cleanup_confirmed: true, error_code: null });
    const lostConfirmation = { ...actual, cleanup_confirmed: false, error_code: "CLEANUP_PENDING" };
    const result = await store.command(
      "plugin_finish_v1",
      { identity: actor.identity },
      crypto.randomUUID(),
      {
        version_id: uploaded.version_id,
        run_id: start.run_id,
        lease_epoch: "1",
        result: lostConfirmation,
        reason: "Synthetic lost cleanup acknowledgment after real helper completion",
      },
      owner,
      f.policy.expires_at,
    );
    expect(result.state).toBe("REJECTED");
    expect((await db`SELECT state,finished_at FROM plugin_validation_runs`)[0]).toEqual({
      state: "CLEANUP_PENDING",
      finished_at: null,
    });
    expect(
      (
        await f.request(`/plugins/${uploaded.version_id}/publish`, {
          command_id: crypto.randomUUID(),
          expected_revision: "2",
          reason: "Cleanup unknown",
        })
      ).status,
    ).toBe(409);
    await expect(
      f.application.validate(actor, uploaded.version_id, {
        command_id: crypto.randomUUID(),
        expected_revision: "2",
        reason: "Cannot replace cleanup-pending run",
      }),
    ).rejects.toThrow("conflicts");
    expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(1);
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("different publication commands at the same expected revision have one winner and stale retries cannot create receipts", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic publication race",
      );
    await f.application.validate(actor, uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "0",
      reason: "Actual fixture run",
    });
    const a = {
        command_id: crypto.randomUUID(),
        expected_revision: "2",
        reason: "Publication race A",
      },
      b = { ...a, command_id: crypto.randomUUID(), reason: "Publication race B" };
    const result = await Promise.all([
      f.request(`/plugins/${uploaded.version_id}/publish`, a),
      f.request(`/plugins/${uploaded.version_id}/publish`, b),
    ]);
    expect(result.map((r) => r.status).sort()).toEqual([200, 409]);
    expect(
      (
        await f.request(`/plugins/${uploaded.version_id}/publish`, {
          command_id: crypto.randomUUID(),
          expected_revision: "999",
          reason: "Wrong revision",
        })
      ).status,
    ).toBe(409);
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(1);
    const secondInstall = install();
    const installed = await f.request("/plugins/install-bundled-release", secondInstall);
    expect(installed.status).toBe(201);
    const secondReplay = await f.request("/plugins/install-bundled-release", secondInstall);
    expect(secondReplay.status).toBe(201);
    expect(secondReplay.value).toEqual(installed.value);
  }));

test("closing catalog while validation is starting waits for rejection and leaves no active validation lease", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic shutdown start race",
      );
    const pending = f.application.validate(actor, uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "0",
      reason: "Shutdown races persisted start",
    });
    await f.application.close();
    const result = await pending;
    expect(result.state).toBe("REJECTED");
    expect((await db`SELECT state,result FROM plugin_validation_runs`)[0]).toEqual({
      state: "FAILED",
      result: { cases: 0, steps: 0, cleanup_confirmed: true, error_code: "CALL_CANCELLED" },
    });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("package signature is still mandatory under a valid separately signed release descriptor", () =>
  withCatalog(async (f, db) => {
    const packagePath = resolve(f.directory, bundledReleaseId, "package.json"),
      releasePath = resolve(f.directory, bundledReleaseId, "release.json"),
      envelope = await Bun.file(packagePath).json();
    envelope.signature.value = "A".repeat(86) + "==";
    const bytes = new TextEncoder().encode(JSON.stringify(envelope)),
      descriptor = { ...f.release.descriptor, envelope_digest: sha256(bytes) };
    const release = {
      descriptor,
      signature: {
        algorithm: "Ed25519",
        key_id: f.release.signature.key_id,
        value: sign(
          null,
          Buffer.from(`gateway-plugin-release-v1\0${canonical(descriptor)}`),
          f.keys.privateKey,
        ).toString("base64"),
      },
    };
    await writeFile(packagePath, bytes);
    await writeFile(releasePath, JSON.stringify(release));
    await writeFile(
      f.policyPath,
      JSON.stringify({
        ...f.policy,
        releases: [{ ...f.policy.releases[0]!, descriptor_digest: sha256(canonical(descriptor)) }],
      }),
    );
    await f.reload();
    expect((await f.request("/plugins/install-bundled-release", install())).status).toBe(400);
    expect((await db`SELECT count(*)::int AS n FROM plugin_packages`)[0].n).toBe(0);
  }));

test("same immutable name version and content cannot be overwritten by differently encoded valid signed package bytes", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic immutable package body",
      );
    const packagePath = resolve(f.directory, bundledReleaseId, "package.json"),
      releasePath = resolve(f.directory, bundledReleaseId, "release.json"),
      before = await Bun.file(packagePath).bytes();
    const changed = Buffer.concat([before, Buffer.from("\n")]),
      descriptor = { ...f.release.descriptor, envelope_digest: sha256(changed) };
    const release = {
      descriptor,
      signature: {
        algorithm: "Ed25519",
        key_id: f.release.signature.key_id,
        value: sign(
          null,
          Buffer.from(`gateway-plugin-release-v1\0${canonical(descriptor)}`),
          f.keys.privateKey,
        ).toString("base64"),
      },
    };
    await writeFile(packagePath, changed);
    await writeFile(releasePath, JSON.stringify(release));
    await writeFile(
      f.policyPath,
      JSON.stringify({
        ...f.policy,
        releases: [{ ...f.policy.releases[0]!, descriptor_digest: sha256(canonical(descriptor)) }],
      }),
    );
    await f.reload();
    expect((await f.request("/plugins/install-bundled-release", install())).status).toBe(409);
    const stored = (await db`SELECT package_bytes FROM plugin_packages`)[0];
    expect(sha256(stored.package_bytes)).toBe(sha256(before));
    expect((await db`SELECT id,state,revision::text FROM plugin_versions`)[0]).toEqual({
      id: uploaded.version_id,
      state: "UPLOADED",
      revision: "0",
    });
  }));

test("expired owned helper registration recovers only after actual reap and replays the original failed recovery receipt", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(actor, crypto.randomUUID(), "Synthetic lost finish"),
      store = createPluginCatalogStore(db),
      d = f.release.descriptor,
      owner = crypto.randomUUID(),
      startCommand = crypto.randomUUID();
    const start = await store.command(
      "plugin_validate_v1",
      { identity: "bootstrap:ADMIN_KEY", bootstrap: true },
      startCommand,
      {
        version_id: uploaded.version_id,
        package_digest: d.package_digest,
        fixture_digest: d.fixture_digest,
        policy_digest: d.validation_policy_digest,
        runtime_digest: d.runtime_digest,
        config_schema_digest: d.config_schema_digest,
        expected_revision: "0",
        reason: "Synthetic worker",
      },
      owner,
      f.policy.expires_at,
    );
    const { verifyRelease } = await import("../../apps/core-api/src/plugins/releases.ts"),
      { validateCatalogPackage } = await import("../../apps/core-api/src/plugins/validator.ts"),
      { catalogHelper } = await import("../support/catalog.ts");
    const verified = await verifyRelease(
      await Bun.file(resolve(f.directory, bundledReleaseId, "package.json")).bytes(),
      f.release,
      f.policy,
      d.runtime_digest,
    );
    const result = await validateCatalogPackage(verified.pkg, catalogHelper, {
      onResource: (resource) =>
        store
          .command(
            "plugin_resources_v1",
            { identity: actor.identity },
            catalogStageCommand(startCommand, "resources"),
            {
              version_id: uploaded.version_id,
              run_id: start.run_id,
              lease_epoch: "1",
              pid: resource.pid,
              connection: resource.connection,
              reason: "Synthetic ownership registration",
            },
            owner,
          )
          .then(() => undefined),
    });
    expect(result.cleanup_confirmed).toBe(true);
    await db`UPDATE plugin_validation_runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=${start.run_id}::uuid`;
    const input = {
      command_id: crypto.randomUUID(),
      run_id: start.run_id,
      lease_epoch: "1",
      reason: "Lost worker completion result stays unknown",
    };
    const recovered = await f.request(`/plugins/${uploaded.version_id}/recover`, input);
    expect(recovered.status).toBe(200);
    expect(recovered.value.state).toBe("REJECTED");
    const replay = await f.request(`/plugins/${uploaded.version_id}/recover`, input);
    expect(replay.status).toBe(200);
    expect(replay.value).toEqual(recovered.value);
    expect(
      (await db`SELECT state,lease_epoch::text,error_code FROM plugin_validation_runs`)[0],
    ).toEqual({ state: "FAILED", lease_epoch: "2", error_code: "OWNER_LOST" });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("release policy and signing key expiring while publication waits on a lock cannot commit PUBLISHED", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic locked trust expiry",
      );
    await f.application.validate(actor, uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "0",
      reason: "Actual fixture run",
    });
    for (const expiry of ["policy", "key"] as const) {
      const until = new Date(Date.now() + 600).toISOString();
      const policy =
        expiry === "policy"
          ? { ...f.policy, expires_at: until }
          : { ...f.policy, keys: [{ ...f.policy.keys[0]!, expires_at: until }] };
      await writeFile(f.policyPath, JSON.stringify(policy));
      await f.reload();
      let release!: () => void, locked!: () => void;
      const gate = new Promise<void>((r) => (release = r)),
        ready = new Promise<void>((r) => (locked = r));
      const blocker = db.begin(async (tx) => {
        await tx`SELECT id FROM plugin_versions WHERE id=${uploaded.version_id}::uuid FOR UPDATE`;
        locked();
        await gate;
      });
      await ready;
      const input = {
        command_id: crypto.randomUUID(),
        expected_revision: "2",
        reason: `Synthetic ${expiry} expiry during lock wait`,
      };
      const pending = f.request(`/plugins/${uploaded.version_id}/publish`, input);
      try {
        let blocked = false;
        const deadline = Date.now() + 500;
        while (Date.now() < deadline) {
          const active =
            await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND cardinality(pg_blocking_pids(pid))>0`;
          if (active.length) {
            blocked = true;
            break;
          }
          await Bun.sleep(5);
        }
        expect(blocked).toBe(true);
        await Bun.sleep(800);
      } finally {
        release();
        await blocker;
      }
      expect((await pending).status).toBe(409);
      expect((await db`SELECT state,revision::text FROM plugin_versions`)[0]).toEqual({
        state: "VALIDATED",
        revision: "2",
      });
      expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
    }
  }));

test("policy expiry does not block cancelled owned validation from reaping and releasing its active slot", () =>
  withCatalog(async (f, db) => {
    const initialActor = await f.actor(),
      uploaded = await f.application.register(
        initialActor,
        crypto.randomUUID(),
        "Synthetic policy expiry cleanup",
      );
    const until = new Date(Date.now() + 600).toISOString();
    await writeFile(f.policyPath, JSON.stringify({ ...f.policy, expires_at: until }));
    await f.reload();
    const controller = new AbortController(),
      pending = f.application.validate(
        await f.actor(),
        uploaded.version_id,
        {
          command_id: crypto.randomUUID(),
          expected_revision: "0",
          reason: "Trust expires during owned validation",
        },
        controller.signal,
      );
    void pending.catch(() => undefined);
    let pid = 0;
    try {
      const deadline = Date.now() + 500;
      while (Date.now() < deadline) {
        const row = (await db`SELECT resource_pid,state FROM plugin_validation_runs`)[0];
        if (row?.resource_pid) {
          pid = row.resource_pid;
          break;
        }
        await Bun.sleep(5);
      }
      expect(pid).toBeGreaterThan(1);
      process.kill(-pid, "SIGSTOP");
      await Bun.sleep(Math.max(1, Date.parse(until) - Date.now() + 50));
    } finally {
      controller.abort();
    }
    const result = await pending;
    expect(result.state).toBe("REJECTED");
    expect(result.result).toEqual({
      cases: 0,
      steps: 0,
      cleanup_confirmed: true,
      error_code: "CALL_CANCELLED",
    });
    expect(() => process.kill(-pid, 0)).toThrow();
    expect((await db`SELECT state,error_code FROM plugin_validation_runs`)[0]).toEqual({
      state: "FAILED",
      error_code: "CALL_CANCELLED",
    });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
    await writeFile(f.policyPath, JSON.stringify(f.policy));
    await f.reload();
    const retried = await f.application.validate(await f.actor(), uploaded.version_id, {
      command_id: crypto.randomUUID(),
      expected_revision: "2",
      reason: "Explicit new validation after failed owned run",
    });
    expect(retried.state).toBe("VALIDATED");
    expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(2);
  }));

test("lost finish transaction cannot rerun the same persisted active validation and owned cleanup remains explicit", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic lost finish regression",
      ),
      input = {
        command_id: crypto.randomUUID(),
        expected_revision: "0",
        reason: "Actual helper succeeds before finish transaction failure",
      };
    await db.unsafe(
      "CREATE FUNCTION reject_lost_validation_finish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='VALIDATED' THEN RAISE EXCEPTION 'synthetic lost finish';END IF;RETURN NEW;END $$; CREATE TRIGGER reject_lost_validation_finish BEFORE UPDATE ON plugin_versions FOR EACH ROW EXECUTE FUNCTION reject_lost_validation_finish()",
    );
    expect((await f.request(`/plugins/${uploaded.version_id}/validate`, input)).status).toBe(503);
    await db.unsafe(
      "DROP TRIGGER reject_lost_validation_finish ON plugin_versions; DROP FUNCTION reject_lost_validation_finish()",
    );
    const before = (
      await db`SELECT id,state,resource_pid,lease_epoch::text FROM plugin_validation_runs`
    )[0];
    expect(before.state).toBe("RUNNING");
    expect(() => process.kill(-before.resource_pid, 0)).toThrow();
    const replay = await f.request(`/plugins/${uploaded.version_id}/validate`, input);
    expect(replay.status).toBe(409);
    expect(
      (await db`SELECT id,state,resource_pid,lease_epoch::text FROM plugin_validation_runs`)[0],
    ).toEqual(before);
    expect(
      (
        await db`SELECT count(*)::int AS n FROM audit_events WHERE action='plugin_validation_resources'`
      )[0].n,
    ).toBe(1);
    await db`UPDATE plugin_validation_runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=${before.id}::uuid`;
    const recovery = await f.request(`/plugins/${uploaded.version_id}/recover`, {
      command_id: crypto.randomUUID(),
      run_id: before.id,
      lease_epoch: "1",
      reason: "Explicit cleanup after unknown finish outcome",
    });
    expect(recovery.status).toBe(200);
    expect(recovery.value.state).toBe("REJECTED");
    expect((await db`SELECT state,error_code FROM plugin_validation_runs`)[0]).toEqual({
      state: "FAILED",
      error_code: "OWNER_LOST",
    });
    expect((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n).toBe(0);
  }));

test("concurrent validation replays share one real run and completed receipt stays queryable after policy expiry", () =>
  withCatalog(async (f, db) => {
    const actor = await f.actor(),
      uploaded = await f.application.register(
        actor,
        crypto.randomUUID(),
        "Synthetic shared validation command",
      ),
      input = {
        command_id: crypto.randomUUID(),
        expected_revision: "0",
        reason: "One public validation intent",
      };
    const first = f.application.validate(actor, uploaded.version_id, input),
      second = f.application.validate(actor, uploaded.version_id, input);
    await expect(
      Promise.resolve().then(() =>
        f.application.validate(actor, uploaded.version_id, {
          ...input,
          reason: "Changed active command body",
        }),
      ),
    ).rejects.toMatchObject({ status: 409, code: "catalog_conflict" });
    const results = await Promise.all([first, second]);
    expect(results[0]).toEqual(results[1]);
    expect(results[0]!.state).toBe("VALIDATED");
    expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(1);
    await writeFile(
      f.policyPath,
      JSON.stringify({ ...f.policy, expires_at: "2020-01-01T00:00:00.000Z" }),
    );
    await f.reload();
    const replay = await f.request(`/plugins/${uploaded.version_id}/validate`, input);
    expect(replay.status).toBe(200);
    expect(replay.value).toEqual(results[0]!);
    expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(1);
  }));
