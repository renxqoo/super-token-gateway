import { expect, test } from "bun:test";
import { SQL } from "bun";
import { resolve } from "node:path";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { prepBindingV2, type ExecutionRuntimeV1 } from "@gateway/contracts";
import { packageTrust, verifyPackage, openAIChatReleaseId } from "@gateway/plugin-sdk";
import { approvePluginRuntime } from "../../scripts/runtime-approval.ts";
import { catalogHelper, withCatalog } from "../support/catalog.ts";
import {
  prepareFinancialAdmission,
  withFinancial,
  financialBlockedBy,
} from "../support/financial-execution.ts";
import { financialRejection } from "../support/financial-rejection.ts";
import { chatReplies, withGatewayStack } from "../support/gateway-process.ts";

function selectRuntime(
  prepared: Awaited<ReturnType<typeof prepareFinancialAdmission>>,
  runtime: ExecutionRuntimeV1,
) {
  prepared.data.dispatch_projection.execution = runtime;
  prepared.data.dispatched_digest = sha256(
    canonical(["stg.internal.dispatch-projection", "2", prepared.data.dispatch_projection]),
  );
  prepared.data.prep_binding_digest = sha256(
    canonical([
      "stg.internal.prep-binding",
      "2",
      prepBindingV2({
        owner: prepared.context.owner,
        project_id: prepared.context.project_id,
        projection: {
          ...prepared.data.dispatch_projection,
          version: 2,
          method: "POST",
          operation: "chat",
          auth: { type: "bearer" },
        },
        prep_session_id: prepared.data.prep_session_id,
        host_config_sha256: sha256(canonical(prepared.candidate.config)),
      }),
    ]),
  );
}

test(
  "PG rejects a Gateway's unapproved execution digest before creating any money or send permit",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client);
      const api = new SQL(f.identity.apiUrl, { max: 1 });
      try {
        const [role] = await api`SELECT current_user name`;
        expect(
          (
            await db`SELECT has_schema_privilege(${role.name},'core_execution_runtime','USAGE') allowed`
          )[0].allowed,
        ).toBe(false);
        for (const fn of [
          "core_execution_runtime.allowed_v1(jsonb)",
          "core_execution_runtime.approve_v1(text,jsonb,jsonb,boolean,text,text)",
        ])
          expect(
            (await db`SELECT has_function_privilege(${role.name},${fn},'EXECUTE') allowed`)[0]
              .allowed,
          ).toBe(false);
        const denied = await api`SELECT * FROM core_execution_runtime.approvals`.catch(
          (error: unknown) => error,
        );
        expect(denied).toMatchObject({ errno: "42501" });
        const [owner] =
          await db`SELECT pg_get_userbyid(nspowner) name FROM pg_namespace WHERE nspname='core_execution_runtime'`;
        const escalation = await api
          .unsafe(`SET ROLE "${String(owner.name).replaceAll('"', '""')}"`)
          .catch((error: unknown) => error);
        expect(escalation).toMatchObject({ errno: "42501" });
      } finally {
        await api.close();
      }
      selectRuntime(prepared, {
        version: 1,
        kind: "builtin-worker",
        artifact_digest: "a".repeat(64),
      });
      expect((await financialRejection(prepared.admit(), 409)).receipt.code).toBe(
        "candidate_stale",
      );
      expect((await db`SELECT count(*)::integer n FROM execution_permits`)[0].n).toBe(0);
      expect((await db`SELECT count(*)::integer n FROM gateway_requests`)[0].n).toBe(0);
      expect(
        (
          await db`SELECT reserved_atoms::text reserved FROM core_upstream_balances_v1 WHERE account_id=${setup.provider.account_id}`
        )[0].reserved,
      ).toBe("0");
    }),
  30000,
);

test(
  "a warm runtime reader fails promptly instead of deadlocking with approval TRUNCATE waiting on its epoch",
  () =>
    withCatalog(async (_f, db) => {
      const held = Promise.withResolvers<number>();
      const resume = Promise.withResolvers<void>();
      const reader = db
        .begin(async (tx) => {
          await tx`SELECT singleton FROM core_catalog_cache.epoch FOR SHARE`;
          held.resolve((await tx`SELECT pg_backend_pid() pid`)[0].pid);
          await resume.promise;
          await tx`SELECT core_execution_runtime.allowed_v1('[]'::jsonb)`;
        })
        .catch((error: unknown) => {
          held.reject(error);
          return error;
        });
      let writer: Promise<unknown> | undefined;
      try {
        const pid = await held.promise;
        writer = db`TRUNCATE core_execution_runtime.approvals`.then((rows) => rows);
        await financialBlockedBy(db, pid);
        resume.resolve();
        expect(await reader).toMatchObject({ errno: "55P03" });
        await writer;
        expect(
          (
            await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_runtime_approval' AND details->>'operation'='TRUNCATE'`
          )[0].n,
        ).toBe(1);
      } finally {
        resume.resolve();
        await reader;
        await writer;
      }
    }),
  30000,
);

test(
  "owner-verified Worker approval changes the actual Gateway send identity online and revocation returns future requests to approved Wasm",
  () =>
    withGatewayStack({ reply: () => chatReplies.json("runtime-canary") }, async (stack) => {
      const first = await stack.send();
      expect(first.status).toBe(200);
      await first.arrayBuffer();
      const [before] =
        await stack.db`SELECT snapshot->'dispatch_projection'->'execution' execution,snapshot->'dispatch_projection'->'candidate_ref'->>'package_digest' package FROM execution_permits ORDER BY created_at`;
      expect(before.execution.kind).toBe("wasm");
      const approved = await approvePluginRuntime(stack.db, {
        packageDigest: before.package,
        kind: "builtin-worker",
        policyFile: stack.f.catalog.policyPath,
        helperExecutable: catalogHelper,
        builtinDirectory: resolve(".local/builtin-adapters"),
        actor: "integration-owner",
        reason: "Verify the complete runtime binding",
      });
      const second = await stack.send();
      expect(second.status).toBe(200);
      expect(await second.text()).toContain("runtime-canary");
      const [sent] =
        await stack.db`SELECT snapshot,authorization_plan_digest FROM execution_permits ORDER BY created_at DESC LIMIT 1`;
      expect(sent.snapshot.version).toBe(2);
      expect(sent.snapshot.dispatch_projection.execution).toEqual(approved.execution);
      expect(sent.authorization_plan_digest).toBe(
        sha256(canonical(["stg.internal.host-authorization-plan", "2", sent.snapshot])),
      );
      const ownerUrl = new URL(process.env.DATABASE_URL!);
      const database = (await stack.db`SELECT current_database() name`)[0].name;
      ownerUrl.pathname = `/${database}`;
      const revocation = Bun.spawn(
        [
          process.execPath,
          "scripts/approve-plugin-runtime.ts",
          "--database",
          database,
          "--package-digest",
          before.package,
          "--kind",
          "builtin-worker",
          "--artifact-digest",
          approved.execution.artifact_digest,
          "--disable",
          "--actor",
          "integration-owner",
          "--reason",
          "CLI revocation boundary",
        ],
        {
          env: { ...process.env, MIGRATION_DATABASE_URL: ownerUrl.toString() },
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        },
      );
      const [code, stdout, stderr] = await Promise.all([
        revocation.exited,
        new Response(revocation.stdout).text(),
        new Response(revocation.stderr).text(),
      ]);
      expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
      expect(JSON.parse(stdout)).toMatchObject({ active: false, execution: approved.execution });
      const third = await stack.send();
      expect(third.status).toBe(200);
      await third.arrayBuffer();
      const [after] =
        await stack.db`SELECT snapshot->'dispatch_projection'->'execution' execution FROM execution_permits ORDER BY created_at DESC LIMIT 1`;
      expect(after.execution).toEqual(before.execution);
      expect(
        (
          await stack.db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_runtime_approval'`
        )[0].n,
      ).toBe(2);
    }),
  30000,
);

test(
  "approval added after a context was issued cannot widen that context's send capabilities",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client);
      const approved = await approvePluginRuntime(db, {
        packageDigest: prepared.candidate.ref.package_digest,
        kind: "builtin-worker",
        policyFile: f.catalog.policyPath,
        helperExecutable: catalogHelper,
        builtinDirectory: resolve(".local/builtin-adapters"),
        actor: "integration-owner",
        reason: "Context capability boundary",
      });
      selectRuntime(prepared, approved.execution);
      expect((await financialRejection(prepared.admit(), 409)).receipt.code).toBe(
        "candidate_stale",
      );
      expect((await db`SELECT count(*)::integer n FROM execution_permits`)[0].n).toBe(0);
    }),
  30000,
);

test(
  "begin rejects a revoked exact Worker build while the reserved permit remains abortable and immutable",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      await client.startup();
      const initial = await prepareFinancialAdmission(setup, client);
      const approved = await approvePluginRuntime(db, {
        packageDigest: initial.candidate.ref.package_digest,
        kind: "builtin-worker",
        policyFile: f.catalog.policyPath,
        helperExecutable: catalogHelper,
        builtinDirectory: resolve(".local/builtin-adapters"),
        actor: "integration-owner",
        reason: "Begin revocation boundary",
      });
      const prepared = await prepareFinancialAdmission(setup, client);
      selectRuntime(prepared, approved.execution);
      const admitted = await prepared.admit();
      expect(admitted.receipt.authorization_plan?.dispatch_projection).toMatchObject({
        version: 2,
        execution: approved.execution,
      });
      const changed =
        await db`UPDATE core_execution_runtime.approvals SET active=false WHERE package_digest=${initial.candidate.ref.package_digest} RETURNING artifact_digest`;
      expect(changed).toHaveLength(1);
      expect(
        (
          await financialRejection(
            client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
              deadlineAt: prepared.deadline,
            }),
            409,
          )
        ).receipt.code,
      ).toBe("candidate_stale");
      const [permit] =
        await db`SELECT snapshot,state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`;
      expect(permit.state).toBe("RESERVED");
      const error =
        await db`UPDATE execution_permits SET snapshot=jsonb_set(snapshot,'{dispatch_projection,execution,artifact_digest}',${JSON.stringify("0".repeat(64))}::jsonb) WHERE attempt_id=${prepared.attemptId}`.catch(
          (failure: unknown) => failure,
        );
      expect(error).toBeInstanceOf(Error);
      const aborted = await client.execution("execution_abort_v1", {
        attempt_id: prepared.attemptId,
        permit_epoch: "1",
        reason: "PREPARE_DISPOSED",
        close_request: true,
      });
      expect(aborted).toMatchObject({ receipt: { state: "ABORTED" } });
      expect(
        (await db`SELECT snapshot FROM execution_permits WHERE attempt_id=${prepared.attemptId}`)[0]
          .snapshot,
      ).toEqual(permit.snapshot);
    }),
  30000,
);

test(
  "runtime catalog roles cannot read, write or approve private execution builds",
  () =>
    withCatalog(async (f, db) => {
      const [role] = await f.appDb`SELECT current_user name`;
      for (const privilege of ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"])
        expect(
          (
            await db`SELECT has_table_privilege(${role.name},'core_execution_runtime.approvals',${privilege}) allowed`
          )[0].allowed,
        ).toBe(false);
      expect(
        (
          await db`SELECT has_schema_privilege(${role.name},'core_execution_runtime','USAGE') allowed`
        )[0].allowed,
      ).toBe(false);
      expect(
        (
          await db`SELECT has_function_privilege(${role.name},'core_execution_runtime.approve_v1(text,jsonb,jsonb,boolean,text,text)','EXECUTE') allowed`
        )[0].allowed,
      ).toBe(false);
    }),
  30000,
);

test(
  "legacy packages with no release descriptor gain only a new verified Wasm attestation without invented historical metadata",
  () =>
    withCatalog(async (f, db) => {
      const bytes = await Bun.file(
        resolve(f.directory, openAIChatReleaseId, "package.json"),
      ).bytes();
      const digest = f.release.descriptor.package_digest;
      const pkg = verifyPackage(bytes, packageTrust(f.policy)(digest));
      const envelope = JSON.parse(new TextDecoder().decode(bytes));
      await db`INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by) VALUES(${digest},${bytes},${pkg.manifest}::jsonb,${envelope.signature.value},${pkg.signingKeyId},'legacy-fixture')`;
      const approved = await approvePluginRuntime(db, {
        packageDigest: digest,
        kind: "wasm",
        policyFile: f.policyPath,
        helperExecutable: catalogHelper,
        actor: "integration-owner",
        reason: "New current attestation only",
      });
      expect(approved.execution.kind).toBe("wasm");
      expect(
        (await db`SELECT release_descriptor FROM plugin_packages WHERE digest=${digest}`)[0]
          .release_descriptor,
      ).toBeNull();
      const [{ value }] =
        await db`SELECT core_execution_runtime.allowed_v1(${[{ ref: { package_digest: digest } }]}::jsonb) value`;
      expect(value[digest]).toEqual([approved.execution]);
      expect(
        (
          await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_runtime_approval'`
        )[0].n,
      ).toBe(1);
    }),
  30000,
);

test(
  "an approval blocked by the catalog epoch cannot commit after its verified trust interval expires",
  () =>
    withCatalog(async (f, db) => {
      await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: openAIChatReleaseId,
        reason: "Expiry control prerequisite",
      });
      const digest = f.release.descriptor.package_digest;
      const approved = await approvePluginRuntime(db, {
        packageDigest: digest,
        kind: "wasm",
        policyFile: f.policyPath,
        helperExecutable: catalogHelper,
        actor: "integration-owner",
        reason: "Initial valid attestation",
      });
      await db`UPDATE core_execution_runtime.approvals SET active=false WHERE package_digest=${digest}`;
      const [before] =
        await db`SELECT facts FROM core_execution_runtime.approvals WHERE package_digest=${digest}`;
      const locked = Promise.withResolvers<number>();
      const release = Promise.withResolvers<void>();
      const blocker = db.begin(async (tx) => {
        await tx`SELECT singleton FROM core_catalog_cache.epoch FOR UPDATE`;
        locked.resolve((await tx`SELECT pg_backend_pid() pid`)[0].pid);
        await release.promise;
      });
      let pending: Promise<unknown> | undefined;
      try {
        const pid = await locked.promise;
        const facts = { ...before.facts, trust_until: new Date(Date.now() + 1000).toISOString() };
        pending =
          db`SELECT core_execution_runtime.approve_v1(${digest},${approved.execution}::jsonb,${facts}::jsonb,true,'integration-owner','Blocked expiry boundary')`
            .then((rows) => rows)
            .catch((error: unknown) => error);
        await financialBlockedBy(db, pid);
        await Bun.sleep(1100);
        release.resolve();
        await blocker;
        const error = await pending;
        expect(error).toBeInstanceOf(Error);
        expect(String(error)).toContain("runtime_trust_expired");
        const [after] =
          await db`SELECT active,facts FROM core_execution_runtime.approvals WHERE package_digest=${digest}`;
        expect(after).toEqual({ active: false, facts: before.facts });
        expect(
          (
            await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_runtime_approval'`
          )[0].n,
        ).toBe(2);
      } finally {
        release.resolve();
        await blocker;
        await pending;
      }
    }),
  30000,
);
