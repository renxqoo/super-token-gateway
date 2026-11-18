import { expect, test } from "bun:test";
import { approvePluginRuntime } from "../../scripts/runtime-approval.ts";
import { catalogHelper, withCatalog } from "../support/catalog.ts";
import { financialBlockedBy } from "../support/financial-execution.ts";
import { openAIChatReleaseId } from "@gateway/plugin-sdk";

for (const mode of ["SIGTERM", "deadline"] as const)
  for (const lock of ["schema", "approval read", "epoch update"] as const)
    test(
      `${mode} cancels owner runtime revocation waiting on ${lock} without changing the approval`,
      () =>
        withCatalog(async (f, db) => {
          await f.application.installBundled(await f.actor(), {
            command_id: crypto.randomUUID(),
            release_id: openAIChatReleaseId,
            reason: "Runtime cancellation prerequisite",
          });
          const approved = await approvePluginRuntime(db, {
            packageDigest: f.release.descriptor.package_digest,
            kind: "wasm",
            policyFile: f.policyPath,
            helperExecutable: catalogHelper,
            actor: "integration-owner",
            reason: "Runtime cancellation prerequisite",
          });
          const before = Number(
            (
              await db`SELECT count(*) n FROM audit_events WHERE action='execution_runtime_approval'`
            )[0].n,
          );
          const release = Promise.withResolvers<void>(),
            ready = Promise.withResolvers<number>();
          const holder = db.begin(async (tx) => {
            if (lock === "schema")
              await tx`LOCK TABLE public.schema_migrations IN ACCESS EXCLUSIVE MODE`;
            else if (lock === "approval read")
              await tx`LOCK TABLE core_execution_runtime.approvals IN ACCESS EXCLUSIVE MODE`;
            else await tx`SELECT revision FROM core_catalog_cache.epoch WHERE singleton FOR UPDATE`;
            ready.resolve(Number((await tx`SELECT pg_backend_pid() pid`)[0].pid));
            await release.promise;
          });
          let child: Bun.Subprocess<"ignore", "pipe", "pipe"> | undefined;
          let stdout: Promise<string> | undefined, stderr: Promise<string> | undefined;
          try {
            const pid = await Promise.race([
              ready.promise,
              holder.then(() => {
                throw new Error("Runtime cancellation holder ended before readiness");
              }),
            ]);
            const name = String((await db`SELECT current_database() name`)[0].name);
            const url = new URL(process.env.DATABASE_URL!);
            url.pathname = `/${name}`;
            child = Bun.spawn(
              [
                process.execPath,
                "scripts/approve-plugin-runtime.ts",
                "--database",
                name,
                "--package-digest",
                f.release.descriptor.package_digest,
                "--kind",
                "wasm",
                "--artifact-digest",
                approved.execution.artifact_digest,
                "--disable",
                "--actor",
                "integration-owner",
                "--reason",
                "Cancellation must not commit a revocation",
                "--timeout-ms",
                mode === "deadline" ? "500" : "60000",
              ],
              {
                env: {
                  PATH: process.env.PATH,
                  DATABASE_URL: url.toString(),
                  MIGRATION_DATABASE_URL: url.toString(),
                },
                stdin: "ignore",
                stdout: "pipe",
                stderr: "pipe",
              },
            );
            stdout = new Response(child.stdout).text();
            stderr = new Response(child.stderr).text();
            await financialBlockedBy(db, pid);
            let exited = false;
            void child.exited.then(() => {
              exited = true;
            });
            if (mode === "SIGTERM") child.kill("SIGTERM");
            let timer: ReturnType<typeof setTimeout> | undefined;
            try {
              await Promise.race([
                child.exited,
                new Promise((resolve) => {
                  timer = setTimeout(resolve, mode === "SIGTERM" ? 500 : 1500);
                }),
              ]);
            } finally {
              clearTimeout(timer);
            }
            expect(exited).toBe(true);
            expect(await child.exited).not.toBe(0);
            expect(await stdout).toBe("");
            expect(await stderr).toContain(
              mode === "SIGTERM" ? "RUNTIME_APPROVAL_CANCELLED" : "RUNTIME_APPROVAL_DEADLINE",
            );
          } finally {
            release.resolve();
            await holder;
            if (child) {
              await Promise.race([child.exited, Bun.sleep(1000)]);
              if (child.exitCode === null) child.kill("SIGKILL");
              await child.exited;
            }
            await Promise.allSettled([stdout, stderr]);
          }
          expect(
            (
              await db`SELECT active FROM core_execution_runtime.approvals WHERE package_digest=${f.release.descriptor.package_digest} AND kind='wasm' AND artifact_digest=${approved.execution.artifact_digest}`
            )[0].active,
          ).toBe(true);
          expect(
            Number(
              (
                await db`SELECT count(*) n FROM audit_events WHERE action='execution_runtime_approval'`
              )[0].n,
            ),
          ).toBe(before);
        }),
      30000,
    );
