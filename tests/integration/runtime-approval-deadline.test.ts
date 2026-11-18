import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { openAIChatReleaseId } from "@gateway/plugin-sdk";
import { approvePluginRuntime } from "../../scripts/runtime-approval.ts";
import { catalogHelper, withCatalog } from "../support/catalog.ts";

test(
  "many individually bounded Worker fixture calls cannot exceed the total approval deadline or register a build",
  () =>
    withCatalog(async (f, db) => {
      await f.application.installBundled(await f.actor(), {
        command_id: crypto.randomUUID(),
        release_id: openAIChatReleaseId,
        reason: "Deadline fixture prerequisite",
      });
      const directory = await mkdtemp(resolve(tmpdir(), "gateway-approval-deadline-"));
      try {
        const marker = resolve(directory, "completed-calls");
        const original = await readFile(".local/builtin-adapters/worker.js", "utf8");
        const code =
          original +
          `\nimport { appendFileSync as recordApprovalStep } from 'node:fs';\nconst approvedCall = self.onmessage; self.onmessage = (event) => { const until = performance.now()+20; while(performance.now()<until) {} approvedCall(event); recordApprovalStep(${JSON.stringify(marker)}, 'step\\n'); };\n`;
        const registry = JSON.parse(
          await readFile(".local/builtin-adapters/registry.json", "utf8"),
        );
        const hash = (value: string) => createHash("sha256").update(value).digest("hex");
        registry.worker_digest = hash(code);
        registry.artifact_digest = hash(
          JSON.stringify([
            "stg.internal.builtin-worker-artifact",
            "1",
            registry.worker_digest,
            registry.runtime_digest,
            "1",
            "1",
          ]),
        );
        await writeFile(resolve(directory, "worker.js"), code);
        await writeFile(resolve(directory, "registry.json"), JSON.stringify(registry));
        const started = performance.now();
        const error = await approvePluginRuntime(
          db,
          {
            packageDigest: f.release.descriptor.package_digest,
            kind: "builtin-worker",
            policyFile: f.policyPath,
            helperExecutable: catalogHelper,
            builtinDirectory: directory,
            actor: "integration-owner",
            reason: "Total deadline with finite short calls",
          },
          { wallMs: 2000 },
        ).catch((failure: unknown) => failure);
        expect(error).toMatchObject({ message: "RUNTIME_APPROVAL_DEADLINE" });
        expect((await readFile(marker, "utf8")).trim().split("\n").length).toBeGreaterThanOrEqual(
          2,
        );
        expect(performance.now() - started).toBeLessThan(5000);
        expect(
          (await db`SELECT count(*)::integer n FROM core_execution_runtime.approvals`)[0].n,
        ).toBe(0);
        expect(
          (
            await db`SELECT count(*)::integer n FROM audit_events WHERE action='execution_runtime_approval'`
          )[0].n,
        ).toBe(0);
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    }),
  30000,
);
