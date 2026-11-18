import { parseArgs } from "node:util";
import {
  createDatabase,
  SCHEMA_VERSION,
  DatabaseCommitUnknown,
  DatabaseDeadlineExceeded,
  runDatabaseQuery,
  withDatabaseTransaction,
  withDatabaseConnection,
} from "@gateway/storage";
import { executionRuntimeV1Schema } from "@gateway/contracts";
import { approvePluginRuntime, RuntimeApprovalOutcomeUnknown } from "./runtime-approval.ts";

const { values } = parseArgs({
  options: {
    database: { type: "string" },
    "package-digest": { type: "string" },
    kind: { type: "string" },
    "policy-file": { type: "string" },
    "helper-executable": { type: "string" },
    "builtin-directory": { type: "string" },
    "artifact-digest": { type: "string" },
    disable: { type: "boolean", default: false },
    "timeout-ms": { type: "string", default: "60000" },
    actor: { type: "string" },
    reason: { type: "string" },
  },
  strict: true,
});
const url = process.env.MIGRATION_DATABASE_URL ?? process.env.DATABASE_URL;
const digest = values["package-digest"];
if (
  !url ||
  !values.database ||
  decodeURIComponent(new URL(url).pathname.slice(1)) !== values.database
)
  throw new Error("Pass --database <exact-name> matching MIGRATION_DATABASE_URL or DATABASE_URL");
if (
  !digest ||
  !/^[a-f0-9]{64}$/.test(digest) ||
  !values.actor ||
  values.actor.length > 100 ||
  !values.reason ||
  values.reason.length > 1000 ||
  (values.kind !== "wasm" && values.kind !== "builtin-worker")
)
  throw new Error("Required: --package-digest --kind wasm|builtin-worker --actor --reason");
if (
  !values.disable &&
  (!values["policy-file"] ||
    !values["helper-executable"] ||
    (values.kind === "builtin-worker" && !values["builtin-directory"]))
)
  throw new Error(
    "Approval requires --policy-file --helper-executable and Worker approval requires --builtin-directory",
  );
const controller = new AbortController();
const wallMs = Number(values["timeout-ms"]);
if (!/^[1-9][0-9]{0,4}$/.test(values["timeout-ms"]!) || wallMs > 60000)
  throw new Error("RUNTIME_APPROVAL_BUDGET_INVALID");
const until = performance.now() + wallMs;
const remainingMs = () => until - performance.now();
const timer = setTimeout(() => controller.abort(new Error("RUNTIME_APPROVAL_DEADLINE")), wallMs);
const stop = () => controller.abort(new Error("RUNTIME_APPROVAL_CANCELLED"));
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
const db = createDatabase(url, 1);
try {
  if (
    (
      await withDatabaseConnection(
        db,
        { signal: controller.signal, deadline: until },
        (connection, signal) =>
          runDatabaseQuery(
            connection`SELECT max(version) version FROM public.schema_migrations`,
            signal,
          ),
      )
    )[0].version !== SCHEMA_VERSION
  )
    throw new Error("RUNTIME_APPROVAL_SCHEMA_MISMATCH");
  if (values.disable) {
    const runtime = executionRuntimeV1Schema.parse({
      version: 1,
      kind: values.kind,
      artifact_digest: values["artifact-digest"],
    });
    await withDatabaseTransaction(
      db,
      { signal: controller.signal, deadline: until },
      async (tx, signal) => {
        signal.throwIfAborted();
        const [row] = await runDatabaseQuery(
          tx`SELECT facts FROM core_execution_runtime.approvals WHERE package_digest=${digest} AND kind=${runtime.kind} AND artifact_digest=${runtime.artifact_digest}`,
          signal,
        );
        if (!row) throw new Error("RUNTIME_APPROVAL_NOT_FOUND");
        await runDatabaseQuery(
          tx`SELECT core_execution_runtime.approve_v1(${digest},${runtime}::jsonb,${row.facts}::jsonb,false,${values.actor},${values.reason})`,
          signal,
        );
        signal.throwIfAborted();
      },
    );
    console.log(JSON.stringify({ package_digest: digest, execution: runtime, active: false }));
  } else {
    const remaining = Math.floor(remainingMs());
    if (remaining < 1) throw new Error("RUNTIME_APPROVAL_DEADLINE");
    console.log(
      JSON.stringify(
        await approvePluginRuntime(
          db,
          {
            packageDigest: digest,
            kind: values.kind,
            policyFile: values["policy-file"]!,
            helperExecutable: values["helper-executable"]!,
            ...(values["builtin-directory"]
              ? { builtinDirectory: values["builtin-directory"] }
              : {}),
            actor: values.actor,
            reason: values.reason,
          },
          { signal: controller.signal, wallMs: remaining },
        ),
      ),
    );
  }
} catch (error) {
  if (error instanceof DatabaseCommitUnknown) throw new RuntimeApprovalOutcomeUnknown(error);
  if (error instanceof RuntimeApprovalOutcomeUnknown) throw error;
  controller.signal.throwIfAborted();
  if (
    error instanceof DatabaseDeadlineExceeded ||
    (error as { errno?: string } | null)?.errno === "25P04"
  )
    throw new Error("RUNTIME_APPROVAL_DEADLINE", { cause: error });
  throw error;
} finally {
  clearTimeout(timer);
  try {
    await db.close({ timeout: 1 });
  } finally {
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
  }
}
