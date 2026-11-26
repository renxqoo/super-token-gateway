import { verify, until } from "./verify-production-checks.ts";
import { join } from "node:path";
import { cp, appendFile } from "node:fs/promises";
import { createLedgerEngine } from "@gateway/ledger";
import { createConsoleRuntime } from "@gateway/console-api";
import { createGatewayRuntime } from "../apps/gateway/src/app.ts";
import { createWorkerRunner } from "../apps/worker/src/runner.ts";

export async function verifyProductionFailover({
  primaryPort,
  backupDir,
  credit,
  owner,
  archiveDir,
  restoreDir,
  restoredPort,
  consoleDatabase,
  consoleUser,
  billingAccount,
  standbyDir,
  standbyPort,
  primaryDir,
  gw,
  infer,
  consoleKeyModels,
  admin,
  redisPort,
  replicaPort,
  redisReplica,
  redisPrimary,
  sentinelPorts,
  runPg,
  startPg,
  database,
  pgUrl,
  ms,
  writer,
  stopPg,
  consoleRuntime,
  workers,
  redisCommand,
}: {
  primaryPort: number;
  backupDir: string;
  credit: (ref: string, amount?: string) => Promise<Record<string, unknown>>;
  owner: Bun.SQL;
  archiveDir: string;
  restoreDir: string;
  restoredPort: number;
  consoleDatabase: "stg_console_drill";
  consoleUser: { access_token: string; default_org_id: string; default_billing_account_id: string };
  billingAccount: Record<string, unknown>;
  standbyDir: string;
  standbyPort: number;
  primaryDir: string;
  gw: Pick<ReturnType<typeof createGatewayRuntime>, "publicApp" | "cluster">;
  infer: () => Response | Promise<Response>;
  consoleKeyModels: () => Response | Promise<Response>;
  admin: (
    role: string,
    route: string,
    method?: string,
    body?: unknown,
  ) => Promise<Record<string, unknown>>;
  redisPort: number;
  replicaPort: number;
  redisReplica: Bun.Subprocess<"ignore", Bun.BunFile, Bun.BunFile>;
  redisPrimary: Bun.Subprocess<"ignore", Bun.BunFile, Bun.BunFile>;
  sentinelPorts: [number, number, number];
  runPg: (name: string, args: string[]) => Promise<string>;
  startPg: (directory: string, listenPort: number) => Promise<void>;
  database: (url: string) => Bun.SQL;
  pgUrl: (p: number, account?: string, database?: string) => string;
  ms: (elapsed: number) => number;
  writer: { switchTo(next?: number): void };
  stopPg: (directory: string, mode: "fast" | "immediate") => Promise<void>;
  consoleRuntime: ReturnType<typeof createConsoleRuntime>;
  workers: ReturnType<typeof createWorkerRunner>[];
  redisCommand: (redisPort: number, command: string, args?: string[]) => Promise<unknown>;
}) {
  await runPg("pg_basebackup", [
    "-h",
    "127.0.0.1",
    "-p",
    String(primaryPort),
    "-U",
    "drill_admin",
    "-D",
    backupDir,
    "-X",
    "stream",
    "--checkpoint=fast",
  ]);
  await runPg("pg_verifybackup", [backupDir]);
  await credit("pitr-visible", "101");
  const [cutoff] =
    await owner`SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00' AS target`;
  const targetTime = String(cutoff!.target);
  await Bun.sleep(80);
  await credit("pitr-excluded", "37");
  const [fault] =
    await owner`SELECT (extract(epoch FROM clock_timestamp()) * 1000)::float8 AS time_ms,
    pg_walfile_name(pg_current_wal_insert_lsn()) AS segment`;
  const restoreStart = performance.now();
  await owner`SELECT pg_switch_wal()`;
  await until(
    async () =>
      (await Bun.file(join(archiveDir, String(fault!.segment))).exists()) ? true : undefined,
    "WAL archive",
  );
  await cp(backupDir, restoreDir, { recursive: true });
  await appendFile(
    join(restoreDir, "postgresql.auto.conf"),
    `
archive_mode = off
restore_command = 'cp ${archiveDir}/%f %p'
recovery_target_time = '${targetTime}'
recovery_target_action = 'promote'
`,
  );
  await Bun.write(join(restoreDir, "recovery.signal"), "");
  await startPg(restoreDir, restoredPort);
  const restoredOwner = database(pgUrl(restoredPort));
  await until(
    async () =>
      (await restoredOwner`SELECT NOT pg_is_in_recovery() AS writable`)[0]?.writable
        ? true
        : undefined,
    "PITR promotion",
  );
  const restoredAdmin = database(pgUrl(restoredPort, "drill_control"));
  const restoreLedger = createLedgerEngine(restoredAdmin, { enableLeases: false });
  const restoredCredits =
    await restoredOwner`SELECT external_ref, amount_atoms::text AS atoms FROM public.funding_operations
     WHERE external_ref IN ('pitr-visible', 'pitr-excluded') ORDER BY external_ref`;
  verify(
    restoredCredits.length === 1 &&
      restoredCredits[0]?.external_ref === "pitr-visible" &&
      restoredCredits[0]?.atoms === "101",
    "PITR cutoff does not match confirmed credits",
  );
  verify((await restoreLedger.auditConservation()).ok, "Restored ledger conservation failed");
  const restoredConsole = database(pgUrl(restoredPort, "drill_console", consoleDatabase));
  const [restoredOrganization] =
    await restoredConsole`SELECT status, inference_synced_revision FROM public.organizations WHERE id = ${consoleUser.default_org_id}`;
  const [restoredAdmission] =
    await restoredAdmin`SELECT suspended, source_revision FROM public.console_organization_admission WHERE organization_id = ${consoleUser.default_org_id}`;
  verify(
    restoredOrganization?.status === "SUSPENDED" &&
      restoredAdmission?.suspended &&
      restoredOrganization.inference_synced_revision === restoredAdmission.source_revision,
    "PITR lost confirmed Console organization admission",
  );
  const restoreWrite = await restoreLedger.creditWallet({
    billing_account_id: String(billingAccount.id),
    currency: "USD",
    kind: "CASH",
    external_ref: "pitr-write-probe",
    amount_atoms: "1",
  });
  verify(restoreWrite, "PITR writer probe failed");
  const pitr = {
    target: targetTime,
    recovery_window_ms: ms(Number(fault!.time_ms) - Date.parse(targetTime)),
    lost_confirmed_before_target: 0,
    rto_ms: ms(performance.now() - restoreStart),
  };

  await runPg("pg_basebackup", [
    "-h",
    "127.0.0.1",
    "-p",
    String(primaryPort),
    "-U",
    "drill_admin",
    "-D",
    standbyDir,
    "-X",
    "stream",
    "--checkpoint=fast",
    "-R",
  ]);
  await appendFile(join(standbyDir, "postgresql.auto.conf"), "\narchive_mode = off\n");
  await startPg(standbyDir, standbyPort);
  await owner`ALTER SYSTEM SET synchronous_standby_names = '*'`;
  await owner`ALTER SYSTEM SET synchronous_commit = 'remote_apply'`;
  await owner`SELECT pg_reload_conf()`;
  await until(async () => {
    const [state] = await owner`SELECT current_setting('synchronous_commit') AS commit,
      EXISTS (SELECT FROM pg_stat_replication WHERE state = 'streaming' AND sync_state = 'sync') AS synced`;
    return state?.commit === "remote_apply" && state.synced ? true : undefined;
  }, "Synchronous remote_apply");
  const syncCount = 8;
  for (let i = 0; i < syncCount; i++) await credit(`pg-ha-${i}`);
  const pgFailure = performance.now();
  const pgFailureWall = Date.now();
  writer.switchTo();
  await stopPg(primaryDir, "immediate");
  const statusDuring = (await gw.publicApp.request("/readyz")).status;
  verify(statusDuring === 503, "Gateway accepted traffic during writer outage");
  await runPg("pg_ctl", ["-D", standbyDir, "-w", "-t", "15", "promote"]);
  writer.switchTo(standbyPort);
  await until(
    async () => ((await gw.publicApp.request("/readyz")).status === 200 ? true : undefined),
    "Gateway writer readiness",
  );
  await credit("pg-after-failover", "29");
  const pgRto = ms(performance.now() - pgFailure);
  verify((await infer()).status === 200, "Inference failed after writer promotion");
  const consoleRto = await until(async () => {
    const [readiness, denied] = await Promise.all([
      consoleRuntime!.fetchClient(new Request("http://console.test/readyz")),
      consoleKeyModels(),
    ]);
    const policy = (await denied.json()) as { error?: { code?: string } };
    return readiness.status === 200 &&
      denied.status === 403 &&
      policy.error?.code === "org_suspended"
      ? ms(performance.now() - pgFailure)
      : undefined;
  }, "Console readiness and suspended key denial after writer promotion");
  const pgInferenceRto = ms(performance.now() - pgFailure);
  const promoted = database(pgUrl(standbyPort));
  const [retained] =
    await promoted`SELECT count(*)::int AS n FROM public.funding_operations WHERE external_ref LIKE 'pg-ha-%'`;
  verify(retained?.n === syncCount, "Lost confirmed PostgreSQL credits");
  await until(async () => {
    const leaders = workers.filter((item) => item.isLeader());
    if (
      leaders.length !== 1 ||
      Date.parse(leaders[0]!.status().last_successful_sweep ?? "") <= pgFailureWall
    )
      return;
    const [lock] = await promoted`SELECT count(*)::int AS n FROM pg_locks
        WHERE locktype = 'advisory' AND classid = 0 AND objid = 81442599 AND objsubid = 1 AND granted`;
    if (!leaders[0]!.status().last_successful_sweep || lock?.n !== 1) return;
    return true;
  }, "Worker leader after promotion");
  const workerRto = ms(performance.now() - pgFailure);
  verify((await admin("auditor", "audit/conservation")).ok, "Promoted ledger conservation failed");

  await redisCommand(redisPort, "SET", ["drill:replicated", "retained"]);
  verify(
    (await gw.cluster.takeIngressWindow("192.0.2.21", 1, 60_000, 1000)) === 0,
    "Redis ingress quota could not admit the test IP before failover",
  );
  await until(
    async () =>
      (await redisCommand(replicaPort, "GET", ["drill:replicated"])) === "retained" &&
      (await redisCommand(replicaPort, "GET", ["stg:ingress:ip:192.0.2.21"])) === "1"
        ? true
        : undefined,
    "Initial Redis marker replication",
  );
  const retainedAt = performance.now();
  redisReplica.kill("SIGSTOP");
  verify(
    Number(await redisCommand(redisPort, "CLIENT", ["KILL", "TYPE", "replica"])) === 1,
    "Redis replication link was not cut before fault injection",
  );
  const permit = await gw.cluster.acquireRateLimit(
    "drill-rpo",
    { concurrency: 1, rpm: 10, tpm: 10 },
    1,
  );
  verify(permit.ok, "Could not acquire Redis permit before failover");
  const quotaAcknowledgedAt = performance.now();
  await Bun.sleep(100);
  const redisFailure = performance.now();
  redisPrimary.kill("SIGKILL");
  await redisPrimary.exited;
  redisReplica.kill("SIGCONT");
  await until(
    async () => (!gw.cluster.isRedisConnected() ? true : undefined),
    "Redis permit revocation after primary loss",
  );
  verify(
    (await gw.cluster.takeIngressWindow("192.0.2.22", 1, 60_000, 1000)) === null,
    "Redis ingress quota admitted a request while the primary was unavailable",
  );
  await until(
    async () => (permit.signal?.aborted ? true : undefined),
    "Unreplicated permit revocation after reconnect",
  );
  await until(async () => {
    const primary = (await redisCommand(sentinelPorts[0]!, "SENTINEL", [
      "get-master-addr-by-name",
      "stg",
    ])) as string[];
    if (
      Number(primary?.[1]) !== replicaPort ||
      (await gw.publicApp.request("/readyz")).status !== 200
    )
      return;
    const probe = await gw.cluster.acquireRateLimit(
      "drill-restored",
      { concurrency: 1, rpm: 10, tpm: 10 },
      1,
    );
    if (probe.ok) {
      probe.release();
      return true;
    }
  }, "Sentinel promotion and Gateway quota write");
  const redisQuotaRto = ms(performance.now() - redisFailure);
  verify(
    ((await gw.cluster.takeIngressWindow("192.0.2.21", 1, 60_000, 1000)) ?? 0) > 0 &&
      (await gw.cluster.takeIngressWindow("192.0.2.22", 1, 60_000, 1000)) === 0,
    "Redis ingress quota did not recover with the replicated window",
  );
  verify(
    (await redisCommand(replicaPort, "GET", ["drill:replicated"])) === "retained",
    "Redis lost replicated marker",
  );
  const lostQuota =
    Number(await redisCommand(replicaPort, "EXISTS", ["stg:rl:rpm:drill-rpo"])) === 0;
  verify(lostQuota, "The unreplicated test permit unexpectedly survived the cut replication link");
  verify(
    (await gw.publicApp.request("/readyz")).status === 200,
    "Gateway did not recover after Sentinel failover",
  );
  const redisRto = ms(performance.now() - redisFailure);
  const afterRedis = await infer();
  verify(
    afterRedis.status === 200,
    `Gateway failed charging after Redis failover: ${afterRedis.status} ${await afterRedis.text()}`,
  );

  return {
    pitr,
    syncCount,
    pgRto,
    pgInferenceRto,
    workerRto,
    consoleRto,
    lostQuota,
    redisFailure,
    retainedAt,
    quotaAcknowledgedAt,
    redisQuotaRto,
    redisRto,
  };
}
