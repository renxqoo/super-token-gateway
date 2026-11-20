import { mkdir } from "node:fs/promises";
import { SQL } from "bun";
import { createCoreRpcClient, CoreRpcError } from "@gateway/core-client";
import { UsageProducer, evidenceChannel, sha256 } from "@gateway/runtime";
import { createNativeFixture, nativeFixtureOptions } from "../tests/support/native-fixture.ts";
import { prepareFinancialAdmission, withFinancial } from "../tests/support/financial-execution.ts";
import { startCoreWorker, withSettlementRole } from "../tests/support/gateway-process.ts";
import { identityAudience } from "../tests/support/internal-identity.ts";
import { event } from "../tests/support/core-settlement.ts";
import { LatencyHistogram } from "./benchmark-lib.ts";
import {
  createDatabaseMetrics,
  startProcessSampler,
  summarizeStatements,
} from "./benchmark-metrics.ts";

const args = Bun.argv.slice(2);
const projectAffinity = args.includes("--project-affinity");
const rates = args.filter((arg) => arg !== "--project-affinity").map(Number);
if (!rates.length || rates.some((r) => !Number.isInteger(r) || r < 1 || r > 3000))
  throw new Error("Supply one or more rates in 1..3000");
const entry = "apps/core-api/src/main.ts";
const run = crypto.randomUUID().slice(0, 8);
const output = ".local/core-funding-bench";
await mkdir(output, { recursive: true });

const native = await createNativeFixture({
  ...nativeFixtureOptions(),
  diskBudgetBytes: 8 * 1024 ** 3,
  truncateProcessOutput: true,
});
try {
  const primary = await native.startPostgres(true);
  const standby = await native.startPostgresStandby(primary);
  const redis = await native.startRedis("data-probe-quota", undefined, true);
  const evidence = await native.startRedis("data-probe-evidence", undefined, true);
  const replica = await native.startRedis("data-probe-evidence-replica", evidence, true);
  await native.waitFor(async () =>
    (await replica.command("INFO", "replication")).includes("master_link_status:up"),
  );
  process.env.DATABASE_URL = primary.url;
  process.env.REDIS_URL = redis.url;
  process.env.QUEUE_REDIS_URL = evidence.url;
  const bootstrap = new SQL(primary.url, { max: 1 });
  try {
    await bootstrap`CREATE EXTENSION IF NOT EXISTS pg_stat_statements`;
    await bootstrap.unsafe("ALTER SYSTEM SET pg_stat_statements.track='all'");
    await bootstrap`SELECT pg_reload_conf()`;
  } finally {
    await bootstrap.close({ timeout: 1 });
  }
  for (const rate of rates)
    await withFinancial(
      (f, db) =>
        withSettlementRole(db, async (workerDatabaseUrl) => {
          async function admin<T>(path: string, input: unknown, method?: string) {
            const res = await f.request(path, input, undefined, method);
            if (res.status >= 300) throw new Error(`fixture request failed ${path}: ${res.status}`);
            return res.value as T;
          }
          const setup = await f.bootstrap("USD", "100000000000");
          const keys = [setup.key];
          await admin(
            `/providers/${setup.provider.id}`,
            { concurrency: 100000, rpm: 10000000 },
            "PATCH",
          );
          const limits = { rpm: 10000000, tpm: 1000000000, concurrency: 100000 };
          await admin(`/projects/${setup.project.id}`, limits, "PATCH");
          for (let index = 1; index < 1000; index++) {
            const project = await admin<{ id: string }>("/projects", {
              name: `data-probe-${index}`,
              billing_currency: "USD",
              ...limits,
            });
            keys.push(
              await admin<typeof setup.key>(`/projects/${project.id}/keys`, {
                name: "data-probe",
                models: [setup.model],
                operations: ["chat"],
              }),
            );
            await admin(`/projects/${project.id}/credits`, {
              external_ref: `probe-${project.id}`,
              currency: "USD",
              amount_atoms: "1000000",
              kind: "CASH",
            });
          }
          const worker = await startCoreWorker(f.namespace, {
            databaseUrl: workerDatabaseUrl,
            identity: f.identity,
            entry: "apps/core-worker/src/main.ts",
            env: { SETTLEMENT_CONCURRENCY: "8" },
          });
          const producers = Array.from(
            { length: 4 },
            () =>
              new UsageProducer({
                channel: evidenceChannel,
                namespace: f.namespace,
                url: evidence.url,
                replicas: 1,
                timeoutMs: 1000,
                maxInFlight: 10000,
              }),
          );
          const clients = ["gateway", "gatewayB", "gateway3", "gateway4"].map((name, index) => {
            const target = index % 2 ? "coreB" : "coreA";
            const upstream = f.identity.certificates.get(target)!;
            const local = f.identity.certificates.get(name)!;
            return createCoreRpcClient({
              origin: `https://127.0.0.1:${index % 2 ? f.identity.b.port : f.identity.a.port}`,
              servername: "localhost",
              key: local.key,
              cert: local.cert,
              ca: f.identity.ca,
              expectedPeerSha256: sha256(upstream.x509.raw),
              expectedSubjectAltname: upstream.x509.subjectAltName!,
              audience: identityAudience,
              connections: 4,
              timeoutMs: 5000,
            });
          });
          const pg = new URL(primary.url);
          pg.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
          const metrics = await createDatabaseMetrics(pg.toString());
          const sampler = startProcessSampler(async () => ({
            core: [f.identity.a.pid, f.identity.b.pid],
            worker: [worker.pid],
            pg_instance: [primary.pid],
            load: [process.pid],
            ...(await metrics.roots()),
          }));
          try {
            const [, sessions] = await Promise.all([
              Promise.all(producers.map((p) => p.ready())),
              Promise.all(clients.map((c) => c.startup())),
            ]);
            if (
              sessions.some((s) => s.service !== "gateway") ||
              new Set(sessions.map((s) => s.session_id)).size !== clients.length ||
              new Set(sessions.map((s) => s.instance_id)).size !== clients.length
            )
              throw new Error("Expected four independent Gateway identities");
            const routing = sessions.map((s, i) => ({
              core: i % 2 ? "coreB" : "coreA",
              gateway: s.instance_id,
              session_id: s.session_id,
              requests: 0,
              requests_by_cell: Array<number>(16).fill(0),
            }));
            const requestsByCore = [0, 0];
            const keyRows = await db<
              { id: string; project_id: string; cell: number }[]
            >`SELECT id,project_id,get_byte(uuid_send(project_id),15)%16 AS cell FROM api_keys`;
            const projects = new Map(keyRows.map((row) => [row.id, row]));
            const wallets = keys.map((key) => {
              const project = projects.get(key.id);
              if (!project) throw new Error("Missing benchmark key project");
              return { key, projectId: project.project_id, cell: project.cell };
            });
            const walletsByCore = [0, 1].map((core) => wallets.filter((w) => w.cell % 2 === core));
            const warmupWallets = clients.map((_, index) => {
              const wallet = projectAffinity
                ? walletsByCore[index % 2]![Math.floor(index / 2)]
                : wallets[index];
              if (!wallet) throw new Error("Missing wallet for Gateway warmup");
              return wallet;
            });
            const phases = ["context", "grant", "publish", "observe"] as const;
            type Phase = (typeof phases)[number];
            const histograms = Object.fromEntries(
              phases.map((p) => [p, new LatencyHistogram()]),
            ) as Record<Phase, LatencyHistogram>;
            const failures: Record<string, number> = {};
            const retryStats = Object.fromEntries(
              phases.map((p) => [
                p,
                { attempts: 0, retries: 0, failed_attempt_ms: 0, backoff_ms: 0 },
              ]),
            ) as Record<
              Phase,
              { attempts: number; retries: number; failed_attempt_ms: number; backoff_ms: number }
            >;
            const completed = new Map<string, boolean>();
            let grants = 0,
              grantsByDeadline = 0,
              durableByDeadline = 0;
            const stageCounts = Object.fromEntries(phases.map((p) => [p, 0])) as Record<
              Phase,
              number
            >;
            let requested = 0,
              shed = 0,
              peak = 0;
            async function invoke(index: number, record: boolean) {
              const deadlineTime = Date.now() + 5000;
              const deadline = new Date(deadlineTime).toISOString();
              const wallet = record ? wallets[index % wallets.length]! : warmupWallets[index]!;
              const owner = wallet.cell % 2;
              const clientIndex =
                projectAffinity && record
                  ? owner + 2 * (requestsByCore[owner]! % 2)
                  : index % clients.length;
              const client = clients[clientIndex]!;
              if (record) {
                const core = clientIndex % 2;
                requestsByCore[core] = requestsByCore[core]! + 1;
                const route = routing[clientIndex]!;
                route.requests++;
                route.requests_by_cell[wallet.cell] = route.requests_by_cell[wallet.cell]! + 1;
              }
              let phase: Phase = "context";
              async function step<T>(name: Phase, operation: () => Promise<T>) {
                phase = name;
                const began = performance.now();
                for (let delay = 25; ; delay = Math.min(2 * delay, 200)) {
                  const attemptStarted = performance.now();
                  if (record) retryStats[name].attempts++;
                  try {
                    const value = await operation();
                    if (record) {
                      histograms[name].add(performance.now() - began);
                      stageCounts[name]++;
                    }
                    return value;
                  } catch (error) {
                    if (
                      !(error instanceof CoreRpcError) ||
                      !error.retryableAdmissionBusy ||
                      Date.now() + delay >= Date.parse(deadline)
                    )
                      throw error;
                    const backoffStarted = performance.now();
                    if (record) {
                      retryStats[name].retries++;
                      retryStats[name].failed_attempt_ms += backoffStarted - attemptStarted;
                    }
                    await Bun.sleep(delay);
                    if (record) retryStats[name].backoff_ms += performance.now() - backoffStarted;
                  }
                }
              }
              try {
                const contextCommandId = crypto.randomUUID();
                const proposed = await step("context", () =>
                  prepareFinancialAdmission(
                    {
                      ...setup,
                      key: wallet.key,
                      project: { ...setup.project, id: wallet.projectId },
                    },
                    client,
                    {
                      routeProjection: true,
                      stream: true,
                      deadline,
                      intent: null,
                      contextCommandId,
                    },
                  ),
                );
                const begun = await step("grant", () =>
                  client.execution(
                    "execution_admit_v1",
                    { ...proposed.data, commit_send: true },
                    {
                      commandId: proposed.command,
                      deadlineAt: deadline,
                      routeTicket: proposed.context.route_ticket!,
                    },
                  ),
                );
                if (!("transient_send" in begun) || !begun.transient_send)
                  throw new Error("missing_send_grant");
                if (record) {
                  grants++;
                  if (Date.now() <= deadlineTime) grantsByDeadline++;
                }
                await step("publish", () =>
                  producers[clientIndex]!.publish({
                    events: [
                      event({ client, permit_epoch: "1" }, proposed.attemptId, {
                        finality: "FINAL",
                        usage: { input_tokens: "30", output_tokens: "15" },
                        final_dimensions: ["input_tokens", "output_tokens"],
                        attempt_state: "SUCCEEDED",
                        request_state: "SUCCEEDED",
                      }),
                    ],
                  }),
                );
                if (record) {
                  const inTime = Date.now() <= deadlineTime;
                  completed.set(proposed.requestId, inTime);
                  if (inTime) durableByDeadline++;
                }
                const observation = crypto.randomUUID();
                await step("observe", () =>
                  client.execution(
                    "execution_quota_v1",
                    {
                      attempt_id: proposed.attemptId,
                      permit_epoch: "1",
                      action: "OBSERVE",
                      observation: {
                        channel_id: proposed.candidate.ref.channel_id,
                        outcome: "SUCCESS",
                        latency_ms: 1,
                      },
                    },
                    { commandId: observation, deadlineAt: deadline },
                  ),
                );
              } catch (error) {
                const code =
                  error instanceof CoreRpcError
                    ? error.detail.code
                    : error instanceof Error
                      ? error.message.slice(0, 80)
                      : "unknown";
                if (!record) throw new Error(`Warmup failed in ${phase}: ${code}`);
                const source =
                  error instanceof CoreRpcError && code === "internal_capacity"
                    ? error.status === undefined
                      ? "local:"
                      : "remote:"
                    : "";
                const label = `${phase}:${source}${code}`;
                failures[label] = (failures[label] ?? 0) + 1;
              }
            }
            await Promise.all(clients.map((_, i) => invoke(i, false)));
            const count = async () =>
              (
                await db`
              SELECT (SELECT count(*)::int FROM billing_charges WHERE amount_atoms=100) AS charged,
                (SELECT count(*)::int FROM usage_events WHERE record_kind='CANONICAL' AND cost_atoms=15) AS costed,
                (SELECT count(*)::int FROM execution_permits WHERE state='SEND_GRANTED') AS granted,
                (SELECT count(*)::int FROM execution_permits WHERE state='ABORTED') AS aborted
            `
              )[0]!;
            const warmUntil = performance.now() + 5000;
            while ((await count()).charged < clients.length && performance.now() < warmUntil)
              await Bun.sleep(25);
            const beforeAccounting = await count();
            if (
              beforeAccounting.charged !== clients.length ||
              beforeAccounting.costed !== clients.length
            )
              throw new Error(`Warmup accounting mismatch: ${JSON.stringify(beforeAccounting)}`);
            await metrics.resetOwned();
            const before = await metrics.snapshot();
            await sampler.start();
            const pending = new Set<Promise<void>>();
            const dispatchLag = new LatencyHistogram();
            const started = performance.now();
            const goal = rate * 3;
            while (requested < goal) {
              const target = Math.min(
                goal,
                Math.floor(((performance.now() - started) * rate) / 1000),
              );
              while (requested < target) {
                const index = requested++;
                if (pending.size >= 10000) {
                  shed++;
                  continue;
                }
                dispatchLag.add(
                  Math.max(0, performance.now() - started - ((index + 1) * 1000) / rate),
                );
                const task = invoke(index, true);
                pending.add(task);
                void task.finally(() => pending.delete(task));
              }
              peak = Math.max(peak, pending.size);
              if (requested < goal) await Bun.sleep(1);
            }
            const dispatchMs = performance.now() - started;
            await Promise.all(pending);
            const dispatchAndResponseMs = performance.now() - started;
            const drainUntil = performance.now() + 15000;
            while (
              (await count()).charged < completed.size + clients.length &&
              performance.now() < drainUntil
            )
              await Bun.sleep(50);
            const endAccounting = await count();
            const elapsed = performance.now() - started;
            const cpu = await sampler.stop(Math.max(1, completed.size));
            const pgStatements = summarizeStatements(
              before,
              await metrics.snapshot(),
              Math.max(1, completed.size),
            );
            const exact = (
              await db`
              SELECT count(*)::int AS settled, count(*) FILTER (WHERE e.in_time)::int AS timely_evidence_settled
              FROM unnest(${[...completed.keys()]}::uuid[], ${[...completed.values()]}::boolean[]) e(id,in_time)
              JOIN billing_charges c ON c.request_id=e.id AND c.amount_atoms=100 AND c.funded_atoms=100
                AND c.debt_atoms=0 AND c.refunded_atoms=0
              JOIN gateway_requests q ON q.id=e.id
              JOIN upstream_holds h ON h.attempt_id=q.selected_attempt_id AND h.state='SETTLED'
                AND h.currency='USD' AND h.final_cost_atoms=15 AND h.current_reserved_atoms=0
              JOIN upstream_cost_heads head ON head.attempt_id=h.attempt_id
              JOIN upstream_costs cost ON (cost.attempt_id,cost.revision)=(head.attempt_id,head.revision)
                AND cost.complete AND NOT cost.disputed AND cost.cost_version_id=h.cost_version_id
                AND cost.currency=h.currency AND cost.cost_atoms=h.final_cost_atoms
              WHERE EXISTS (SELECT 1 FROM usage_events u WHERE u.request_id=e.id AND u.attempt_id=h.attempt_id
                AND u.record_kind='CANONICAL' AND u.finality='FINAL' AND u.cost_complete AND u.cost_atoms=15
                AND u.usage='{"input_tokens":"30","output_tokens":"15"}'::jsonb)
              AND (SELECT SUM(entry.atoms) FROM upstream_journals j JOIN upstream_entries entry ON entry.journal_id=j.id
                WHERE j.attempt_id=h.attempt_id AND entry.account='upstream:expense')=15
              AND NOT EXISTS (SELECT 1 FROM upstream_journals j JOIN upstream_entries entry ON entry.journal_id=j.id
                WHERE j.attempt_id=h.attempt_id AND entry.account='upstream:expense'
                  AND (j.account_id,j.channel_id,j.currency,entry.currency)
                    IS DISTINCT FROM (h.account_id,h.channel_id,h.currency,h.currency))
            `
            )[0]!;
            const audit = {
              mode: "direct_core_streaming_chat_no_plugin_or_upstream",
              routing: {
                mode: projectAffinity ? "project_cell_affinity" : "round_robin",
                requests_by_core: { coreA: requestsByCore[0], coreB: requestsByCore[1] },
                gateways: routing,
              },
              stage_retries: retryStats,
              entry,
              bun: Bun.version,
              rate,
              seconds: 3,
              deadlineMs: 5000,
              wallets: keys.length,
              requested,
              shed,
              peak,
              dispatchMs,
              dispatchLagP99Ms: dispatchLag.quantile(0.99),
              stages: Object.fromEntries(
                phases.map((p) => [
                  p,
                  { count: stageCounts[p], p99_ms: histograms[p].quantile(0.99) },
                ]),
              ),
              failures,
              grants,
              grantsByDeadline,
              durableEvidence: completed.size,
              durableByDeadline,
              exact,
              metTarget:
                shed === 0 &&
                grantsByDeadline === goal &&
                durableByDeadline === goal &&
                exact.timely_evidence_settled === goal,
              dispatchAndResponseMs,
              elapsed,
              beforeAccounting,
              endAccounting,
              cpu,
              pgStatements,
              settings: metrics.settings,
            };
            const path = `${output}/${run}-${rate}.json`;
            await Bun.write(path, JSON.stringify(audit, null, 2) + "\n");
            console.log(
              JSON.stringify({
                path,
                rate,
                requested,
                shed,
                stages: audit.stages,
                failures,
                grants,
                grantsByDeadline,
                durableEvidence: completed.size,
                durableByDeadline,
                exact,
                endAccounting,
                dbCpu: cpu.processes.pg_instance?.cpu_seconds,
              }),
            );
          } finally {
            await sampler.close();
            await metrics.close();
            for (const p of producers) p.close();
            await Promise.allSettled(clients.map((c) => c.close()));
            if (worker.alive()) await worker.stop();
          }
        }),
      {
        entry,
        gateways: 4,
        redisUrl: redis.url,
        async terminalSettlement(db) {
          const [{ name }] = await db`SELECT current_database() AS name`;
          await db.unsafe(await Bun.file("infra/core-terminal-settlement-roles.sql").text());
          const password = crypto.randomUUID().replaceAll("-", "");
          const suffix = crypto.randomUUID().replaceAll("-", "");
          const writerRole = `bench_writer_${suffix}`;
          const witnessRole = `bench_witness_${suffix}`;
          await db.unsafe(
            `CREATE ROLE ${writerRole} LOGIN PASSWORD '${password}' IN ROLE core_terminal_writer`,
          );
          await db.unsafe(
            `CREATE ROLE ${witnessRole} LOGIN PASSWORD '${password}' IN ROLE core_terminal_confirmation`,
          );
          const writer = new URL(primary.url);
          writer.pathname = `/${name}`;
          writer.username = writerRole;
          writer.password = password;
          const witness = new URL(standby.url);
          witness.pathname = writer.pathname;
          witness.username = witnessRole;
          witness.password = password;
          return {
            database_url: writer.toString(),
            standby_url: witness.toString(),
            short_chat_funding_epoch: 1,
            recovery_queue: {
              url: evidence.url,
              namespace: `funding-bench-${run}-${rate}`,
              required_replicas: 1,
              ack_timeout_ms: 1000,
            },
          };
        },
      },
    );
} catch (error) {
  native.assertHealthy();
  throw error;
} finally {
  await native.close();
}
