import assert from "node:assert/strict";
import { dirname } from "node:path";
import type { SQL } from "bun";
import type { UsageProducer } from "@gateway/runtime";
import type { CoreEvidenceGroupV1, ExecutionReply } from "@gateway/contracts";
import { createNativeFixture, within } from "../tests/support/native-fixture.ts";

async function initializeFixture() {
  let interrupted = false;
  const onSignal = () => {
    interrupted = true;
  };
  process.once("SIGTERM", onSignal);
  process.once("SIGINT", onSignal);
  try {
    const fixture = await createNativeFixture();
    if (interrupted) {
      await fixture.close();
      throw new Error("Durability interrupted during native preflight");
    }
    return fixture;
  } finally {
    process.removeListener("SIGTERM", onSignal);
    process.removeListener("SIGINT", onSignal);
  }
}

async function supplier(db: SQL, account: string) {
  return (
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`
  )[0] as { balance: string; reserved: string; spent: string };
}

async function verify() {
  if (Bun.argv.length !== 2)
    throw new Error(
      "test:durability accepts no arguments; configure native tool paths in the environment",
    );
  const fixture = await initializeFixture();
  const until = fixture.waitFor;
  const id = crypto.randomUUID().replaceAll("-", "");
  const label = `gateway-durability-${id}`;
  let checkpoint = "fixture initialization";
  let producer: UsageProducer<CoreEvidenceGroupV1> | undefined;
  let worker: { close(): Promise<void> } | undefined;
  let financial: Promise<void> | undefined;
  const checks: string[] = [];
  let failureCause: unknown;
  const cleanupErrors: unknown[] = [];
  let result: Record<string, unknown> | undefined;
  const interrupted = Promise.withResolvers<never>();
  const onSignal = () => {
    failureCause ??= new Error("Durability interrupted");
    interrupted.reject(failureCause);
    void fixture.close().catch((error: unknown) => {
      cleanupErrors.push(error);
    });
  };
  process.once("SIGTERM", onSignal);
  process.once("SIGINT", onSignal);
  const deadline = setTimeout(onSignal, 120000);
  console.log(JSON.stringify({ durabilityFixture: "started", directory: fixture.directory }));
  try {
    checkpoint = "business dependency loading";
    const [
      { UsageProducer, evidenceChannel },
      { createSettlementWorker },
      { withFinancial, prepareFinancialAdmission },
    ] = await Promise.race([
      Promise.all([
        import("@gateway/runtime"),
        import("../apps/core-worker/src/settlement/worker.ts"),
        import("../tests/support/financial-execution.ts"),
      ]),
      interrupted.promise,
    ]);
    const primary = await fixture.startRedis("primary");
    const replica = await fixture.startRedis("replica", primary);
    const primaryURL = primary.url;
    const replicaURL = replica.url;
    await until(async () =>
      (await replica.command("INFO", "replication")).includes("master_link_status:up"),
    );
    producer = new UsageProducer({
      channel: evidenceChannel,
      url: primaryURL,
      namespace: `${label}:topology`,
      replicas: 1,
      timeoutMs: 1000,
      requireAllReplicas: true,
    });
    checkpoint = "strict production topology bootstrap";
    await until(async () => {
      try {
        await producer!.ready();
        return true;
      } catch {
        return false;
      }
    });
    checkpoint = "live durability configuration drift";
    await primary.command("CONFIG", "SET", "maxmemory-policy", "allkeys-lru");
    await Bun.sleep(110);
    await assert.rejects(producer!.ready());
    producer.close();
    producer = undefined;
    await primary.command("CONFIG", "SET", "maxmemory-policy", "noeviction");
    producer = new UsageProducer({
      channel: evidenceChannel,
      url: primaryURL,
      namespace: `${label}:topology`,
      replicas: 0,
      timeoutMs: 1000,
      requireAllReplicas: true,
    });
    await assert.rejects(producer.ready());
    producer.close();
    producer = undefined;
    checks.push(
      "production topology acknowledges every connected candidate and rejects live persistence drift",
    );
    const postgres = await fixture.startPostgres();
    const admission = await fixture.startRedis("admission");
    process.env.DATABASE_URL = postgres.url;
    process.env.REDIS_URL = admission.url;
    process.env.PATH = [dirname(process.execPath), process.env.PATH].filter(Boolean).join(":");
    checkpoint = "v2 financial fixture";
    financial = withFinancial(
      async (f, db) => {
        console.log(
          JSON.stringify({
            durabilityFixture: "ready",
            ownedResources: await fixture.resources(),
          }),
        );
        const setup = await f.bootstrap();
        const client = f.client();
        await client.startup();
        const account = setup.provider.account_id;
        const evidence = async () => {
          const prepared = await prepareFinancialAdmission(setup, client);
          const admitted = (await prepared.admit()) as ExecutionReply;
          await client.execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
            deadlineAt: prepared.deadline,
          });
          const fact = client.signEvidence({
            event_id: crypto.randomUUID(),
            attempt_id: prepared.attemptId,
            permit_epoch: "1",
            source: "execution",
            source_revision: "1",
            finality: "FINAL",
            usage: { input_tokens: "30", output_tokens: "15" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          });
          return {
            requestId: prepared.requestId,
            group: { events: [fact] } as CoreEvidenceGroupV1,
          };
        };
        const namespace = `${label}:AOF`;
        const settled = await evidence();
        assert.deepEqual(await supplier(db, account), {
          balance: "1000",
          reserved: "30",
          spent: "0",
        });
        checkpoint = "replica fsync acknowledgement after a stalled replica";
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: primaryURL,
          namespace: `${label}:stall`,
          replicas: 1,
          timeoutMs: 3000,
        });
        await producer.publish(settled.group);
        for (let round = 0; round < 5; round++) {
          let published: Promise<void> | undefined;
          let resumed = 0;
          process.kill(replica.pid, "SIGSTOP");
          try {
            published = producer.publish(settled.group);
            await Bun.sleep(200);
          } finally {
            resumed = performance.now();
            process.kill(replica.pid, "SIGCONT");
          }
          await published;
          const waited = performance.now() - resumed;
          assert.ok(waited < 300, `replica acknowledgement waited ${Math.round(waited)}ms`);
        }
        producer.close();
        producer = undefined;
        checks.push(
          "a replica that resumes with a stale fsync acknowledgement confirms evidence without waiting for its periodic ACK",
        );
        checkpoint = "acknowledged v2 evidence";
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: primaryURL,
          namespace,
          replicas: 1,
          timeoutMs: 1000,
        });
        await producer.publish(settled.group);
        assert.equal((await producer.stats()).waiting, 1);
        producer.close();
        producer = undefined;
        await Promise.all([primary.stop("SIGKILL"), replica.stop("SIGKILL")]);
        checkpoint = "primary AOF restart";
        await primary.restart();
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: primaryURL,
          namespace,
          replicas: 0,
          timeoutMs: 1000,
        });
        assert.equal((await producer.stats()).waiting, 1);
        producer.close();
        producer = undefined;
        checks.push("primary hard restart retains acknowledged v2 evidence from its AOF");
        await primary.stop("SIGKILL");
        checkpoint = "replica AOF promotion";
        await replica.restart();
        await replica.command("REPLICAOF", "NO", "ONE");
        worker = createSettlementWorker({
          db,
          url: replicaURL,
          namespace,
          replicas: 0,
          timeoutMs: 1000,
          concurrency: 1,
        });
        const settlement = worker as ReturnType<typeof createSettlementWorker>;
        await settlement.ready();
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: replicaURL,
          namespace,
          replicas: 0,
          timeoutMs: 1000,
        });
        const inspector = producer;
        await until(async () => Object.values(await inspector.stats()).every((n) => n === 0));
        producer.close();
        producer = undefined;
        await within(settlement.close(), 3000, "Settlement worker close");
        worker = undefined;
        assert.deepEqual(
          [
            ...(await db`SELECT amount_atoms::text amount FROM billing_charges WHERE request_id=${settled.requestId}`),
          ],
          [{ amount: "100" }],
        );
        assert.deepEqual(await supplier(db, account), {
          balance: "985",
          reserved: "0",
          spent: "15",
        });
        checks.push(
          "acknowledged replica promotion retains v2 evidence and the settlement worker settles it once",
        );
        checkpoint = "unsafe ingress rejection";
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: replicaURL,
          namespace,
          replicas: 1,
          timeoutMs: 200,
        });
        await assert.rejects(producer.ready());
        producer.close();
        producer = undefined;
        checks.push("promoted node without required durable replica rejects admission");
        await replica.command("CONFIG", "SET", "appendfsync", "everysec");
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: replicaURL,
          namespace,
          replicas: 0,
          timeoutMs: 200,
        });
        await assert.rejects(producer.ready());
        producer.close();
        producer = undefined;
        await replica.command("CONFIG", "SET", "appendfsync", "always");
        checks.push("unsafe persistence configuration rejects admission");
        checkpoint = "strict evidence payload";
        const held = await evidence();
        producer = new UsageProducer({
          channel: evidenceChannel,
          url: replicaURL,
          namespace,
          replicas: 0,
          timeoutMs: 1000,
        });
        await assert.rejects(
          producer.publish({
            events: [
              Object.assign({ body: "PRIVATE_LLM_CONTENT_SENTINEL" }, held.group.events[0]!),
            ],
          }),
        );
        assert.equal((await producer.stats()).waiting, 0);
        producer.close();
        producer = undefined;
        const ledger = (
          await db`SELECT
            (SELECT count(*)::int FROM billing_charges) charges,
            (SELECT count(*)::int FROM core_evidence_receipts) receipts,
            (SELECT state FROM billing_holds WHERE request_id=${held.requestId}) held,
            (SELECT count(*)::int FROM (SELECT journal_id FROM upstream_entries GROUP BY journal_id HAVING sum(atoms)<>0) j) unbalanced`
        )[0] as { charges: number; receipts: number; held: string; unbalanced: number };
        assert.deepEqual(ledger, { charges: 1, receipts: 1, held: "HELD", unbalanced: 0 });
        assert.deepEqual(
          await supplier(db, account),
          { balance: "985", reserved: "30", spent: "15" },
          "Invalid evidence cannot silently release an already-created hold",
        );
        checks.push(
          "strict evidence payload rejects body; charges, receipts and supplier journals remain singular",
        );
        result = {
          durability: "pass",
          fixture: "native",
          scope: "v2-core-settlement",
          checks,
          charges: ledger.charges,
          supplier: await supplier(db, account),
          tools: fixture.tools,
        };
      },
      { redisUrl: admission.url },
    );
    await Promise.race([financial, interrupted.promise]);
  } catch (error) {
    failureCause = new Error(`Durability fixture failed at ${checkpoint}`, { cause: error });
  } finally {
    clearTimeout(deadline);
    async function clean(action: () => unknown | Promise<unknown>) {
      try {
        await within(Promise.resolve().then(action), 4000, "Durability cleanup");
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    await Promise.all([
      clean(() => worker?.close()),
      clean(() => producer?.close()),
      clean(() => financial?.catch(() => undefined)),
    ]);
    await clean(() => fixture.close());
    process.removeListener("SIGTERM", onSignal);
    process.removeListener("SIGINT", onSignal);
  }
  const resources = await fixture.resources();
  if (
    resources.processes.length ||
    resources.directories.length ||
    resources.ports.length ||
    resources.timers
  )
    cleanupErrors.push(new Error("Durability owned resources remain"));
  try {
    fixture.assertHealthy();
  } catch (error) {
    cleanupErrors.push(error);
  }
  if (failureCause || cleanupErrors.length) {
    console.error(JSON.stringify({ durability: "fail", ownedResources: resources }));
    throw new AggregateError(
      failureCause ? [failureCause, ...cleanupErrors] : cleanupErrors,
      "Durability fixture failed",
    );
  }
  if (!result) throw new Error("Durability fixture completed without evidence");
  console.log(JSON.stringify({ ...result, ownedResources: resources }));
}

await verify();
