import assert from "node:assert/strict";
import type { UsageProducer, UsageConsumer } from "@gateway/runtime";
import type { createDatabase } from "@gateway/storage";
import type { Credential, UsageEvidence } from "@gateway/contracts";
import {
  createNativeFixture,
  within,
  type NativeProcess,
} from "../tests/support/native-fixture.ts";

async function worker() {
  if (
    process.env.DURABILITY_PHASE !== "BEFORE_COMMIT" &&
    process.env.DURABILITY_PHASE !== "AFTER_COMMIT"
  )
    throw new Error("Invalid durability worker phase");
  if (
    !process.env.DURABILITY_MARKER ||
    !process.env.DATABASE_URL ||
    !process.env.QUEUE_REDIS_URL ||
    !process.env.REDIS_NAMESPACE
  )
    throw new Error("Incomplete durability worker configuration");
  setTimeout(() => process.exit(2), 60000);
  const [{ UsageConsumer }, { createDatabase, Store }] = await Promise.all([
    import("@gateway/runtime"),
    import("@gateway/storage"),
  ]);
  const db = createDatabase(process.env.DATABASE_URL!, 2);
  const store = new Store(db);
  const consumer = new UsageConsumer({
    url: process.env.QUEUE_REDIS_URL!,
    namespace: process.env.REDIS_NAMESPACE!,
    replicas: 1,
    timeoutMs: 1000,
    concurrency: 1,
    lockDurationMs: 1000,
    stalledIntervalMs: 1000,
    process: async (events) => {
      if (process.env.DURABILITY_PHASE === "BEFORE_COMMIT") {
        await db.begin(async (tx) => {
          const rows = await tx`SELECT persist_usage_batch(${events}::jsonb) AS result`;
          assert(
            Object.values(rows[0].result as Record<string, { status: string }>).every(
              (r) => r.status !== "ERROR",
            ),
          );
          await Bun.write(
            process.env.DURABILITY_MARKER!,
            "uncommitted evidence and ledger written",
          );
          await new Promise(() => {});
        });
      }
      await store.persistUsage(events);
      await Bun.write(process.env.DURABILITY_MARKER!, "reached");
      await new Promise(() => {});
    },
  });
  await consumer.ready();
  await new Promise(() => {});
}
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
  let db: ReturnType<typeof createDatabase> | undefined;
  let consumer: UsageConsumer | undefined;
  let producer: UsageProducer | undefined;
  let child: NativeProcess | undefined;
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
    const [{ UsageProducer, UsageConsumer }, { createDatabase, migrate, Store, FundsStore }] =
      await Promise.race([
        Promise.all([import("@gateway/runtime"), import("@gateway/storage")]),
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
    const url = new URL(postgres.url);
    db = createDatabase(url.toString(), 4);
    await migrate(db);
    console.log(
      JSON.stringify({ durabilityFixture: "ready", ownedResources: await fixture.resources() }),
    );
    const store = new Store(db);
    const funds = new FundsStore(db);
    const project = await store.createProject({
      name: id,
      rpm: 1000,
      tpm: 1000000,
      concurrency: 100,
    });
    const key = await store.createCredential(
      project,
      { name: "faults", models: [], operations: ["chat"] },
      id,
      "fixture",
    );
    await funds.credit(project, {
      currency: "USD",
      external_ref: "fixture",
      amount_atoms: "10000",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
    const provider = crypto.randomUUID();
    await store.createProvider(
      provider,
      {
        name: id,
        kind: "openai",
        base_url: "https://fixture.invalid/v1",
        rpm: 1000,
        concurrency: 100,
      },
      "fixture",
    );
    await store.createModel({ name: id, operations: ["chat"], max_output_tokens: 100 });
    const rates = {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
    };
    await store.createRoute(id, {
      provider_id: provider,
      upstream_model: id,
      operations: ["chat"],
      priority: 1,
      weight: 1,
      pricing: { currency: "USD", rates },
      ceilings: { input_tokens: "100", output_tokens: "100" },
    });
    const route = (await store.routes(id, "chat"))[0]!;
    const tariff = await funds.setTariff({
      model: id,
      operation: "chat",
      basis: "PROVIDER_USAGE",
      pricing: { currency: "USD", rates },
      ceilings: { input_tokens: "100", output_tokens: "100" },
    });
    const evidence = async (): Promise<UsageEvidence> => {
      const admitted = await store.admitInitial(
        crypto.randomUUID(),
        { id: key, project_id: project } as Credential,
        "chat",
        id,
        null,
        "fixture",
        route,
        {
          currency: "USD",
          amount_atoms: "100",
          input_characters: "0",
          prices: { [provider]: tariff.pricing_id },
        },
      );
      return {
        attemptId: admitted.attempt_id!,
        source: "execution",
        state: "SUCCEEDED",
        finality: "FINAL",
        usage: { input_tokens: "7", output_tokens: "5" },
        requestState: "SUCCEEDED",
      };
    };
    let expected = 0;
    const consume = async (namespace: string, endpoint: string) => {
      consumer = new UsageConsumer({
        url: endpoint,
        namespace,
        replicas: 0,
        timeoutMs: 1000,
        concurrency: 1,
        lockDurationMs: 1000,
        stalledIntervalMs: 1000,
        process: (events) => store.persistUsage(events),
      });
      await consumer.ready();
      await until(
        async () =>
          Number(
            (
              await db!`SELECT count(*)::int AS n FROM billing_charges WHERE amount_atoms=12 AND funded_atoms=12`
            )[0].n,
          ) === expected,
      );
      const inspector = new UsageProducer({
        url: endpoint,
        namespace,
        replicas: 0,
        timeoutMs: 1000,
      });
      try {
        await until(async () => {
          const counts = await inspector.stats();
          return Object.values(counts).every((n) => n === 0);
        });
      } finally {
        inspector.close();
      }
      await within(consumer.close(), 3000, "Consumer close");
      consumer = undefined;
    };
    for (const phase of ["BEFORE_COMMIT", "AFTER_COMMIT"]) {
      checkpoint = phase;
      const namespace = `${label}:${phase}`;
      const marker = await fixture.write(`${phase}.marker`, "");
      producer = new UsageProducer({ url: primaryURL, namespace, replicas: 1, timeoutMs: 1000 });
      child = await fixture.startBun(
        `worker-${phase}`,
        import.meta.path,
        {
          DURABILITY_PHASE: phase,
          DURABILITY_MARKER: marker,
          DATABASE_URL: url.toString(),
          QUEUE_REDIS_URL: primaryURL,
          REDIS_NAMESPACE: namespace,
        },
        ["--durability-worker"],
      );
      await producer.publish(await evidence());
      await until(async () => (await Bun.file(marker).text()) !== "");
      await child.stop("SIGKILL");
      child = undefined;
      expected++;
      if (phase === "AFTER_COMMIT")
        assert.equal(
          Number((await db`SELECT count(*)::int AS n FROM billing_charges`)[0].n),
          expected,
        );
      await consume(namespace, primaryURL);
      producer.close();
      producer = undefined;
      assert.equal(
        Number((await db`SELECT count(*)::int AS n FROM money_journal WHERE kind='SETTLE'`)[0].n),
        expected,
      );
      checks.push(`SIGKILL ${phase}: recovered exactly once`);
    }
    const namespace = `${label}:AOF`;
    producer = new UsageProducer({ url: primaryURL, namespace, replicas: 1, timeoutMs: 1000 });
    await producer.publish(await evidence());
    expected++;
    assert.equal((await producer.stats()).waiting, 1);
    producer.close();
    producer = undefined;
    await Promise.all([primary.stop("SIGKILL"), replica.stop("SIGKILL")]);
    checkpoint = "primary AOF restart";
    await primary.restart();
    producer = new UsageProducer({ url: primaryURL, namespace, replicas: 0, timeoutMs: 1000 });
    assert.equal((await producer.stats()).waiting, 1);
    producer.close();
    producer = undefined;
    checks.push("primary hard restart retains acknowledged evidence from its AOF");
    await primary.stop("SIGKILL");
    checkpoint = "replica AOF promotion";
    await replica.restart();
    await replica.command("REPLICAOF", "NO", "ONE");
    await consume(namespace, replicaURL);
    checks.push("acknowledged replica promotion retains evidence and exact settlement");
    checkpoint = "unsafe ingress rejection";
    producer = new UsageProducer({ url: replicaURL, namespace, replicas: 1, timeoutMs: 200 });
    await assert.rejects(producer.ready());
    producer.close();
    producer = undefined;
    checks.push("promoted node without required durable replica rejects admission");
    await replica.command("CONFIG", "SET", "appendfsync", "everysec");
    producer = new UsageProducer({ url: replicaURL, namespace, replicas: 0, timeoutMs: 200 });
    await assert.rejects(producer.ready());
    producer.close();
    producer = undefined;
    await replica.command("CONFIG", "SET", "appendfsync", "always");
    checks.push("unsafe persistence configuration rejects admission");
    producer = new UsageProducer({ url: replicaURL, namespace, replicas: 0, timeoutMs: 1000 });
    await assert.rejects(
      producer.publish({
        ...(await evidence()),
        body: "PRIVATE_LLM_CONTENT_SENTINEL",
      } as UsageEvidence),
    );
    producer.close();
    producer = undefined;
    const wallet = (
      await db`SELECT available_atoms::text,reserved_atoms::text,spent_atoms::text,debt_atoms::text FROM wallets`
    )[0];
    assert.equal(wallet.spent_atoms, String(expected * 12));
    assert.equal(wallet.debt_atoms, "0");
    assert.equal(
      wallet.reserved_atoms,
      "100",
      "Invalid evidence cannot silently release an already-created hold",
    );
    assert.equal(Number((await db`SELECT count(*)::int AS n FROM usage_events`)[0].n), expected);
    assert.equal(
      Number((await db`SELECT count(*)::int AS n FROM money_journal WHERE kind='SETTLE'`)[0].n),
      expected,
    );
    checks.push("strict usage payload rejects body; all monetary journals remain singular");
    result = {
      durability: "pass",
      fixture: "native",
      scope: "legacy-single-ledger",
      checks,
      charges: expected,
      wallet,
      tools: fixture.tools,
    };
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
      clean(() => child?.stop("SIGKILL")),
      clean(() => consumer?.close()),
      clean(() => producer?.close()),
      clean(() => db?.close({ timeout: 1 })),
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

if (Bun.argv.length === 3 && Bun.argv[2] === "--durability-worker") await worker();
else await verify();
