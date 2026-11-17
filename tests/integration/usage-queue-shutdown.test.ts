import { expect, test } from "bun:test";
import { connect, createServer, type Socket } from "node:net";
import { UsageConsumer, UsageProducer, evidenceChannel } from "@gateway/runtime";
import { createSettlementStore } from "@gateway/storage/settlement";
import { event, granted } from "../support/core-settlement.ts";
import { withFinancial } from "../support/financial-execution.ts";
import { eventually } from "../support/gateway-process.ts";
import { createUsageQueueCleanup } from "../support/usage-queue-cleanup.ts";

async function gatedRedis(target: string) {
  const source = new URL(target);
  const upstreamHost = source.hostname;
  const upstreamPort = Number(source.port);
  const sockets = new Set<Socket>();
  const buffered = new Map<Socket, Buffer[]>();
  let accepted = 0;
  let holding = false;
  const server = createServer((client) => {
    accepted++;
    const upstream = connect({ host: upstreamHost, port: upstreamPort });
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on("error", () => undefined);
      socket.on("close", () => {
        sockets.delete(socket);
        client.destroy();
        upstream.destroy();
      });
    }
    client.on("data", (chunk) => {
      if (holding) {
        const queued = buffered.get(upstream) ?? [];
        queued.push(Buffer.from(chunk));
        buffered.set(upstream, queued);
      } else upstream.write(chunk);
    });
    upstream.pipe(client);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Redis gate did not bind");
  source.hostname = "127.0.0.1";
  source.port = String(address.port);
  function release() {
    holding = false;
    for (const [socket, chunks] of buffered) {
      if (!socket.destroyed) for (const chunk of chunks) socket.write(chunk);
    }
    buffered.clear();
  }
  return {
    url: source.toString(),
    hold() {
      holding = true;
    },
    buffered: () => [...buffered.values()].some((chunks) => chunks.length > 0),
    connections: () => sockets.size,
    accepted: () => accepted,
    release,
    async close() {
      release();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function cleanupQueue(
  url: string,
  cleanup: ReturnType<typeof createUsageQueueCleanup>,
  operations: Array<() => unknown>,
) {
  const results = await Promise.allSettled(
    operations.map((operation) => Promise.resolve().then(operation)),
  );
  results.push(
    ...(await Promise.allSettled([
      Promise.resolve().then(async () => {
        await cleanup.remove(url);
        expect(await cleanup.keys(url)).toEqual([]);
      }),
    ])),
  );
  const errors = results.flatMap((result) => (result.status === "rejected" ? [result.reason] : []));
  if (errors.length) throw new AggregateError(errors, "Evidence queue cleanup failed");
}

test("graceful evidence shutdown waits for Redis ACK after the funding commit instead of stranding an active job", () =>
  withFinancial(async (fixture, db) => {
    const setup = await fixture.bootstrap();
    const client = fixture.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const url = process.env.QUEUE_REDIS_URL!;
    const namespace = `test-usage:shutdown:${crypto.randomUUID()}`;
    const cleanup = createUsageQueueCleanup(namespace);
    const gate = await gatedRedis(url);
    const options = { namespace, replicas: 0, timeoutMs: 2000, channel: evidenceChannel };
    const producer = new UsageProducer({ ...options, url });
    const store = createSettlementStore(db);
    const consumer = new UsageConsumer({
      ...options,
      url: gate.url,
      concurrency: 1,
      process: async (groups) => {
        const result = await store.persist(groups);
        gate.hold();
        return result;
      },
    });
    let closing: Promise<void> | undefined;
    try {
      await consumer.ready();
      await producer.ready();
      await producer.publish({
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
            usage: { input_tokens: "7", output_tokens: "5" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      });
      await eventually(async () => gate.buffered(), "post-commit Redis ACK blocked", 5000);
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(1);
      expect((await producer.stats()).active).toBe(1);
      let closed = 0;
      closing = Promise.all([
        consumer.close().then(() => {
          closed++;
        }),
        consumer.close().then(() => {
          closed++;
        }),
      ]).then(() => undefined);
      await Bun.sleep(50);
      expect(closed).toBe(0);
      gate.release();
      await closing;
      expect(closed).toBe(2);
      expect((await producer.stats()).active).toBe(0);
      expect((await producer.stats()).waiting).toBe(0);
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(1);
    } finally {
      gate.release();
      await cleanupQueue(url, cleanup, [
        () => closing ?? consumer.close(),
        () => producer.close(),
        () => gate.close(),
      ]);
    }
  }));

test("an ACK blocked past the shutdown budget remains recoverable and cannot charge twice after restart", () =>
  withFinancial(async (fixture, db) => {
    const setup = await fixture.bootstrap();
    const client = fixture.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const url = process.env.QUEUE_REDIS_URL!;
    const namespace = `test-usage:shutdown-force:${crypto.randomUUID()}`;
    const cleanup = createUsageQueueCleanup(namespace);
    const gate = await gatedRedis(url);
    const options = {
      namespace,
      replicas: 0,
      timeoutMs: 2000,
      channel: evidenceChannel,
      concurrency: 1,
      lockDurationMs: 1000,
      stalledIntervalMs: 100,
    };
    const producer = new UsageProducer({ ...options, url });
    const store = createSettlementStore(db);
    const consumer = new UsageConsumer({
      ...options,
      url: gate.url,
      process: async (groups) => {
        const result = await store.persist(groups);
        gate.hold();
        return result;
      },
    });
    let replacement: UsageConsumer<Parameters<typeof store.persist>[0][number]> | undefined;
    try {
      await consumer.ready();
      await producer.ready();
      await producer.publish({
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
            usage: { input_tokens: "7", output_tokens: "5" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      });
      await eventually(async () => gate.buffered(), "post-commit ACK remains blocked", 5000);
      const started = Date.now();
      await consumer.close();
      expect(Date.now() - started).toBeGreaterThanOrEqual(9000);
      expect(Date.now() - started).toBeLessThan(15000);
      expect((await producer.stats()).active).toBe(1);
      const accepted = gate.accepted();
      await eventually(
        async () => gate.connections() === 0,
        "forced worker connections closed",
        2000,
      );
      await Bun.sleep(100);
      expect(gate.accepted()).toBe(accepted);
      expect(gate.connections()).toBe(0);
      gate.release();
      replacement = new UsageConsumer({
        ...options,
        url,
        process: (groups) => store.persist(groups),
      });
      await replacement.ready();
      await eventually(
        async () => {
          const stats = await producer.stats();
          return stats.active === 0 && stats.waiting === 0;
        },
        "abandoned ACK recovered",
        5000,
      );
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(1);
      expect(
        (
          await db`SELECT count(*)::int n FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
        )[0].n,
      ).toBe(1);
    } finally {
      gate.release();
      await cleanupQueue(url, cleanup, [
        () => consumer.close(),
        () => replacement?.close(),
        () => producer.close(),
        () => gate.close(),
      ]);
    }
  }));

test("closing evidence consumption during a blackholed Redis startup stays bounded and does not reopen connections", async () => {
  const url = process.env.QUEUE_REDIS_URL!;
  const namespace = `test-usage:shutdown-startup:${crypto.randomUUID()}`;
  const cleanup = createUsageQueueCleanup(namespace);
  const gate = await gatedRedis(url);
  gate.hold();
  const consumer = new UsageConsumer({
    url: gate.url,
    namespace,
    replicas: 0,
    timeoutMs: 1000,
    channel: evidenceChannel,
    process: async () => [],
  });
  try {
    await eventually(async () => gate.buffered(), "Redis startup handshake blocked", 5000);
    const started = Date.now();
    await consumer.close();
    expect(Date.now() - started).toBeLessThan(15000);
    await eventually(async () => gate.connections() === 0, "startup connections closed", 2000);
    const accepted = gate.accepted();
    gate.release();
    await Bun.sleep(100);
    expect(gate.accepted()).toBe(accepted);
    expect(gate.connections()).toBe(0);
  } finally {
    gate.release();
    await cleanupQueue(url, cleanup, [() => consumer.close(), () => gate.close()]);
  }
});

test("forced shutdown before the funding commit retains evidence for late completion and replay without a second charge", () =>
  withFinancial(async (fixture, db) => {
    const setup = await fixture.bootstrap();
    const client = fixture.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const url = process.env.QUEUE_REDIS_URL!;
    const namespace = `test-usage:shutdown-process:${crypto.randomUUID()}`;
    const cleanup = createUsageQueueCleanup(namespace);
    const options = {
      url,
      namespace,
      replicas: 0,
      timeoutMs: 2000,
      channel: evidenceChannel,
      concurrency: 1,
      lockDurationMs: 1000,
      stalledIntervalMs: 100,
    };
    const producer = new UsageProducer(options);
    const store = createSettlementStore(db);
    const entered = Promise.withResolvers<void>();
    const proceed = Promise.withResolvers<void>();
    const finished = Promise.withResolvers<void>();
    const consumer = new UsageConsumer({
      ...options,
      process: async (groups) => {
        entered.resolve();
        await proceed.promise;
        try {
          return await store.persist(groups);
        } finally {
          finished.resolve();
        }
      },
    });
    let replacement: UsageConsumer<Parameters<typeof store.persist>[0][number]> | undefined;
    try {
      await consumer.ready();
      await producer.ready();
      await producer.publish({
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
            usage: { input_tokens: "7", output_tokens: "5" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      });
      await entered.promise;
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(0);
      const started = Date.now();
      await consumer.close();
      expect(Date.now() - started).toBeGreaterThanOrEqual(9000);
      expect(Date.now() - started).toBeLessThan(15000);
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(0);
      expect((await producer.stats()).active).toBe(1);
      proceed.resolve();
      await finished.promise;
      replacement = new UsageConsumer({ ...options, process: (groups) => store.persist(groups) });
      await replacement.ready();
      await eventually(
        async () => {
          const stats = await producer.stats();
          return stats.active === 0 && stats.waiting === 0;
        },
        "pre-commit shutdown evidence recovered",
        5000,
      );
      expect(
        (
          await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${prepared.requestId}`
        )[0].n,
      ).toBe(1);
      expect(
        (
          await db`SELECT count(*)::int n FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
        )[0].n,
      ).toBe(1);
    } finally {
      proceed.resolve();
      await cleanupQueue(url, cleanup, [
        () => consumer.close(),
        () => replacement?.close(),
        () => producer.close(),
      ]);
    }
  }));
