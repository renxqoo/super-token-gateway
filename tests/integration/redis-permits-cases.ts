import { expect, spyOn, test } from "bun:test";
import { once } from "node:events";
import { ClusterCoordinator } from "@gateway/runtime";
import {
  redisUrl,
  waitForRedisState,
  redisProc,
  startEphemeralRedis,
} from "./cluster-and-media-fixture.ts";

async function expectPermitReleased(redis: Bun.RedisClient, key: string) {
  const deadline = performance.now() + 1_000;
  let remaining: unknown;
  do {
    remaining = await redis.send("ZCARD", [key]);
    if (remaining === 0) break;
    await Bun.sleep(20);
  } while (performance.now() < deadline);
  expect(remaining).toBe(0);
}

export function registerPermitCases() {
  test("Redis script cache eviction keeps renewals and shared admission limits intact", async () => {
    const redis = new Bun.RedisClient(redisUrl, { autoReconnect: false });
    const namespace = `script-cache-${crypto.randomUUID()}`;
    const first = new ClusterCoordinator({ redisUrl, namespace });
    const second = new ClusterCoordinator({ redisUrl, namespace });
    const limits = { rpm: 2, tpm: 6, concurrency: 1 };
    try {
      await Promise.all([first.start(), second.start(), redis.connect()]);
      const permit = await first.acquireRateLimit("test", limits, 3, 150);
      if (!permit.ok) throw new Error(`Initial Redis permit failed: ${permit.reason}`);
      try {
        await redis.send("SCRIPT", ["FLUSH", "SYNC"]);
        await Bun.sleep(260);
        expect(permit.signal?.aborted).toBe(false);
        expect(await second.acquireRateLimit("test", limits, 3, 150)).toEqual({
          ok: false,
          reason: "concurrency",
        });
      } finally {
        permit.release();
      }
      let admitted;
      for (let attempt = 0; attempt < 10; attempt++) {
        admitted = await second.acquireRateLimit("test", limits, 3, 150);
        if (admitted.ok) break;
        await Bun.sleep(10);
      }
      if (!admitted?.ok) throw new Error("Released Redis permit continued to block admission");
      admitted.release();
      expect(await redis.send("GET", [`${namespace}:rl:rpm:test`])).toBe("2");
      expect(await redis.send("GET", [`${namespace}:rl:tpm:test`])).toBe("6");
    } finally {
      redis.close();
      await Promise.all([first.close(), second.close()]);
    }
  });

  test.each([60_000, 90])(
    "a live Redis concurrency permit with %i ms TTL remains visible to another Gateway",
    async (leaseMs) => {
      const namespace = `concurrency-${crypto.randomUUID()}`;
      const nodeA = new ClusterCoordinator({ redisUrl, namespace });
      const nodeB = new ClusterCoordinator({ redisUrl, namespace });
      const limits = { rpm: 100, tpm: 100, concurrency: 1 };
      try {
        await nodeA.start();
        await nodeB.start();
        const first = await nodeA.acquireRateLimit("active-billingAccount", limits, 1, leaseMs);
        if (!first.ok) throw new Error("First concurrency permit rejected");
        try {
          await Bun.sleep(150);
          expect(await nodeB.acquireRateLimit("active-billingAccount", limits, 1, leaseMs)).toEqual(
            {
              ok: false,
              reason: "concurrency",
            },
          );
        } finally {
          first.release();
        }
      } finally {
        await nodeB.close();
        await nodeA.close();
      }
    },
  );

  test("batched Redis permits flush pending releases before dispatching new acquires", async () => {
    const redis = new Bun.RedisClient(redisUrl, { autoReconnect: false });
    const namespace = `batch-rel-acq-${crypto.randomUUID()}`;
    const coord = new ClusterCoordinator({ redisUrl, namespace });
    const holdPermits: { release: () => void }[] = [];
    try {
      await Promise.all([coord.start(), redis.connect()]);
      for (let i = 0; i < 6; i++) {
        const p = await coord.acquireRateLimit(
          `bg-${i}`,
          { rpm: 1000, tpm: 1000, concurrency: 10 },
          1,
          60_000,
        );
        if (!p.ok) throw new Error(`Background permit failed: ${p.reason}`);
        holdPermits.push(p);
      }
      const tight = { rpm: 1000, tpm: 1000, concurrency: 1 };
      for (let round = 0; round < 15; round++) {
        if (round === 5) await redis.send("SCRIPT", ["FLUSH", "SYNC"]);
        const [pA, pB] = await Promise.all([
          coord.acquireRateLimit("tight-slot-a", tight, 1, 60_000),
          coord.acquireRateLimit("tight-slot-b", tight, 1, 60_000),
        ]);
        expect(pA.ok).toBe(true);
        expect(pB.ok).toBe(true);
        if (pA.ok) pA.release();
        if (pB.ok) pB.release();
      }
    } finally {
      for (const p of holdPermits) p.release();
      redis.close();
      await coord.close();
    }
  });

  test("transient Redis socket drop preserves live permits, flushes offline releases on reconnect, and revokes permits before expiry if Redis stays down", async () => {
    const admin = new Bun.RedisClient(redisUrl, { autoReconnect: false });
    const namespace = `resilience-${crypto.randomUUID()}`;
    const coordA = new ClusterCoordinator({ redisUrl, namespace });
    const coordB = new ClusterCoordinator({ redisUrl, namespace });
    const limits = { rpm: 10_000, tpm: 10_000, concurrency: 2 };
    try {
      await Promise.all([coordA.start(), coordB.start(), admin.connect()]);
      const live = await coordA.acquireRateLimit("stream-key", limits, 1, 10_000);
      const releasedOffline = await coordA.acquireRateLimit("stream-key", limits, 1, 10_000);
      if (!live.ok || !releasedOffline.ok) throw new Error("Initial permits failed");
      expect(await coordB.acquireRateLimit("stream-key", limits, 1, 10_000)).toEqual({
        ok: false,
        reason: "concurrency",
      });

      await admin.send("CLIENT", ["KILL", "TYPE", "normal", "SKIPME", "yes"]);
      await waitForRedisState(false, coordA);
      expect(live.signal?.aborted).toBe(false);
      releasedOffline.release();

      await waitForRedisState(true, coordA, coordB);
      expect(live.signal?.aborted).toBe(false);
      const concKey = `${namespace}:rl:conc:stream-key`;
      expect(Number(await admin.send("ZCARD", [concKey]))).toBe(1);
      const reopened = await coordB.acquireRateLimit("stream-key", limits, 1, 10_000);
      expect(reopened.ok).toBe(true);
      if (reopened.ok) reopened.release();
      live.release();

      const batchPermits = await Promise.all(
        Array.from({ length: 300 }, (_, i) =>
          coordA.acquireRateLimit(
            `batch-${i % 10}`,
            { rpm: 10_000, tpm: 10_000, concurrency: 100 },
            1,
            1200,
          ),
        ),
      );
      expect(batchPermits.every((p) => p.ok)).toBe(true);
      await Bun.sleep(1500);
      for (const p of batchPermits) {
        if (p.ok) expect(p.signal?.aborted).toBe(false);
      }
      for (const p of batchPermits) {
        if (p.ok) p.release();
      }

      const shortPermit = await coordA.acquireRateLimit(
        "expiry-cutoff",
        { rpm: 100, tpm: 100, concurrency: 1 },
        1,
        450,
      );
      if (!shortPermit.ok) throw new Error("Short permit failed");
      const redisExited = once(redisProc!, "exit");
      redisProc!.kill("SIGTERM");
      await redisExited;
      await waitForRedisState(false, coordA);
      const abortDeadline = Date.now() + 900;
      while (!shortPermit.signal?.aborted && Date.now() < abortDeadline) {
        await Bun.sleep(15);
      }
      expect(shortPermit.signal?.aborted).toBe(true);

      await startEphemeralRedis(Number(new URL(redisUrl).port));
      await waitForRedisState(true, coordA, coordB);
    } finally {
      admin.close();
      await Promise.all([coordA.close(), coordB.close()]);
    }
  });

  test("symptom: a timed-out Redis acquire creates a leaked permit after the first cleanup", async () => {
    const admin = new Bun.RedisClient(redisUrl, { autoReconnect: false });
    const namespace = `late-acquire-${crypto.randomUUID()}`;
    const key = `${namespace}:rl:conc:late-slot`;
    const first = new ClusterCoordinator({ redisUrl, namespace });
    const second = new ClusterCoordinator({ redisUrl, namespace });
    const limits = { rpm: 1000, tpm: 1000, concurrency: 1 };
    const forward = Promise.withResolvers<void>();
    const earlyCleanup = Promise.withResolvers<unknown>();
    const executed = Promise.withResolvers<unknown>();
    const send = Bun.RedisClient.prototype.send;
    let intercepted = false;
    const mock = spyOn(Bun.RedisClient.prototype, "send").mockImplementation(async function (
      this: Bun.RedisClient,
      command,
      args,
    ) {
      if (!intercepted && command === "EVALSHA" && args.includes(key)) {
        intercepted = true;
        await forward.promise;
        const result = await send.call(this, command, args);
        executed.resolve(result);
        return result;
      }
      const result = await send.call(this, command, args);
      if (command === "ZREM" && args[0] === key) earlyCleanup.resolve(result);
      return result;
    });
    try {
      await Promise.all([first.start(), second.start(), admin.connect()]);
      expect(await first.acquireRateLimit("late-slot", limits, 1, 30_000)).toEqual({
        ok: false,
        reason: "unavailable",
      });
      expect(first.isRedisConnected()).toBe(true);
      expect(await earlyCleanup.promise).toBe(0);
      forward.resolve();
      expect(await executed.promise).toEqual([0]);
      await expectPermitReleased(admin, key);
      const next = await second.acquireRateLimit("late-slot", limits, 1, 30_000);
      expect(next.ok).toBe(true);
      if (next.ok) next.release();
    } finally {
      forward.resolve();
      mock.mockRestore();
      admin.close();
      await Promise.all([first.close(), second.close()]);
    }
  });

  test.each([1, 16])(
    "symptom: queued Redis requests hang behind %i timed-out acquire batches",
    async (batches) => {
      const namespace = `stalled-acquire-${crypto.randomUUID()}`;
      const coordinator = new ClusterCoordinator({ redisUrl, namespace });
      const limits = { rpm: 1000, tpm: 1000, concurrency: 1 };
      const started = Array.from({ length: batches }, () => Promise.withResolvers<void>());
      const resume = Promise.withResolvers<void>();
      const failed = { ok: false, reason: "unavailable" } as const;
      const background: (() => void)[] = [];
      const send = Bun.RedisClient.prototype.send;
      let index = 0;
      const mock = spyOn(Bun.RedisClient.prototype, "send").mockImplementation(async function (
        this: Bun.RedisClient,
        command,
        args,
      ) {
        if (
          command === "EVALSHA" &&
          args.some((arg) => arg.startsWith(`${namespace}:rl:conc:held-`))
        ) {
          started[index++]?.resolve();
          await resume.promise;
          throw new Error("Delayed Redis command failed");
        }
        return send.call(this, command, args);
      });
      try {
        await coordinator.start();
        for (let i = 0; i < 4; i++) {
          const permit = await coordinator.acquireRateLimit(`live-${i}`, limits, 1, 30_000);
          if (!permit.ok) throw new Error(`Preflight permit failed: ${permit.reason}`);
          background.push(permit.release);
        }
        const active = [];
        for (const [position, start] of started.entries()) {
          active.push(coordinator.acquireRateLimit(`held-${position}`, limits, 1, 2_900));
          await start.promise;
        }
        const queued = coordinator.acquireRateLimit("queued", limits, 1, 30_000);
        expect(await Promise.all(active)).toEqual(started.map(() => failed));
        const later = coordinator.acquireRateLimit("later", limits, 1, 30_000);
        const results = await Promise.race([
          Promise.all([queued, later]),
          Bun.sleep(600).then(() => "acquires still pending"),
        ]);
        if (batches === 1) {
          expect(results).toMatchObject([{ ok: true }, { ok: true }]);
        } else {
          expect(results).toEqual([failed, failed]);
        }
        if (Array.isArray(results)) for (const result of results) if (result.ok) result.release();
      } finally {
        resume.resolve();
        mock.mockRestore();
        for (const release of background) release();
        await coordinator.close();
      }
    },
  );

  test("symptom: a NOSCRIPT reply after the Redis acquire deadline still starts a quota write", async () => {
    const namespace = `expired-script-${crypto.randomUUID()}`;
    const key = `${namespace}:rl:conc:target`;
    const coordinator = new ClusterCoordinator({ redisUrl, namespace });
    const deliverCacheMiss = Promise.withResolvers<void>();
    const commandClient = Promise.withResolvers<Bun.RedisClient>();
    const send = Bun.RedisClient.prototype.send;
    let fallbackStarted = false;
    const mock = spyOn(Bun.RedisClient.prototype, "send").mockImplementation(async function (
      this: Bun.RedisClient,
      command,
      args,
    ) {
      if (command === "EVALSHA" && args.includes(key)) {
        await deliverCacheMiss.promise;
        commandClient.resolve(this);
        throw new Error("NOSCRIPT No matching script");
      }
      if (command === "EVAL" && args.includes(key)) fallbackStarted = true;
      return send.call(this, command, args);
    });
    try {
      await coordinator.start();
      expect(
        await coordinator.acquireRateLimit(
          "target",
          {
            rpm: 1000,
            tpm: 1000,
            concurrency: 1,
          },
          1,
          30_000,
        ),
      ).toEqual({ ok: false, reason: "unavailable" });
      deliverCacheMiss.resolve();
      const client = await commandClient.promise;
      expect(await send.call(client, "PING", [])).toBe("PONG");
      expect(await send.call(client, "GET", [`${namespace}:rl:rpm:target`])).toBeNull();
      expect(fallbackStarted).toBe(false);
    } finally {
      deliverCacheMiss.resolve();
      mock.mockRestore();
      await coordinator.close();
    }
  });

  test("a failed Redis acquire batch recovers the permit release sent with it", async () => {
    const admin = new Bun.RedisClient(redisUrl, { autoReconnect: false });
    const namespace = `piggyback-release-${crypto.randomUUID()}`;
    const coordA = new ClusterCoordinator({ redisUrl, namespace });
    const coordB = new ClusterCoordinator({ redisUrl, namespace });
    const tight = { rpm: 1000, tpm: 1000, concurrency: 1 };
    const releaseKey = `${namespace}:rl:conc:piggy-slot`;
    const background: (() => void)[] = [];
    const send = Bun.RedisClient.prototype.send;
    let combinedCommand: string[] | undefined;
    const mock = spyOn(Bun.RedisClient.prototype, "send").mockImplementation(async function (
      this: Bun.RedisClient,
      command,
      args,
    ) {
      if (command === "EVALSHA" && args.includes(`${namespace}:rl:conc:batch-slot`)) {
        combinedCommand = args;
        throw new Error("Redis command reply unavailable");
      }
      return send.call(this, command, args);
    });
    try {
      await Promise.all([coordA.start(), coordB.start(), admin.connect()]);
      for (let index = 0; index < 4; index++) {
        const permit = await coordA.acquireRateLimit(`background-${index}`, tight, 1, 30_000);
        if (!permit.ok) throw new Error(`Background permit failed: ${permit.reason}`);
        background.push(permit.release);
      }
      const held = await coordA.acquireRateLimit("piggy-slot", tight, 1, 30_000);
      if (!held.ok) throw new Error(`Initial permit failed: ${held.reason}`);
      expect(await coordB.acquireRateLimit("piggy-slot", tight, 1, 30_000)).toEqual({
        ok: false,
        reason: "concurrency",
      });

      held.release();
      const batch = await Promise.all(
        Array.from({ length: 256 }, () => coordA.acquireRateLimit("batch-slot", tight, 1, 30_000)),
      );
      expect(combinedCommand).toContain(releaseKey);
      expect(batch.every((result) => !result.ok && result.reason === "unavailable")).toBe(true);
      expect(coordA.isRedisConnected()).toBe(true);
      await expectPermitReleased(admin, releaseKey);
      const next = await coordB.acquireRateLimit("piggy-slot", tight, 1, 30_000);
      expect(next.ok).toBe(true);
      if (next.ok) next.release();
    } finally {
      mock.mockRestore();
      for (const release of background) release();
      admin.close();
      await Promise.all([coordA.close(), coordB.close()]);
    }
  });
}
