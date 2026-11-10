import { RedisClient } from "bun";
import { Queue, Worker, createBunRedisClient } from "bullmq";
import { Batcher, GatewayError, usageEvidenceSchema, usageBatchSchema } from "@gateway/kernel";
import type { UsageEvidence } from "@gateway/kernel";

type Payload = { version: 1; events: UsageEvidence[] };
export interface UsageQueueOptions {
  url: string;
  namespace: string;
  replicas: number;
  timeoutMs: number;
  batchSize?: number;
  batchDelayMs?: number;
  maxPendingJobs?: number;
  maxInFlight?: number;
  onError?: () => void;
}
interface Generation {
  raw: RedisClient;
  adapter: ReturnType<typeof createBunRedisClient>;
  queue: Queue<Payload>;
  broken: boolean;
  ready: Promise<void>;
}
const queueName = "usage-v1";
const queuePrefix = (namespace: string) => `{${namespace}:usage}`;
const failure = () =>
  new GatewayError(
    503,
    "usage_queue_unavailable",
    "Durable usage acceptance is unavailable; query the original request before retrying",
  );

/** The publisher never reconnects a connection inside an add/WAITAOF pair. */
export class UsageProducer {
  private generation: Generation | undefined;
  private readonly stopping = new AbortController();
  private readonly batch: Batcher<UsageEvidence, void>;
  private backlog = 0;
  private backlogCheckedAt = 0;
  private backlogProbe: Promise<void> | undefined;
  constructor(readonly options: UsageQueueOptions) {
    this.batch = new Batcher(
      (events) => this.publishBatch(events),
      options.batchSize ?? 64,
      options.batchDelayMs ?? 1,
      2,
      // One awaited fact per admitted HTTP operation, plus bounded recovery/poll work.
      (options.maxInFlight ?? 10000) + 128,
    );
  }
  private invalidate(g: Generation): void {
    if (g.broken) return;
    g.broken = true;
    g.adapter.status = "end";
    g.raw.close();
    if (this.generation === g) this.generation = undefined;
    void g.queue.close().catch(() => undefined);
  }
  private async bounded<T>(operation: () => Promise<T>, g: Generation): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let abort: (() => void) | undefined;
    try {
      this.stopping.signal.throwIfAborted();
      return await Promise.race([
        operation(),
        new Promise<never>((_, reject) => {
          abort = () => reject(failure());
          this.stopping.signal.addEventListener("abort", abort, { once: true });
          timer = setTimeout(() => reject(failure()), this.options.timeoutMs);
        }),
      ]);
    } catch (error) {
      if (error instanceof GatewayError && error.status < 500) throw error;
      this.invalidate(g);
      this.options.onError?.();
      throw failure();
    } finally {
      clearTimeout(timer);
      if (abort) this.stopping.signal.removeEventListener("abort", abort);
    }
  }
  private create(): Generation {
    const raw = new RedisClient(this.options.url, {
      connectionTimeout: 1000,
      idleTimeout: 0,
      maxRetries: 0,
      autoReconnect: false,
      enableOfflineQueue: false,
    });
    const adapter = createBunRedisClient(raw, { lazyConnect: true });
    const queue = new Queue<Payload>(queueName, {
      connection: adapter,
      prefix: queuePrefix(this.options.namespace),
      defaultJobOptions: {
        attempts: 10,
        backoff: { type: "exponential", delay: 250 },
        removeOnComplete: true,
        removeOnFail: false,
      },
    });
    const g: Generation = { raw, adapter, queue, broken: false, ready: Promise.resolve() };
    // Stop adapter-level reconnection as well as native automatic reconnection.
    adapter.on("close", () => this.invalidate(g));
    adapter.on("error", () => this.options.onError?.());
    queue.on("error", () => this.options.onError?.());
    g.ready = this.bounded(async () => {
      await queue.waitUntilReady();
      const settings = (await raw.send("CONFIG", [
        "GET",
        "appendonly",
        "appendfsync",
        "maxmemory-policy",
        "no-appendfsync-on-rewrite",
        "aof-load-truncated",
      ])) as string[] | Record<string, string>;
      const config = new Map<string, string>();
      if (Array.isArray(settings))
        for (let i = 0; i < settings.length; i += 2) config.set(settings[i]!, settings[i + 1]!);
      else for (const [key, value] of Object.entries(settings)) config.set(key, value);
      if (
        config.get("appendonly") !== "yes" ||
        config.get("appendfsync") !== "always" ||
        config.get("maxmemory-policy") !== "noeviction" ||
        config.get("no-appendfsync-on-rewrite") !== "no" ||
        config.get("aof-load-truncated") !== "no"
      )
        throw failure();
      const persistence = String(await raw.send("INFO", ["persistence"]));
      if (!/^aof_last_write_status:ok\r?$/m.test(persistence)) throw failure();
      const proof = (await raw.send("WAITAOF", [
        "1",
        String(this.options.replicas),
        String(this.options.timeoutMs),
      ])) as number[];
      if (
        g.broken ||
        !raw.connected ||
        Number(proof[0]) !== 1 ||
        Number(proof[1]) < this.options.replicas
      )
        throw failure();
    }, g);
    return g;
  }
  private async connection(): Promise<Generation> {
    if (this.stopping.signal.aborted) throw failure();
    let g = this.generation;
    if (!g || g.broken) {
      g = this.create();
      this.generation = g;
    }
    await g.ready;
    if (g.broken) throw failure();
    return g;
  }
  async ready(): Promise<void> {
    const g = await this.connection();
    if (
      this.backlog >= (this.options.maxPendingJobs ?? 1000) ||
      Date.now() - this.backlogCheckedAt >= 100
    ) {
      if (!this.backlogProbe)
        this.backlogProbe = this.bounded(async () => {
          const counts = await g.queue.getJobCounts("waiting", "active", "delayed", "failed");
          this.backlog = Object.values(counts).reduce((sum, n) => sum + n, 0);
          this.backlogCheckedAt = Date.now();
        }, g).finally(() => {
          this.backlogProbe = undefined;
        });
      await this.backlogProbe;
    }
    if (g.broken || this.backlog >= (this.options.maxPendingJobs ?? 1000)) throw failure();
  }
  async stats(): Promise<Record<string, number>> {
    const g = await this.connection();
    return this.bounded(() => g.queue.getJobCounts("waiting", "active", "delayed", "failed"), g);
  }
  async retry(jobId: string): Promise<void> {
    if (!/^[a-f0-9-]{36}$/.test(jobId))
      throw new GatewayError(400, "invalid_job", "Invalid usage job identifier");
    const g = await this.connection();
    await this.bounded(async () => {
      const job = await g.queue.getJob(jobId);
      if (!job) throw new GatewayError(404, "not_found", "Usage job not found");
      if ((await job.getState()) !== "failed")
        throw new GatewayError(409, "job_not_failed", "Only failed usage jobs can be retried");
      await job.retry();
      const proof = (await g.raw.send("WAITAOF", [
        "1",
        String(this.options.replicas),
        String(this.options.timeoutMs),
      ])) as number[];
      if (g.broken || Number(proof[0]) !== 1 || Number(proof[1]) < this.options.replicas)
        throw failure();
    }, g);
  }
  async publish(value: UsageEvidence): Promise<void> {
    const parsed = usageEvidenceSchema.safeParse(value);
    if (!parsed.success)
      throw new GatewayError(
        502,
        "invalid_usage_evidence",
        "Metering produced invalid numeric evidence or identifiers",
      );
    const input = parsed.data;
    if (this.stopping.signal.aborted) throw failure();
    try {
      await this.batch.add(input);
    } catch {
      throw failure();
    }
  }
  private async publishBatch(events: UsageEvidence[]): Promise<void[]> {
    const jobId = crypto.randomUUID();
    const payload: Payload = { version: 1, events };
    // An unknown confirmation may be retried with the same identity. PG also deduplicates after job removal.
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        // Backlog gates new upstream admission. Already-executed calls must still record their facts.
        const g = await this.connection();
        await this.bounded(async () => {
          await g.queue.add("evidence", payload, { jobId });
          this.backlog++;
          const proof = (await g.raw.send("WAITAOF", [
            "1",
            String(this.options.replicas),
            String(this.options.timeoutMs),
          ])) as number[];
          if (
            g.broken ||
            !g.raw.connected ||
            Number(proof[0]) !== 1 ||
            Number(proof[1]) < this.options.replicas
          )
            throw failure();
        }, g);
        return events.map(() => undefined);
      } catch {
        if (attempt === 1 || this.stopping.signal.aborted) throw failure();
      }
    }
    throw failure();
  }
  close(): void {
    this.stopping.abort();
    const g = this.generation;
    if (g) this.invalidate(g);
  }
}

export interface UsageConsumerOptions extends UsageQueueOptions {
  concurrency?: number;
  process: (events: UsageEvidence[]) => Promise<unknown>;
  lockDurationMs?: number;
  stalledIntervalMs?: number;
  onCommitted?: (events: number, lagSeconds: number) => void;
}
export class UsageConsumer {
  readonly worker: Worker<Payload>;
  private readonly adapter: ReturnType<typeof createBunRedisClient>;
  private readonly raw: RedisClient;
  private readonly pending = new Set<Promise<unknown>>();
  constructor(readonly options: UsageConsumerOptions) {
    const raw = new RedisClient(options.url, {
      connectionTimeout: 1000,
      idleTimeout: 0,
      maxRetries: 3,
      enableOfflineQueue: true,
    });
    this.raw = raw;
    this.adapter = createBunRedisClient(raw);
    this.worker = new Worker<Payload>(
      queueName,
      async (job) => {
        const input = usageBatchSchema.parse(job.data);
        const processing = options.process(input.events);
        this.pending.add(processing);
        try {
          const result = await processing;
          options.onCommitted?.(
            input.events.length,
            Math.max(0, Date.now() - job.timestamp) / 1000,
          );
          return result;
        } finally {
          this.pending.delete(processing);
        }
      },
      {
        connection: this.adapter,
        prefix: queuePrefix(options.namespace),
        concurrency: options.concurrency ?? 8,
        lockDuration: options.lockDurationMs ?? 30000,
        stalledInterval: options.stalledIntervalMs ?? 5000,
        maxStalledCount: 3,
      },
    );
    this.worker.on("error", () => options.onError?.());
    this.adapter.on("error", () => options.onError?.());
  }
  healthy(): boolean {
    return this.adapter.status === "ready";
  }
  async ready(): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.worker.waitUntilReady(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(failure()), this.options.timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }
  async close(): Promise<void> {
    // Wait for financial transactions, then force-close Redis ACK/receive operations if the network is a blackhole.
    await this.worker.pause(true);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.allSettled(this.pending).then(() => undefined),
        new Promise<void>((resolve) => {
          timer = setTimeout(() => {
            resolve();
          }, 10000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      await this.worker.close(true);
      this.adapter.status = "end";
      this.raw.close();
    }
  }
}
