import { RedisClient } from "bun";
import {
  BATCH_ACQUIRE_LUA,
  RENEW_LUA,
  BATCH_RENEW_LUA,
  BATCH_INGRESS_WINDOW_LUA,
  BATCH_RELEASE_LUA,
  evalPermitPrepared,
  batchIngressWindowSha,
  withRedisDeadline,
} from "./cluster-redis.ts";
import type { CircuitTransition } from "./router.ts";

type PendingIngressCheck = {
  ip: string;
  limit: number;
  windowMs: number;
  timeoutMs: number;
  resolve: (wait: number | null) => void;
};

const INGRESS_BATCH_MAX = 256;
const MAX_INGRESS_IN_FLIGHT = 8;
const INGRESS_KEY_CACHE_MAX = 4096;

export type ClusterCircuitEvent = CircuitTransition & { node_id: string };

export type ClusterEpochEvent = {
  node_id: string;
  epoch: number;
};

export type ClusterCoordinatorOptions = {
  redisUrl?: string;
  sentinel?: { urls: readonly string[]; master: string };
  namespace?: string;
  nodeId?: string;
  onCircuitEvent?: (event: ClusterCircuitEvent) => void;
  onConfigEpoch?: (event: ClusterEpochEvent) => void;
};

type RedisConnectionLifecycle = {
  beforeDisconnect(): void;
  afterDetach(client: RedisClient): void;
  afterDisconnect(): void;
  beforeReady(client: RedisClient): Promise<void>;
  afterReady(): void;
  beforeClose(): void;
};

export class RedisClusterConnection {
  readonly nodeId: string;
  readonly namespace: string;
  readonly redisUrl: string | undefined;
  private readonly onCircuitEvent: ClusterCoordinatorOptions["onCircuitEvent"];
  private readonly onConfigEpoch: ClusterCoordinatorOptions["onConfigEpoch"];
  private client: RedisClient | null = null;
  private maintClient: RedisClient | null = null;
  private subscriber: RedisClient | null = null;
  private primary: string | undefined;
  private ingressConnection:
    { client: RedisClient; ready: Promise<unknown>; readyDone: boolean } | undefined;
  private pendingIngressChecks: PendingIngressCheck[] = [];
  private ingressMicrotask = false;
  private ingressInFlight = 0;
  private readonly ingressKeyCache = new Map<string, string>();
  private connected = false;
  private closed = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private sentinelTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly sentinel: ClusterCoordinatorOptions["sentinel"];
  private consecutiveAcquireErrors = 0;
  private probingHealth = false;
  private reconnectAttempt = 0;

  constructor(
    options: ClusterCoordinatorOptions,
    private readonly lifecycle: RedisConnectionLifecycle,
  ) {
    this.redisUrl = options.redisUrl;
    this.sentinel = options.sentinel;
    this.namespace = options.namespace ?? "stg";
    this.nodeId = options.nodeId ?? crypto.randomUUID();
    this.onCircuitEvent = options.onCircuitEvent;
    this.onConfigEpoch = options.onConfigEpoch;
  }

  get commandClient() {
    return this.client;
  }

  get maintenanceClient() {
    return this.maintClient;
  }

  get active() {
    return this.connected;
  }

  get stopped() {
    return this.closed;
  }

  reportAcquireSuccess(): void {
    this.consecutiveAcquireErrors = 0;
  }

  reportAcquireFailure(client: RedisClient): void {
    if (!client.connected || this.closed || this.client !== client) {
      this.disconnect(client);
    } else if (++this.consecutiveAcquireErrors >= 3) {
      this.probeConnectionHealth(client);
    }
  }

  isRedisConnected(): boolean {
    return (
      this.connected && Boolean(this.client?.connected) && Boolean(this.maintClient?.connected)
    );
  }

  private circuitTopic(): string {
    return `${this.namespace}:circuit`;
  }

  private epochTopic(): string {
    return `${this.namespace}:epoch`;
  }

  async start(): Promise<void> {
    this.closed = false;
    await this.connect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.retryTimer) return;
    const base = Math.min(1500, 150 * 2 ** Math.min(this.reconnectAttempt, 4));
    this.reconnectAttempt++;
    this.retryTimer = setTimeout(
      () => {
        this.retryTimer = undefined;
        void this.connect();
      },
      base + Math.random() * 100,
    );
    this.retryTimer.unref();
  }

  private probeConnectionHealth(client: RedisClient): void {
    if (this.probingHealth || this.closed || this.client !== client) return;
    this.probingHealth = true;
    void withRedisDeadline(client.send("PING", []), 500)
      .then((reply) => {
        if (reply !== "PONG") this.disconnect(client);
        else this.consecutiveAcquireErrors = 0;
      })
      .catch(() => {
        this.disconnect(client);
      })
      .finally(() => {
        this.probingHealth = false;
      });
  }

  private disconnect(client: RedisClient): void {
    if (this.client !== client) return;
    this.connected = false;
    this.consecutiveAcquireErrors = 0;
    clearTimeout(this.sentinelTimer);
    this.sentinelTimer = undefined;
    this.lifecycle.beforeDisconnect();
    const subscriber = this.subscriber;
    const maint = this.maintClient;
    this.subscriber = null;
    this.maintClient = null;
    this.client = null;
    this.primary = undefined;
    this.closeIngressConnection();
    this.lifecycle.afterDetach(client);
    try {
      subscriber?.close();
    } catch {
      void 0;
    }
    try {
      maint?.close();
    } catch {
      void 0;
    }
    try {
      client.close();
    } catch {
      void 0;
    }
    this.lifecycle.afterDisconnect();
    this.scheduleReconnect();
  }

  private async discoverRedis(template: string): Promise<string> {
    if (!this.sentinel) return template;
    const { urls, master } = this.sentinel;
    const replies = await Promise.allSettled(
      urls.map(async (url) => {
        const sentinel = new RedisClient(url, {
          connectionTimeout: 500,
          autoReconnect: false,
          enableOfflineQueue: false,
        });
        try {
          await withRedisDeadline(sentinel.connect(), 500);
          const address: unknown = await withRedisDeadline(
            sentinel.send("SENTINEL", ["get-master-addr-by-name", master]),
            500,
          );
          if (
            !Array.isArray(address) ||
            typeof address[0] !== "string" ||
            !/^[A-Za-z0-9.:-]+$/.test(address[0]) ||
            !/^\d+$/.test(String(address[1]))
          ) {
            throw new Error("Redis Sentinel returned an invalid primary address");
          }
          const primary = new URL(template);
          primary.hostname = address[0].includes(":") ? `[${address[0]}]` : address[0];
          primary.port = String(address[1]);
          if (
            primary.port !== String(address[1]) ||
            Number(primary.port) > 65535 ||
            Number(primary.port) < 1
          ) {
            throw new Error("Redis Sentinel returned an invalid primary port");
          }
          return primary.toString();
        } finally {
          sentinel.close();
        }
      }),
    );
    const votes = new Map<string, number>();
    for (const reply of replies) {
      if (reply.status !== "fulfilled") continue;
      const count = (votes.get(reply.value) ?? 0) + 1;
      if (count > urls.length / 2) return reply.value;
      votes.set(reply.value, count);
    }
    throw new Error("Configured Redis Sentinels disagree on the primary");
  }

  private monitorPrimary(client: RedisClient, primary: string): void {
    if (!this.sentinel || !this.redisUrl) return;
    const template = this.redisUrl;
    this.sentinelTimer = setTimeout(async () => {
      if (this.closed || this.client !== client) return;
      try {
        const address = await this.discoverRedis(template);
        if (address !== primary) return this.disconnect(client);
      } catch {
        return this.disconnect(client);
      }
      if (!this.closed && this.client === client) this.monitorPrimary(client, primary);
    }, 2000);
    this.sentinelTimer.unref();
  }

  private async connect(): Promise<void> {
    if (!this.redisUrl || this.closed) return;
    let client: RedisClient | undefined;
    let maint: RedisClient | null = null;
    let sub: RedisClient | null = null;
    try {
      const primary = await this.discoverRedis(this.redisUrl);
      if (this.closed) return;
      const connecting = new RedisClient(primary, {
        connectionTimeout: 2000,
        autoReconnect: false,
        enableOfflineQueue: false,
      });
      client = connecting;
      await withRedisDeadline(connecting.connect(), 2000);
      if (this.sentinel) {
        const role: unknown = await withRedisDeadline(connecting.send("ROLE", []), 1000);
        if (!Array.isArray(role) || role[0] !== "master") {
          throw new Error("Discovered Redis node is not writable");
        }
      }
      await withRedisDeadline(
        Promise.all([
          connecting.send("SCRIPT", ["LOAD", BATCH_ACQUIRE_LUA]),
          connecting.send("SCRIPT", ["LOAD", RENEW_LUA]),
          connecting.send("SCRIPT", ["LOAD", BATCH_RENEW_LUA]),
          connecting.send("SCRIPT", ["LOAD", BATCH_INGRESS_WINDOW_LUA]),
          connecting.send("SCRIPT", ["LOAD", BATCH_RELEASE_LUA]),
        ]),
        2000,
      );
      maint = await withRedisDeadline(connecting.duplicate(), 2000);
      sub = await withRedisDeadline(connecting.duplicate(), 2000);
      await withRedisDeadline(
        sub.subscribe(this.circuitTopic(), (message) => {
          try {
            const parsed = JSON.parse(message) as ClusterCircuitEvent;
            if (parsed.node_id && parsed.node_id !== this.nodeId) {
              this.onCircuitEvent?.(parsed);
            }
          } catch {
            void 0;
          }
        }),
        2000,
      );
      await withRedisDeadline(
        sub.subscribe(this.epochTopic(), (message) => {
          try {
            const parsed = JSON.parse(message) as ClusterEpochEvent;
            if (parsed.node_id && parsed.node_id !== this.nodeId) {
              this.onConfigEpoch?.(parsed);
            }
          } catch {
            void 0;
          }
        }),
        2000,
      );
      if (this.closed) {
        sub.close();
        maint.close();
        client.close();
        return;
      }
      await this.lifecycle.beforeReady(maint);
      if (this.closed) {
        sub.close();
        maint.close();
        client.close();
        return;
      }
      this.client = client;
      this.maintClient = maint;
      this.subscriber = sub;
      this.primary = primary;
      this.connected = true;
      this.reconnectAttempt = 0;
      this.consecutiveAcquireErrors = 0;
      connecting.onclose = () => this.disconnect(connecting);
      maint.onclose = () => this.disconnect(connecting);
      sub.onclose = () => this.disconnect(connecting);
      if (!connecting.connected || !maint.connected || !sub.connected) {
        this.disconnect(connecting);
        return;
      }
      this.lifecycle.afterReady();
      this.monitorPrimary(connecting, primary);
    } catch {
      sub?.close();
      maint?.close();
      client?.close();
      this.scheduleReconnect();
    }
  }

  ingressKeyFor(ip: string): string {
    let key = this.ingressKeyCache.get(ip);
    if (key === undefined) {
      if (this.ingressKeyCache.size >= INGRESS_KEY_CACHE_MAX) this.ingressKeyCache.clear();
      key = `${this.namespace}:ingress:ip:${ip}`;
      this.ingressKeyCache.set(ip, key);
    }
    return key;
  }

  private closeIngressConnection(): void {
    const connection = this.ingressConnection;
    this.ingressConnection = undefined;
    connection?.client.close();
    if (this.pendingIngressChecks.length > 0) {
      const dropped = this.pendingIngressChecks;
      this.pendingIngressChecks = [];
      for (let i = 0; i < dropped.length; i++) dropped[i]!.resolve(null);
    }
  }

  private scheduleIngressFlush(): void {
    if (this.ingressMicrotask) return;
    this.ingressMicrotask = true;
    setImmediate(() => {
      this.ingressMicrotask = false;
      this.flushIngressChecks();
    });
  }

  private flushIngressChecks(): void {
    if (this.ingressInFlight >= MAX_INGRESS_IN_FLIGHT) return;
    const total = this.pendingIngressChecks.length;
    if (total === 0) return;
    const batch =
      total <= INGRESS_BATCH_MAX
        ? this.pendingIngressChecks
        : this.pendingIngressChecks.splice(0, INGRESS_BATCH_MAX);
    if (total <= INGRESS_BATCH_MAX) this.pendingIngressChecks = [];
    else if (this.ingressInFlight + 1 < MAX_INGRESS_IN_FLIGHT) this.scheduleIngressFlush();

    if (!this.primary || !this.isRedisConnected() || this.closed) {
      for (let i = 0; i < batch.length; i++) batch[i]!.resolve(null);
      return;
    }

    let minTimeoutMs = batch[0]!.timeoutMs;
    for (let i = 1; i < batch.length; i++) {
      const t = batch[i]!.timeoutMs;
      if (t < minTimeoutMs) minTimeoutMs = t;
    }

    if (
      !this.ingressConnection ||
      (this.ingressConnection.readyDone && !this.ingressConnection.client.connected)
    ) {
      this.ingressConnection?.client.close();
      const client = new RedisClient(this.primary, {
        connectionTimeout: minTimeoutMs,
        autoReconnect: false,
        enableOfflineQueue: false,
      });
      const entry = {
        client,
        ready: client.connect(),
        readyDone: false,
      };
      client.onclose = () => {
        if (this.ingressConnection === entry) this.ingressConnection = undefined;
      };
      void entry.ready.then(
        () => {
          entry.readyDone = true;
        },
        () => {
          if (this.ingressConnection === entry) this.ingressConnection = undefined;
        },
      );
      this.ingressConnection = entry;
    }

    void this.runIngressBatch(this.ingressConnection, batch, minTimeoutMs);
  }

  private async runIngressBatch(
    connection: { client: RedisClient; ready: Promise<unknown>; readyDone: boolean },
    batch: PendingIngressCheck[],
    timeoutMs: number,
  ): Promise<void> {
    this.ingressInFlight++;
    const first = batch[0]!;
    let singleGroup = true;
    for (let i = 1; i < batch.length; i++) {
      const it = batch[i]!;
      if (it.ip !== first.ip || it.limit !== first.limit || it.windowMs !== first.windowMs) {
        singleGroup = false;
        break;
      }
    }

    let shaArgs: string[];
    let groups: PendingIngressCheck[][] | null = null;
    if (singleGroup) {
      shaArgs = [
        batchIngressWindowSha,
        "1",
        this.ingressKeyFor(first.ip),
        String(first.limit),
        String(first.windowMs),
        String(batch.length),
      ];
    } else {
      const grouped = new Map<
        string,
        { key: string; limit: number; windowMs: number; items: PendingIngressCheck[] }
      >();
      for (let i = 0; i < batch.length; i++) {
        const it = batch[i]!;
        const gk = `${it.ip}\0${it.limit}\0${it.windowMs}`;
        let g = grouped.get(gk);
        if (!g) {
          g = {
            key: this.ingressKeyFor(it.ip),
            limit: it.limit,
            windowMs: it.windowMs,
            items: [],
          };
          grouped.set(gk, g);
        }
        g.items.push(it);
      }
      const list = [...grouped.values()];
      groups = list.map((g) => g.items);
      shaArgs = [batchIngressWindowSha, String(list.length)];
      for (let i = 0; i < list.length; i++) shaArgs.push(list[i]!.key);
      for (let i = 0; i < list.length; i++) {
        const g = list[i]!;
        shaArgs.push(String(g.limit), String(g.windowMs), String(g.items.length));
      }
    }

    try {
      const work = connection.readyDone
        ? evalPermitPrepared(connection.client, BATCH_INGRESS_WINDOW_LUA, shaArgs)
        : connection.ready.then(() =>
            evalPermitPrepared(connection.client, BATCH_INGRESS_WINDOW_LUA, shaArgs),
          );
      const raw = await withRedisDeadline(work, timeoutMs);
      const expectedGroups = groups ? groups.length : 1;
      if (
        this.ingressConnection !== connection ||
        !this.connected ||
        !Array.isArray(raw) ||
        raw.length !== expectedGroups * 2
      ) {
        for (let i = 0; i < batch.length; i++) batch[i]!.resolve(null);
        return;
      }
      if (!groups) {
        const allowed = Number(raw[0]);
        const ttl = Number(raw[1]);
        if (
          !Number.isSafeInteger(allowed) ||
          allowed < 0 ||
          !Number.isSafeInteger(ttl) ||
          ttl < 0
        ) {
          for (let i = 0; i < batch.length; i++) batch[i]!.resolve(null);
          return;
        }
        const limit = Math.min(allowed, batch.length);
        for (let i = 0; i < limit; i++) batch[i]!.resolve(0);
        for (let i = limit; i < batch.length; i++) batch[i]!.resolve(ttl);
        return;
      }
      for (let g = 0; g < groups.length; g++) {
        const items = groups[g]!;
        const allowed = Number(raw[g * 2]);
        const ttl = Number(raw[g * 2 + 1]);
        if (
          !Number.isSafeInteger(allowed) ||
          allowed < 0 ||
          !Number.isSafeInteger(ttl) ||
          ttl < 0
        ) {
          for (let i = 0; i < items.length; i++) items[i]!.resolve(null);
          continue;
        }
        const limit = Math.min(allowed, items.length);
        for (let i = 0; i < limit; i++) items[i]!.resolve(0);
        for (let i = limit; i < items.length; i++) items[i]!.resolve(ttl);
      }
    } catch {
      if (this.ingressConnection === connection) this.closeIngressConnection();
      for (let i = 0; i < batch.length; i++) batch[i]!.resolve(null);
    } finally {
      this.ingressInFlight--;
      if (
        this.pendingIngressChecks.length > 0 &&
        !this.closed &&
        this.ingressInFlight < MAX_INGRESS_IN_FLIGHT
      ) {
        this.scheduleIngressFlush();
      }
    }
  }

  takeIngressWindow(
    ip: string,
    limit: number,
    windowMs: number,
    timeoutMs: number,
  ): Promise<number | null> {
    if (!this.primary || !this.isRedisConnected() || this.closed) return Promise.resolve(null);
    return new Promise<number | null>((resolve) => {
      this.pendingIngressChecks.push({ ip, limit, windowMs, timeoutMs, resolve });
      if (
        this.pendingIngressChecks.length >= INGRESS_BATCH_MAX &&
        this.ingressInFlight < MAX_INGRESS_IN_FLIGHT
      ) {
        this.flushIngressChecks();
        return;
      }
      if (this.ingressInFlight === 0) {
        this.scheduleIngressFlush();
      }
    });
  }

  publishCircuitEvent(event: Omit<ClusterCircuitEvent, "node_id">): void {
    if (!this.client || !this.connected) return;
    const payload: ClusterCircuitEvent = { node_id: this.nodeId, ...event };
    void this.client.publish(this.circuitTopic(), JSON.stringify(payload)).catch(() => undefined);
  }

  publishConfigEpoch(epoch: number): void {
    if (!this.client || !this.connected) return;
    const payload: ClusterEpochEvent = { node_id: this.nodeId, epoch };
    void this.client.publish(this.epochTopic(), JSON.stringify(payload)).catch(() => undefined);
  }

  async close(): Promise<void> {
    this.connected = false;
    this.closed = true;
    clearTimeout(this.retryTimer);
    this.retryTimer = undefined;
    clearTimeout(this.sentinelTimer);
    this.sentinelTimer = undefined;
    this.lifecycle.beforeClose();
    const sub = this.subscriber;
    const maint = this.maintClient;
    const client = this.client;
    this.subscriber = null;
    this.maintClient = null;
    this.client = null;
    this.primary = undefined;
    this.closeIngressConnection();
    try {
      sub?.close();
    } catch {
      void 0;
    }
    try {
      maint?.close();
    } catch {
      void 0;
    }
    try {
      client?.close();
    } catch {
      void 0;
    }
  }
}
