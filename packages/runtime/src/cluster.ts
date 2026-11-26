import type { RedisClient } from "bun";
import {
  createPermitReleaseRecovery,
  groupPiggybackReleases,
  releaseRedisPermits,
  type PendingRelease,
} from "./cluster-release.ts";
import { buildPermitRenewalArgs, jitteredRenewDelay, safetyMarginFor } from "./cluster-renewal.ts";
import { LocalRateLimiter } from "./router.ts";
import {
  RedisClusterConnection,
  type ClusterCircuitEvent,
  type ClusterCoordinatorOptions,
} from "./cluster-connection.ts";
export type {
  ClusterCoordinatorOptions,
  ClusterCircuitEvent,
  ClusterEpochEvent,
} from "./cluster-connection.ts";
import {
  BATCH_ACQUIRE_LUA,
  RENEW_LUA,
  BATCH_RENEW_LUA,
  buildBatchAcquireArgs,
  decodeAcquireFailure,
  failAcquire,
  renewSha,
  evalPermitPrepared,
  withRedisDeadline,
  createQuotaKeyCache,
  type ActivePermit,
  type IngressIpCheck,
  type PendingAcquire,
  type RateLimitAcquireResult,
} from "./cluster-redis.ts";
export type { IngressIpCheck, RateLimitAcquireResult } from "./cluster-redis.ts";

const ACQUIRE_BATCH_MAX = 256;
const MAX_ACQUIRE_IN_FLIGHT = 16;
const ACQUIRE_TIMEOUT_MS = 2000;
const RENEW_BATCH_MAX = 256;
const RENEW_TIMEOUT_MS = 2000;
const RELEASE_BATCH_MAX = 512;

export class ClusterCoordinator {
  readonly nodeId: string;
  private readonly connection: RedisClusterConnection;
  private readonly memberPrefix: string;
  private readonly localLimiter = new LocalRateLimiter();
  private readonly getKeyTriple: ReturnType<typeof createQuotaKeyCache>;
  private readonly shortPermits = new Set<ActivePermit>();
  private readonly sweepPermits = new Set<ActivePermit>();
  private sweepTimer: ReturnType<typeof setInterval> | undefined;
  private permitSeq = 0;
  private pendingAcquires: PendingAcquire[] = [];
  private acquireMicrotask = false;
  private acquireInFlight = 0;
  private lateAcquireBatches = 0;
  private pendingReleases: PendingRelease[] = [];
  private readonly releaseRecovery = createPermitReleaseRecovery(
    RELEASE_BATCH_MAX,
    RENEW_TIMEOUT_MS,
  );
  private releaseTimer: ReturnType<typeof setTimeout> | undefined;
  private releaseMicrotask = false;

  constructor(options: ClusterCoordinatorOptions = {}) {
    this.connection = new RedisClusterConnection(options, {
      beforeDisconnect: () => this.stopPermitTimers(),
      afterDetach: (client) => this.failPendingFor(client),
      afterDisconnect: () => this.onPermitsDisconnected(),
      beforeReady: (client) => this.reconcileAfterReconnect(client),
      afterReady: () => this.resumePermitRenewal(),
      beforeClose: () => {
        this.stopPermitTimers();
        this.closePermits();
      },
    });
    this.nodeId = this.connection.nodeId;
    this.getKeyTriple = createQuotaKeyCache(this.connection.namespace);
    this.memberPrefix = `${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}:`;
  }

  start(): Promise<void> {
    return this.connection.start();
  }

  close(): Promise<void> {
    return this.connection.close();
  }

  isRedisConnected(): boolean {
    return this.connection.isRedisConnected();
  }

  takeIngressWindow(ip: string, limit: number, windowMs: number, timeoutMs: number) {
    return this.connection.takeIngressWindow(ip, limit, windowMs, timeoutMs);
  }

  publishCircuitEvent(event: Omit<ClusterCircuitEvent, "node_id">): void {
    this.connection.publishCircuitEvent(event);
  }

  publishConfigEpoch(epoch: number): void {
    this.connection.publishConfigEpoch(epoch);
  }

  private failPendingFor(client?: RedisClient): void {
    this.pendingAcquires = this.pendingAcquires.filter((item) => {
      if (client && item.redis !== client) return true;
      item.resolve(failAcquire(item.ingressIp));
      return false;
    });
    if (client && this.pendingReleases.length > 0) {
      const remaining: PendingRelease[] = [];
      for (const item of this.pendingReleases) {
        if (item.redis === client) {
          this.releaseRecovery.record(item.concKey, item.member, item.expiresAtMs);
        } else {
          remaining.push(item);
        }
      }
      this.pendingReleases = remaining;
    }
  }

  private stopSweep(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = undefined;
    }
  }

  private releasePermit(entry: ActivePermit, isShort: boolean, sendToRedis = true): void {
    if (entry.released) return;
    entry.released = true;
    if (entry.shortTimer) {
      clearTimeout(entry.shortTimer);
      entry.shortTimer = undefined;
    }
    if (isShort) this.shortPermits.delete(entry);
    else this.sweepPermits.delete(entry);
    if (!sendToRedis) return;
    const activeClient = this.connection.commandClient;
    if (
      activeClient &&
      this.connection.active &&
      activeClient.connected &&
      !this.connection.stopped
    ) {
      this.enqueueRelease(
        activeClient,
        entry.concKey,
        entry.member,
        entry.confirmedExpiresAtMs,
        isShort,
      );
    } else if (!this.connection.stopped) {
      this.releaseRecovery.record(entry.concKey, entry.member, entry.confirmedExpiresAtMs);
    }
  }

  private revokePermit(entry: ActivePermit, isShort: boolean, stillInRedis = true): void {
    if (entry.released) return;
    this.releasePermit(entry, isShort, stillInRedis);
    if (entry.onRevoke) entry.onRevoke();
    else entry.revoked?.abort(new Error("Shared concurrency permit could not be renewed"));
  }

  private async runRenewBatch(
    redis: RedisClient,
    batch: ActivePermit[],
    isShortBatch: boolean,
  ): Promise<void> {
    const n = batch.length;
    if (n === 0) return;
    const shaArgs = buildPermitRenewalArgs(batch);
    const sentAt = Date.now();
    try {
      const raw = await withRedisDeadline(
        evalPermitPrepared(redis, BATCH_RENEW_LUA, shaArgs),
        RENEW_TIMEOUT_MS,
      );
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis batch renew reply");
      }
      const postNow = Date.now();
      for (let i = 0; i < n; i++) {
        const permit = batch[i]!;
        permit.renewing = false;
        if (permit.released) continue;
        if (Number(raw[i]) === 1) {
          permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
          permit.nextRenewAtMs = postNow + jitteredRenewDelay(permit.renewEveryMs);
          if (isShortBatch) this.scheduleShortRenew(permit);
        } else {
          this.revokePermit(permit, isShortBatch, false);
        }
      }
    } catch {
      const postNow = Date.now();
      for (let i = 0; i < n; i++) {
        const permit = batch[i]!;
        permit.renewing = false;
        if (permit.released) continue;
        if (postNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
          this.revokePermit(permit, isShortBatch);
        } else {
          permit.nextRenewAtMs = postNow + Math.min(1000, permit.renewEveryMs);
          if (isShortBatch) this.scheduleShortRenew(permit);
        }
      }
    }
  }

  private scheduleShortRenew(permit: ActivePermit): void {
    if (permit.released) return;
    if (permit.shortTimer) {
      clearTimeout(permit.shortTimer);
      permit.shortTimer = undefined;
    }
    const now = Date.now();
    const cutoff = permit.confirmedExpiresAtMs - permit.safetyMarginMs;
    if (now >= cutoff) {
      this.revokePermit(permit, true);
      return;
    }
    const target =
      this.connection.active && this.connection.maintenanceClient?.connected
        ? Math.min(permit.nextRenewAtMs, cutoff)
        : cutoff;
    const delay = Math.max(1, target - now);
    permit.shortTimer = setTimeout(() => {
      permit.shortTimer = undefined;
      if (permit.released || permit.renewing) return;
      const tickNow = Date.now();
      if (tickNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
        this.revokePermit(permit, true);
        return;
      }
      const maint = this.connection.maintenanceClient;
      if (!this.connection.active || !maint?.connected || this.connection.stopped) {
        this.scheduleShortRenew(permit);
        return;
      }
      permit.renewing = true;
      const sentAt = Date.now();
      void withRedisDeadline(
        evalPermitPrepared(maint, RENEW_LUA, [
          renewSha,
          "1",
          permit.concKey,
          permit.member,
          permit.leaseTtlStr,
        ]),
        Math.min(1000, Math.max(100, permit.renewEveryMs)),
      )
        .then((renewed) => {
          permit.renewing = false;
          if (permit.released) return;
          if (Number(renewed) === 1) {
            permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
            permit.nextRenewAtMs = Date.now() + permit.renewEveryMs;
            this.scheduleShortRenew(permit);
          } else {
            this.revokePermit(permit, true, false);
          }
        })
        .catch(() => {
          permit.renewing = false;
          if (permit.released) return;
          const failNow = Date.now();
          if (failNow >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
            this.revokePermit(permit, true);
          } else {
            permit.nextRenewAtMs = failNow + Math.min(200, permit.renewEveryMs);
            this.scheduleShortRenew(permit);
          }
        });
    }, delay);
    permit.shortTimer.unref();
  }

  private ensureSweepTimer(): void {
    if (this.sweepTimer) return;
    this.sweepTimer = setInterval(() => {
      if (this.sweepPermits.size === 0) return;
      const now = Date.now();
      const maint = this.connection.maintenanceClient;
      const canRenew =
        this.connection.active && Boolean(maint?.connected) && !this.connection.stopped;
      const due: ActivePermit[] = [];
      for (const permit of this.sweepPermits) {
        if (permit.released) continue;
        if (now >= permit.confirmedExpiresAtMs - permit.safetyMarginMs) {
          this.revokePermit(permit, false);
          continue;
        }
        if (canRenew && !permit.renewing && now >= permit.nextRenewAtMs) {
          permit.renewing = true;
          due.push(permit);
        }
      }
      if (due.length === 0 || !maint) return;
      for (let i = 0; i < due.length; i += RENEW_BATCH_MAX) {
        void this.runRenewBatch(maint, due.slice(i, i + RENEW_BATCH_MAX), false);
      }
    }, 250);
    this.sweepTimer.unref();
  }

  private async reconcileAfterReconnect(maint: RedisClient): Promise<void> {
    await this.releaseRecovery.flush(maint);
    const activeSweep: ActivePermit[] = [];
    for (const p of this.sweepPermits) {
      if (!p.released) {
        p.renewing = true;
        activeSweep.push(p);
      }
    }
    const activeShort: ActivePermit[] = [];
    for (const p of this.shortPermits) {
      if (!p.released) {
        if (p.shortTimer) {
          clearTimeout(p.shortTimer);
          p.shortTimer = undefined;
        }
        p.renewing = true;
        activeShort.push(p);
      }
    }
    const verifyGroup = async (group: ActivePermit[], isShort: boolean) => {
      for (let offset = 0; offset < group.length; offset += RENEW_BATCH_MAX) {
        const slice = group.slice(offset, offset + RENEW_BATCH_MAX);
        const shaArgs = buildPermitRenewalArgs(slice);
        const sentAt = Date.now();
        const raw = await withRedisDeadline(
          evalPermitPrepared(maint, BATCH_RENEW_LUA, shaArgs),
          RENEW_TIMEOUT_MS,
        );
        if (!Array.isArray(raw) || raw.length !== slice.length) {
          throw new Error("Invalid Redis reconcile renew reply");
        }
        const postNow = Date.now();
        for (let i = 0; i < slice.length; i++) {
          const permit = slice[i]!;
          permit.renewing = false;
          if (permit.released) continue;
          if (Number(raw[i]) === 1) {
            permit.confirmedExpiresAtMs = sentAt + permit.leaseTtlMs;
            permit.nextRenewAtMs =
              postNow + (isShort ? permit.renewEveryMs : jitteredRenewDelay(permit.renewEveryMs));
            if (isShort) this.scheduleShortRenew(permit);
          } else {
            this.revokePermit(permit, isShort, false);
          }
        }
      }
    };
    try {
      await verifyGroup(activeSweep, false);
      await verifyGroup(activeShort, true);
      if (this.releaseRecovery.hasPending) {
        await this.releaseRecovery.flush(maint);
      }
    } catch (error) {
      for (const p of activeSweep) p.renewing = false;
      for (const p of activeShort) {
        p.renewing = false;
        if (!p.released) this.scheduleShortRenew(p);
      }
      throw error;
    }
  }

  private recordOrphanedRelease = (concKey: string, member: string, expiresAtMs: number): void => {
    this.releaseRecovery.record(concKey, member, expiresAtMs);
    this.scheduleReleaseRecoveryFlush();
  };

  private scheduleReleaseRecoveryFlush(): void {
    this.releaseRecovery.scheduleFlush(
      () => (this.connection.active ? this.connection.maintenanceClient : null),
      () => this.connection.stopped,
    );
  }

  private scheduleReleaseFlush(immediate: boolean): void {
    if (immediate) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      if (!this.releaseMicrotask) {
        this.releaseMicrotask = true;
        queueMicrotask(() => {
          this.releaseMicrotask = false;
          this.flushReleases();
        });
      }
      return;
    }
    if (this.releaseTimer || this.releaseMicrotask) return;
    this.releaseTimer = setTimeout(() => {
      this.releaseTimer = undefined;
      this.flushReleases();
    }, 1);
    this.releaseTimer.unref();
  }

  private enqueueRelease(
    redis: RedisClient,
    concKey: string,
    member: string,
    expiresAtMs: number,
    immediate: boolean,
  ): void {
    if (!redis.connected || this.connection.stopped || this.connection.commandClient !== redis) {
      if (!this.connection.stopped) this.recordOrphanedRelease(concKey, member, expiresAtMs);
      return;
    }
    this.pendingReleases.push({ redis, concKey, member, expiresAtMs });
    if (this.pendingReleases.length >= RELEASE_BATCH_MAX) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      this.flushReleases();
      return;
    }
    this.scheduleReleaseFlush(immediate || this.sweepPermits.size + this.shortPermits.size < 4);
  }

  private flushReleases(): void {
    const total = this.pendingReleases.length;
    if (total === 0) return;
    const batch =
      total <= RELEASE_BATCH_MAX
        ? this.pendingReleases
        : this.pendingReleases.splice(0, RELEASE_BATCH_MAX);
    if (total <= RELEASE_BATCH_MAX) this.pendingReleases = [];
    else this.scheduleReleaseFlush(true);

    releaseRedisPermits(batch, () => this.connection.stopped, this.recordOrphanedRelease);
  }

  private registerPermit(
    concKey: string,
    member: string,
    leaseTtlMs: number,
    leaseTtlStr: string,
    onRevoke: (() => void) | undefined,
    batchNowMs: number,
  ): RateLimitAcquireResult {
    const revoked = onRevoke ? null : new AbortController();
    const renewEveryMs = Math.max(1, Math.floor(leaseTtlMs / 3));
    const safetyMarginMs = safetyMarginFor(leaseTtlMs);
    const isShort = leaseTtlMs < 3000;
    const entry: ActivePermit = {
      concKey,
      member,
      leaseTtlMs,
      leaseTtlStr,
      renewEveryMs,
      safetyMarginMs,
      confirmedExpiresAtMs: batchNowMs + leaseTtlMs,
      nextRenewAtMs: batchNowMs + (isShort ? renewEveryMs : jitteredRenewDelay(renewEveryMs)),
      renewing: false,
      released: false,
      onRevoke,
      revoked,
    };
    if (isShort) {
      this.shortPermits.add(entry);
      this.scheduleShortRenew(entry);
    } else {
      this.sweepPermits.add(entry);
      if (!this.sweepTimer) this.ensureSweepTimer();
    }
    const release = () => this.releasePermit(entry, isShort);
    return revoked ? { ok: true, release, signal: revoked.signal } : { ok: true, release };
  }

  private scheduleAcquireFlush(): void {
    if (!this.acquireMicrotask) {
      this.acquireMicrotask = true;
      setImmediate(() => {
        this.acquireMicrotask = false;
        this.flushAcquires();
      });
    }
  }

  private takePiggybackReleases(activeClient: RedisClient): PendingRelease[] | undefined {
    const relTotal = this.pendingReleases.length;
    if (relTotal === 0) return undefined;
    if (this.releaseTimer) {
      clearTimeout(this.releaseTimer);
      this.releaseTimer = undefined;
    }
    if (relTotal <= RELEASE_BATCH_MAX) {
      let relMatch = true;
      for (let i = 0; i < relTotal; i++) {
        if (this.pendingReleases[i]!.redis !== activeClient) {
          relMatch = false;
          break;
        }
      }
      if (relMatch) {
        const out = this.pendingReleases;
        this.pendingReleases = [];
        return out;
      }
    }
    while (this.pendingReleases.length > 0) this.flushReleases();
    return undefined;
  }

  private flushAcquires(): void {
    if (this.acquireInFlight >= MAX_ACQUIRE_IN_FLIGHT) return;
    const total = this.pendingAcquires.length;
    if (total === 0) return;
    const batch =
      total <= ACQUIRE_BATCH_MAX
        ? this.pendingAcquires
        : this.pendingAcquires.splice(0, ACQUIRE_BATCH_MAX);
    if (total <= ACQUIRE_BATCH_MAX) this.pendingAcquires = [];
    else if (this.acquireInFlight + 1 < MAX_ACQUIRE_IN_FLIGHT) this.scheduleAcquireFlush();

    const activeClient = this.connection.commandClient;
    if (
      activeClient &&
      !this.connection.stopped &&
      this.connection.active &&
      activeClient.connected
    ) {
      let allMatch = true;
      for (let i = 0; i < batch.length; i++) {
        if (batch[i]!.redis !== activeClient) {
          allMatch = false;
          break;
        }
      }
      if (allMatch) {
        void this.runAcquireBatch(activeClient, batch, this.takePiggybackReleases(activeClient));
        return;
      }
    }

    if (this.pendingReleases.length > 0) {
      if (this.releaseTimer) {
        clearTimeout(this.releaseTimer);
        this.releaseTimer = undefined;
      }
      while (this.pendingReleases.length > 0) this.flushReleases();
    }
    const groups = new Map<RedisClient, PendingAcquire[]>();
    for (const item of batch) {
      if (
        this.connection.stopped ||
        this.connection.commandClient !== item.redis ||
        !this.connection.active ||
        !item.redis.connected
      ) {
        item.resolve(failAcquire(item.ingressIp));
        continue;
      }
      let list = groups.get(item.redis);
      if (!list) {
        list = [];
        groups.set(item.redis, list);
      }
      list.push(item);
    }
    for (const [redis, items] of groups) {
      void this.runAcquireBatch(redis, items);
    }
  }

  private async runAcquireBatch(
    redis: RedisClient,
    items: PendingAcquire[],
    piggybackReleases?: PendingRelease[],
  ): Promise<void> {
    this.acquireInFlight++;
    const n = items.length;
    const relGroups = piggybackReleases ? groupPiggybackReleases(piggybackReleases) : undefined;
    const { shaArgs, minIpTimeoutMs } = buildBatchAcquireArgs(items, relGroups, (ip) =>
      this.connection.ingressKeyFor(ip),
    );
    const batchTimeoutMs =
      minIpTimeoutMs === undefined
        ? ACQUIRE_TIMEOUT_MS
        : Math.min(ACQUIRE_TIMEOUT_MS, minIpTimeoutMs);
    const sentAtMs = Date.now();
    let expired = false;
    const work = evalPermitPrepared(redis, BATCH_ACQUIRE_LUA, shaArgs, () => !expired);
    const recoverAcquires = (at: number) => {
      if (this.connection.stopped) return;
      for (const item of items) {
        this.releaseRecovery.record(item.keys.concKey, item.member, at + item.leaseTtlMs);
      }
      this.scheduleReleaseRecoveryFlush();
    };
    try {
      const raw = await withRedisDeadline(work, batchTimeoutMs, () => {
        expired = true;
      });
      if (!Array.isArray(raw) || raw.length !== n) {
        throw new Error("Invalid Redis rate limit reply");
      }
      this.connection.reportAcquireSuccess();
      const disconnected =
        this.connection.stopped ||
        this.connection.commandClient !== redis ||
        !this.connection.active;
      for (let i = 0; i < n; i++) {
        const it = items[i]!;
        const denied = decodeAcquireFailure(Number(raw[i]), it.ingressIp);
        if (denied) {
          it.resolve(denied);
        } else if (disconnected) {
          if (redis.connected) {
            void redis.send("ZREM", [it.keys.concKey, it.member]).catch(() => {
              if (!this.connection.stopped) {
                this.recordOrphanedRelease(it.keys.concKey, it.member, sentAtMs + it.leaseTtlMs);
              }
            });
          } else if (!this.connection.stopped) {
            this.recordOrphanedRelease(it.keys.concKey, it.member, sentAtMs + it.leaseTtlMs);
          }
          it.resolve(failAcquire(it.ingressIp));
        } else {
          it.resolve(
            this.registerPermit(
              it.keys.concKey,
              it.member,
              it.leaseTtlMs,
              it.leaseTtlStr,
              it.onRevoke,
              sentAtMs,
            ),
          );
        }
      }
    } catch {
      if (!this.connection.stopped && piggybackReleases) {
        for (const release of piggybackReleases) {
          this.releaseRecovery.record(release.concKey, release.member, release.expiresAtMs);
        }
      }
      recoverAcquires(sentAtMs);
      for (const it of items) it.resolve(failAcquire(it.ingressIp));
      this.connection.reportAcquireFailure(redis);
      if (expired) {
        this.lateAcquireBatches++;
        if (this.acquireInFlight >= MAX_ACQUIRE_IN_FLIGHT) this.failPendingFor();
        else if (this.pendingAcquires.length > 0) this.scheduleAcquireFlush();
        await work.catch(() => undefined);
        this.lateAcquireBatches--;
        recoverAcquires(Date.now());
      }
    } finally {
      this.acquireInFlight--;
      if (
        this.pendingAcquires.length > 0 &&
        !this.connection.stopped &&
        this.acquireInFlight < MAX_ACQUIRE_IN_FLIGHT
      ) {
        this.scheduleAcquireFlush();
      }
    }
  }

  acquireRateLimit(
    key: string,
    limits: { rpm: number; tpm: number; concurrency: number },
    estimatedTokens: number,
    leaseTtlMs = 120_000,
    onRevoke?: () => void,
    ingressIp?: IngressIpCheck,
  ): Promise<RateLimitAcquireResult> {
    if (!this.connection.redisUrl) {
      if (ingressIp) return Promise.resolve({ ok: false, reason: "ingress_quota_unavailable" });
      return Promise.resolve(this.localLimiter.acquire(key, limits, estimatedTokens));
    }
    const redis = this.connection.commandClient;
    if (this.lateAcquireBatches > 0 && this.acquireInFlight >= MAX_ACQUIRE_IN_FLIGHT)
      return Promise.resolve(failAcquire(ingressIp));
    if (!redis?.connected || !this.connection.active || this.connection.stopped) {
      return Promise.resolve(failAcquire(ingressIp));
    }
    const keys = this.getKeyTriple(key);
    const member = `${this.memberPrefix}${++this.permitSeq}`;
    const safeTpmLimit = Math.min(limits.tpm, Number.MAX_SAFE_INTEGER);
    return new Promise<RateLimitAcquireResult>((resolve) => {
      this.pendingAcquires.push({
        redis,
        keys,
        rpm: String(limits.rpm),
        tpm: String(safeTpmLimit),
        concurrency: String(limits.concurrency),
        estimatedTokens: String(estimatedTokens),
        leaseTtlMs,
        leaseTtlStr: String(leaseTtlMs),
        member,
        ingressIp,
        onRevoke,
        resolve,
      });
      if (
        this.pendingAcquires.length >= ACQUIRE_BATCH_MAX &&
        this.acquireInFlight < MAX_ACQUIRE_IN_FLIGHT
      ) {
        this.flushAcquires();
        return;
      }
      if (
        this.acquireInFlight === 0 ||
        this.lateAcquireBatches > 0 ||
        leaseTtlMs < 3000 ||
        this.sweepPermits.size + this.shortPermits.size < 4
      ) {
        this.scheduleAcquireFlush();
      }
    });
  }

  private stopPermitTimers(): void {
    this.releaseRecovery.stopTimer();
    if (this.releaseTimer) {
      clearTimeout(this.releaseTimer);
      this.releaseTimer = undefined;
    }
  }

  private onPermitsDisconnected(): void {
    const now = Date.now();
    for (const entry of this.sweepPermits) {
      entry.renewing = false;
      if (now >= entry.confirmedExpiresAtMs - entry.safetyMarginMs) {
        this.revokePermit(entry, false);
      }
    }
    for (const entry of this.shortPermits) {
      entry.renewing = false;
      if (now >= entry.confirmedExpiresAtMs - entry.safetyMarginMs) {
        this.revokePermit(entry, true);
      } else {
        this.scheduleShortRenew(entry);
      }
    }
    this.resumePermitRenewal();
  }

  private resumePermitRenewal(): void {
    if (this.sweepPermits.size > 0) this.ensureSweepTimer();
  }

  private closePermits(): void {
    this.stopSweep();
    this.failPendingFor();
    this.pendingReleases = [];
    this.releaseRecovery.clear();
    const activeShort = [...this.shortPermits];
    this.shortPermits.clear();
    for (const entry of activeShort) this.revokePermit(entry, true);
    const activeSweep = [...this.sweepPermits];
    this.sweepPermits.clear();
    for (const entry of activeSweep) this.revokePermit(entry, false);
  }
}
