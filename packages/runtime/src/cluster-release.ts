import type { RedisClient } from "bun";
import {
  BATCH_RELEASE_LUA,
  batchReleaseSha,
  evalPermitPrepared,
  withRedisDeadline,
} from "./cluster-redis.ts";

export type PendingRelease = {
  redis: RedisClient;
  concKey: string;
  member: string;
  expiresAtMs: number;
};

export function groupPiggybackReleases(
  releases: PendingRelease[],
): Map<string, string[]> | undefined {
  if (releases.length === 0) return undefined;
  const groups = new Map<string, string[]>();
  for (let i = 0; i < releases.length; i++) {
    const rel = releases[i]!;
    let members = groups.get(rel.concKey);
    if (!members) {
      members = [];
      groups.set(rel.concKey, members);
    }
    members.push(rel.member);
  }
  return groups;
}

function sendGroupedReleases(
  redis: RedisClient,
  batch: PendingRelease[],
  isClosed: () => boolean,
  recordOrphanedRelease: (concKey: string, member: string, expiresAtMs: number) => void,
) {
  const byKey = new Map<string, string[]>();
  for (let i = 0; i < batch.length; i++) {
    const item = batch[i]!;
    let members = byKey.get(item.concKey);
    if (!members) {
      members = [];
      byKey.set(item.concKey, members);
    }
    members.push(item.member);
  }
  if (byKey.size === 1) {
    const firstEntry = byKey.entries().next().value!;
    const args = [firstEntry[0], ...firstEntry[1]];
    void redis.send("ZREM", args).catch(() => {
      if (!isClosed()) {
        for (let i = 0; i < batch.length; i++) {
          const item = batch[i]!;
          recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
        }
      }
    });
    return;
  }
  const shaArgs: string[] = [batchReleaseSha, String(byKey.size)];
  for (const key of byKey.keys()) shaArgs.push(key);
  for (const members of byKey.values()) {
    shaArgs.push(String(members.length));
    for (let i = 0; i < members.length; i++) shaArgs.push(members[i]!);
  }
  void evalPermitPrepared(redis, BATCH_RELEASE_LUA, shaArgs).catch(() => {
    if (!isClosed()) {
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i]!;
        recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
      }
    }
  });
}

export function releaseRedisPermits(
  batch: PendingRelease[],
  isClosed: () => boolean,
  recordOrphanedRelease: (concKey: string, member: string, expiresAtMs: number) => void,
) {
  if (batch.length === 1) {
    const only = batch[0]!;
    if (only.redis.connected) {
      void only.redis.send("ZREM", [only.concKey, only.member]).catch(() => {
        if (!isClosed()) {
          recordOrphanedRelease(only.concKey, only.member, only.expiresAtMs);
        }
      });
    } else if (!isClosed()) {
      recordOrphanedRelease(only.concKey, only.member, only.expiresAtMs);
    }
    return;
  }

  const firstRedis = batch[0]!.redis;
  let sameClient = true;
  for (let i = 1; i < batch.length; i++) {
    if (batch[i]!.redis !== firstRedis) {
      sameClient = false;
      break;
    }
  }
  if (sameClient) {
    if (!firstRedis.connected) {
      if (!isClosed()) {
        for (let i = 0; i < batch.length; i++) {
          const item = batch[i]!;
          recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
        }
      }
      return;
    }
    sendGroupedReleases(firstRedis, batch, isClosed, recordOrphanedRelease);
    return;
  }

  const byClient = new Map<RedisClient, PendingRelease[]>();
  for (const item of batch) {
    if (!item.redis.connected) {
      if (!isClosed()) {
        recordOrphanedRelease(item.concKey, item.member, item.expiresAtMs);
      }
      continue;
    }
    let list = byClient.get(item.redis);
    if (!list) {
      list = [];
      byClient.set(item.redis, list);
    }
    list.push(item);
  }
  for (const [redis, items] of byClient) {
    sendGroupedReleases(redis, items, isClosed, recordOrphanedRelease);
  }
}

type OrphanedRelease = {
  concKey: string;
  member: string;
  expiresAtMs: number;
};

const ORPHAN_RELEASE_MAX = 100_000;

export function createPermitReleaseRecovery(batchSize: number, timeoutMs: number) {
  let entries: OrphanedRelease[] = [];
  let retryTimer: ReturnType<typeof setTimeout> | undefined;
  let flushing = false;
  function record(concKey: string, member: string, expiresAtMs: number): void {
    const now = Date.now();
    if (expiresAtMs <= now) return;
    if (entries.length >= ORPHAN_RELEASE_MAX) {
      entries = entries.filter((item) => item.expiresAtMs > now);
      if (entries.length >= ORPHAN_RELEASE_MAX) {
        entries.splice(0, entries.length - ORPHAN_RELEASE_MAX + 1);
      }
    }
    entries.push({ concKey, member, expiresAtMs });
  }

  async function flush(maint: RedisClient): Promise<void> {
    if (entries.length === 0) return;
    const now = Date.now();
    const pending = entries;
    entries = [];
    const byKey = new Map<string, OrphanedRelease[]>();
    for (let i = 0; i < pending.length; i++) {
      const item = pending[i]!;
      if (item.expiresAtMs <= now) continue;
      let list = byKey.get(item.concKey);
      if (!list) {
        list = [];
        byKey.set(item.concKey, list);
      }
      list.push(item);
    }
    const groups = [...byKey.entries()];
    for (let e = 0; e < groups.length; e++) {
      const [concKey, items] = groups[e]!;
      for (let i = 0; i < items.length; i += batchSize) {
        const chunk = items.slice(i, i + batchSize);
        try {
          await withRedisDeadline(
            maint.send("ZREM", [concKey, ...chunk.map((item) => item.member)]),
            timeoutMs,
          );
        } catch (error) {
          for (let r = i; r < items.length; r++) {
            const rem = items[r]!;
            record(rem.concKey, rem.member, rem.expiresAtMs);
          }
          for (let nextE = e + 1; nextE < groups.length; nextE++) {
            const nextItems = groups[nextE]![1];
            for (let r = 0; r < nextItems.length; r++) {
              const rem = nextItems[r]!;
              record(rem.concKey, rem.member, rem.expiresAtMs);
            }
          }
          throw error;
        }
      }
    }
  }

  function stopTimer(): void {
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = undefined;
    }
  }

  function scheduleFlush(getMaint: () => RedisClient | null, isStopped: () => boolean): void {
    if (entries.length === 0 || isStopped() || retryTimer || flushing) return;
    retryTimer = setTimeout(() => {
      retryTimer = undefined;
      if (entries.length === 0 || isStopped()) return;
      const maint = getMaint();
      if (!maint?.connected) return;
      flushing = true;
      void flush(maint)
        .catch(() => undefined)
        .finally(() => {
          flushing = false;
          if (entries.length > 0 && !isStopped()) scheduleFlush(getMaint, isStopped);
        });
    }, 200);
    retryTimer.unref();
  }

  return {
    record,
    flush,
    scheduleFlush,
    stopTimer,
    get hasPending() {
      return entries.length > 0;
    },
    clear() {
      stopTimer();
      entries = [];
    },
  };
}
