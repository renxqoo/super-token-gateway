import type { SQL } from "bun";
import {
  closeSupplierLease,
  closeWalletLease,
  type ActiveSupplierLease,
  type ActiveWalletLease,
  type LeasedHoldMeta,
} from "./leases.ts";
import type { AcquireHoldInput } from "./types.ts";
import { walletKey, supplierKey, walletLane } from "./funds.ts";

const LEASE_TTL_MS = 15_000;
const LEASE_RENEW_MARGIN_MS = 6_000;
const LEASE_IDLE_RELEASE_MS = 450;

export type LeaseOptions = {
  nodeId?: string;
  enableLeases?: boolean;
  leaseIdleReleaseMs?: number;
  leaseTtlMs?: number;
  onLeaseRenewalFailure?: () => void;
};

export type FastHoldRequest = {
  input: AcquireHoldInput;
  customerAtoms: bigint;
  supplierAtoms: bigint;
  walletLease: ActiveWalletLease;
  supplierLease: ActiveSupplierLease;
};

export type LeaseManager = ReturnType<typeof createLeaseManager>;

export function createLeaseManager(db: SQL, options: LeaseOptions) {
  const nodeId = options.nodeId ?? crypto.randomUUID();
  const leaseEnabled = options.enableLeases ?? true;
  const leaseIdleReleaseMs = options.leaseIdleReleaseMs ?? LEASE_IDLE_RELEASE_MS;
  const leaseTtlMs = options.leaseTtlMs ?? LEASE_TTL_MS;
  let draining = false;
  const activeWalletLeases = new Map<string, ActiveWalletLease>();
  const walletLeasesById = new Map<string, ActiveWalletLease>();
  const closingWalletLeases = new Set<ActiveWalletLease>();

  const activeSupplierLeases = new Map<string, ActiveSupplierLease>();
  const supplierLeasesById = new Map<string, ActiveSupplierLease>();
  const closingSupplierLeases = new Set<ActiveSupplierLease>();

  const leasedHolds = new Map<string, LeasedHoldMeta>();
  const releasingLeaseIds = new Set<string>();

  function retireWalletLease(lease: ActiveWalletLease): void {
    lease.closing = true;
    const wk = walletKey(lease.projectId, lease.currency);
    if (activeWalletLeases.get(wk) === lease) {
      activeWalletLeases.delete(wk);
    }
    closingWalletLeases.add(lease);
  }

  const supplierLeaseKey = (key: string, lane: number) => `${key}:${lane}`;

  function retireSupplierLease(lease: ActiveSupplierLease): void {
    lease.closing = true;
    const sk = supplierLeaseKey(supplierKey(lease.accountId, lease.currency), lease.lane);
    if (activeSupplierLeases.get(sk) === lease) {
      activeSupplierLeases.delete(sk);
    }
    closingSupplierLeases.add(lease);
  }

  function invalidateWallet(projectId: string, currency: string): void {
    const existing = activeWalletLeases.get(walletKey(projectId, currency));
    if (existing) retireWalletLease(existing);
  }

  function notifyDetachedLocal(
    requestId: string,
    detachedWalletLeaseId: string | null,
    heldCustomer: bigint,
    detachedSupplierLeaseId: string | null,
    detachedSupplierAtoms: bigint,
  ): void {
    const meta = leasedHolds.get(requestId);
    leasedHolds.delete(requestId);
    const wl = walletLeasesById.get(detachedWalletLeaseId ?? meta?.walletLeaseId ?? "");
    if (wl) {
      if (wl.inFlightHolds > 0) wl.inFlightHolds--;
      if (detachedWalletLeaseId) {
        wl.remainingInPg = wl.remainingInPg >= heldCustomer ? wl.remainingInPg - heldCustomer : 0n;
      } else {
        retireWalletLease(wl);
      }
    }
    const sl = supplierLeasesById.get(detachedSupplierLeaseId ?? meta?.supplierLeaseId ?? "");
    if (sl) {
      if (sl.inFlightHolds > 0) sl.inFlightHolds--;
      if (detachedSupplierLeaseId) {
        sl.remainingInPg =
          sl.remainingInPg >= detachedSupplierAtoms ? sl.remainingInPg - detachedSupplierAtoms : 0n;
      } else {
        retireSupplierLease(sl);
      }
    }
  }

  function notifySupplierDetachedLocal(
    requestId: string,
    detachedSupplierLeaseId: string | null,
    detachedSupplierAtoms: bigint,
  ): void {
    const meta = leasedHolds.get(requestId);
    leasedHolds.delete(requestId);
    const sl = supplierLeasesById.get(detachedSupplierLeaseId ?? meta?.supplierLeaseId ?? "");
    if (sl) {
      if (sl.inFlightHolds > 0) sl.inFlightHolds--;
      if (detachedSupplierLeaseId) {
        sl.remainingInPg =
          sl.remainingInPg >= detachedSupplierAtoms ? sl.remainingInPg - detachedSupplierAtoms : 0n;
      } else {
        retireSupplierLease(sl);
      }
    }
  }

  function rebindHoldAttempt(
    requestId: string,
    nextAttemptId: string,
    nextChannelId: string,
  ): void {
    const meta = leasedHolds.get(requestId);
    if (!meta) return;
    meta.attemptId = nextAttemptId;
    meta.channelId = nextChannelId;
  }

  async function closeWalletLeaseInPg(leaseId: string, expiredOnly = false): Promise<boolean> {
    if (releasingLeaseIds.has(leaseId)) return false;
    releasingLeaseIds.add(leaseId);
    try {
      const closed = await closeWalletLease(db, leaseId, expiredOnly);
      if (closed === null || (expiredOnly && !closed)) return false;
      const local = walletLeasesById.get(leaseId);
      if (local) {
        retireWalletLease(local);
        closingWalletLeases.delete(local);
        walletLeasesById.delete(leaseId);
      }
      return closed;
    } finally {
      releasingLeaseIds.delete(leaseId);
    }
  }

  async function closeSupplierLeaseInPg(leaseId: string, expiredOnly = false): Promise<boolean> {
    if (releasingLeaseIds.has(leaseId)) return false;
    releasingLeaseIds.add(leaseId);
    try {
      const closed = await closeSupplierLease(db, leaseId, expiredOnly);
      if (closed === null || (expiredOnly && !closed)) return false;
      const local = supplierLeasesById.get(leaseId);
      if (local) {
        retireSupplierLease(local);
        closingSupplierLeases.delete(local);
        supplierLeasesById.delete(leaseId);
      }
      return closed;
    } finally {
      releasingLeaseIds.delete(leaseId);
    }
  }

  async function maintainLeases(forceReleaseAll = false): Promise<void> {
    const nowMs = Date.now();
    const wlToClose: ActiveWalletLease[] = [];
    const wlToRenew: string[] = [];
    const slToClose: ActiveSupplierLease[] = [];
    const slToRenew: string[] = [];

    for (const wl of activeWalletLeases.values()) {
      const lotNearExpiry =
        wl.earliestLotExpiresAtMs !== null && nowMs + 1000 >= wl.earliestLotExpiresAtMs;
      if (
        forceReleaseAll ||
        lotNearExpiry ||
        (wl.inFlightHolds === 0 && nowMs - wl.lastUsedAtMs >= leaseIdleReleaseMs)
      ) {
        retireWalletLease(wl);
      } else if (wl.inFlightHolds > 0 && wl.expiresAtMs - nowMs < LEASE_RENEW_MARGIN_MS) {
        wlToRenew.push(wl.leaseId);
      }
    }
    for (const wl of closingWalletLeases) {
      if (forceReleaseAll || wl.inFlightHolds === 0) {
        wlToClose.push(wl);
      }
    }

    for (const sl of activeSupplierLeases.values()) {
      if (
        forceReleaseAll ||
        (sl.inFlightHolds === 0 && nowMs - sl.lastUsedAtMs >= leaseIdleReleaseMs)
      ) {
        retireSupplierLease(sl);
      } else if (sl.inFlightHolds > 0 && sl.expiresAtMs - nowMs < LEASE_RENEW_MARGIN_MS) {
        slToRenew.push(sl.leaseId);
      }
    }
    for (const sl of closingSupplierLeases) {
      if (forceReleaseAll || sl.inFlightHolds === 0) {
        slToClose.push(sl);
      }
    }

    if (wlToRenew.length > 0) {
      const renewed = await db<{ id: string; expires_ms: number }[]>`
        WITH eligible AS MATERIALIZED (
          SELECT l.id
          FROM public.wallet_leases l
          JOIN public.wallets w ON w.id = l.wallet_id
          WHERE l.id = ANY(${wlToRenew}::uuid[]) AND l.node_id = ${nodeId}
            AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp() AND w.freeze_reason IS NULL
          ORDER BY l.id FOR NO KEY UPDATE OF l
        )
        UPDATE public.wallet_leases l
        SET expires_at = GREATEST(l.expires_at, clock_timestamp() + ${leaseTtlMs} * interval '1 millisecond'), updated_at = now()
        FROM eligible
        WHERE l.id = eligible.id
        RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::float8 AS expires_ms
      `.catch(() => {
        options.onLeaseRenewalFailure?.();
        return [];
      });
      const expiries = new Map(renewed.map((r) => [String(r.id), Number(r.expires_ms)]));
      for (const id of wlToRenew) {
        const lease = walletLeasesById.get(id);
        if (!lease) continue;
        const expires = expiries.get(id);
        if (expires === undefined) retireWalletLease(lease);
        else lease.expiresAtMs = Math.max(lease.expiresAtMs, expires);
      }
    }
    if (slToRenew.length > 0) {
      const renewed = await db<{ id: string; expires_ms: number }[]>`
        WITH eligible AS MATERIALIZED (
          SELECT l.id
          FROM public.supplier_leases l
          JOIN public.upstream_accounts a ON a.id = l.account_id
          WHERE l.id = ANY(${slToRenew}::uuid[]) AND l.node_id = ${nodeId}
            AND l.state = 'ACTIVE' AND l.expires_at > clock_timestamp() AND a.state = 'ACTIVE'
          ORDER BY l.id FOR NO KEY UPDATE OF l
        )
        UPDATE public.supplier_leases l
        SET expires_at = GREATEST(l.expires_at, clock_timestamp() + ${leaseTtlMs} * interval '1 millisecond'), updated_at = now()
        FROM eligible
        WHERE l.id = eligible.id
        RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::float8 AS expires_ms
      `.catch(() => {
        options.onLeaseRenewalFailure?.();
        return [];
      });
      const expiries = new Map(renewed.map((r) => [String(r.id), Number(r.expires_ms)]));
      for (const id of slToRenew) {
        const lease = supplierLeasesById.get(id);
        if (!lease) continue;
        const expires = expiries.get(id);
        if (expires === undefined) retireSupplierLease(lease);
        else lease.expiresAtMs = Math.max(lease.expiresAtMs, expires);
      }
    }

    async function closeInParallel<T>(items: T[], worker: (item: T) => Promise<void>) {
      if (items.length === 0) return;
      const concurrency = Math.min(12, items.length);
      let cursor = 0;
      await Promise.all(
        Array.from({ length: concurrency }, async () => {
          while (cursor < items.length) {
            const item = items[cursor++]!;
            await worker(item);
          }
        }),
      );
    }

    await closeInParallel(wlToClose, async (wl) => {
      await closeWalletLeaseInPg(wl.leaseId).catch(() => undefined);
    });
    await closeInParallel(slToClose, async (sl) => {
      await closeSupplierLeaseInPg(sl.leaseId).catch(() => undefined);
    });
  }

  let maintenance: Promise<void> | undefined;
  let maintenanceTimer: ReturnType<typeof setInterval> | null = null;
  if (leaseEnabled) {
    maintenanceTimer = setInterval(
      () => {
        if (maintenance || draining) return;
        maintenance = maintainLeases(false).finally(() => {
          maintenance = undefined;
        });
      },
      Math.max(100, Math.min(leaseIdleReleaseMs, 150)),
    );
    maintenanceTimer.unref();
  }

  function reserveHold(input: AcquireHoldInput, cust: bigint, sup: bigint): FastHoldRequest | null {
    const nowMs = Date.now();
    const wk = walletKey(input.project_id, input.currency);
    const sk = supplierLeaseKey(
      supplierKey(input.supplier_account_id, input.supplier_currency),
      walletLane(input.project_id),
    );
    const wl = activeWalletLeases.get(wk);
    const sl = activeSupplierLeases.get(sk);
    if (
      wl &&
      (nowMs + 2000 >= wl.expiresAtMs ||
        (wl.earliestLotExpiresAtMs !== null && nowMs + 1000 >= wl.earliestLotExpiresAtMs) ||
        wl.freeInMemory < cust)
    ) {
      retireWalletLease(wl);
    }
    if (sl && (nowMs + 2000 >= sl.expiresAtMs || sl.freeInMemory < sup)) {
      retireSupplierLease(sl);
    }
    const activeWl = activeWalletLeases.get(wk);
    const activeSl = activeSupplierLeases.get(sk);
    if (
      activeWl &&
      activeSl &&
      !activeWl.closing &&
      !activeSl.closing &&
      activeWl.freeInMemory >= cust &&
      activeSl.freeInMemory >= sup
    ) {
      activeWl.freeInMemory -= cust;
      activeWl.inFlightHolds++;
      activeWl.lastUsedAtMs = nowMs;
      activeSl.freeInMemory -= sup;
      activeSl.inFlightHolds++;
      activeSl.lastUsedAtMs = nowMs;
      return {
        input,
        customerAtoms: cust,
        supplierAtoms: sup,
        walletLease: activeWl,
        supplierLease: activeSl,
      };
    }
    return null;
  }
  async function sweepExpiredLeases(limit = 100): Promise<number> {
    let swept = 0;
    const expiredWalletLeases = await db`
        SELECT id
        FROM public.wallet_leases
        WHERE state = 'ACTIVE' AND expires_at <= now()
        ORDER BY expires_at
        LIMIT ${limit}
      `;
    for (const row of expiredWalletLeases) {
      const lid = String(row.id);
      const closed = await closeWalletLeaseInPg(lid, true);
      if (closed) swept++;
    }

    const expiredSupplierLeases = await db`
        SELECT id
        FROM public.supplier_leases
        WHERE state = 'ACTIVE' AND expires_at <= now()
        ORDER BY expires_at
        LIMIT ${limit}
      `;
    for (const row of expiredSupplierLeases) {
      const lid = String(row.id);
      const closed = await closeSupplierLeaseInPg(lid, true);
      if (closed) swept++;
    }
    return swept;
  }

  function settleHold(
    meta: LeasedHoldMeta,
    customerAtoms: bigint,
    supplierAtoms: bigint,
    wlExpiresAtMs: number,
    slExpiresAtMs: number,
    postNow: number,
  ): void {
    leasedHolds.delete(meta.requestId);
    const wl = walletLeasesById.get(meta.walletLeaseId);
    if (wl) {
      wl.remainingInPg -= customerAtoms;
      wl.freeInMemory += meta.heldCustomerAtoms - customerAtoms;
      if (wl.inFlightHolds > 0) wl.inFlightHolds--;
      if (wlExpiresAtMs > wl.expiresAtMs) wl.expiresAtMs = wlExpiresAtMs;
      wl.lastUsedAtMs = postNow;
    }
    const sl = supplierLeasesById.get(meta.supplierLeaseId);
    if (sl) {
      sl.remainingInPg -= supplierAtoms;
      sl.freeInMemory += meta.heldSupplierAtoms - supplierAtoms;
      if (sl.inFlightHolds > 0) sl.inFlightHolds--;
      if (slExpiresAtMs > sl.expiresAtMs) sl.expiresAtMs = slExpiresAtMs;
      sl.lastUsedAtMs = postNow;
    }
  }

  function registerWallet(lease: ActiveWalletLease) {
    const key = walletKey(lease.projectId, lease.currency);
    const previous = activeWalletLeases.get(key);
    if (previous) retireWalletLease(previous);
    walletLeasesById.set(lease.leaseId, lease);
    activeWalletLeases.set(key, lease);
  }

  function registerSupplier(lease: ActiveSupplierLease) {
    const key = supplierLeaseKey(supplierKey(lease.accountId, lease.currency), lease.lane);
    const previous = activeSupplierLeases.get(key);
    if (previous) retireSupplierLease(previous);
    supplierLeasesById.set(lease.leaseId, lease);
    activeSupplierLeases.set(key, lease);
  }

  return {
    nodeId,
    ttlMs: leaseTtlMs,
    canAcquire: () => leaseEnabled && !draining,
    reserveHold,
    hasWallet: (key: string) => activeWalletLeases.has(key),
    hasSupplier: (key: string, lane: number) =>
      activeSupplierLeases.has(supplierLeaseKey(key, lane)),
    hasAnyWallet: () => activeWalletLeases.size > 0,
    walletById: (id: string) => walletLeasesById.get(id),
    supplierById: (id: string) => supplierLeasesById.get(id),
    holdById: (id: string) => leasedHolds.get(id),
    trackHold: (meta: LeasedHoldMeta) => {
      leasedHolds.set(meta.requestId, meta);
    },
    retireWalletLease,
    retireSupplierLease,
    invalidateWallet,
    registerWallet,
    registerSupplier,
    notifyDetachedLocal,
    notifySupplierDetachedLocal,
    rebindHoldAttempt,
    settleHold,
    sweepExpiredLeases,
    async releaseIdleLeases(): Promise<void> {
      await maintenance;
      await maintainLeases(true);
    },
    stopAcquiring(): void {
      draining = true;
      if (maintenanceTimer) {
        clearInterval(maintenanceTimer);
        maintenanceTimer = null;
      }
    },
  };
}
