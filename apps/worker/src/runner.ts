import type { SQL } from "bun";
import { createLedgerEngine, prepareLedgerMonth } from "@gateway/ledger";

export type WorkerTickStats = {
  is_leader: boolean;
  swept_holds: number;
  expired_wallets: number;
  collected_debts: number;
  rebalanced_accounts: number;
  delivered_outbox_events: number;
  prepared_months: string[];
};

export type WorkerRunnerOptions = {
  db: SQL;
  sweepIntervalMs?: number;
  maintenanceIntervalMs?: number;
  outboxIntervalMs?: number;
  partitionIntervalMs?: number;
  businessWebhookUrl?: string;
  businessWebhookToken?: string;
  outboxConsumerName?: string;
  leaseKey?: number;
  fetchImpl?: typeof fetch;
};

function currentAndNextMonths(now = new Date()): [string, string] {
  const fmt = (d: Date) => `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
  const current = fmt(now);
  const nextDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
  return [current, fmt(nextDate)];
}

export function createWorkerRunner(options: WorkerRunnerOptions) {
  const db = options.db;
  const ledger = createLedgerEngine(db, { batchWindowMs: 5, maxBatchSize: 16 });
  const leaseKey = options.leaseKey ?? 81442599;
  const consumerName = options.outboxConsumerName ?? "business_webhook";
  const fetchImpl = options.fetchImpl ?? fetch;

  let reservedConn: Awaited<ReturnType<SQL["reserve"]>> | null = null;
  let hasLeadership = false;
  let running = false;
  let stopped = false;
  const timers: ReturnType<typeof setInterval>[] = [];
  let inFlight: Promise<unknown> = Promise.resolve();

  async function ensureLeadership(): Promise<boolean> {
    if (leaseKey === 0) {
      hasLeadership = true;
      return true;
    }
    if (hasLeadership && reservedConn) {
      try {
        await reservedConn`SELECT 1`;
        return true;
      } catch {
        hasLeadership = false;
        try {
          reservedConn.release();
        } catch {
          void 0;
        }
        reservedConn = null;
      }
    }
    try {
      if (!reservedConn) {
        reservedConn = await db.reserve();
      }
      const rows = await reservedConn`SELECT pg_try_advisory_lock(${leaseKey}) AS locked`;
      hasLeadership = Boolean(rows[0]?.locked);
      if (!hasLeadership) {
        reservedConn.release();
        reservedConn = null;
      }
      return hasLeadership;
    } catch {
      hasLeadership = false;
      if (reservedConn) {
        try {
          reservedConn.release();
        } catch {
          void 0;
        }
        reservedConn = null;
      }
      return false;
    }
  }

  async function releaseLeadership(): Promise<void> {
    if (!reservedConn) {
      hasLeadership = false;
      return;
    }
    const conn = reservedConn;
    reservedConn = null;
    const wasLeader = hasLeadership;
    hasLeadership = false;
    try {
      if (wasLeader && leaseKey !== 0) {
        await conn`SELECT pg_advisory_unlock(${leaseKey})`;
      }
    } catch {
      void 0;
    } finally {
      try {
        conn.release();
      } catch {
        void 0;
      }
    }
  }

  async function rebalanceImbalancedSuppliers(): Promise<number> {
    const candidates = await db`
      SELECT account_id, currency
      FROM public.supplier_cells
      GROUP BY account_id, currency
      HAVING sum(balance_atoms - reserved_atoms) > 0
         AND min(balance_atoms - reserved_atoms) * 4 < max(balance_atoms - reserved_atoms)
      LIMIT 32
    `;
    let count = 0;
    for (const row of candidates) {
      const ok = await ledger.rebalanceSupplierCells(String(row.account_id), String(row.currency));
      if (ok) count++;
    }
    return count;
  }

  async function deliverOutboxBatch(): Promise<number> {
    if (!options.businessWebhookUrl) return 0;
    const batch = await ledger.claimOutboxEvents(consumerName, 100);
    if (batch.events.length === 0) return 0;
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (options.businessWebhookToken) {
      headers.authorization = `Bearer ${options.businessWebhookToken}`;
    }
    const res = await fetchImpl(options.businessWebhookUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({
        consumer: consumerName,
        events: batch.events,
      }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) return 0;
    const lastSeq = batch.events.at(-1)!.seq;
    await ledger.ackOutboxEvents(consumerName, batch.lease_token, lastSeq);
    return batch.events.length;
  }

  async function ensurePartitions(): Promise<string[]> {
    const months = currentAndNextMonths();
    for (const m of months) {
      await prepareLedgerMonth(db, m);
    }
    return months;
  }

  async function runOnce(includePartitions = true): Promise<WorkerTickStats> {
    const leader = await ensureLeadership();
    if (!leader) {
      return {
        is_leader: false,
        swept_holds: 0,
        expired_wallets: 0,
        collected_debts: 0,
        rebalanced_accounts: 0,
        delivered_outbox_events: 0,
        prepared_months: [],
      };
    }
    const swept_holds = await ledger.sweepTimedOutHolds(200);
    const expired_wallets = await ledger.expireAllLots(100);
    const collected_debts = await ledger.collectAllDebts(100);
    const rebalanced_accounts = await rebalanceImbalancedSuppliers();
    const delivered_outbox_events = await deliverOutboxBatch();
    const prepared_months = includePartitions ? await ensurePartitions() : [];
    return {
      is_leader: true,
      swept_holds,
      expired_wallets,
      collected_debts,
      rebalanced_accounts,
      delivered_outbox_events,
      prepared_months,
    };
  }

  function scheduleTask(fn: () => Promise<unknown>, intervalMs: number): void {
    const timer = setInterval(() => {
      if (stopped) return;
      inFlight = inFlight.then(async () => {
        if (stopped) return;
        try {
          await fn();
        } catch (err) {
          console.error(
            JSON.stringify({
              service: "worker",
              level: "error",
              error: err instanceof Error ? err.message : String(err),
            }),
          );
        }
      });
    }, intervalMs);
    timers.push(timer);
  }

  return {
    isLeader(): boolean {
      return hasLeadership;
    },

    runOnce,

    async start(): Promise<void> {
      if (running) return;
      running = true;
      stopped = false;
      const leader = await ensureLeadership();
      if (leader) {
        await ensurePartitions().catch(() => undefined);
      }
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ledger.sweepTimedOutHolds(200);
        }
      }, options.sweepIntervalMs ?? 2000);
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ledger.expireAllLots(100);
          await ledger.collectAllDebts(100);
          await rebalanceImbalancedSuppliers();
        }
      }, options.maintenanceIntervalMs ?? 15_000);
      if (options.businessWebhookUrl) {
        scheduleTask(async () => {
          if (await ensureLeadership()) {
            await deliverOutboxBatch();
          }
        }, options.outboxIntervalMs ?? 1000);
      }
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ensurePartitions();
        }
      }, options.partitionIntervalMs ?? 3_600_000);
    },

    async stop(): Promise<void> {
      stopped = true;
      running = false;
      for (const t of timers) clearInterval(t);
      timers.length = 0;
      await inFlight;
      await releaseLeadership();
    },
  };
}
