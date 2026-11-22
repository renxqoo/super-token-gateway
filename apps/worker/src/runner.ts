import type { SQL } from "bun";
import {
  createGatewayConfigNodeRepository,
  createLedgerEngine,
  prepareLedgerMonth,
} from "@gateway/ledger";

export type WorkerTickStats = {
  is_leader: boolean;
  swept_holds: number;
  swept_leases: number;
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
  monitorIntervalMs?: number;
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
  const ledger = createLedgerEngine(db, { enableLeases: false });
  const configNodes = createGatewayConfigNodeRepository(db);
  const leaseKey = options.leaseKey ?? 81442599;
  const consumerName = options.outboxConsumerName ?? "business_webhook";
  const fetchImpl = options.fetchImpl ?? fetch;

  let reservedConn: Awaited<ReturnType<SQL["reserve"]>> | null = null;
  let hasLeadership = false;
  let running = false;
  let stopped = false;
  const timers: ReturnType<typeof setInterval>[] = [];
  const lanes = {
    ledger: Promise.resolve(),
    outbox: Promise.resolve(),
    monitoring: Promise.resolve(),
  };
  let leadershipCheck: Promise<boolean> | null = null;
  let lastSuccessfulSweep: string | null = null;
  let lastSuccessfulDelivery: string | null = null;
  let lastOperationsSample = 0;
  let operationsSample: Record<string, number> = {};
  let deliveryFailures = 0;

  function ensureLeadership(): Promise<boolean> {
    leadershipCheck ??= checkLeadership().finally(() => {
      leadershipCheck = null;
    });
    return leadershipCheck;
  }

  async function checkLeadership(): Promise<boolean> {
    if (hasLeadership && reservedConn) {
      try {
        const [lock] = await reservedConn`SELECT NOT pg_is_in_recovery() AND EXISTS (
          SELECT FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
            AND objsubid = 1 AND ((classid::bigint << 32) | objid::bigint) = ${leaseKey}
            AND mode = 'ExclusiveLock' AND granted
        ) AS held`;
        if (lock?.held) return true;
        throw new Error("Worker session no longer owns the ledger lock");
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
      const rows = await reservedConn`SELECT CASE WHEN NOT pg_is_in_recovery()
        AND current_setting('transaction_read_only') = 'off'
        THEN pg_try_advisory_lock(${leaseKey}) ELSE false END AS locked`;
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
      if (wasLeader) {
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

  async function deliverOutboxBatch(limit = 500): Promise<number> {
    if (!options.businessWebhookUrl) return 0;
    const batch = await ledger.claimOutboxEvents(consumerName, limit);
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
    await res.body?.cancel().catch(() => undefined);
    if (!res.ok) throw new Error(`Outbox HTTP delivery failed with status ${res.status}`);
    const lastSeq = batch.events.at(-1)!.seq;
    if (!(await ledger.ackOutboxEvents(consumerName, batch.lease_token, lastSeq))) {
      throw new Error("Outbox acknowledgement no longer owns the claimed batch");
    }
    lastSuccessfulDelivery = new Date().toISOString();
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
        swept_leases: 0,
        expired_wallets: 0,
        collected_debts: 0,
        rebalanced_accounts: 0,
        delivered_outbox_events: 0,
        prepared_months: [],
      };
    }
    const swept_holds = await ledger.sweepTimedOutHolds(200);
    const swept_leases = await ledger.sweepExpiredLeases(100);
    lastSuccessfulSweep = new Date().toISOString();
    const expired_wallets = await ledger.expireAllLots(100);
    const collected_debts = await ledger.collectAllDebts(100);
    const rebalanced_accounts = await rebalanceImbalancedSuppliers();
    const delivered_outbox_events = await deliverOutboxBatch();
    const prepared_months = includePartitions ? await ensurePartitions() : [];
    return {
      is_leader: true,
      swept_holds,
      swept_leases,
      expired_wallets,
      collected_debts,
      rebalanced_accounts,
      delivered_outbox_events,
      prepared_months,
    };
  }

  function scheduleTask(
    fn: () => Promise<unknown>,
    intervalMs: number,
    lane: keyof typeof lanes = "ledger",
  ): void {
    let pending = false;
    const timer = setInterval(() => {
      if (stopped || pending) return;
      pending = true;
      lanes[lane] = lanes[lane]
        .then(async () => {
          if (stopped) return;
          await fn();
        })
        .catch((err: unknown) => {
          console.error(
            JSON.stringify({
              service: "worker",
              level: "error",
              error: err instanceof Error ? err.message : String(err),
            }),
          );
        })
        .finally(() => {
          pending = false;
        });
    }, intervalMs);
    timers.push(timer);
  }

  return {
    isLeader(): boolean {
      return hasLeadership;
    },

    status() {
      return {
        leader: hasLeadership,
        last_successful_sweep: lastSuccessfulSweep,
        last_successful_delivery: lastSuccessfulDelivery,
      };
    },

    metrics(): string {
      const values = {
        leader: Number(hasLeadership),
        last_successful_sweep_timestamp_seconds: lastSuccessfulSweep
          ? Date.parse(lastSuccessfulSweep) / 1000
          : 0,
        last_successful_delivery_timestamp_seconds: lastSuccessfulDelivery
          ? Date.parse(lastSuccessfulDelivery) / 1000
          : 0,
        outbox_enabled: Number(Boolean(options.businessWebhookUrl)),
        outbox_delivery_failures_total: deliveryFailures,
        operations_sample_timestamp_seconds: lastOperationsSample,
        ...operationsSample,
      };
      return Object.entries(values)
        .map(([key, value]) => `stg_worker_${key} ${value}\n`)
        .join("");
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
      if (stopped) return;
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ledger.sweepTimedOutHolds(200);
          await ledger.sweepExpiredLeases(100);
          lastSuccessfulSweep = new Date().toISOString();
        }
      }, options.sweepIntervalMs ?? 2000);
      scheduleTask(
        async () => {
          if (!hasLeadership) return;
          const monitorConsumer = options.businessWebhookUrl ? consumerName : null;
          const [row] = await db`SELECT * FROM public.operations_health(${monitorConsumer}::text)`;
          operationsSample = Object.fromEntries(
            Object.entries(row ?? {}).map(([key, value]) => [
              `${key}_timestamp_seconds`,
              Number(value),
            ]),
          );
          lastOperationsSample = Date.now() / 1000;
        },
        options.monitorIntervalMs ?? 15_000,
        "monitoring",
      );
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ledger.expireAllLots(100);
          await ledger.collectAllDebts(100);
          await rebalanceImbalancedSuppliers();
        }
      }, options.maintenanceIntervalMs ?? 15_000);
      if (options.businessWebhookUrl) {
        scheduleTask(
          async () => {
            if (!(await ensureLeadership())) return;
            const until = performance.now() + (options.outboxIntervalMs ?? 1000);
            const batchSize = 500;
            try {
              do {
                if ((await deliverOutboxBatch(batchSize)) < batchSize) break;
              } while (!stopped && performance.now() < until);
            } catch (error) {
              deliveryFailures++;
              throw error;
            }
          },
          options.outboxIntervalMs ?? 1000,
          "outbox",
        );
      }
      scheduleTask(async () => {
        if (await ensureLeadership()) {
          await ensurePartitions();
          await configNodes.pruneStaleNodes(24).catch(() => undefined);
        }
      }, options.partitionIntervalMs ?? 3_600_000);
    },

    async stop(): Promise<void> {
      stopped = true;
      running = false;
      for (const t of timers) clearInterval(t);
      timers.length = 0;
      await Promise.all(Object.values(lanes));
      await ledger.drain();
      await releaseLeadership();
    },
  };
}
