import type { SQL } from "bun";
import { createDrizzle } from "./schema.ts";
import { createLeaseManager, type LeaseOptions } from "./lease-manager.ts";
import { createHoldAcquirer } from "./hold-acquisition.ts";
import { createSettlementQueue } from "./settlement-dispatch.ts";
import { createWalletOperations } from "./wallet-operations.ts";
import { createSupplierFunds } from "./supplier-funds.ts";
import { createHoldTransitions } from "./hold-transitions.ts";
import { createReconciliation } from "./reconciliation.ts";
import { createOutbox } from "./outbox.ts";
import { auditConservation } from "./conservation.ts";

export type LedgerOptions = LeaseOptions & {
  batchWindowMs?: number;
  maxBatchSize?: number;
  maxBatchConcurrency?: number;
};

export function createLedgerEngine(db: SQL, options: LedgerOptions = {}) {
  const orm = createDrizzle(db);
  const batchWindowMs = options.batchWindowMs ?? 2;
  const maxBatchSize = options.maxBatchSize ?? 128;
  const leases = createLeaseManager(db, options);
  const acquisition = createHoldAcquirer(
    db,
    leases,
    batchWindowMs,
    maxBatchSize,
    options.maxBatchConcurrency ?? 4,
  );
  const settlement = createSettlementQueue(db, leases, batchWindowMs, maxBatchSize);
  const holds = createHoldTransitions(db, orm, leases, batchWindowMs, maxBatchSize);

  return {
    acquireHold: acquisition.acquireHold,
    commitTerminal: settlement.commitTerminal,
    switchSupplierAttempt: holds.switchSupplierAttempt,
    releaseHold: holds.releaseHold,
    markUnknown: holds.markUnknown,
    sweepTimedOutHolds: holds.sweepTimedOutHolds,
    ...createWalletOperations(db, orm, leases.invalidateWallet),
    ...createSupplierFunds(db, orm),
    ...createReconciliation(db, leases.notifyDetachedLocal),
    ...createOutbox(orm),
    sweepExpiredLeases: leases.sweepExpiredLeases,
    releaseIdleLeases: leases.releaseIdleLeases,
    auditConservation: () => auditConservation(db),
    async drain(): Promise<void> {
      leases.stopAcquiring();
      await acquisition.drain();
      await holds.drain();
      await settlement.drain();
      await leases.releaseIdleLeases();
    },
  };
}
