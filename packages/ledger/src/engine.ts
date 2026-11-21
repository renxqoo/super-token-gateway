import type { SQL } from "bun";
import { and, asc, eq, gt, inArray, lte, sql } from "drizzle-orm";
import { calculateCost, canonical, type Pricing, type Usage } from "@gateway/kernel";
import {
  carveFromLeaseLots,
  detachHoldFromLeaseTx,
  earliestExpiryOf,
  type ActiveSupplierLease,
  type ActiveWalletLease,
  type LeasedHoldMeta,
} from "./leases.ts";
import {
  billingHolds,
  createDrizzle,
  outboxConsumers,
  outboxEvents,
  supplierCells,
  supplierRootBalances,
  wallets,
} from "./schema.ts";
import type {
  AcquireHoldInput,
  AcquireHoldResult,
  AdjustChargeInput,
  ChargeLotAllocation,
  CommitTerminalInput,
  CommitTerminalResult,
  ConservationAudit,
  CreditWalletInput,
  HoldLotAllocation,
  LeaseLotAllocation,
  OutboxEventRecord,
  RechargeSupplierInput,
  RefundChargeInput,
  ResolveRequestInput,
  SupplierHoldEntry,
  SwitchSupplierInput,
  SwitchSupplierResult,
  WalletPolicy,
  WalletSnapshot,
} from "./types.ts";

const MAX_ATOMS = 10n ** 38n - 1n;
const CELL_COUNT = 16;
const LEASE_TTL_MS = 15_000;
const LEASE_RENEW_MARGIN_MS = 6_000;
const LEASE_IDLE_RELEASE_MS = 250;
const LEASE_MIN_MULTIPLIER = 64n;
const LEASE_TARGET_MULTIPLIER = 4096n;
const LEASE_MIN_WALLET_CHUNK = 50_000_000n;
const LEASE_MIN_SUPPLIER_CHUNK = 50_000_000n;

type WalletRow = {
  id: string;
  project_id: string;
  currency: string;
  policy: WalletPolicy;
  available_atoms: bigint;
  reserved_atoms: bigint;
  spent_atoms: bigint;
  expired_atoms: bigint;
  debt_atoms: bigint;
  credit_limit_atoms: bigint;
  credit_reserved_atoms: bigint;
  credit_policy_version: bigint;
  manual_frozen: boolean;
  evidence_disputed: boolean;
  freeze_reason: string | null;
  dirty: boolean;
};

type LotRow = {
  id: string;
  wallet_id: string;
  kind: "CASH" | "GRANT";
  priority: number;
  models: string[];
  operations: string[];
  expires_at_ms: number | null;
  created_at_ms: number;
  issued_atoms: bigint;
  available_atoms: bigint;
  reserved_atoms: bigint;
  spent_atoms: bigint;
  expired_atoms: bigint;
  dirty: boolean;
};

type SupplierCellRow = {
  account_id: string;
  currency: string;
  cell: number;
  balance_atoms: bigint;
  reserved_atoms: bigint;
  spent_atoms: bigint;
  revision: bigint;
  dirty: boolean;
};

type JournalLine = {
  journal_id: string;
  ordinal: number;
  side: "CUSTOMER" | "SUPPLIER";
  entity_id: string;
  request_id: string | null;
  reference: string;
  kind:
    | "CREDIT"
    | "SETTLE"
    | "RELEASE"
    | "REFUND"
    | "ADJUST"
    | "EXPIRE"
    | "DEBT_COLLECT"
    | "MODERATION_ABSORB"
    | "SUPPLIER_RECHARGE"
    | "SUPPLIER_COST"
    | "SUPPLIER_ADJUST";
  currency: string;
  account: string;
  atoms: bigint;
};

type FastHoldRequest = {
  input: AcquireHoldInput;
  customerAtoms: bigint;
  supplierAtoms: bigint;
  walletLease: ActiveWalletLease;
  supplierLease: ActiveSupplierLease;
};

function parseAtoms(value: string): bigint | null {
  if (!/^(0|[1-9][0-9]{0,37})$/.test(value)) return null;
  const n = BigInt(value);
  if (n < 0n || n > MAX_ATOMS) return null;
  return n;
}

function parseSignedAtoms(value: string): bigint | null {
  if (!/^-?(0|[1-9][0-9]{0,37})$/.test(value)) return null;
  const n = BigInt(value);
  if (n < -MAX_ATOMS || n > MAX_ATOMS) return null;
  return n;
}

function sha256Hex(value: unknown): string {
  return new Bun.CryptoHasher("sha256").update(canonical(value)).digest("hex");
}

function computeFreezeReason(w: WalletRow): string | null {
  if (w.evidence_disputed) return "EVIDENCE_CONFLICT";
  if (w.manual_frozen) return "MANUAL";
  if (w.debt_atoms > w.credit_limit_atoms) return "DEBT_EXCEEDS_CREDIT_LIMIT";
  if (w.debt_atoms + w.credit_reserved_atoms > w.credit_limit_atoms) return "CREDIT_LIMIT_EXCEEDED";
  return null;
}

function sortLots(lots: LotRow[], policy: WalletPolicy): LotRow[] {
  return [...lots].sort((a, b) => {
    if (policy === "GIFT_FIRST") {
      const ka = a.kind === "GRANT" ? 0 : 1;
      const kb = b.kind === "GRANT" ? 0 : 1;
      if (ka !== kb) return ka - kb;
    } else if (policy === "CASH_FIRST") {
      const ka = a.kind === "CASH" ? 0 : 1;
      const kb = b.kind === "CASH" ? 0 : 1;
      if (ka !== kb) return ka - kb;
    } else if (policy === "EARLIEST_EXPIRY") {
      const ea = a.expires_at_ms ?? Number.MAX_SAFE_INTEGER;
      const eb = b.expires_at_ms ?? Number.MAX_SAFE_INTEGER;
      if (ea !== eb) return ea - eb;
    }
    if (a.priority !== b.priority) return a.priority - b.priority;
    const ea = a.expires_at_ms ?? Number.MAX_SAFE_INTEGER;
    const eb = b.expires_at_ms ?? Number.MAX_SAFE_INTEGER;
    if (ea !== eb) return ea - eb;
    if (a.created_at_ms !== b.created_at_ms) return a.created_at_ms - b.created_at_ms;
    return a.id.localeCompare(b.id);
  });
}

function lotEligible(lot: LotRow, model: string, operation: string, nowMs: number): boolean {
  if (lot.available_atoms <= 0n) return false;
  if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) return false;
  if (lot.models.length > 0 && !lot.models.includes(model)) return false;
  if (lot.operations.length > 0 && !lot.operations.includes(operation)) return false;
  return true;
}

function preferredCell(seed: string): number {
  let h = 2166136261;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0) % CELL_COUNT;
}

function cellKey(accountId: string, currency: string, cell: number): string {
  return `${accountId}:${currency}:${cell}`;
}

function walletKey(projectId: string, currency: string): string {
  return `${projectId}:${currency}`;
}

function supplierKey(accountId: string, currency: string): string {
  return `${accountId}:${currency}`;
}

function toWalletRow(r: Record<string, unknown>): WalletRow {
  return {
    id: String(r.id),
    project_id: String(r.project_id),
    currency: String(r.currency),
    policy: r.policy as WalletPolicy,
    available_atoms: BigInt(String(r.available_atoms)),
    reserved_atoms: BigInt(String(r.reserved_atoms)),
    spent_atoms: BigInt(String(r.spent_atoms)),
    expired_atoms: BigInt(String(r.expired_atoms)),
    debt_atoms: BigInt(String(r.debt_atoms)),
    credit_limit_atoms: BigInt(String(r.credit_limit_atoms)),
    credit_reserved_atoms: BigInt(String(r.credit_reserved_atoms)),
    credit_policy_version: BigInt(String(r.credit_policy_version)),
    manual_frozen: Boolean(r.manual_frozen),
    evidence_disputed: Boolean(r.evidence_disputed),
    freeze_reason: r.freeze_reason ? String(r.freeze_reason) : null,
    dirty: false,
  };
}

function toLotRow(r: Record<string, unknown>): LotRow {
  return {
    id: String(r.id),
    wallet_id: String(r.wallet_id),
    kind: r.kind as "CASH" | "GRANT",
    priority: Number(r.priority),
    models: (r.models ?? []) as string[],
    operations: (r.operations ?? []) as string[],
    expires_at_ms: r.expires_at ? Date.parse(String(r.expires_at)) : null,
    created_at_ms: Date.parse(String(r.created_at)),
    issued_atoms: BigInt(String(r.issued_atoms)),
    available_atoms: BigInt(String(r.available_atoms)),
    reserved_atoms: BigInt(String(r.reserved_atoms)),
    spent_atoms: BigInt(String(r.spent_atoms)),
    expired_atoms: BigInt(String(r.expired_atoms)),
    dirty: false,
  };
}

function toCellRow(r: Record<string, unknown>): SupplierCellRow {
  return {
    account_id: String(r.account_id),
    currency: String(r.currency),
    cell: Number(r.cell),
    balance_atoms: BigInt(String(r.balance_atoms)),
    reserved_atoms: BigInt(String(r.reserved_atoms)),
    spent_atoms: BigInt(String(r.spent_atoms)),
    revision: BigInt(String(r.revision)),
    dirty: false,
  };
}

async function insertJournalLines(tx: SQL, lines: JournalLine[]): Promise<void> {
  if (!lines.length) return;
  const journalIds = lines.map((l) => l.journal_id);
  const ordinals = lines.map((l) => l.ordinal);
  const sides = lines.map((l) => l.side);
  const entities = lines.map((l) => l.entity_id);
  const requests = lines.map((l) => l.request_id);
  const references = lines.map((l) => l.reference);
  const kinds = lines.map((l) => l.kind);
  const currencies = lines.map((l) => l.currency);
  const accounts = lines.map((l) => l.account);
  const atoms = lines.map((l) => l.atoms.toString());
  await tx`
    INSERT INTO public.ledger_entries (journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms)
    SELECT * FROM unnest(
      ${journalIds}::uuid[],
      ${ordinals}::smallint[],
      ${sides}::text[],
      ${entities}::uuid[],
      ${requests}::uuid[],
      ${references}::text[],
      ${kinds}::text[],
      ${currencies}::text[],
      ${accounts}::text[],
      ${atoms}::numeric[]
    )
  `;
}

async function flushDirtyWallets(tx: SQL, walletRows: Iterable<WalletRow>): Promise<void> {
  const dirty = [...walletRows].filter((w) => w.dirty);
  if (!dirty.length) return;
  await tx`
    UPDATE public.wallets AS w SET
      available_atoms = u.available_atoms,
      reserved_atoms = u.reserved_atoms,
      spent_atoms = u.spent_atoms,
      expired_atoms = u.expired_atoms,
      debt_atoms = u.debt_atoms,
      credit_reserved_atoms = u.credit_reserved_atoms,
      evidence_disputed = u.evidence_disputed,
      freeze_reason = u.freeze_reason
    FROM unnest(
      ${dirty.map((w) => w.id)}::uuid[],
      ${dirty.map((w) => w.available_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.reserved_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.spent_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.expired_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.debt_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.credit_reserved_atoms.toString())}::numeric[],
      ${dirty.map((w) => w.evidence_disputed)}::boolean[],
      ${dirty.map((w) => w.freeze_reason)}::text[]
    ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms, debt_atoms, credit_reserved_atoms, evidence_disputed, freeze_reason)
    WHERE w.id = u.id
  `;
}

async function flushDirtyLots(tx: SQL, lots: Iterable<LotRow>): Promise<void> {
  const dirty = [...lots].filter((l) => l.dirty);
  if (!dirty.length) return;
  await tx`
    UPDATE public.wallet_lots AS l SET
      available_atoms = u.available_atoms,
      reserved_atoms = u.reserved_atoms,
      spent_atoms = u.spent_atoms,
      expired_atoms = u.expired_atoms
    FROM unnest(
      ${dirty.map((l) => l.id)}::uuid[],
      ${dirty.map((l) => l.available_atoms.toString())}::numeric[],
      ${dirty.map((l) => l.reserved_atoms.toString())}::numeric[],
      ${dirty.map((l) => l.spent_atoms.toString())}::numeric[],
      ${dirty.map((l) => l.expired_atoms.toString())}::numeric[]
    ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms)
    WHERE l.id = u.id
  `;
}

async function flushDirtyCells(tx: SQL, cells: Iterable<SupplierCellRow>): Promise<void> {
  const dirty = [...cells].filter((c) => c.dirty);
  if (!dirty.length) return;
  await tx`
    UPDATE public.supplier_cells AS c SET
      balance_atoms = u.balance_atoms,
      reserved_atoms = u.reserved_atoms,
      spent_atoms = u.spent_atoms,
      revision = u.revision
    FROM unnest(
      ${dirty.map((c) => c.account_id)}::uuid[],
      ${dirty.map((c) => c.currency)}::text[],
      ${dirty.map((c) => c.cell)}::smallint[],
      ${dirty.map((c) => c.balance_atoms.toString())}::numeric[],
      ${dirty.map((c) => c.reserved_atoms.toString())}::numeric[],
      ${dirty.map((c) => c.spent_atoms.toString())}::numeric[],
      ${dirty.map((c) => c.revision.toString())}::numeric[]
    ) AS u(account_id, currency, cell, balance_atoms, reserved_atoms, spent_atoms, revision)
    WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
  `;
}

function expireLotsInMemory(
  wallet: WalletRow,
  lots: LotRow[],
  nowMs: number,
  journals: JournalLine[],
): bigint {
  let expiredTotal = 0n;
  for (const lot of lots) {
    if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs && lot.available_atoms > 0n) {
      const amt = lot.available_atoms;
      lot.available_atoms = 0n;
      lot.expired_atoms += amt;
      lot.dirty = true;
      wallet.available_atoms -= amt;
      wallet.expired_atoms += amt;
      wallet.dirty = true;
      expiredTotal += amt;
      const jid = crypto.randomUUID();
      journals.push(
        {
          journal_id: jid,
          ordinal: 0,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: null,
          reference: `expire:${lot.id}:${jid}`,
          kind: "EXPIRE",
          currency: wallet.currency,
          account: `lot:${lot.id}:available`,
          atoms: -amt,
        },
        {
          journal_id: jid,
          ordinal: 1,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: null,
          reference: `expire:${lot.id}:${jid}`,
          kind: "EXPIRE",
          currency: wallet.currency,
          account: `lot:${lot.id}:expired`,
          atoms: amt,
        },
      );
    }
  }
  return expiredTotal;
}

class MicroBatchQueue<I, O> {
  private pending: { input: I; resolve: (value: O) => void; reject: (err: unknown) => void }[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private inflight = 0;
  private drainWaiters: (() => void)[] = [];

  constructor(
    private readonly executeBatch: (items: I[]) => Promise<O[]>,
    private readonly maxBatch: number,
    private readonly waitMs: number,
    private readonly maxConcurrency: number,
  ) {}

  enqueue(input: I): Promise<O> {
    return new Promise<O>((resolve, reject) => {
      this.pending.push({ input, resolve, reject });
      if (this.pending.length >= this.maxBatch) {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = undefined;
        }
        this.pump();
      } else if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = undefined;
          this.pump();
        }, this.waitMs);
      }
    });
  }

  private checkDrained(): void {
    if (this.pending.length === 0 && this.inflight === 0 && this.drainWaiters.length > 0) {
      const waiters = this.drainWaiters.splice(0, this.drainWaiters.length);
      for (const resolve of waiters) resolve();
    }
  }

  private pump(): void {
    while (this.pending.length > 0 && this.inflight < this.maxConcurrency) {
      const slice = this.pending.splice(0, this.maxBatch);
      this.inflight++;
      void this.executeBatch(slice.map((s) => s.input))
        .then(
          (results) => {
            for (let i = 0; i < slice.length; i++) slice[i]!.resolve(results[i]!);
          },
          (error) => {
            for (const entry of slice) entry.reject(error);
          },
        )
        .finally(() => {
          this.inflight--;
          if (this.pending.length > 0) this.pump();
          else this.checkDrained();
        });
    }
  }

  async drain(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    this.pump();
    if (this.pending.length === 0 && this.inflight === 0) return;
    await new Promise<void>((resolve) => {
      this.drainWaiters.push(resolve);
    });
  }
}

export type LedgerOptions = {
  batchWindowMs?: number;
  maxBatchSize?: number;
  maxBatchConcurrency?: number;
  nodeId?: string;
  enableLeases?: boolean;
  leaseIdleReleaseMs?: number;
  leaseTtlMs?: number;
};

export function createLedgerEngine(db: SQL, options: LedgerOptions = {}) {
  const orm = createDrizzle(db);
  const batchWindowMs = options.batchWindowMs ?? 2;
  const maxBatchSize = options.maxBatchSize ?? 128;
  const maxBatchConcurrency = options.maxBatchConcurrency ?? 4;
  const nodeId = options.nodeId ?? crypto.randomUUID();
  const leaseEnabled = options.enableLeases ?? true;
  const leaseIdleReleaseMs = options.leaseIdleReleaseMs ?? LEASE_IDLE_RELEASE_MS;
  const leaseTtlMs = options.leaseTtlMs ?? LEASE_TTL_MS;

  let batchCellCursor = Math.floor(Math.random() * CELL_COUNT);
  let draining = false;

  const activeWalletLeases = new Map<string, ActiveWalletLease>();
  const walletLeasesById = new Map<string, ActiveWalletLease>();
  const closingWalletLeases = new Set<ActiveWalletLease>();

  const activeSupplierLeases = new Map<string, ActiveSupplierLease>();
  const supplierLeasesById = new Map<string, ActiveSupplierLease>();
  const closingSupplierLeases = new Set<ActiveSupplierLease>();

  const leasedHolds = new Map<string, LeasedHoldMeta>();
  const recentWalletHits = new Map<string, { count: number; windowStart: number }>();
  const recentSupplierHits = new Map<string, { count: number; windowStart: number }>();
  const releasingLeaseIds = new Set<string>();

  function recordHit(
    map: Map<string, { count: number; windowStart: number }>,
    key: string,
    nowMs: number,
  ): number {
    const prev = map.get(key);
    if (!prev || nowMs - prev.windowStart > 2000) {
      map.set(key, { count: 1, windowStart: nowMs });
      return 1;
    }
    prev.count++;
    return prev.count;
  }

  function retireWalletLease(lease: ActiveWalletLease): void {
    lease.closing = true;
    const wk = walletKey(lease.projectId, lease.currency);
    if (activeWalletLeases.get(wk) === lease) {
      activeWalletLeases.delete(wk);
    }
    closingWalletLeases.add(lease);
  }

  function retireSupplierLease(lease: ActiveSupplierLease): void {
    lease.closing = true;
    const sk = supplierKey(lease.accountId, lease.currency);
    if (activeSupplierLeases.get(sk) === lease) {
      activeSupplierLeases.delete(sk);
    }
    closingSupplierLeases.add(lease);
  }

  function retireWalletLeaseByKey(projectId: string, currency: string): void {
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
    leasedHolds.delete(requestId);
    if (detachedWalletLeaseId) {
      const wl = walletLeasesById.get(detachedWalletLeaseId);
      if (wl) {
        if (wl.inFlightHolds > 0) wl.inFlightHolds--;
        wl.remainingInPg = wl.remainingInPg >= heldCustomer ? wl.remainingInPg - heldCustomer : 0n;
      }
    }
    if (detachedSupplierLeaseId) {
      const sl = supplierLeasesById.get(detachedSupplierLeaseId);
      if (sl) {
        if (sl.inFlightHolds > 0) sl.inFlightHolds--;
        sl.remainingInPg =
          sl.remainingInPg >= detachedSupplierAtoms ? sl.remainingInPg - detachedSupplierAtoms : 0n;
      }
    }
  }

  async function closeWalletLeaseInPg(lease: ActiveWalletLease): Promise<void> {
    if (releasingLeaseIds.has(lease.leaseId)) return;
    releasingLeaseIds.add(lease.leaseId);
    try {
      await db.begin(async (tx) => {
        const openHolds = await tx`
          SELECT request_id, held_atoms::text
          FROM public.billing_holds
          WHERE wallet_lease_id = ${lease.leaseId} AND state IN ('HELD', 'UNKNOWN')
          ORDER BY request_id
          FOR NO KEY UPDATE
        `;
        const wlRows = await tx`
          SELECT id, wallet_id, currency, lot_allocations, remaining_atoms::text, state
          FROM public.wallet_leases
          WHERE id = ${lease.leaseId}
          FOR NO KEY UPDATE
        `;
        if (!wlRows.length || wlRows[0]!.state !== "ACTIVE") return;
        const wl = wlRows[0]!;
        let leaseLots = (wl.lot_allocations ?? []) as LeaseLotAllocation[];
        let remainingAtoms = BigInt(String(wl.remaining_atoms));

        for (const oh of openHolds) {
          const amt = BigInt(String(oh.held_atoms));
          const { carved, remaining, taken } = carveFromLeaseLots(leaseLots, amt);
          leaseLots = remaining;
          remainingAtoms -= taken;
          await tx`
            UPDATE public.billing_holds
            SET lot_allocations = ${carved}::jsonb, wallet_lease_id = NULL
            WHERE request_id = ${oh.request_id}
          `;
        }

        const nowMs = Date.now();
        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${wl.wallet_id} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const lotIds = [...new Set(leaseLots.map((l) => l.lot_id))].sort();
        const lotRowsRaw = lotIds.length
          ? await tx`
              SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
                issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
              FROM public.wallet_lots WHERE id = ANY(${lotIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE
            `
          : [];
        const lotsById = new Map<string, LotRow>();
        for (const r of lotRowsRaw) {
          const l = toLotRow(r);
          lotsById.set(l.id, l);
        }

        const journals: JournalLine[] = [];
        let returnedAvail = 0n;
        let expiredAmt = 0n;
        for (const entry of leaseLots) {
          const amt = BigInt(entry.amount_atoms);
          if (amt <= 0n) continue;
          const lot = lotsById.get(entry.lot_id);
          if (!lot) continue;
          lot.reserved_atoms -= amt;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
            lot.expired_atoms += amt;
            expiredAmt += amt;
            const jid = crypto.randomUUID();
            journals.push(
              {
                journal_id: jid,
                ordinal: 0,
                side: "CUSTOMER",
                entity_id: wallet.id,
                request_id: null,
                reference: `lease_expire:${lease.leaseId}:${lot.id}`,
                kind: "EXPIRE",
                currency: wallet.currency,
                account: `lot:${lot.id}:available`,
                atoms: -amt,
              },
              {
                journal_id: jid,
                ordinal: 1,
                side: "CUSTOMER",
                entity_id: wallet.id,
                request_id: null,
                reference: `lease_expire:${lease.leaseId}:${lot.id}`,
                kind: "EXPIRE",
                currency: wallet.currency,
                account: `lot:${lot.id}:expired`,
                atoms: amt,
              },
            );
          } else {
            lot.available_atoms += amt;
            returnedAvail += amt;
          }
          lot.dirty = true;
        }

        if (remainingAtoms > 0n) {
          wallet.reserved_atoms -= remainingAtoms;
          wallet.available_atoms += returnedAvail;
          wallet.expired_atoms += expiredAmt;
          wallet.freeze_reason = computeFreezeReason(wallet);
          wallet.dirty = true;
        }

        await flushDirtyWallets(tx, [wallet]);
        await flushDirtyLots(tx, lotsById.values());
        await insertJournalLines(tx, journals);
        await tx`
          UPDATE public.wallet_leases
          SET state = 'CLOSED', remaining_atoms = 0, lot_allocations = '[]'::jsonb, updated_at = now()
          WHERE id = ${lease.leaseId}
        `;
      });
      closingWalletLeases.delete(lease);
      walletLeasesById.delete(lease.leaseId);
    } finally {
      releasingLeaseIds.delete(lease.leaseId);
    }
  }

  async function closeSupplierLeaseInPg(lease: ActiveSupplierLease): Promise<void> {
    if (releasingLeaseIds.has(lease.leaseId)) return;
    releasingLeaseIds.add(lease.leaseId);
    try {
      await db.begin(async (tx) => {
        const openHolds = await tx`
          SELECT request_id, supplier_holds
          FROM public.billing_holds
          WHERE supplier_lease_id = ${lease.leaseId} AND state IN ('HELD', 'UNKNOWN')
          ORDER BY request_id
          FOR NO KEY UPDATE
        `;
        const slRows = await tx`
          SELECT id, account_id, currency, cell, remaining_atoms::text, state
          FROM public.supplier_leases
          WHERE id = ${lease.leaseId}
          FOR NO KEY UPDATE
        `;
        if (!slRows.length || slRows[0]!.state !== "ACTIVE") return;
        const sl = slRows[0]!;
        let remainingAtoms = BigInt(String(sl.remaining_atoms));
        for (const oh of openHolds) {
          const holds = (oh.supplier_holds ?? []) as SupplierHoldEntry[];
          const heldSum = holds.reduce((acc, s) => acc + BigInt(s.held_atoms), 0n);
          remainingAtoms = remainingAtoms >= heldSum ? remainingAtoms - heldSum : 0n;
          await tx`
            UPDATE public.billing_holds SET supplier_lease_id = NULL WHERE request_id = ${oh.request_id}
          `;
        }
        if (remainingAtoms > 0n) {
          await tx`
            UPDATE public.supplier_cells
            SET reserved_atoms = reserved_atoms - ${remainingAtoms.toString()}::numeric,
                revision = revision + 1
            WHERE account_id = ${sl.account_id} AND currency = ${sl.currency} AND cell = ${Number(sl.cell)}
          `;
        }
        await tx`
          UPDATE public.supplier_leases
          SET state = 'CLOSED', remaining_atoms = 0, updated_at = now()
          WHERE id = ${lease.leaseId}
        `;
      });
      closingSupplierLeases.delete(lease);
      supplierLeasesById.delete(lease.leaseId);
    } finally {
      releasingLeaseIds.delete(lease.leaseId);
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
      const nextExp = new Date(nowMs + leaseTtlMs).toISOString();
      await db`
        UPDATE public.wallet_leases AS l
        SET expires_at = ${nextExp}::timestamptz, updated_at = now()
        FROM public.wallets w
        WHERE l.id = ANY(${wlToRenew}::uuid[])
          AND l.state = 'ACTIVE'
          AND w.id = l.wallet_id
          AND w.freeze_reason IS NULL
      `.catch(() => undefined);
      for (const id of wlToRenew) {
        const wl = walletLeasesById.get(id);
        if (wl) wl.expiresAtMs = nowMs + leaseTtlMs;
      }
    }
    if (slToRenew.length > 0) {
      const nextExp = new Date(nowMs + leaseTtlMs).toISOString();
      await db`
        UPDATE public.supplier_leases
        SET expires_at = ${nextExp}::timestamptz, updated_at = now()
        WHERE id = ANY(${slToRenew}::uuid[]) AND state = 'ACTIVE'
      `.catch(() => undefined);
      for (const id of slToRenew) {
        const sl = supplierLeasesById.get(id);
        if (sl) sl.expiresAtMs = nowMs + leaseTtlMs;
      }
    }

    for (const wl of wlToClose) {
      await closeWalletLeaseInPg(wl).catch(() => undefined);
    }
    for (const sl of slToClose) {
      await closeSupplierLeaseInPg(sl).catch(() => undefined);
    }
  }

  let maintenanceRunning = false;
  let maintenanceTimer: ReturnType<typeof setInterval> | null = null;
  if (leaseEnabled) {
    maintenanceTimer = setInterval(
      () => {
        if (maintenanceRunning || draining) return;
        maintenanceRunning = true;
        void maintainLeases(false).finally(() => {
          maintenanceRunning = false;
        });
      },
      Math.max(100, Math.min(leaseIdleReleaseMs, 500)),
    );
    maintenanceTimer.unref();
  }

  async function runFastHoldBatch(items: FastHoldRequest[]): Promise<AcquireHoldResult[]> {
    const reqIds = items.map((x) => x.input.request_id);
    const projIds = items.map((x) => x.input.project_id);
    const walletIds = items.map((x) => x.walletLease.walletId);
    const ops = items.map((x) => x.input.operation);
    const models = items.map((x) => x.input.model);
    const fps = items.map((x) => x.input.fingerprint);
    const helds = items.map((x) => x.customerAtoms.toString());
    const supHoldsJson = items.map((x) =>
      JSON.stringify([
        {
          attempt_id: x.input.attempt_id,
          account_id: x.input.supplier_account_id,
          channel_id: x.input.supplier_channel_id,
          currency: x.input.supplier_currency,
          cell: x.supplierLease.cell,
          held_atoms: x.supplierAtoms.toString(),
          state: "HELD",
        },
      ]),
    );
    const wLeaseIds = items.map((x) => x.walletLease.leaseId);
    const sLeaseIds = items.map((x) => x.supplierLease.leaseId);
    const expAts = items.map((x) => x.input.expires_at);

    try {
      await db`
        INSERT INTO public.billing_holds (
          request_id, project_id, wallet_id, operation, model, idempotency_key, fingerprint,
          held_atoms, credit_atoms, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state, expires_at
        )
        SELECT
          u.request_id, u.project_id, u.wallet_id, u.operation, u.model, NULL, u.fingerprint,
          u.held_atoms, 0, '[]'::jsonb, u.supplier_holds::jsonb, u.wallet_lease_id, u.supplier_lease_id, 'HELD', u.expires_at
        FROM unnest(
          ${reqIds}::uuid[],
          ${projIds}::uuid[],
          ${walletIds}::uuid[],
          ${ops}::text[],
          ${models}::text[],
          ${fps}::text[],
          ${helds}::numeric[],
          ${supHoldsJson}::text[],
          ${wLeaseIds}::uuid[],
          ${sLeaseIds}::uuid[],
          ${expAts}::timestamptz[]
        ) AS u(
          request_id, project_id, wallet_id, operation, model, fingerprint,
          held_atoms, supplier_holds, wallet_lease_id, supplier_lease_id, expires_at
        )
      `;
    } catch (err) {
      for (const x of items) {
        x.walletLease.freeInMemory += x.customerAtoms;
        if (x.walletLease.inFlightHolds > 0) x.walletLease.inFlightHolds--;
        x.supplierLease.freeInMemory += x.supplierAtoms;
        if (x.supplierLease.inFlightHolds > 0) x.supplierLease.inFlightHolds--;
      }
      throw err;
    }

    const out: AcquireHoldResult[] = [];
    for (const x of items) {
      leasedHolds.set(x.input.request_id, {
        requestId: x.input.request_id,
        walletLeaseId: x.walletLease.leaseId,
        supplierLeaseId: x.supplierLease.leaseId,
        walletId: x.walletLease.walletId,
        projectId: x.input.project_id,
        currency: x.input.currency,
        operation: x.input.operation,
        model: x.input.model,
        heldCustomerAtoms: x.customerAtoms,
        heldSupplierAtoms: x.supplierAtoms,
        attemptId: x.input.attempt_id,
        accountId: x.input.supplier_account_id,
        channelId: x.input.supplier_channel_id,
        supplierCurrency: x.input.supplier_currency,
        cell: x.supplierLease.cell,
      });
      out.push({
        ok: true,
        replayed: false,
        request_id: x.input.request_id,
        wallet_id: x.walletLease.walletId,
        held_atoms: x.customerAtoms.toString(),
        credit_atoms: "0",
        supplier_cell: x.supplierLease.cell,
      });
    }
    return out;
  }

  async function runAcquireBatch(items: AcquireHoldInput[]): Promise<AcquireHoldResult[]> {
    const results = Array.from<AcquireHoldResult>({ length: items.length });
    const parsed = items.map((item) => ({
      item,
      customerAtoms: parseAtoms(item.customer_atoms),
      supplierAtoms: parseAtoms(item.supplier_atoms),
    }));

    const projectIds = [...new Set(items.map((i) => i.project_id))];
    const supplierAccounts = [...new Set(items.map((i) => i.supplier_account_id))];
    const idempotencyKeys = items
      .filter((i) => i.idempotency_key !== null)
      .map((i) => i.idempotency_key!);

    const createdWalletLeases: ActiveWalletLease[] = [];
    const createdSupplierLeases: ActiveSupplierLease[] = [];

    await db.begin(async (tx) => {
      const nowMs = Date.now();
      const lockedWallets = await tx`
        SELECT id, project_id, currency, policy,
          available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
          debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
          credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
        FROM public.wallets
        WHERE project_id = ANY(${projectIds}::uuid[])
        ORDER BY id
        FOR NO KEY UPDATE
      `;
      const walletsByKey = new Map<string, WalletRow>();
      const walletsById = new Map<string, WalletRow>();
      for (const r of lockedWallets) {
        const w = toWalletRow(r);
        walletsByKey.set(walletKey(w.project_id, w.currency), w);
        walletsById.set(w.id, w);
      }

      const walletIds = [...walletsById.keys()];
      const lotRowsRaw = walletIds.length
        ? await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots
            WHERE wallet_id = ANY(${walletIds}::uuid[]) AND available_atoms > 0
            ORDER BY id
            FOR NO KEY UPDATE
          `
        : [];
      const lotsByWallet = new Map<string, LotRow[]>();
      const allLots: LotRow[] = [];
      for (const r of lotRowsRaw) {
        const l = toLotRow(r);
        allLots.push(l);
        const arr = lotsByWallet.get(l.wallet_id);
        if (arr) arr.push(l);
        else lotsByWallet.set(l.wallet_id, [l]);
      }
      for (const [wid, arr] of lotsByWallet) {
        const w = walletsById.get(wid)!;
        lotsByWallet.set(wid, sortLots(arr, w.policy));
      }

      const accountRows = supplierAccounts.length
        ? await tx`
            SELECT id, state FROM public.upstream_accounts
            WHERE id = ANY(${supplierAccounts}::uuid[])
            ORDER BY id
            FOR SHARE
          `
        : [];
      const activeAccounts = new Set<string>();
      for (const r of accountRows) {
        if (r.state === "ACTIVE") activeAccounts.add(String(r.id));
      }

      const targetCell = batchCellCursor++ % CELL_COUNT;
      const cellRowsRaw = supplierAccounts.length
        ? await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ANY(${supplierAccounts}::uuid[]) AND cell = ${targetCell}
            ORDER BY account_id, currency, cell
            FOR NO KEY UPDATE
          `
        : [];
      const cellsByAcctCur = new Map<string, SupplierCellRow[]>();
      const allCells: SupplierCellRow[] = [];
      const expandedAcctCur = new Set<string>();
      for (const r of cellRowsRaw) {
        const c = toCellRow(r);
        allCells.push(c);
        const k = supplierKey(c.account_id, c.currency);
        const list = cellsByAcctCur.get(k);
        if (list) list.push(c);
        else cellsByAcctCur.set(k, [c]);
      }

      const existingIdem = new Map<
        string,
        {
          request_id: string;
          wallet_id: string;
          fingerprint: string;
          held_atoms: string;
          credit_atoms: string;
          state: string;
          supplier_holds: SupplierHoldEntry[];
        }
      >();
      if (idempotencyKeys.length > 0) {
        const rows = await tx`
          SELECT request_id, project_id, operation, idempotency_key, wallet_id, fingerprint,
            held_atoms::text, credit_atoms::text, state, supplier_holds
          FROM public.billing_holds
          WHERE project_id = ANY(${projectIds}::uuid[])
            AND idempotency_key = ANY(${idempotencyKeys}::text[])
        `;
        for (const r of rows) {
          existingIdem.set(`${r.project_id}:${r.operation}:${r.idempotency_key}`, {
            request_id: String(r.request_id),
            wallet_id: String(r.wallet_id),
            fingerprint: String(r.fingerprint),
            held_atoms: String(r.held_atoms),
            credit_atoms: String(r.credit_atoms),
            state: String(r.state),
            supplier_holds: (r.supplier_holds ?? []) as SupplierHoldEntry[],
          });
        }
      }

      const journals: JournalLine[] = [];
      for (const w of walletsById.values()) {
        const wLots = lotsByWallet.get(w.id);
        if (wLots) expireLotsInMemory(w, wLots, nowMs, journals);
      }

      const newHolds: {
        request_id: string;
        project_id: string;
        wallet_id: string;
        operation: string;
        model: string;
        idempotency_key: string | null;
        fingerprint: string;
        held_atoms: string;
        credit_atoms: string;
        lot_allocations: HoldLotAllocation[];
        supplier_holds: SupplierHoldEntry[];
        expires_at: string;
      }[] = [];

      const walletLeaseDemand = new Map<
        string,
        { wallet: WalletRow; maxQuote: bigint; count: number }
      >();
      const supplierLeaseDemand = new Map<
        string,
        { cell: SupplierCellRow; maxQuote: bigint; count: number }
      >();

      for (let i = 0; i < parsed.length; i++) {
        const { item, customerAtoms, supplierAtoms } = parsed[i]!;
        if (customerAtoms === null || supplierAtoms === null) {
          results[i] = { ok: false, code: "invalid_amount", message: "Invalid reservation atoms" };
          continue;
        }
        if (item.idempotency_key !== null) {
          const ik = `${item.project_id}:${item.operation}:${item.idempotency_key}`;
          const prev = existingIdem.get(ik);
          if (prev) {
            if (prev.fingerprint !== item.fingerprint) {
              results[i] = {
                ok: false,
                code: "idempotency_conflict",
                message: "Idempotency key reused with different request fingerprint",
              };
            } else {
              results[i] = {
                ok: true,
                replayed: true,
                request_id: prev.request_id,
                wallet_id: prev.wallet_id,
                held_atoms: prev.held_atoms,
                credit_atoms: prev.credit_atoms,
                supplier_cell: prev.supplier_holds[0]?.cell ?? 0,
                existing_state: prev.state,
              };
            }
            continue;
          }
        }

        const wk = walletKey(item.project_id, item.currency);
        const wallet = walletsByKey.get(wk);
        if (!wallet) {
          results[i] = { ok: false, code: "wallet_not_found", message: "Project wallet not found" };
          continue;
        }
        const freeze = wallet.freeze_reason ?? computeFreezeReason(wallet);
        if (freeze !== null) {
          results[i] = { ok: false, code: "wallet_frozen", message: `Wallet frozen: ${freeze}` };
          continue;
        }

        if (!activeAccounts.has(item.supplier_account_id)) {
          results[i] = {
            ok: false,
            code: "supplier_unavailable",
            message: "Supplier account is not active",
          };
          continue;
        }

        const acctCurKey = supplierKey(item.supplier_account_id, item.supplier_currency);
        let acctCells = cellsByAcctCur.get(acctCurKey) ?? [];
        const pickCell = (list: SupplierCellRow[]): SupplierCellRow | undefined => {
          if (list.length === 0) return undefined;
          const start = preferredCell(item.attempt_id);
          for (let offset = 0; offset < list.length; offset++) {
            const candidate = list[(start + offset) % list.length]!;
            if (candidate.balance_atoms - candidate.reserved_atoms >= supplierAtoms) {
              return candidate;
            }
          }
          return undefined;
        };
        let chosenCell = pickCell(acctCells);
        if (!chosenCell && !expandedAcctCur.has(acctCurKey)) {
          expandedAcctCur.add(acctCurKey);
          const extraRows = await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ${item.supplier_account_id} AND currency = ${item.supplier_currency} AND cell <> ${targetCell}
            ORDER BY cell
            FOR NO KEY UPDATE SKIP LOCKED
          `;
          if (extraRows.length > 0) {
            const nextList = [...acctCells];
            for (const r of extraRows) {
              const c = toCellRow(r);
              allCells.push(c);
              nextList.push(c);
            }
            cellsByAcctCur.set(acctCurKey, nextList);
            acctCells = nextList;
            chosenCell = pickCell(acctCells);
          }
        }
        if (!chosenCell) {
          results[i] = {
            ok: false,
            code: "supplier_insufficient_balance",
            message: "Supplier funding cells lack sufficient balance",
          };
          continue;
        }

        const walletLots = lotsByWallet.get(wallet.id) ?? [];
        let remaining = customerAtoms;
        const plan: { lot: LotRow; take: bigint }[] = [];
        for (const lot of walletLots) {
          if (remaining === 0n) break;
          if (!lotEligible(lot, item.model, item.operation, nowMs)) continue;
          const take = remaining < lot.available_atoms ? remaining : lot.available_atoms;
          remaining -= take;
          plan.push({ lot, take });
        }
        const creditPart = remaining;
        const fundedPart = customerAtoms - creditPart;
        if (
          wallet.debt_atoms + wallet.credit_reserved_atoms + creditPart >
          wallet.credit_limit_atoms
        ) {
          results[i] = {
            ok: false,
            code: "insufficient_balance",
            message: "Insufficient wallet balance and credit limit",
          };
          continue;
        }
        if (
          wallet.reserved_atoms + fundedPart > MAX_ATOMS ||
          wallet.credit_reserved_atoms + creditPart > MAX_ATOMS
        ) {
          results[i] = {
            ok: false,
            code: "invalid_amount",
            message: "Wallet reservation exceeds NUMERIC(38,0)",
          };
          continue;
        }

        const allocations: HoldLotAllocation[] = [];
        for (const step of plan) {
          step.lot.available_atoms -= step.take;
          step.lot.reserved_atoms += step.take;
          step.lot.dirty = true;
          allocations.push({ lot_id: step.lot.id, amount_atoms: step.take.toString() });
        }
        wallet.available_atoms -= fundedPart;
        wallet.reserved_atoms += fundedPart;
        wallet.credit_reserved_atoms += creditPart;
        wallet.dirty = true;

        chosenCell.reserved_atoms += supplierAtoms;
        chosenCell.revision += 1n;
        chosenCell.dirty = true;

        const supplierHold: SupplierHoldEntry = {
          attempt_id: item.attempt_id,
          account_id: item.supplier_account_id,
          channel_id: item.supplier_channel_id,
          currency: item.supplier_currency,
          cell: chosenCell.cell,
          held_atoms: supplierAtoms.toString(),
          state: "HELD",
        };

        newHolds.push({
          request_id: item.request_id,
          project_id: item.project_id,
          wallet_id: wallet.id,
          operation: item.operation,
          model: item.model,
          idempotency_key: item.idempotency_key,
          fingerprint: item.fingerprint,
          held_atoms: customerAtoms.toString(),
          credit_atoms: creditPart.toString(),
          lot_allocations: allocations,
          supplier_holds: [supplierHold],
          expires_at: item.expires_at,
        });

        if (item.idempotency_key !== null) {
          existingIdem.set(`${item.project_id}:${item.operation}:${item.idempotency_key}`, {
            request_id: item.request_id,
            wallet_id: wallet.id,
            fingerprint: item.fingerprint,
            held_atoms: customerAtoms.toString(),
            credit_atoms: creditPart.toString(),
            state: "HELD",
            supplier_holds: [supplierHold],
          });
        } else if (leaseEnabled && !draining && creditPart === 0n && customerAtoms > 0n) {
          const wHits = recordHit(recentWalletHits, wk, nowMs);
          if (!activeWalletLeases.has(wk)) {
            const d = walletLeaseDemand.get(wk);
            if (d) {
              if (customerAtoms > d.maxQuote) d.maxQuote = customerAtoms;
              d.count++;
            } else {
              walletLeaseDemand.set(wk, { wallet, maxQuote: customerAtoms, count: 1 });
            }
          }
          if (wHits >= 16 && !activeWalletLeases.has(wk)) {
            const d = walletLeaseDemand.get(wk);
            if (d && d.count < 4) d.count = 4;
          }
          if (supplierAtoms > 0n) {
            const sHits = recordHit(recentSupplierHits, acctCurKey, nowMs);
            if (!activeSupplierLeases.has(acctCurKey)) {
              const sd = supplierLeaseDemand.get(acctCurKey);
              if (sd) {
                if (supplierAtoms > sd.maxQuote) sd.maxQuote = supplierAtoms;
                sd.count++;
              } else {
                supplierLeaseDemand.set(acctCurKey, {
                  cell: chosenCell,
                  maxQuote: supplierAtoms,
                  count: 1,
                });
              }
            }
            if (sHits >= 16 && !activeSupplierLeases.has(acctCurKey)) {
              const sd = supplierLeaseDemand.get(acctCurKey);
              if (sd && sd.count < 4) sd.count = 4;
            }
          }
        }

        results[i] = {
          ok: true,
          replayed: false,
          request_id: item.request_id,
          wallet_id: wallet.id,
          held_atoms: customerAtoms.toString(),
          credit_atoms: creditPart.toString(),
          supplier_cell: chosenCell.cell,
        };
      }

      const newWalletLeaseRows: {
        id: string;
        wallet_id: string;
        currency: string;
        lot_allocations: LeaseLotAllocation[];
        remaining_atoms: string;
        earliest_lot_expires_at: string | null;
        expires_at: string;
      }[] = [];

      for (const [wk, demand] of walletLeaseDemand) {
        if (demand.count < 4 || activeWalletLeases.has(wk)) continue;
        const w = demand.wallet;
        if (w.freeze_reason !== null || w.debt_atoms > 0n || w.credit_reserved_atoms > 0n) continue;
        if (w.available_atoms < demand.maxQuote * LEASE_MIN_MULTIPLIER) continue;
        const wLots = lotsByWallet.get(w.id) ?? [];
        const activeLots = wLots.filter((l) => l.available_atoms > 0n);
        if (activeLots.length === 0) continue;
        if (activeLots.some((l) => l.models.length > 0 || l.operations.length > 0)) continue;
        const target = demand.maxQuote * LEASE_TARGET_MULTIPLIER;
        const desired = target > LEASE_MIN_WALLET_CHUNK ? target : LEASE_MIN_WALLET_CHUNK;
        const halfAvail = w.available_atoms / 2n;
        const chunk = desired < halfAvail ? desired : halfAvail;
        if (chunk < demand.maxQuote * 8n) continue;

        let rem = chunk;
        const leaseLots: LeaseLotAllocation[] = [];
        for (const lot of activeLots) {
          if (rem === 0n) break;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs + 5000) continue;
          const take = rem < lot.available_atoms ? rem : lot.available_atoms;
          if (take <= 0n) continue;
          rem -= take;
          lot.available_atoms -= take;
          lot.reserved_atoms += take;
          lot.dirty = true;
          leaseLots.push({
            lot_id: lot.id,
            amount_atoms: take.toString(),
            expires_at_ms: lot.expires_at_ms,
          });
        }
        const reserved = chunk - rem;
        if (reserved < demand.maxQuote * 8n) {
          for (const alloc of leaseLots) {
            const lot = activeLots.find((l) => l.id === alloc.lot_id)!;
            const amt = BigInt(alloc.amount_atoms);
            lot.reserved_atoms -= amt;
            lot.available_atoms += amt;
          }
          continue;
        }
        w.available_atoms -= reserved;
        w.reserved_atoms += reserved;
        w.dirty = true;
        const leaseId = crypto.randomUUID();
        const earliestMs = earliestExpiryOf(leaseLots);
        const expiresAtMs = nowMs + leaseTtlMs;
        newWalletLeaseRows.push({
          id: leaseId,
          wallet_id: w.id,
          currency: w.currency,
          lot_allocations: leaseLots,
          remaining_atoms: reserved.toString(),
          earliest_lot_expires_at: earliestMs === null ? null : new Date(earliestMs).toISOString(),
          expires_at: new Date(expiresAtMs).toISOString(),
        });
        createdWalletLeases.push({
          leaseId,
          walletId: w.id,
          projectId: w.project_id,
          currency: w.currency,
          remainingInPg: reserved,
          freeInMemory: reserved,
          earliestLotExpiresAtMs: earliestMs,
          expiresAtMs,
          inFlightHolds: 0,
          closing: false,
          lastUsedAtMs: nowMs,
        });
      }

      const newSupplierLeaseRows: {
        id: string;
        account_id: string;
        currency: string;
        cell: number;
        remaining_atoms: string;
        expires_at: string;
      }[] = [];

      const hasAnyWalletLease = activeWalletLeases.size > 0 || createdWalletLeases.length > 0;
      for (const [sk, demand] of supplierLeaseDemand) {
        if (!hasAnyWalletLease || demand.count < 4 || activeSupplierLeases.has(sk)) continue;
        const cell = demand.cell;
        const free = cell.balance_atoms - cell.reserved_atoms;
        if (free < demand.maxQuote * LEASE_MIN_MULTIPLIER) continue;
        const target = demand.maxQuote * LEASE_TARGET_MULTIPLIER;
        const desired = target > LEASE_MIN_SUPPLIER_CHUNK ? target : LEASE_MIN_SUPPLIER_CHUNK;
        const halfFree = free / 2n;
        const chunk = desired < halfFree ? desired : halfFree;
        if (chunk < demand.maxQuote * 8n) continue;
        cell.reserved_atoms += chunk;
        cell.revision += 1n;
        cell.dirty = true;
        const leaseId = crypto.randomUUID();
        const expiresAtMs = nowMs + leaseTtlMs;
        newSupplierLeaseRows.push({
          id: leaseId,
          account_id: cell.account_id,
          currency: cell.currency,
          cell: cell.cell,
          remaining_atoms: chunk.toString(),
          expires_at: new Date(expiresAtMs).toISOString(),
        });
        createdSupplierLeases.push({
          leaseId,
          accountId: cell.account_id,
          currency: cell.currency,
          cell: cell.cell,
          remainingInPg: chunk,
          freeInMemory: chunk,
          expiresAtMs,
          inFlightHolds: 0,
          closing: false,
          lastUsedAtMs: nowMs,
        });
      }

      await flushDirtyWallets(tx, walletsById.values());
      await flushDirtyLots(tx, allLots);
      await flushDirtyCells(tx, allCells);
      await insertJournalLines(tx, journals);

      if (newWalletLeaseRows.length > 0) {
        await tx`
          INSERT INTO public.wallet_leases (
            id, node_id, wallet_id, currency, lot_allocations, remaining_atoms, earliest_lot_expires_at, state, expires_at
          )
          SELECT
            u.id, ${nodeId}, u.wallet_id, u.currency, u.lot_allocations::jsonb, u.remaining_atoms,
            u.earliest_lot_expires_at, 'ACTIVE', u.expires_at
          FROM unnest(
            ${newWalletLeaseRows.map((l) => l.id)}::uuid[],
            ${newWalletLeaseRows.map((l) => l.wallet_id)}::uuid[],
            ${newWalletLeaseRows.map((l) => l.currency)}::text[],
            ${newWalletLeaseRows.map((l) => JSON.stringify(l.lot_allocations))}::text[],
            ${newWalletLeaseRows.map((l) => l.remaining_atoms)}::numeric[],
            ${newWalletLeaseRows.map((l) => l.earliest_lot_expires_at)}::timestamptz[],
            ${newWalletLeaseRows.map((l) => l.expires_at)}::timestamptz[]
          ) AS u(id, wallet_id, currency, lot_allocations, remaining_atoms, earliest_lot_expires_at, expires_at)
        `;
      }

      if (newSupplierLeaseRows.length > 0) {
        await tx`
          INSERT INTO public.supplier_leases (
            id, node_id, account_id, currency, cell, remaining_atoms, state, expires_at
          )
          SELECT
            u.id, ${nodeId}, u.account_id, u.currency, u.cell, u.remaining_atoms, 'ACTIVE', u.expires_at
          FROM unnest(
            ${newSupplierLeaseRows.map((l) => l.id)}::uuid[],
            ${newSupplierLeaseRows.map((l) => l.account_id)}::uuid[],
            ${newSupplierLeaseRows.map((l) => l.currency)}::text[],
            ${newSupplierLeaseRows.map((l) => l.cell)}::smallint[],
            ${newSupplierLeaseRows.map((l) => l.remaining_atoms)}::numeric[],
            ${newSupplierLeaseRows.map((l) => l.expires_at)}::timestamptz[]
          ) AS u(id, account_id, currency, cell, remaining_atoms, expires_at)
        `;
      }

      if (newHolds.length > 0) {
        await tx`
          INSERT INTO public.billing_holds (
            request_id, project_id, wallet_id, operation, model, idempotency_key, fingerprint,
            held_atoms, credit_atoms, lot_allocations, supplier_holds, state, expires_at
          )
          SELECT
            u.request_id, u.project_id, u.wallet_id, u.operation, u.model, u.idempotency_key, u.fingerprint,
            u.held_atoms, u.credit_atoms, u.lot_allocations::jsonb, u.supplier_holds::jsonb, 'HELD', u.expires_at
          FROM unnest(
            ${newHolds.map((h) => h.request_id)}::uuid[],
            ${newHolds.map((h) => h.project_id)}::uuid[],
            ${newHolds.map((h) => h.wallet_id)}::uuid[],
            ${newHolds.map((h) => h.operation)}::text[],
            ${newHolds.map((h) => h.model)}::text[],
            ${newHolds.map((h) => h.idempotency_key)}::text[],
            ${newHolds.map((h) => h.fingerprint)}::text[],
            ${newHolds.map((h) => h.held_atoms)}::numeric[],
            ${newHolds.map((h) => h.credit_atoms)}::numeric[],
            ${newHolds.map((h) => JSON.stringify(h.lot_allocations))}::text[],
            ${newHolds.map((h) => JSON.stringify(h.supplier_holds))}::text[],
            ${newHolds.map((h) => h.expires_at)}::timestamptz[]
          ) AS u(
            request_id, project_id, wallet_id, operation, model, idempotency_key, fingerprint,
            held_atoms, credit_atoms, lot_allocations, supplier_holds, expires_at
          )
        `;
      }
    });

    for (const wl of createdWalletLeases) {
      const wk = walletKey(wl.projectId, wl.currency);
      walletLeasesById.set(wl.leaseId, wl);
      const prev = activeWalletLeases.get(wk);
      if (prev) retireWalletLease(prev);
      activeWalletLeases.set(wk, wl);
    }
    for (const sl of createdSupplierLeases) {
      const sk = supplierKey(sl.accountId, sl.currency);
      supplierLeasesById.set(sl.leaseId, sl);
      const prev = activeSupplierLeases.get(sk);
      if (prev) retireSupplierLease(prev);
      activeSupplierLeases.set(sk, sl);
    }

    return results;
  }

  type FastCommitCandidate = {
    index: number;
    item: CommitTerminalInput;
    meta: LeasedHoldMeta;
    customerAtoms: bigint;
    supplierAtoms: bigint;
  };

  async function runFastCommitBatch(
    candidates: FastCommitCandidate[],
    results: CommitTerminalResult[],
  ): Promise<{ index: number; item: CommitTerminalInput }[]> {
    const fallback: { index: number; item: CommitTerminalInput }[] = [];
    if (candidates.length === 0) return fallback;

    const reqIds = [...new Set(candidates.map((c) => c.item.request_id))].sort();
    const wLeaseIds = [...new Set(candidates.map((c) => c.meta.walletLeaseId))].sort();
    const sLeaseIds = [...new Set(candidates.map((c) => c.meta.supplierLeaseId))].sort();
    const walletIds = [...new Set(candidates.map((c) => c.meta.walletId))].sort();

    const committed: FastCommitCandidate[] = [];
    const nowMs = Date.now();
    const nextExpIso = new Date(nowMs + leaseTtlMs).toISOString();

    await db.begin(async (tx) => {
      const holdRows = await tx`
        SELECT request_id, state, wallet_lease_id, supplier_lease_id
        FROM public.billing_holds
        WHERE request_id = ANY(${reqIds}::uuid[])
        ORDER BY request_id
        FOR NO KEY UPDATE
      `;
      const holdByReq = new Map<
        string,
        { state: string; wallet_lease_id: string | null; supplier_lease_id: string | null }
      >();
      for (const r of holdRows) {
        holdByReq.set(String(r.request_id), {
          state: String(r.state),
          wallet_lease_id: r.wallet_lease_id ? String(r.wallet_lease_id) : null,
          supplier_lease_id: r.supplier_lease_id ? String(r.supplier_lease_id) : null,
        });
      }

      const wlRows = await tx`
        SELECT id, wallet_id, currency, lot_allocations, remaining_atoms::text, earliest_lot_expires_at, state
        FROM public.wallet_leases
        WHERE id = ANY(${wLeaseIds}::uuid[])
        ORDER BY id
        FOR NO KEY UPDATE
      `;
      const wlById = new Map<
        string,
        {
          id: string;
          wallet_id: string;
          currency: string;
          lots: LeaseLotAllocation[];
          remaining: bigint;
          earliestMs: number | null;
          state: string;
          dirty: boolean;
        }
      >();
      for (const r of wlRows) {
        wlById.set(String(r.id), {
          id: String(r.id),
          wallet_id: String(r.wallet_id),
          currency: String(r.currency),
          lots: (r.lot_allocations ?? []) as LeaseLotAllocation[],
          remaining: BigInt(String(r.remaining_atoms)),
          earliestMs: r.earliest_lot_expires_at
            ? Date.parse(String(r.earliest_lot_expires_at))
            : null,
          state: String(r.state),
          dirty: false,
        });
      }

      const slRows = await tx`
        SELECT id, account_id, currency, cell, remaining_atoms::text, state
        FROM public.supplier_leases
        WHERE id = ANY(${sLeaseIds}::uuid[])
        ORDER BY id
        FOR NO KEY UPDATE
      `;
      const slById = new Map<
        string,
        {
          id: string;
          account_id: string;
          currency: string;
          cell: number;
          remaining: bigint;
          state: string;
          dirty: boolean;
        }
      >();
      for (const r of slRows) {
        slById.set(String(r.id), {
          id: String(r.id),
          account_id: String(r.account_id),
          currency: String(r.currency),
          cell: Number(r.cell),
          remaining: BigInt(String(r.remaining_atoms)),
          state: String(r.state),
          dirty: false,
        });
      }

      const wRows = await tx`
        SELECT id, currency, available_atoms::text, reserved_atoms::text, spent_atoms::text
        FROM public.wallets
        WHERE id = ANY(${walletIds}::uuid[])
        ORDER BY id
        FOR NO KEY UPDATE
      `;
      const wAvailAfter = new Map<string, bigint>();
      for (const r of wRows) {
        wAvailAfter.set(String(r.id), BigInt(String(r.available_atoms)));
      }

      const allLotIds = [
        ...new Set([...wlById.values()].flatMap((wl) => wl.lots.map((l) => l.lot_id))),
      ].sort();
      const cellMap = new Map<string, { account_id: string; currency: string; cell: number }>();
      for (const sl of slById.values()) {
        cellMap.set(cellKey(sl.account_id, sl.currency, sl.cell), {
          account_id: sl.account_id,
          currency: sl.currency,
          cell: sl.cell,
        });
      }
      const cellsSorted = [...cellMap.values()].sort((a, b) =>
        a.account_id !== b.account_id
          ? a.account_id.localeCompare(b.account_id)
          : a.currency !== b.currency
            ? a.currency.localeCompare(b.currency)
            : a.cell - b.cell,
      );
      if (allLotIds.length > 0 || cellsSorted.length > 0) {
        await tx`
          WITH
            lock_l AS (
              SELECT id FROM public.wallet_lots
              WHERE id = ANY(${allLotIds}::uuid[])
              ORDER BY id
              FOR NO KEY UPDATE
            ),
            lock_c AS (
              SELECT c.account_id
              FROM public.supplier_cells c
              JOIN unnest(
                ${cellsSorted.map((x) => x.account_id)}::uuid[],
                ${cellsSorted.map((x) => x.currency)}::text[],
                ${cellsSorted.map((x) => x.cell)}::smallint[]
              ) AS k(account_id, currency, cell)
                ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
              ORDER BY c.account_id, c.currency, c.cell
              FOR NO KEY UPDATE
            )
          SELECT (SELECT count(*) FROM lock_l), (SELECT count(*) FROM lock_c)
        `;
      }

      const walletSpentDelta = new Map<string, bigint>();
      const lotSpentDelta = new Map<string, bigint>();
      const cellSpentDelta = new Map<
        string,
        { account_id: string; currency: string; cell: number; spent: bigint }
      >();

      type PreparedFastCharge = {
        cand: FastCommitCandidate;
        chargeId: string;
        usageDigest: string;
        chargeAllocations: ChargeLotAllocation[];
        customerJournalId: string | null;
        supplierJournalId: string | null;
      };
      const prepared: PreparedFastCharge[] = [];
      const custGroups = new Map<
        string,
        {
          walletId: string;
          currency: string;
          lotDebits: Map<string, bigint>;
          total: bigint;
          items: PreparedFastCharge[];
        }
      >();
      const supGroups = new Map<
        string,
        {
          accountId: string;
          currency: string;
          total: bigint;
          items: PreparedFastCharge[];
        }
      >();

      for (const cand of candidates) {
        const h = holdByReq.get(cand.item.request_id);
        const wl = wlById.get(cand.meta.walletLeaseId);
        const sl = slById.get(cand.meta.supplierLeaseId);
        if (
          !h ||
          h.state !== "HELD" ||
          h.wallet_lease_id !== cand.meta.walletLeaseId ||
          h.supplier_lease_id !== cand.meta.supplierLeaseId ||
          !wl ||
          wl.state !== "ACTIVE" ||
          wl.remaining < cand.customerAtoms ||
          (wl.earliestMs !== null && wl.earliestMs <= nowMs) ||
          !sl ||
          sl.state !== "ACTIVE" ||
          sl.remaining < cand.supplierAtoms
        ) {
          fallback.push({ index: cand.index, item: cand.item });
          continue;
        }

        const carved = carveFromLeaseLots(wl.lots, cand.customerAtoms);
        if (carved.taken !== cand.customerAtoms) {
          fallback.push({ index: cand.index, item: cand.item });
          continue;
        }
        wl.lots = carved.remaining;
        wl.remaining -= cand.customerAtoms;
        wl.earliestMs = earliestExpiryOf(wl.lots);
        wl.dirty = true;

        sl.remaining -= cand.supplierAtoms;
        sl.dirty = true;

        if (cand.customerAtoms > 0n) {
          walletSpentDelta.set(
            wl.wallet_id,
            (walletSpentDelta.get(wl.wallet_id) ?? 0n) + cand.customerAtoms,
          );
          for (const alloc of carved.carved) {
            const a = BigInt(alloc.amount_atoms);
            lotSpentDelta.set(alloc.lot_id, (lotSpentDelta.get(alloc.lot_id) ?? 0n) + a);
          }
        }
        if (cand.supplierAtoms > 0n) {
          const ck = cellKey(sl.account_id, sl.currency, sl.cell);
          const prev = cellSpentDelta.get(ck);
          if (prev) prev.spent += cand.supplierAtoms;
          else {
            cellSpentDelta.set(ck, {
              account_id: sl.account_id,
              currency: sl.currency,
              cell: sl.cell,
              spent: cand.supplierAtoms,
            });
          }
        }

        const chargeAllocations: ChargeLotAllocation[] = carved.carved.map((a) => ({
          lot_id: a.lot_id,
          amount_atoms: a.amount_atoms,
          refunded_atoms: "0",
        }));
        const entry: PreparedFastCharge = {
          cand,
          chargeId: crypto.randomUUID(),
          usageDigest: sha256Hex(cand.item.usage),
          chargeAllocations,
          customerJournalId: null,
          supplierJournalId: null,
        };
        prepared.push(entry);
        committed.push(cand);

        if (cand.customerAtoms > 0n) {
          const gk = `${wl.wallet_id}:${wl.currency}`;
          let g = custGroups.get(gk);
          if (!g) {
            g = {
              walletId: wl.wallet_id,
              currency: wl.currency,
              lotDebits: new Map(),
              total: 0n,
              items: [],
            };
            custGroups.set(gk, g);
          }
          g.total += cand.customerAtoms;
          g.items.push(entry);
          for (const alloc of carved.carved) {
            const a = BigInt(alloc.amount_atoms);
            g.lotDebits.set(alloc.lot_id, (g.lotDebits.get(alloc.lot_id) ?? 0n) + a);
          }
        }
        if (cand.supplierAtoms > 0n) {
          const sk = `${sl.account_id}:${sl.currency}`;
          let sg = supGroups.get(sk);
          if (!sg) {
            sg = { accountId: sl.account_id, currency: sl.currency, total: 0n, items: [] };
            supGroups.set(sk, sg);
          }
          sg.total += cand.supplierAtoms;
          sg.items.push(entry);
        }
      }

      if (prepared.length === 0) return;

      const journals: JournalLine[] = [];
      for (const g of custGroups.values()) {
        const jid = crypto.randomUUID();
        const singleReqId = g.items.length === 1 ? g.items[0]!.cand.item.request_id : null;
        const ref = singleReqId ? `settle:${singleReqId}` : `settle_batch:${jid}`;
        for (const it of g.items) it.customerJournalId = jid;
        let ord = 0;
        for (const [lotId, amt] of g.lotDebits) {
          journals.push({
            journal_id: jid,
            ordinal: ord++,
            side: "CUSTOMER",
            entity_id: g.walletId,
            request_id: singleReqId,
            reference: ref,
            kind: "SETTLE",
            currency: g.currency,
            account: `lot:${lotId}:available`,
            atoms: -amt,
          });
        }
        journals.push({
          journal_id: jid,
          ordinal: ord,
          side: "CUSTOMER",
          entity_id: g.walletId,
          request_id: singleReqId,
          reference: ref,
          kind: "SETTLE",
          currency: g.currency,
          account: "system:revenue",
          atoms: g.total,
        });
      }

      for (const sg of supGroups.values()) {
        const sj = crypto.randomUUID();
        const singleReqId = sg.items.length === 1 ? sg.items[0]!.cand.item.request_id : null;
        const ref = singleReqId
          ? `supplier_cost:${sg.items[0]!.cand.item.selected_attempt_id}`
          : `supplier_cost_batch:${sj}`;
        for (const it of sg.items) it.supplierJournalId = sj;
        journals.push(
          {
            journal_id: sj,
            ordinal: 0,
            side: "SUPPLIER",
            entity_id: sg.accountId,
            request_id: singleReqId,
            reference: ref,
            kind: "SUPPLIER_COST",
            currency: sg.currency,
            account: "upstream:available",
            atoms: -sg.total,
          },
          {
            journal_id: sj,
            ordinal: 1,
            side: "SUPPLIER",
            entity_id: sg.accountId,
            request_id: singleReqId,
            reference: ref,
            kind: "SUPPLIER_COST",
            currency: sg.currency,
            account: "upstream:expense",
            atoms: sg.total,
          },
        );
      }

      const dirtyWl = [...wlById.values()].filter((w) => w.dirty);
      const dirtySl = [...slById.values()].filter((s) => s.dirty);
      const wEntries = [...walletSpentDelta.entries()];
      const lEntries = [...lotSpentDelta.entries()];
      const cEntries = [...cellSpentDelta.values()];

      await tx`
        WITH
          upd_wl AS (
            UPDATE public.wallet_leases AS l SET
              lot_allocations = u.lot_allocations::jsonb,
              remaining_atoms = u.remaining_atoms,
              earliest_lot_expires_at = u.earliest_lot_expires_at,
              expires_at = ${nextExpIso}::timestamptz,
              updated_at = now()
            FROM unnest(
              ${dirtyWl.map((w) => w.id)}::uuid[],
              ${dirtyWl.map((w) => JSON.stringify(w.lots))}::text[],
              ${dirtyWl.map((w) => w.remaining.toString())}::numeric[],
              ${dirtyWl.map((w) =>
                w.earliestMs === null ? null : new Date(w.earliestMs).toISOString(),
              )}::timestamptz[]
            ) AS u(id, lot_allocations, remaining_atoms, earliest_lot_expires_at)
            WHERE l.id = u.id
          ),
          upd_sl AS (
            UPDATE public.supplier_leases AS l SET
              remaining_atoms = u.remaining_atoms,
              expires_at = ${nextExpIso}::timestamptz,
              updated_at = now()
            FROM unnest(
              ${dirtySl.map((s) => s.id)}::uuid[],
              ${dirtySl.map((s) => s.remaining.toString())}::numeric[]
            ) AS u(id, remaining_atoms)
            WHERE l.id = u.id
          ),
          upd_w AS (
            UPDATE public.wallets AS w SET
              reserved_atoms = w.reserved_atoms - u.spent,
              spent_atoms = w.spent_atoms + u.spent
            FROM unnest(
              ${wEntries.map(([id]) => id)}::uuid[],
              ${wEntries.map(([, s]) => s.toString())}::numeric[]
            ) AS u(id, spent)
            WHERE w.id = u.id
          ),
          upd_l AS (
            UPDATE public.wallet_lots AS l SET
              reserved_atoms = l.reserved_atoms - u.spent,
              spent_atoms = l.spent_atoms + u.spent
            FROM unnest(
              ${lEntries.map(([id]) => id)}::uuid[],
              ${lEntries.map(([, s]) => s.toString())}::numeric[]
            ) AS u(id, spent)
            WHERE l.id = u.id
          ),
          upd_c AS (
            UPDATE public.supplier_cells AS c SET
              reserved_atoms = c.reserved_atoms - u.spent,
              balance_atoms = c.balance_atoms - u.spent,
              spent_atoms = c.spent_atoms + u.spent,
              revision = c.revision + 1
            FROM unnest(
              ${cEntries.map((c) => c.account_id)}::uuid[],
              ${cEntries.map((c) => c.currency)}::text[],
              ${cEntries.map((c) => c.cell)}::smallint[],
              ${cEntries.map((c) => c.spent.toString())}::numeric[]
            ) AS u(account_id, currency, cell, spent)
            WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
          ),
          upd_h AS (
            UPDATE public.billing_holds
            SET state = 'SETTLED', closed_at = now()
            WHERE request_id = ANY(${prepared.map((p) => p.cand.item.request_id)}::uuid[])
          ),
          ins_ch AS (
            INSERT INTO public.billing_charges (
              id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
              model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
              customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
              lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
            )
            SELECT
              u.id, u.request_id, u.project_id, u.wallet_id, u.selected_attempt_id, u.channel_id,
              u.model, u.operation, u.currency, u.billing_price_id, u.cost_version_id, u.usage::jsonb, u.usage_digest,
              u.customer_atoms, u.funded_atoms, 0, u.supplier_cost_atoms, u.supplier_currency,
              u.lot_allocations::jsonb, u.plugin_audit::jsonb, u.charge_policy, 'SETTLED',
              u.customer_journal_id, u.supplier_journal_id
            FROM unnest(
              ${prepared.map((p) => p.chargeId)}::uuid[],
              ${prepared.map((p) => p.cand.item.request_id)}::uuid[],
              ${prepared.map((p) => p.cand.meta.projectId)}::uuid[],
              ${prepared.map((p) => p.cand.meta.walletId)}::uuid[],
              ${prepared.map((p) => p.cand.item.selected_attempt_id)}::uuid[],
              ${prepared.map((p) => p.cand.item.channel_id)}::uuid[],
              ${prepared.map((p) => p.cand.meta.model)}::text[],
              ${prepared.map((p) => p.cand.meta.operation)}::text[],
              ${prepared.map((p) => p.cand.meta.currency)}::text[],
              ${prepared.map((p) => p.cand.item.billing_price_id)}::uuid[],
              ${prepared.map((p) => p.cand.item.cost_version_id)}::uuid[],
              ${prepared.map((p) => JSON.stringify(p.cand.item.usage))}::text[],
              ${prepared.map((p) => p.usageDigest)}::text[],
              ${prepared.map((p) => p.cand.customerAtoms.toString())}::numeric[],
              ${prepared.map((p) => p.cand.customerAtoms.toString())}::numeric[],
              ${prepared.map((p) => p.cand.supplierAtoms.toString())}::numeric[],
              ${prepared.map((p) => p.cand.item.supplier_currency)}::text[],
              ${prepared.map((p) => JSON.stringify(p.chargeAllocations))}::text[],
              ${prepared.map((p) => JSON.stringify(p.cand.item.plugin_audit ?? []))}::text[],
              ${prepared.map((p) => p.cand.item.charge_policy ?? "CHARGE_CUSTOMER")}::text[],
              ${prepared.map((p) => p.customerJournalId)}::uuid[],
              ${prepared.map((p) => p.supplierJournalId)}::uuid[]
            ) AS u(
              id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
              model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
              customer_atoms, funded_atoms, supplier_cost_atoms, supplier_currency,
              lot_allocations, plugin_audit, charge_policy, customer_journal_id, supplier_journal_id
            )
          ),
          ins_le AS (
            INSERT INTO public.ledger_entries (
              journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms
            )
            SELECT * FROM unnest(
              ${journals.map((l) => l.journal_id)}::uuid[],
              ${journals.map((l) => l.ordinal)}::smallint[],
              ${journals.map((l) => l.side)}::text[],
              ${journals.map((l) => l.entity_id)}::uuid[],
              ${journals.map((l) => l.request_id)}::uuid[],
              ${journals.map((l) => l.reference)}::text[],
              ${journals.map((l) => l.kind)}::text[],
              ${journals.map((l) => l.currency)}::text[],
              ${journals.map((l) => l.account)}::text[],
              ${journals.map((l) => l.atoms.toString())}::numeric[]
            )
          ),
          ins_ob AS (
            INSERT INTO public.outbox_events (event_id, kind, request_id, project_id, payload)
            SELECT u.event_id, 'billing.settled', u.request_id, u.project_id, u.payload::jsonb
            FROM unnest(
              ${prepared.map(() => crypto.randomUUID())}::uuid[],
              ${prepared.map((p) => p.cand.item.request_id)}::uuid[],
              ${prepared.map((p) => p.cand.meta.projectId)}::uuid[],
              ${prepared.map((p) =>
                JSON.stringify({
                  charge_id: p.chargeId,
                  request_id: p.cand.item.request_id,
                  attempt_id: p.cand.item.selected_attempt_id,
                  channel_id: p.cand.item.channel_id,
                  model: p.cand.meta.model,
                  operation: p.cand.meta.operation,
                  currency: p.cand.meta.currency,
                  usage: p.cand.item.usage,
                  customer_atoms: p.cand.customerAtoms.toString(),
                  funded_atoms: p.cand.customerAtoms.toString(),
                  debt_atoms: "0",
                  supplier_cost_atoms: p.cand.supplierAtoms.toString(),
                  outcome: "SETTLED",
                  wallet_available_atoms: (wAvailAfter.get(p.cand.meta.walletId) ?? 0n).toString(),
                }),
              )}::text[]
            ) AS u(event_id, request_id, project_id, payload)
          )
        SELECT 1
      `;

      for (const p of prepared) {
        results[p.cand.index] = {
          ok: true,
          replayed: false,
          charge_id: p.chargeId,
          outcome: "SETTLED",
          customer_atoms: p.cand.customerAtoms.toString(),
          funded_atoms: p.cand.customerAtoms.toString(),
          debt_atoms: "0",
          supplier_cost_atoms: p.cand.supplierAtoms.toString(),
        };
      }
    });

    const postNow = Date.now();
    for (const cand of committed) {
      leasedHolds.delete(cand.item.request_id);
      const wl = walletLeasesById.get(cand.meta.walletLeaseId);
      if (wl) {
        wl.remainingInPg -= cand.customerAtoms;
        wl.freeInMemory += cand.meta.heldCustomerAtoms - cand.customerAtoms;
        if (wl.inFlightHolds > 0) wl.inFlightHolds--;
        wl.expiresAtMs = postNow + leaseTtlMs;
        wl.lastUsedAtMs = postNow;
      }
      const sl = supplierLeasesById.get(cand.meta.supplierLeaseId);
      if (sl) {
        sl.remainingInPg -= cand.supplierAtoms;
        sl.freeInMemory += cand.meta.heldSupplierAtoms - cand.supplierAtoms;
        if (sl.inFlightHolds > 0) sl.inFlightHolds--;
        sl.expiresAtMs = postNow + leaseTtlMs;
        sl.lastUsedAtMs = postNow;
      }
    }

    return fallback;
  }

  async function runDirectCommitBatch(
    directEntries: { index: number; item: CommitTerminalInput }[],
    results: CommitTerminalResult[],
  ): Promise<void> {
    if (directEntries.length === 0) return;
    const requestIds = [...new Set(directEntries.map((e) => e.item.request_id))].sort();
    const detachedNotifications: {
      requestId: string;
      wlId: string | null;
      heldCust: bigint;
      slId: string | null;
      supAtoms: bigint;
    }[] = [];

    await db.begin(async (tx) => {
      const nowMs = Date.now();
      const holdRows = await tx`
        SELECT request_id, project_id, wallet_id, operation, model, held_atoms::text, credit_atoms::text,
          lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
        FROM public.billing_holds
        WHERE request_id = ANY(${requestIds}::uuid[])
        ORDER BY request_id
        FOR NO KEY UPDATE
      `;
      const holdsByReq = new Map<
        string,
        {
          request_id: string;
          project_id: string;
          wallet_id: string;
          operation: string;
          model: string;
          held_atoms: bigint;
          credit_atoms: bigint;
          lot_allocations: HoldLotAllocation[];
          supplier_holds: SupplierHoldEntry[];
          wallet_lease_id: string | null;
          supplier_lease_id: string | null;
          state: string;
        }
      >();
      for (const r of holdRows) {
        holdsByReq.set(String(r.request_id), {
          request_id: String(r.request_id),
          project_id: String(r.project_id),
          wallet_id: String(r.wallet_id),
          operation: String(r.operation),
          model: String(r.model),
          held_atoms: BigInt(String(r.held_atoms)),
          credit_atoms: BigInt(String(r.credit_atoms)),
          lot_allocations: (r.lot_allocations ?? []) as HoldLotAllocation[],
          supplier_holds: (r.supplier_holds ?? []) as SupplierHoldEntry[],
          wallet_lease_id: r.wallet_lease_id ? String(r.wallet_lease_id) : null,
          supplier_lease_id: r.supplier_lease_id ? String(r.supplier_lease_id) : null,
          state: String(r.state),
        });
      }

      const holdsNeedingDetach = [...holdsByReq.values()].filter(
        (h) =>
          (h.state === "HELD" || h.state === "UNKNOWN") &&
          (h.wallet_lease_id !== null || h.supplier_lease_id !== null),
      );
      if (holdsNeedingDetach.length > 0) {
        const wlIds = [
          ...new Set(
            holdsNeedingDetach.map((h) => h.wallet_lease_id).filter((x): x is string => x !== null),
          ),
        ].sort();
        const slIds = [
          ...new Set(
            holdsNeedingDetach
              .map((h) => h.supplier_lease_id)
              .filter((x): x is string => x !== null),
          ),
        ].sort();
        if (wlIds.length > 0) {
          await tx`SELECT id FROM public.wallet_leases WHERE id = ANY(${wlIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE`;
        }
        if (slIds.length > 0) {
          await tx`SELECT id FROM public.supplier_leases WHERE id = ANY(${slIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE`;
        }
        for (const h of holdsNeedingDetach) {
          const det = await detachHoldFromLeaseTx(tx, h);
          h.lot_allocations = det.lot_allocations;
          h.wallet_lease_id = null;
          h.supplier_lease_id = null;
          detachedNotifications.push({
            requestId: h.request_id,
            wlId: det.detachedWalletLeaseId,
            heldCust: h.held_atoms,
            slId: det.detachedSupplierLeaseId,
            supAtoms: det.detachedSupplierAtoms,
          });
        }
      }

      const existingChargesRaw = await tx`
        SELECT id, request_id, outcome, customer_atoms::text, funded_atoms::text, debt_atoms::text, supplier_cost_atoms::text
        FROM public.billing_charges
        WHERE request_id = ANY(${requestIds}::uuid[])
      `;
      const existingCharges = new Map<
        string,
        {
          id: string;
          outcome: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
          customer_atoms: string;
          funded_atoms: string;
          debt_atoms: string;
          supplier_cost_atoms: string;
        }
      >();
      for (const r of existingChargesRaw) {
        existingCharges.set(String(r.request_id), {
          id: String(r.id),
          outcome: r.outcome as
            "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED",
          customer_atoms: String(r.customer_atoms),
          funded_atoms: String(r.funded_atoms),
          debt_atoms: String(r.debt_atoms),
          supplier_cost_atoms: String(r.supplier_cost_atoms),
        });
      }

      const walletIds = [...new Set([...holdsByReq.values()].map((h) => h.wallet_id))].sort();
      const lockedWallets = walletIds.length
        ? await tx`
            SELECT id, project_id, currency, policy,
              available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
              debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
              credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
            FROM public.wallets
            WHERE id = ANY(${walletIds}::uuid[])
            ORDER BY id
            FOR NO KEY UPDATE
          `
        : [];
      const walletsById = new Map<string, WalletRow>();
      for (const r of lockedWallets) {
        const w = toWalletRow(r);
        walletsById.set(w.id, w);
      }

      const lotRowsRaw = walletIds.length
        ? await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots
            WHERE wallet_id = ANY(${walletIds}::uuid[]) AND (available_atoms > 0 OR reserved_atoms > 0)
            ORDER BY id
            FOR NO KEY UPDATE
          `
        : [];
      const lotsById = new Map<string, LotRow>();
      const lotsByWallet = new Map<string, LotRow[]>();
      for (const r of lotRowsRaw) {
        const l = toLotRow(r);
        lotsById.set(l.id, l);
        const list = lotsByWallet.get(l.wallet_id);
        if (list) list.push(l);
        else lotsByWallet.set(l.wallet_id, [l]);
      }
      for (const [wid, arr] of lotsByWallet) {
        const w = walletsById.get(wid)!;
        lotsByWallet.set(wid, sortLots(arr, w.policy));
      }

      const neededCellMap = new Map<
        string,
        { account_id: string; currency: string; cell: number }
      >();
      for (const h of holdsByReq.values()) {
        for (const s of h.supplier_holds) {
          const k = cellKey(s.account_id, s.currency, s.cell);
          if (!neededCellMap.has(k)) {
            neededCellMap.set(k, {
              account_id: s.account_id,
              currency: s.currency,
              cell: s.cell,
            });
          }
        }
      }
      const neededCells = [...neededCellMap.values()].sort((a, b) =>
        a.account_id !== b.account_id
          ? a.account_id.localeCompare(b.account_id)
          : a.currency !== b.currency
            ? a.currency.localeCompare(b.currency)
            : a.cell - b.cell,
      );
      const cellRowsRaw = neededCells.length
        ? await tx`
            SELECT c.account_id, c.currency, c.cell, c.balance_atoms::text, c.reserved_atoms::text, c.spent_atoms::text, c.revision::text
            FROM public.supplier_cells c
            JOIN unnest(
              ${neededCells.map((x) => x.account_id)}::uuid[],
              ${neededCells.map((x) => x.currency)}::text[],
              ${neededCells.map((x) => x.cell)}::smallint[]
            ) AS k(account_id, currency, cell)
              ON c.account_id = k.account_id AND c.currency = k.currency AND c.cell = k.cell
            ORDER BY c.account_id, c.currency, c.cell
            FOR NO KEY UPDATE
          `
        : [];
      const cellsByKey = new Map<string, SupplierCellRow>();
      for (const r of cellRowsRaw) {
        const c = toCellRow(r);
        cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
      }

      const journals: JournalLine[] = [];
      const reconcileAccounts = new Set<string>();
      const holdUpdates: {
        request_id: string;
        state: string;
        supplier_holds: SupplierHoldEntry[];
      }[] = [];
      const newCharges: {
        id: string;
        request_id: string;
        project_id: string;
        wallet_id: string;
        selected_attempt_id: string;
        channel_id: string;
        model: string;
        operation: string;
        currency: string;
        billing_price_id: string;
        cost_version_id: string;
        usage: Usage;
        usage_digest: string;
        customer_atoms: string;
        funded_atoms: string;
        debt_atoms: string;
        supplier_cost_atoms: string;
        supplier_currency: string;
        lot_allocations: ChargeLotAllocation[];
        plugin_audit: unknown[];
        charge_policy: "CHARGE_CUSTOMER" | "PLATFORM_ABSORB";
        outcome: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED";
        customer_journal_id: string | null;
        supplier_journal_id: string | null;
      }[] = [];
      const outboxBatch: {
        event_id: string;
        kind: string;
        request_id: string;
        project_id: string;
        payload: Record<string, unknown>;
      }[] = [];

      for (const { index: i, item } of directEntries) {
        const existing = existingCharges.get(item.request_id);
        if (existing) {
          results[i] = {
            ok: true,
            replayed: true,
            charge_id: existing.id,
            outcome: existing.outcome,
            customer_atoms: existing.customer_atoms,
            funded_atoms: existing.funded_atoms,
            debt_atoms: existing.debt_atoms,
            supplier_cost_atoms: existing.supplier_cost_atoms,
          };
          continue;
        }
        const hold = holdsByReq.get(item.request_id);
        if (!hold) {
          results[i] = {
            ok: false,
            code: "hold_not_found",
            message: "Active billing hold not found",
          };
          continue;
        }
        if (hold.state !== "HELD" && hold.state !== "UNKNOWN") {
          results[i] = {
            ok: false,
            code: "invalid_state",
            message: `Hold in state ${hold.state}`,
          };
          continue;
        }
        const wallet = walletsById.get(hold.wallet_id)!;
        if (item.billing_pricing.currency !== wallet.currency) {
          results[i] = {
            ok: false,
            code: "currency_mismatch",
            message: "Billing pricing currency mismatches wallet currency",
          };
          continue;
        }

        const effectiveUsage: Usage =
          item.billing_basis === "REQUEST_COMPLETION"
            ? { requests: "1" }
            : item.billing_basis === "INPUT_CHARACTERS"
              ? { characters: item.input_characters ?? "0", requests: "1" }
              : item.usage;

        let computedCustomerAtoms: bigint;
        let supplierCostAtoms: bigint;
        try {
          computedCustomerAtoms = BigInt(calculateCost(effectiveUsage, item.billing_pricing));
          if (item.supplier_cost_atoms_override !== undefined) {
            const parsedSup = parseAtoms(item.supplier_cost_atoms_override);
            if (parsedSup === null || parsedSup < 0n) {
              throw new Error("Invalid supplier_cost_atoms_override");
            }
            supplierCostAtoms = parsedSup;
          } else {
            supplierCostAtoms = BigInt(calculateCost(item.usage, item.supplier_pricing));
          }
        } catch (err) {
          results[i] = {
            ok: false,
            code: "pricing_error",
            message: err instanceof Error ? err.message : "Pricing error",
          };
          continue;
        }

        const chargePolicy = item.charge_policy ?? "CHARGE_CUSTOMER";
        const absorbed = Boolean(item.moderation_blocked && chargePolicy === "PLATFORM_ABSORB");
        const billableCustomerAtoms = absorbed ? 0n : computedCustomerAtoms;
        const waivedCustomerAtoms = absorbed ? computedCustomerAtoms : 0n;

        let remaining = billableCustomerAtoms;
        let funded = 0n;
        let releasedToAvailable = 0n;
        let expiredFromHold = 0n;
        let extraFromAvailable = 0n;
        const chargeAllocMap = new Map<string, bigint>();
        const custLines: { account: string; atoms: bigint }[] = [];

        for (const alloc of hold.lot_allocations) {
          const lot = lotsById.get(alloc.lot_id);
          if (!lot) throw new Error(`Missing reserved lot ${alloc.lot_id}`);
          const heldAmt = BigInt(alloc.amount_atoms);
          const take = remaining < heldAmt ? remaining : heldAmt;
          const returned = heldAmt - take;
          remaining -= take;
          funded += take;
          lot.reserved_atoms -= heldAmt;
          lot.spent_atoms += take;
          lot.dirty = true;
          if (take > 0n) {
            chargeAllocMap.set(lot.id, (chargeAllocMap.get(lot.id) ?? 0n) + take);
            custLines.push({ account: `lot:${lot.id}:available`, atoms: -take });
          }
          if (returned > 0n) {
            if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
              lot.expired_atoms += returned;
              expiredFromHold += returned;
              custLines.push(
                { account: `lot:${lot.id}:available`, atoms: -returned },
                { account: `lot:${lot.id}:expired`, atoms: returned },
              );
            } else {
              lot.available_atoms += returned;
              releasedToAvailable += returned;
            }
          }
        }

        const walletLots = lotsByWallet.get(wallet.id) ?? [];
        if (remaining > 0n) {
          expireLotsInMemory(wallet, walletLots, nowMs, journals);
          for (const lot of walletLots) {
            if (remaining === 0n) break;
            if (!lotEligible(lot, hold.model, hold.operation, nowMs)) continue;
            const take = remaining < lot.available_atoms ? remaining : lot.available_atoms;
            remaining -= take;
            funded += take;
            extraFromAvailable += take;
            lot.available_atoms -= take;
            lot.spent_atoms += take;
            lot.dirty = true;
            chargeAllocMap.set(lot.id, (chargeAllocMap.get(lot.id) ?? 0n) + take);
            custLines.push({ account: `lot:${lot.id}:available`, atoms: -take });
          }
        }

        const debtPart = remaining;
        if (billableCustomerAtoms > 0n) {
          if (debtPart > 0n) {
            custLines.push({ account: "system:receivable", atoms: -debtPart });
          }
          custLines.push({ account: "system:revenue", atoms: billableCustomerAtoms });
        }

        const heldFunded = hold.held_atoms - hold.credit_atoms;
        wallet.reserved_atoms -= heldFunded;
        wallet.credit_reserved_atoms -= hold.credit_atoms;
        wallet.available_atoms = wallet.available_atoms + releasedToAvailable - extraFromAvailable;
        wallet.spent_atoms += funded;
        wallet.expired_atoms += expiredFromHold;
        wallet.debt_atoms += debtPart;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;

        let customerJournalId: string | null = null;
        if (custLines.length >= 2) {
          const jid = crypto.randomUUID();
          customerJournalId = jid;
          const kind = billableCustomerAtoms === 0n ? "EXPIRE" : "SETTLE";
          for (let ord = 0; ord < custLines.length; ord++) {
            journals.push({
              journal_id: jid,
              ordinal: ord,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: item.request_id,
              reference: `settle:${item.request_id}`,
              kind,
              currency: wallet.currency,
              account: custLines[ord]!.account,
              atoms: custLines[ord]!.atoms,
            });
          }
        }

        if (waivedCustomerAtoms > 0n) {
          const mj = crypto.randomUUID();
          if (!customerJournalId) customerJournalId = mj;
          journals.push(
            {
              journal_id: mj,
              ordinal: 0,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: item.request_id,
              reference: `moderation_absorb:${item.request_id}`,
              kind: "MODERATION_ABSORB",
              currency: wallet.currency,
              account: "system:revenue",
              atoms: waivedCustomerAtoms,
            },
            {
              journal_id: mj,
              ordinal: 1,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: item.request_id,
              reference: `moderation_absorb:${item.request_id}`,
              kind: "MODERATION_ABSORB",
              currency: wallet.currency,
              account: "system:moderation_loss",
              atoms: -waivedCustomerAtoms,
            },
          );
        }

        let supplierJournalId: string | null = null;
        const updatedSupplierHolds: SupplierHoldEntry[] = [];
        for (const sh of hold.supplier_holds) {
          const canSettleAttempt =
            sh.attempt_id === item.selected_attempt_id &&
            (sh.state === "HELD" || (hold.state === "UNKNOWN" && sh.state === "UNKNOWN"));
          if (canSettleAttempt) {
            const cell = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
            if (cell) {
              const heldSup = BigInt(sh.held_atoms);
              cell.reserved_atoms -= heldSup;
              cell.balance_atoms -= supplierCostAtoms;
              cell.spent_atoms += supplierCostAtoms;
              cell.revision += 1n;
              cell.dirty = true;
              if (supplierCostAtoms > heldSup || cell.balance_atoms < 0n) {
                reconcileAccounts.add(sh.account_id);
              }
            }
            if (supplierCostAtoms > 0n) {
              const sj = crypto.randomUUID();
              supplierJournalId = sj;
              journals.push(
                {
                  journal_id: sj,
                  ordinal: 0,
                  side: "SUPPLIER",
                  entity_id: sh.account_id,
                  request_id: item.request_id,
                  reference: `supplier_cost:${item.selected_attempt_id}`,
                  kind: "SUPPLIER_COST",
                  currency: sh.currency,
                  account: "upstream:available",
                  atoms: -supplierCostAtoms,
                },
                {
                  journal_id: sj,
                  ordinal: 1,
                  side: "SUPPLIER",
                  entity_id: sh.account_id,
                  request_id: item.request_id,
                  reference: `supplier_cost:${item.selected_attempt_id}`,
                  kind: "SUPPLIER_COST",
                  currency: sh.currency,
                  account: "upstream:expense",
                  atoms: supplierCostAtoms,
                },
              );
            }
            updatedSupplierHolds.push({ ...sh, state: "SETTLED" });
          } else if (sh.state === "HELD") {
            const cell = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
            if (cell) {
              cell.reserved_atoms -= BigInt(sh.held_atoms);
              cell.revision += 1n;
              cell.dirty = true;
            }
            updatedSupplierHolds.push({ ...sh, state: "RELEASED" });
          } else {
            updatedSupplierHolds.push(sh);
          }
        }

        const chargeAllocations: ChargeLotAllocation[] = [...chargeAllocMap.entries()].map(
          ([lot_id, amt]) => ({
            lot_id,
            amount_atoms: amt.toString(),
            refunded_atoms: "0",
          }),
        );

        const outcome: "SETTLED" | "RELEASED" | "DEBT" | "MODERATION_BLOCKED" | "UNKNOWN_RESOLVED" =
          item.moderation_blocked
            ? "MODERATION_BLOCKED"
            : hold.state === "UNKNOWN"
              ? "UNKNOWN_RESOLVED"
              : debtPart > 0n
                ? "DEBT"
                : "SETTLED";

        const chargeId = crypto.randomUUID();
        const usageDigest = sha256Hex(item.usage);

        holdUpdates.push({
          request_id: item.request_id,
          state: "SETTLED",
          supplier_holds: updatedSupplierHolds,
        });
        newCharges.push({
          id: chargeId,
          request_id: item.request_id,
          project_id: hold.project_id,
          wallet_id: wallet.id,
          selected_attempt_id: item.selected_attempt_id,
          channel_id: item.channel_id,
          model: hold.model,
          operation: hold.operation,
          currency: wallet.currency,
          billing_price_id: item.billing_price_id,
          cost_version_id: item.cost_version_id,
          usage: item.usage,
          usage_digest: usageDigest,
          customer_atoms: billableCustomerAtoms.toString(),
          funded_atoms: funded.toString(),
          debt_atoms: debtPart.toString(),
          supplier_cost_atoms: supplierCostAtoms.toString(),
          supplier_currency: item.supplier_currency,
          lot_allocations: chargeAllocations,
          plugin_audit: item.plugin_audit ?? [],
          charge_policy: chargePolicy,
          outcome,
          customer_journal_id: customerJournalId,
          supplier_journal_id: supplierJournalId,
        });
        outboxBatch.push({
          event_id: crypto.randomUUID(),
          kind: "billing.settled",
          request_id: item.request_id,
          project_id: hold.project_id,
          payload: {
            charge_id: chargeId,
            request_id: item.request_id,
            attempt_id: item.selected_attempt_id,
            channel_id: item.channel_id,
            model: hold.model,
            operation: hold.operation,
            currency: wallet.currency,
            usage: item.usage,
            customer_atoms: billableCustomerAtoms.toString(),
            funded_atoms: funded.toString(),
            debt_atoms: debtPart.toString(),
            supplier_cost_atoms: supplierCostAtoms.toString(),
            lot_allocations: chargeAllocations,
            plugin_audit: item.plugin_audit ?? [],
            charge_policy: chargePolicy,
            outcome,
            wallet_available_atoms: wallet.available_atoms.toString(),
          },
        });

        existingCharges.set(item.request_id, {
          id: chargeId,
          outcome,
          customer_atoms: billableCustomerAtoms.toString(),
          funded_atoms: funded.toString(),
          debt_atoms: debtPart.toString(),
          supplier_cost_atoms: supplierCostAtoms.toString(),
        });

        results[i] = {
          ok: true,
          replayed: false,
          charge_id: chargeId,
          outcome,
          customer_atoms: billableCustomerAtoms.toString(),
          funded_atoms: funded.toString(),
          debt_atoms: debtPart.toString(),
          supplier_cost_atoms: supplierCostAtoms.toString(),
        };
      }

      await flushDirtyWallets(tx, walletsById.values());
      await flushDirtyLots(tx, lotsById.values());
      await flushDirtyCells(tx, cellsByKey.values());
      await insertJournalLines(tx, journals);

      if (reconcileAccounts.size > 0) {
        const ids = [...reconcileAccounts];
        await tx`
          UPDATE public.upstream_accounts
          SET state = 'RECONCILE_REQUIRED', revision = revision + 1, updated_at = now()
          WHERE id = ANY(${ids}::uuid[]) AND state = 'ACTIVE'
        `;
      }

      if (holdUpdates.length > 0) {
        await tx`
          UPDATE public.billing_holds AS h SET
            state = u.state,
            supplier_holds = u.supplier_holds::jsonb,
            closed_at = now()
          FROM unnest(
            ${holdUpdates.map((h) => h.request_id)}::uuid[],
            ${holdUpdates.map((h) => h.state)}::text[],
            ${holdUpdates.map((h) => JSON.stringify(h.supplier_holds))}::text[]
          ) AS u(request_id, state, supplier_holds)
          WHERE h.request_id = u.request_id
        `;
      }

      if (newCharges.length > 0) {
        await tx`
          INSERT INTO public.billing_charges (
            id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
            model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
            customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
            lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
          )
          SELECT
            u.id, u.request_id, u.project_id, u.wallet_id, u.selected_attempt_id, u.channel_id,
            u.model, u.operation, u.currency, u.billing_price_id, u.cost_version_id, u.usage::jsonb, u.usage_digest,
            u.customer_atoms, u.funded_atoms, u.debt_atoms, u.supplier_cost_atoms, u.supplier_currency,
            u.lot_allocations::jsonb, u.plugin_audit::jsonb, u.charge_policy, u.outcome,
            u.customer_journal_id, u.supplier_journal_id
          FROM unnest(
            ${newCharges.map((c) => c.id)}::uuid[],
            ${newCharges.map((c) => c.request_id)}::uuid[],
            ${newCharges.map((c) => c.project_id)}::uuid[],
            ${newCharges.map((c) => c.wallet_id)}::uuid[],
            ${newCharges.map((c) => c.selected_attempt_id)}::uuid[],
            ${newCharges.map((c) => c.channel_id)}::uuid[],
            ${newCharges.map((c) => c.model)}::text[],
            ${newCharges.map((c) => c.operation)}::text[],
            ${newCharges.map((c) => c.currency)}::text[],
            ${newCharges.map((c) => c.billing_price_id)}::uuid[],
            ${newCharges.map((c) => c.cost_version_id)}::uuid[],
            ${newCharges.map((c) => JSON.stringify(c.usage))}::text[],
            ${newCharges.map((c) => c.usage_digest)}::text[],
            ${newCharges.map((c) => c.customer_atoms)}::numeric[],
            ${newCharges.map((c) => c.funded_atoms)}::numeric[],
            ${newCharges.map((c) => c.debt_atoms)}::numeric[],
            ${newCharges.map((c) => c.supplier_cost_atoms)}::numeric[],
            ${newCharges.map((c) => c.supplier_currency)}::text[],
            ${newCharges.map((c) => JSON.stringify(c.lot_allocations))}::text[],
            ${newCharges.map((c) => JSON.stringify(c.plugin_audit))}::text[],
            ${newCharges.map((c) => c.charge_policy)}::text[],
            ${newCharges.map((c) => c.outcome)}::text[],
            ${newCharges.map((c) => c.customer_journal_id)}::uuid[],
            ${newCharges.map((c) => c.supplier_journal_id)}::uuid[]
          ) AS u(
            id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
            model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
            customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
            lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
          )
        `;
      }

      if (outboxBatch.length > 0) {
        await tx`
          INSERT INTO public.outbox_events (event_id, kind, request_id, project_id, payload)
          SELECT u.event_id, u.kind, u.request_id, u.project_id, u.payload::jsonb
          FROM unnest(
            ${outboxBatch.map((o) => o.event_id)}::uuid[],
            ${outboxBatch.map((o) => o.kind)}::text[],
            ${outboxBatch.map((o) => o.request_id)}::uuid[],
            ${outboxBatch.map((o) => o.project_id)}::uuid[],
            ${outboxBatch.map((o) => JSON.stringify(o.payload))}::text[]
          ) AS u(event_id, kind, request_id, project_id, payload)
        `;
      }
    });

    for (const d of detachedNotifications) {
      notifyDetachedLocal(d.requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
    }
  }

  async function runCommitBatch(items: CommitTerminalInput[]): Promise<CommitTerminalResult[]> {
    const results = Array.from<CommitTerminalResult>({ length: items.length });
    const fastCandidates: FastCommitCandidate[] = [];
    const directEntries: { index: number; item: CommitTerminalInput }[] = [];
    const seenFastReqIds = new Set<string>();
    const nowMs = Date.now();

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!;
      const meta = leasedHolds.get(item.request_id);
      if (
        meta &&
        !seenFastReqIds.has(item.request_id) &&
        !item.moderation_blocked &&
        item.selected_attempt_id === meta.attemptId &&
        item.billing_pricing.currency === meta.currency &&
        item.supplier_currency === meta.supplierCurrency
      ) {
        const wl = walletLeasesById.get(meta.walletLeaseId);
        const sl = supplierLeasesById.get(meta.supplierLeaseId);
        const lotValid =
          wl && (wl.earliestLotExpiresAtMs === null || wl.earliestLotExpiresAtMs > nowMs + 500);
        if (wl && sl && lotValid) {
          const effectiveUsage: Usage =
            item.billing_basis === "REQUEST_COMPLETION"
              ? { requests: "1" }
              : item.billing_basis === "INPUT_CHARACTERS"
                ? { characters: item.input_characters ?? "0", requests: "1" }
                : item.usage;
          try {
            const custAtoms = BigInt(calculateCost(effectiveUsage, item.billing_pricing));
            let supAtoms: bigint;
            if (item.supplier_cost_atoms_override !== undefined) {
              const parsedSup = parseAtoms(item.supplier_cost_atoms_override);
              if (parsedSup === null || parsedSup < 0n) {
                throw new Error("Invalid supplier_cost_atoms_override");
              }
              supAtoms = parsedSup;
            } else {
              supAtoms = BigInt(calculateCost(item.usage, item.supplier_pricing));
            }
            if (custAtoms <= meta.heldCustomerAtoms && supAtoms <= meta.heldSupplierAtoms) {
              seenFastReqIds.add(item.request_id);
              fastCandidates.push({
                index: i,
                item,
                meta,
                customerAtoms: custAtoms,
                supplierAtoms: supAtoms,
              });
              continue;
            }
          } catch {
            void 0;
          }
        }
      }
      directEntries.push({ index: i, item });
    }

    if (fastCandidates.length > 0) {
      const fallback = await runFastCommitBatch(fastCandidates, results);
      for (const f of fallback) directEntries.push(f);
    }
    if (directEntries.length > 0) {
      await runDirectCommitBatch(directEntries, results);
    }
    return results;
  }

  const fastHoldQueue = new MicroBatchQueue(
    runFastHoldBatch,
    Math.max(maxBatchSize, 512),
    batchWindowMs,
    Math.max(maxBatchConcurrency, 4),
  );
  const acquireQueue = new MicroBatchQueue(runAcquireBatch, maxBatchSize, batchWindowMs, 1);
  const commitQueue = new MicroBatchQueue(
    runCommitBatch,
    Math.max(maxBatchSize, 512),
    batchWindowMs,
    2,
  );

  async function collectDebtInTx(tx: SQL, walletId: string, nowMs: number): Promise<number> {
    const wRows = await tx`
      SELECT id, project_id, currency, policy,
        available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
        debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
        credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
      FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
    `;
    if (!wRows.length) return 0;
    const wallet = toWalletRow(wRows[0]!);
    if (wallet.debt_atoms <= 0n || wallet.available_atoms <= 0n) return 0;

    const chargeRows = await tx`
      SELECT id, request_id, model, operation, debt_atoms::text, funded_atoms::text, lot_allocations
      FROM public.billing_charges
      WHERE wallet_id = ${walletId} AND debt_atoms > 0
      ORDER BY created_at, id
      LIMIT 32
      FOR UPDATE
    `;
    if (!chargeRows.length) return 0;

    const lotRowsRaw = await tx`
      SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
        issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
      FROM public.wallet_lots
      WHERE wallet_id = ${walletId} AND available_atoms > 0
      ORDER BY id
      FOR NO KEY UPDATE
    `;
    const lots = sortLots(
      lotRowsRaw.map((r: Record<string, unknown>) => toLotRow(r)),
      wallet.policy,
    );
    const journals: JournalLine[] = [];
    expireLotsInMemory(wallet, lots, nowMs, journals);

    let collectedCharges = 0;
    for (const ch of chargeRows) {
      let debt = BigInt(String(ch.debt_atoms));
      if (debt <= 0n) continue;
      let paid = 0n;
      const allocs = [...((ch.lot_allocations ?? []) as ChargeLotAllocation[])];
      const allocByLot = new Map<string, ChargeLotAllocation>();
      for (const a of allocs) allocByLot.set(a.lot_id, { ...a });
      const lines: { account: string; atoms: bigint }[] = [];
      for (const lot of lots) {
        if (debt === 0n) break;
        if (!lotEligible(lot, String(ch.model), String(ch.operation), nowMs)) continue;
        const take = debt < lot.available_atoms ? debt : lot.available_atoms;
        debt -= take;
        paid += take;
        lot.available_atoms -= take;
        lot.spent_atoms += take;
        lot.dirty = true;
        const existing = allocByLot.get(lot.id);
        if (existing) {
          existing.amount_atoms = (BigInt(existing.amount_atoms) + take).toString();
        } else {
          allocByLot.set(lot.id, {
            lot_id: lot.id,
            amount_atoms: take.toString(),
            refunded_atoms: "0",
          });
        }
        lines.push({ account: `lot:${lot.id}:available`, atoms: -take });
      }
      if (paid === 0n) continue;
      collectedCharges++;
      wallet.available_atoms -= paid;
      wallet.spent_atoms += paid;
      wallet.debt_atoms -= paid;
      wallet.freeze_reason = computeFreezeReason(wallet);
      wallet.dirty = true;
      lines.push({ account: "system:receivable", atoms: paid });
      const jid = crypto.randomUUID();
      for (let ord = 0; ord < lines.length; ord++) {
        journals.push({
          journal_id: jid,
          ordinal: ord,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: String(ch.request_id),
          reference: `debt_collect:${ch.id}:${jid}`,
          kind: "DEBT_COLLECT",
          currency: wallet.currency,
          account: lines[ord]!.account,
          atoms: lines[ord]!.atoms,
        });
      }
      const newFunded = BigInt(String(ch.funded_atoms)) + paid;
      const newOutcome = debt === 0n ? "SETTLED" : "DEBT";
      await tx`
        UPDATE public.billing_charges
        SET funded_atoms = ${newFunded.toString()}::numeric,
            debt_atoms = ${debt.toString()}::numeric,
            lot_allocations = ${[...allocByLot.values()]}::jsonb,
            outcome = ${newOutcome}
        WHERE id = ${ch.id}
      `;
    }

    await flushDirtyWallets(tx, [wallet]);
    await flushDirtyLots(tx, lots);
    await insertJournalLines(tx, journals);
    return collectedCharges;
  }

  return {
    acquireHold(input: AcquireHoldInput): Promise<AcquireHoldResult> {
      if (leaseEnabled && !draining && input.idempotency_key === null) {
        const cust = parseAtoms(input.customer_atoms);
        const sup = parseAtoms(input.supplier_atoms);
        if (cust !== null && sup !== null) {
          const nowMs = Date.now();
          const wk = walletKey(input.project_id, input.currency);
          const sk = supplierKey(input.supplier_account_id, input.supplier_currency);
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
            return fastHoldQueue.enqueue({
              input,
              customerAtoms: cust,
              supplierAtoms: sup,
              walletLease: activeWl,
              supplierLease: activeSl,
            });
          }
        }
      }
      return acquireQueue.enqueue(input);
    },

    commitTerminal(input: CommitTerminalInput): Promise<CommitTerminalResult> {
      return commitQueue.enqueue(input);
    },

    async switchSupplierAttempt(input: SwitchSupplierInput): Promise<SwitchSupplierResult> {
      const nextSup = parseAtoms(input.next_supplier_atoms);
      if (nextSup === null) {
        return {
          ok: false,
          code: "supplier_insufficient_balance",
          message: "Invalid supplier atoms",
        };
      }
      const reqCust =
        input.required_customer_atoms !== undefined
          ? parseAtoms(input.required_customer_atoms)
          : null;
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const result = await db.begin(async (tx): Promise<SwitchSupplierResult> => {
        const nowMs = Date.now();
        const holdRows = await tx`
          SELECT request_id, project_id, wallet_id, operation, model, held_atoms::text, credit_atoms::text,
            lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${input.request_id} FOR NO KEY UPDATE
        `;
        if (!holdRows.length || holdRows[0]!.state !== "HELD") {
          return {
            ok: false,
            code: "hold_not_active",
            message: "Billing hold is no longer active",
          };
        }
        const hold = holdRows[0]!;
        let heldAtoms = BigInt(String(hold.held_atoms));
        let creditAtoms = BigInt(String(hold.credit_atoms));
        let lotAllocs = [...((hold.lot_allocations ?? []) as HoldLotAllocation[])];
        const supplierHolds = [...((hold.supplier_holds ?? []) as SupplierHoldEntry[])];
        if (hold.wallet_lease_id || hold.supplier_lease_id) {
          const det = await detachHoldFromLeaseTx(tx, {
            request_id: String(hold.request_id),
            held_atoms: heldAtoms,
            wallet_lease_id: hold.wallet_lease_id ? String(hold.wallet_lease_id) : null,
            supplier_lease_id: hold.supplier_lease_id ? String(hold.supplier_lease_id) : null,
            lot_allocations: lotAllocs,
            supplier_holds: supplierHolds,
          });
          lotAllocs = det.lot_allocations;
          detached = {
            wlId: det.detachedWalletLeaseId,
            heldCust: heldAtoms,
            slId: det.detachedSupplierLeaseId,
            supAtoms: det.detachedSupplierAtoms,
          };
        }

        const walletId = String(hold.wallet_id);
        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const journals: JournalLine[] = [];

        if (reqCust !== null && reqCust > heldAtoms) {
          const freeze = wallet.freeze_reason ?? computeFreezeReason(wallet);
          if (freeze !== null) {
            return { ok: false, code: "wallet_frozen", message: `Wallet frozen: ${freeze}` };
          }
          const delta = reqCust - heldAtoms;
          const lotRowsRaw = await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots WHERE wallet_id = ${walletId} AND available_atoms > 0
            ORDER BY id FOR NO KEY UPDATE
          `;
          const lots = sortLots(
            lotRowsRaw.map((r: Record<string, unknown>) => toLotRow(r)),
            wallet.policy,
          );
          expireLotsInMemory(wallet, lots, nowMs, journals);
          let rem = delta;
          const addPlan: { lot: LotRow; take: bigint }[] = [];
          for (const lot of lots) {
            if (rem === 0n) break;
            if (!lotEligible(lot, String(hold.model), String(hold.operation), nowMs)) continue;
            const take = rem < lot.available_atoms ? rem : lot.available_atoms;
            rem -= take;
            addPlan.push({ lot, take });
          }
          const addCredit = rem;
          const addFunded = delta - addCredit;
          if (
            wallet.debt_atoms + wallet.credit_reserved_atoms + addCredit >
            wallet.credit_limit_atoms
          ) {
            return {
              ok: false,
              code: "insufficient_balance",
              message: "Insufficient balance for fallback route top-up",
            };
          }
          for (const step of addPlan) {
            step.lot.available_atoms -= step.take;
            step.lot.reserved_atoms += step.take;
            step.lot.dirty = true;
            const existing = lotAllocs.find((a) => a.lot_id === step.lot.id);
            if (existing) {
              existing.amount_atoms = (BigInt(existing.amount_atoms) + step.take).toString();
            } else {
              lotAllocs.push({ lot_id: step.lot.id, amount_atoms: step.take.toString() });
            }
          }
          wallet.available_atoms -= addFunded;
          wallet.reserved_atoms += addFunded;
          wallet.credit_reserved_atoms += addCredit;
          wallet.dirty = true;
          heldAtoms += delta;
          creditAtoms += addCredit;
          await flushDirtyLots(tx, lots);
          await flushDirtyWallets(tx, [wallet]);
          await insertJournalLines(tx, journals);
        }

        const acctRows = await tx`
          SELECT id, state FROM public.upstream_accounts
          WHERE id = ${input.next_account_id} FOR SHARE
        `;
        if (!acctRows.length || acctRows[0]!.state !== "ACTIVE") {
          return {
            ok: false,
            code: "supplier_unavailable",
            message: "Fallback supplier account is not active",
          };
        }

        const touchedAccounts = [
          ...new Set([...supplierHolds.map((s) => s.account_id), input.next_account_id]),
        ].sort();
        const cellRowsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ANY(${touchedAccounts}::uuid[])
          ORDER BY account_id, currency, cell
          FOR NO KEY UPDATE
        `;
        const cells: SupplierCellRow[] = cellRowsRaw.map((r: Record<string, unknown>) =>
          toCellRow(r),
        );
        const cellsByKey = new Map<string, SupplierCellRow>();
        for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);

        for (let idx = 0; idx < supplierHolds.length; idx++) {
          const sh = supplierHolds[idx]!;
          if (sh.attempt_id === input.failed_attempt_id && sh.state === "HELD") {
            if (input.failed_outcome === "FAILED") {
              const oldCell = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
              if (oldCell) {
                oldCell.reserved_atoms -= BigInt(sh.held_atoms);
                oldCell.revision += 1n;
                oldCell.dirty = true;
              }
              supplierHolds[idx] = { ...sh, state: "RELEASED" };
            } else {
              supplierHolds[idx] = { ...sh, state: "UNKNOWN" };
            }
          }
        }

        const nextAcctCells = cells.filter(
          (c) => c.account_id === input.next_account_id && c.currency === input.next_currency,
        );
        let chosen: SupplierCellRow | undefined;
        if (nextAcctCells.length > 0) {
          const start = preferredCell(input.next_attempt_id);
          for (let offset = 0; offset < nextAcctCells.length; offset++) {
            const c = nextAcctCells[(start + offset) % nextAcctCells.length]!;
            if (c.balance_atoms - c.reserved_atoms >= nextSup) {
              chosen = c;
              break;
            }
          }
        }
        if (!chosen) {
          return {
            ok: false,
            code: "supplier_insufficient_balance",
            message: "Fallback supplier account lacks balance",
          };
        }
        chosen.reserved_atoms += nextSup;
        chosen.revision += 1n;
        chosen.dirty = true;

        supplierHolds.push({
          attempt_id: input.next_attempt_id,
          account_id: input.next_account_id,
          channel_id: input.next_channel_id,
          currency: input.next_currency,
          cell: chosen.cell,
          held_atoms: nextSup.toString(),
          state: "HELD",
        });

        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.billing_holds
          SET held_atoms = ${heldAtoms.toString()}::numeric,
              credit_atoms = ${creditAtoms.toString()}::numeric,
              lot_allocations = ${lotAllocs}::jsonb,
              supplier_holds = ${supplierHolds}::jsonb
          WHERE request_id = ${input.request_id}
        `;

        return { ok: true, next_cell: chosen.cell, held_atoms: heldAtoms.toString() };
      });
      if (detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        notifyDetachedLocal(input.request_id, d.wlId, d.heldCust, d.slId, d.supAtoms);
      }
      return result;
    },

    async releaseHold(requestId: string): Promise<boolean> {
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const released = await db.begin(async (tx) => {
        const nowMs = Date.now();
        const rows = await tx`
          SELECT request_id, wallet_id, held_atoms::text, credit_atoms::text, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${requestId} FOR NO KEY UPDATE
        `;
        if (!rows.length) return false;
        const hold = rows[0]!;
        if (hold.state === "RELEASED") return true;
        if (hold.state !== "HELD" && hold.state !== "UNKNOWN") return false;

        const heldAtoms = BigInt(String(hold.held_atoms));
        const creditAtoms = BigInt(String(hold.credit_atoms));
        let lotAllocs = (hold.lot_allocations ?? []) as HoldLotAllocation[];
        const supHolds = (hold.supplier_holds ?? []) as SupplierHoldEntry[];
        if (hold.wallet_lease_id || hold.supplier_lease_id) {
          const det = await detachHoldFromLeaseTx(tx, {
            request_id: String(hold.request_id),
            held_atoms: heldAtoms,
            wallet_lease_id: hold.wallet_lease_id ? String(hold.wallet_lease_id) : null,
            supplier_lease_id: hold.supplier_lease_id ? String(hold.supplier_lease_id) : null,
            lot_allocations: lotAllocs,
            supplier_holds: supHolds,
          });
          lotAllocs = det.lot_allocations;
          detached = {
            wlId: det.detachedWalletLeaseId,
            heldCust: heldAtoms,
            slId: det.detachedSupplierLeaseId,
            supAtoms: det.detachedSupplierAtoms,
          };
        }

        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${hold.wallet_id} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const lotIds = lotAllocs.map((a) => a.lot_id);
        const lotRowsRaw = lotIds.length
          ? await tx`
              SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
                issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
              FROM public.wallet_lots WHERE id = ANY(${lotIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE
            `
          : [];
        const lotsById = new Map<string, LotRow>();
        for (const r of lotRowsRaw) {
          const l = toLotRow(r);
          lotsById.set(l.id, l);
        }

        const journals: JournalLine[] = [];
        let returnedAvail = 0n;
        let expiredHold = 0n;
        for (const alloc of lotAllocs) {
          const lot = lotsById.get(alloc.lot_id)!;
          const amt = BigInt(alloc.amount_atoms);
          lot.reserved_atoms -= amt;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
            lot.expired_atoms += amt;
            expiredHold += amt;
            const jid = crypto.randomUUID();
            journals.push(
              {
                journal_id: jid,
                ordinal: 0,
                side: "CUSTOMER",
                entity_id: wallet.id,
                request_id: requestId,
                reference: `release_expire:${requestId}:${lot.id}`,
                kind: "EXPIRE",
                currency: wallet.currency,
                account: `lot:${lot.id}:available`,
                atoms: -amt,
              },
              {
                journal_id: jid,
                ordinal: 1,
                side: "CUSTOMER",
                entity_id: wallet.id,
                request_id: requestId,
                reference: `release_expire:${requestId}:${lot.id}`,
                kind: "EXPIRE",
                currency: wallet.currency,
                account: `lot:${lot.id}:expired`,
                atoms: amt,
              },
            );
          } else {
            lot.available_atoms += amt;
            returnedAvail += amt;
          }
          lot.dirty = true;
        }

        wallet.reserved_atoms -= heldAtoms - creditAtoms;
        wallet.credit_reserved_atoms -= creditAtoms;
        wallet.available_atoms += returnedAvail;
        wallet.expired_atoms += expiredHold;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;

        const supAccts = [...new Set(supHolds.map((s) => s.account_id))].sort();
        const cellsRaw = supAccts.length
          ? await tx`
              SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
              FROM public.supplier_cells WHERE account_id = ANY(${supAccts}::uuid[])
              ORDER BY account_id, currency, cell FOR NO KEY UPDATE
            `
          : [];
        const cells = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        const cellsByKey = new Map<string, SupplierCellRow>();
        for (const c of cells) cellsByKey.set(cellKey(c.account_id, c.currency, c.cell), c);
        const updatedSup = supHolds.map((sh) => {
          if (sh.state === "HELD" || sh.state === "UNKNOWN") {
            const c = cellsByKey.get(cellKey(sh.account_id, sh.currency, sh.cell));
            if (c) {
              c.reserved_atoms -= BigInt(sh.held_atoms);
              c.revision += 1n;
              c.dirty = true;
            }
            return { ...sh, state: "RELEASED" as const };
          }
          return sh;
        });

        await flushDirtyWallets(tx, [wallet]);
        await flushDirtyLots(tx, lotsById.values());
        await flushDirtyCells(tx, cells);
        await insertJournalLines(tx, journals);
        await tx`
          UPDATE public.billing_holds
          SET state = 'RELEASED', supplier_holds = ${updatedSup}::jsonb, closed_at = now()
          WHERE request_id = ${requestId}
        `;
        return true;
      });
      if (detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        notifyDetachedLocal(requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
      }
      return released;
    },

    async markUnknown(requestId: string): Promise<boolean> {
      let detached: {
        wlId: string | null;
        heldCust: bigint;
        slId: string | null;
        supAtoms: bigint;
      } | null = null;
      const marked = await db.begin(async (tx) => {
        const rows = await tx`
          SELECT request_id, held_atoms::text, lot_allocations, supplier_holds, wallet_lease_id, supplier_lease_id, state
          FROM public.billing_holds WHERE request_id = ${requestId} FOR NO KEY UPDATE
        `;
        if (!rows.length || rows[0]!.state !== "HELD") return false;
        const h = rows[0]!;
        const heldAtoms = BigInt(String(h.held_atoms));
        const supHolds = (h.supplier_holds ?? []) as SupplierHoldEntry[];
        if (h.wallet_lease_id || h.supplier_lease_id) {
          const det = await detachHoldFromLeaseTx(tx, {
            request_id: String(h.request_id),
            held_atoms: heldAtoms,
            wallet_lease_id: h.wallet_lease_id ? String(h.wallet_lease_id) : null,
            supplier_lease_id: h.supplier_lease_id ? String(h.supplier_lease_id) : null,
            lot_allocations: (h.lot_allocations ?? []) as HoldLotAllocation[],
            supplier_holds: supHolds,
          });
          detached = {
            wlId: det.detachedWalletLeaseId,
            heldCust: heldAtoms,
            slId: det.detachedSupplierLeaseId,
            supAtoms: det.detachedSupplierAtoms,
          };
        }
        await tx`
          UPDATE public.billing_holds SET state = 'UNKNOWN'
          WHERE request_id = ${requestId} AND state = 'HELD'
        `;
        return true;
      });
      if (detached) {
        const d = detached as {
          wlId: string | null;
          heldCust: bigint;
          slId: string | null;
          supAtoms: bigint;
        };
        notifyDetachedLocal(requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
      }
      return marked;
    },

    async creditWallet(input: CreditWalletInput): Promise<{
      lot_id: string;
      wallet_id: string;
      replayed: boolean;
      available_atoms: string;
      debt_collected_charges: number;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n) {
        throw new Error("Credit amount must be a positive integer");
      }
      retireWalletLeaseByKey(input.project_id, input.currency);
      const fingerprint = sha256Hex({
        project_id: input.project_id,
        currency: input.currency,
        amount_atoms: input.amount_atoms,
        kind: input.kind,
        priority: input.priority ?? 100,
        expires_at: input.expires_at ?? null,
        models: input.models ?? [],
        operations: input.operations ?? [],
      });
      return db.begin(async (tx) => {
        const nowMs = Date.now();
        await tx`
          INSERT INTO public.wallets (id, project_id, currency)
          VALUES (${crypto.randomUUID()}, ${input.project_id}, ${input.currency})
          ON CONFLICT (project_id, currency) DO NOTHING
        `;
        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE project_id = ${input.project_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const existingOp = await tx`
          SELECT fingerprint, receipt FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${wallet.id} AND kind = 'CREDIT' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Credit external_ref conflict with different payload");
          }
          const rec = existingOp[0]!.receipt as { lot_id: string };
          return {
            lot_id: rec.lot_id,
            wallet_id: wallet.id,
            replayed: true,
            available_atoms: wallet.available_atoms.toString(),
            debt_collected_charges: 0,
          };
        }

        const lotId = crypto.randomUUID();
        const opId = crypto.randomUUID();
        const priority = input.priority ?? 100;
        const models = input.models ?? [];
        const operations = input.operations ?? [];
        const expiresAt = input.expires_at ?? null;

        await tx`
          INSERT INTO public.wallet_lots (id, wallet_id, external_ref, kind, priority, models, operations, expires_at, issued_atoms, available_atoms)
          VALUES (${lotId}, ${wallet.id}, ${input.external_ref}, ${input.kind}, ${priority}, ${models}, ${operations}, ${expiresAt}::timestamptz, ${atoms.toString()}::numeric, ${atoms.toString()}::numeric)
        `;
        wallet.available_atoms += atoms;
        wallet.dirty = true;
        await flushDirtyWallets(tx, [wallet]);

        const jid = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: jid,
            ordinal: 0,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `credit:${input.external_ref}`,
            kind: "CREDIT",
            currency: wallet.currency,
            account: `lot:${lotId}:available`,
            atoms,
          },
          {
            journal_id: jid,
            ordinal: 1,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `credit:${input.external_ref}`,
            kind: "CREDIT",
            currency: wallet.currency,
            account: "system:funding",
            atoms: -atoms,
          },
        ]);

        const collected = await collectDebtInTx(tx, wallet.id, nowMs);
        const refreshed =
          await tx`SELECT available_atoms::text FROM public.wallets WHERE id = ${wallet.id}`;
        const receipt = { lot_id: lotId, wallet_id: wallet.id, amount_atoms: atoms.toString() };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${wallet.id}, 'CREDIT', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${wallet.currency}, ${input.reason ?? ""}, ${receipt}::jsonb)
        `;
        return {
          lot_id: lotId,
          wallet_id: wallet.id,
          replayed: false,
          available_atoms: String(refreshed[0]!.available_atoms),
          debt_collected_charges: collected,
        };
      });
    },

    async refundCharge(input: RefundChargeInput): Promise<{
      operation_id: string;
      replayed: boolean;
      refunded_atoms: string;
      waived_debt_atoms: string;
      expired_atoms: string;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n) throw new Error("Refund amount must be positive");
      const fingerprint = sha256Hex({
        charge_id: input.charge_id,
        amount_atoms: input.amount_atoms,
        reason: input.reason,
      });
      return db.begin(async (tx) => {
        const nowMs = Date.now();
        const chRows = await tx`
          SELECT id, request_id, wallet_id, currency, customer_atoms::text, funded_atoms::text,
            debt_atoms::text, refunded_atoms::text, debt_refunded_atoms::text, lot_allocations
          FROM public.billing_charges WHERE id = ${input.charge_id} FOR UPDATE
        `;
        if (!chRows.length) throw new Error("Charge not found");
        const ch = chRows[0]!;
        const walletId = String(ch.wallet_id);

        const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${ch.id} AND kind = 'REFUND' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Refund external_ref conflict");
          }
          const rec = existingOp[0]!.receipt as {
            refunded_atoms: string;
            waived_debt_atoms: string;
            expired_atoms: string;
          };
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            refunded_atoms: rec.refunded_atoms,
            waived_debt_atoms: rec.waived_debt_atoms,
            expired_atoms: rec.expired_atoms,
          };
        }

        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);

        const customerAtoms = BigInt(String(ch.customer_atoms));
        const alreadyRefunded = BigInt(String(ch.refunded_atoms));
        if (atoms > customerAtoms - alreadyRefunded) {
          throw new Error("Refund exceeds remaining refundable charge amount");
        }

        const chargeDebt = BigInt(String(ch.debt_atoms));
        const waivedDebt = atoms < chargeDebt ? atoms : chargeDebt;
        let rem = atoms - waivedDebt;
        const refundedFromFunded = rem;

        const allocs = [...((ch.lot_allocations ?? []) as ChargeLotAllocation[])].map((a) => ({
          ...a,
        }));
        const lotIds = allocs.map((a) => a.lot_id);
        const lotRowsRaw = lotIds.length
          ? await tx`
              SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
                issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
              FROM public.wallet_lots WHERE id = ANY(${lotIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE
            `
          : [];
        const lotsById = new Map<string, LotRow>();
        for (const r of lotRowsRaw) {
          const l = toLotRow(r);
          lotsById.set(l.id, l);
        }

        const lines: { account: string; atoms: bigint }[] = [
          { account: "system:revenue", atoms: -atoms },
        ];
        if (waivedDebt > 0n) lines.push({ account: "system:receivable", atoms: waivedDebt });

        let availBack = 0n;
        let expBack = 0n;
        for (let idx = allocs.length - 1; idx >= 0 && rem > 0n; idx--) {
          const a = allocs[idx]!;
          const refundable = BigInt(a.amount_atoms) - BigInt(a.refunded_atoms);
          if (refundable <= 0n) continue;
          const take = rem < refundable ? rem : refundable;
          rem -= take;
          a.refunded_atoms = (BigInt(a.refunded_atoms) + take).toString();
          const lot = lotsById.get(a.lot_id)!;
          lot.spent_atoms -= take;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
            lot.expired_atoms += take;
            expBack += take;
            lines.push({ account: `lot:${lot.id}:expired`, atoms: take });
          } else {
            lot.available_atoms += take;
            availBack += take;
            lines.push({ account: `lot:${lot.id}:available`, atoms: take });
          }
          lot.dirty = true;
        }
        if (rem !== 0n) throw new Error("Refund allocation invariant violation");

        wallet.available_atoms += availBack;
        wallet.expired_atoms += expBack;
        wallet.spent_atoms -= refundedFromFunded;
        wallet.debt_atoms -= waivedDebt;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;

        await flushDirtyLots(tx, lotsById.values());
        await flushDirtyWallets(tx, [wallet]);

        const opId = crypto.randomUUID();
        await insertJournalLines(
          tx,
          lines.map((line, ord) => ({
            journal_id: opId,
            ordinal: ord,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: String(ch.request_id),
            reference: `refund:${input.external_ref}`,
            kind: "REFUND",
            currency: wallet.currency,
            account: line.account,
            atoms: line.atoms,
          })),
        );

        await tx`
          UPDATE public.billing_charges
          SET debt_atoms = ${(chargeDebt - waivedDebt).toString()}::numeric,
              debt_refunded_atoms = ${(BigInt(String(ch.debt_refunded_atoms)) + waivedDebt).toString()}::numeric,
              refunded_atoms = ${(alreadyRefunded + atoms).toString()}::numeric,
              lot_allocations = ${allocs}::jsonb
          WHERE id = ${ch.id}
        `;
        const receipt = {
          refunded_atoms: atoms.toString(),
          waived_debt_atoms: waivedDebt.toString(),
          expired_atoms: expBack.toString(),
        };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${ch.id}, 'REFUND', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${wallet.currency}, ${input.reason}, ${receipt}::jsonb)
        `;

        if (availBack > 0n && wallet.debt_atoms > 0n) {
          await collectDebtInTx(tx, wallet.id, nowMs);
        }

        return {
          operation_id: opId,
          replayed: false,
          ...receipt,
        };
      });
    },

    async adjustCharge(input: AdjustChargeInput): Promise<{
      operation_id: string;
      replayed: boolean;
      delta_atoms: string;
    }> {
      const delta = parseSignedAtoms(input.delta_atoms);
      if (delta === null || delta === 0n) throw new Error("Adjustment delta must be non-zero");
      if (delta < 0n) {
        const res = await this.refundCharge({
          charge_id: input.charge_id,
          external_ref: input.external_ref,
          amount_atoms: (-delta).toString(),
          reason: input.reason,
        });
        return {
          operation_id: res.operation_id,
          replayed: res.replayed,
          delta_atoms: delta.toString(),
        };
      }
      const fingerprint = sha256Hex({
        charge_id: input.charge_id,
        delta_atoms: input.delta_atoms,
        reason: input.reason,
      });
      return db.begin(async (tx) => {
        const chRows = await tx`
          SELECT id, request_id, wallet_id, currency, customer_atoms::text, debt_atoms::text
          FROM public.billing_charges WHERE id = ${input.charge_id} FOR UPDATE
        `;
        if (!chRows.length) throw new Error("Charge not found");
        const ch = chRows[0]!;
        const existingOp = await tx`
          SELECT id, fingerprint FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${ch.id} AND kind = 'ADJUST' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Adjustment external_ref conflict");
          }
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            delta_atoms: delta.toString(),
          };
        }
        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${ch.wallet_id} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        wallet.debt_atoms += delta;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;
        await flushDirtyWallets(tx, [wallet]);
        const opId = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: opId,
            ordinal: 0,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: String(ch.request_id),
            reference: `adjust:${input.external_ref}`,
            kind: "ADJUST",
            currency: wallet.currency,
            account: "system:receivable",
            atoms: -delta,
          },
          {
            journal_id: opId,
            ordinal: 1,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: String(ch.request_id),
            reference: `adjust:${input.external_ref}`,
            kind: "ADJUST",
            currency: wallet.currency,
            account: "system:revenue",
            atoms: delta,
          },
        ]);
        await tx`
          UPDATE public.billing_charges
          SET customer_atoms = ${(BigInt(String(ch.customer_atoms)) + delta).toString()}::numeric,
              debt_atoms = ${(BigInt(String(ch.debt_atoms)) + delta).toString()}::numeric,
              outcome = 'DEBT'
          WHERE id = ${ch.id}
        `;
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${ch.id}, 'ADJUST', ${input.external_ref}, ${fingerprint}, ${delta.toString()}::numeric, ${wallet.currency}, ${input.reason}, ${{ delta_atoms: delta.toString() }}::jsonb)
        `;
        await collectDebtInTx(tx, wallet.id, Date.now());
        return { operation_id: opId, replayed: false, delta_atoms: delta.toString() };
      });
    },

    async configureWalletCredit(input: {
      project_id: string;
      currency: string;
      expected_version: string;
      credit_limit_atoms: string;
      policy?: WalletPolicy;
    }): Promise<WalletSnapshot> {
      const limit = parseAtoms(input.credit_limit_atoms);
      if (limit === null) throw new Error("Invalid credit_limit_atoms");
      retireWalletLeaseByKey(input.project_id, input.currency);
      return db.begin(async (tx) => {
        const rows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE project_id = ${input.project_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        if (!rows.length) throw new Error("Wallet not found");
        const w = toWalletRow(rows[0]!);
        if (w.credit_policy_version.toString() !== input.expected_version) {
          throw new Error("Credit policy version conflict");
        }
        w.credit_limit_atoms = limit;
        w.credit_policy_version += 1n;
        if (input.policy) w.policy = input.policy;
        w.freeze_reason = computeFreezeReason(w);
        await tx`
          UPDATE public.wallets
          SET credit_limit_atoms = ${w.credit_limit_atoms.toString()}::numeric,
              credit_policy_version = ${w.credit_policy_version.toString()}::numeric,
              policy = ${w.policy},
              freeze_reason = ${w.freeze_reason}
          WHERE id = ${w.id}
        `;
        return {
          id: w.id,
          project_id: w.project_id,
          currency: w.currency,
          policy: w.policy,
          available_atoms: w.available_atoms.toString(),
          reserved_atoms: w.reserved_atoms.toString(),
          spent_atoms: w.spent_atoms.toString(),
          expired_atoms: w.expired_atoms.toString(),
          debt_atoms: w.debt_atoms.toString(),
          credit_limit_atoms: w.credit_limit_atoms.toString(),
          credit_reserved_atoms: w.credit_reserved_atoms.toString(),
          credit_policy_version: w.credit_policy_version.toString(),
          manual_frozen: w.manual_frozen,
          evidence_disputed: w.evidence_disputed,
          freeze_reason: w.freeze_reason,
        };
      });
    },

    async rechargeSupplier(input: RechargeSupplierInput): Promise<{
      operation_id: string;
      replayed: boolean;
      total_balance_atoms: string;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n) throw new Error("Recharge amount must be positive");
      const fingerprint = sha256Hex({
        account_id: input.account_id,
        currency: input.currency,
        amount_atoms: input.amount_atoms,
        reason: input.reason ?? "",
      });
      return db.begin(async (tx) => {
        const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'SUPPLIER' AND entity_id = ${input.account_id} AND kind = 'SUPPLIER_RECHARGE' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Supplier recharge external_ref conflict");
          }
          const rec = existingOp[0]!.receipt as { total_balance_atoms: string };
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            total_balance_atoms: rec.total_balance_atoms,
          };
        }

        await tx`
          INSERT INTO public.supplier_root_balances (account_id, currency, balance_atoms)
          VALUES (${input.account_id}, ${input.currency}, 0)
          ON CONFLICT (account_id, currency) DO NOTHING
        `;
        await tx`
          INSERT INTO public.supplier_cells (account_id, currency, cell, balance_atoms)
          SELECT ${input.account_id}, ${input.currency}, g::smallint, 0
          FROM generate_series(0, ${CELL_COUNT - 1}) AS g
          ON CONFLICT (account_id, currency, cell) DO NOTHING
        `;
        const rootRows = await tx`
          SELECT balance_atoms::text, revision::text
          FROM public.supplier_root_balances
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        const cellsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
          ORDER BY cell
          FOR NO KEY UPDATE
        `;
        const cells: SupplierCellRow[] = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        const rootBal = BigInt(String(rootRows[0]!.balance_atoms)) + atoms;
        const perCell = rootBal / BigInt(CELL_COUNT);
        const rem = rootBal % BigInt(CELL_COUNT);
        for (let i = 0; i < cells.length; i++) {
          const share = perCell + (BigInt(i) < rem ? 1n : 0n);
          cells[i]!.balance_atoms += share;
          cells[i]!.revision += 1n;
          cells[i]!.dirty = true;
        }
        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.supplier_root_balances
          SET balance_atoms = 0, revision = revision + 1
          WHERE account_id = ${input.account_id} AND currency = ${input.currency}
        `;
        const total = cells.reduce((acc, c) => acc + c.balance_atoms, 0n);
        const opId = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: opId,
            ordinal: 0,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_recharge:${input.external_ref}`,
            kind: "SUPPLIER_RECHARGE",
            currency: input.currency,
            account: "upstream:available",
            atoms,
          },
          {
            journal_id: opId,
            ordinal: 1,
            side: "SUPPLIER",
            entity_id: input.account_id,
            request_id: null,
            reference: `supplier_recharge:${input.external_ref}`,
            kind: "SUPPLIER_RECHARGE",
            currency: input.currency,
            account: "upstream:funding",
            atoms: -atoms,
          },
        ]);
        const receipt = { total_balance_atoms: total.toString() };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'SUPPLIER', ${input.account_id}, 'SUPPLIER_RECHARGE', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${input.currency}, ${input.reason ?? ""}, ${receipt}::jsonb)
        `;
        return { operation_id: opId, replayed: false, total_balance_atoms: total.toString() };
      });
    },

    async rebalanceSupplierCells(accountId: string, currency: string): Promise<boolean> {
      return db.begin(async (tx) => {
        const rootRows = await tx`
          SELECT balance_atoms::text FROM public.supplier_root_balances
          WHERE account_id = ${accountId} AND currency = ${currency} FOR NO KEY UPDATE
        `;
        if (!rootRows.length) return false;
        const cellsRaw = await tx`
          SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
          FROM public.supplier_cells
          WHERE account_id = ${accountId} AND currency = ${currency}
          ORDER BY cell FOR NO KEY UPDATE
        `;
        if (!cellsRaw.length) return false;
        const cells: SupplierCellRow[] = cellsRaw.map((r: Record<string, unknown>) => toCellRow(r));
        let freePool = BigInt(String(rootRows[0]!.balance_atoms));
        for (const c of cells) {
          const free = c.balance_atoms - c.reserved_atoms;
          if (free > 0n) {
            c.balance_atoms -= free;
            freePool += free;
            c.dirty = true;
          }
        }
        if (freePool <= 0n) return false;
        const perCell = freePool / BigInt(cells.length);
        const rem = freePool % BigInt(cells.length);
        for (let i = 0; i < cells.length; i++) {
          const grant = perCell + (BigInt(i) < rem ? 1n : 0n);
          cells[i]!.balance_atoms += grant;
          cells[i]!.revision += 1n;
          cells[i]!.dirty = true;
        }
        await flushDirtyCells(tx, cells);
        await tx`
          UPDATE public.supplier_root_balances
          SET balance_atoms = 0, revision = revision + 1
          WHERE account_id = ${accountId} AND currency = ${currency}
        `;
        return true;
      });
    },

    async resolveRequest(
      input: ResolveRequestInput,
    ): Promise<{ replayed: boolean; state: string }> {
      const existing = await db`
        SELECT action FROM public.billing_resolutions WHERE request_id = ${input.request_id}
      `;
      if (existing.length > 0) {
        return { replayed: true, state: String(existing[0]!.action) };
      }
      const holdRows = await db`
        SELECT state, supplier_holds FROM public.billing_holds WHERE request_id = ${input.request_id}
      `;
      if (!holdRows.length) throw new Error("Billing hold not found");
      const holdState = String(holdRows[0]!.state);
      if (holdState !== "HELD" && holdState !== "UNKNOWN") {
        throw new Error(`Hold already finalized in state ${holdState}`);
      }

      if (input.action === "RELEASE") {
        const ok = await this.releaseHold(input.request_id);
        if (!ok) throw new Error("Failed to release billing hold");
        await db`
          INSERT INTO public.billing_resolutions (id, request_id, action, billing_price_id, usage, reason, actor)
          VALUES (${crypto.randomUUID()}, ${input.request_id}, 'RELEASE', NULL, '{}'::jsonb, ${input.reason}, ${input.actor})
          ON CONFLICT (request_id) DO NOTHING
        `;
        return { replayed: false, state: "RELEASED" };
      }

      if (!input.billing_price_id) throw new Error("billing_price_id required for SETTLE");
      const priceRows = await db`
        SELECT id, currency, basis, rates FROM public.price_versions WHERE id = ${input.billing_price_id}
      `;
      if (!priceRows.length) throw new Error("Billing price version not found");
      const pr = priceRows[0]!;
      const allSup = (holdRows[0]!.supplier_holds ?? []) as SupplierHoldEntry[];
      const sh =
        [...allSup].reverse().find((s) => s.state === "HELD" || s.state === "UNKNOWN") ?? allSup[0];
      const supPricing: Pricing = {
        currency: sh?.currency ?? String(pr.currency),
        rates: {},
      };
      const commitRes = await this.commitTerminal({
        request_id: input.request_id,
        selected_attempt_id: sh?.attempt_id ?? crypto.randomUUID(),
        channel_id: sh?.channel_id ?? crypto.randomUUID(),
        billing_price_id: String(pr.id),
        cost_version_id: String(pr.id),
        usage: input.usage ?? { requests: "1" },
        billing_pricing: { currency: String(pr.currency), rates: pr.rates as Pricing["rates"] },
        billing_basis: pr.basis as "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS",
        supplier_pricing: supPricing,
        supplier_currency: sh?.currency ?? String(pr.currency),
        ...(input.supplier_cost_atoms !== undefined
          ? { supplier_cost_atoms_override: input.supplier_cost_atoms }
          : {}),
      });
      if (!commitRes.ok) throw new Error(commitRes.message);
      await db`
        INSERT INTO public.billing_resolutions (id, request_id, action, billing_price_id, usage, reason, actor)
        VALUES (${crypto.randomUUID()}, ${input.request_id}, 'SETTLE', ${input.billing_price_id}, ${input.usage ?? {}}::jsonb, ${input.reason}, ${input.actor})
        ON CONFLICT (request_id) DO NOTHING
      `;
      return { replayed: false, state: commitRes.outcome };
    },

    async expireAllLots(limit = 100): Promise<number> {
      const targetWallets = await db`
        SELECT DISTINCT wallet_id FROM public.wallet_lots
        WHERE available_atoms > 0 AND expires_at IS NOT NULL AND expires_at <= now()
        LIMIT ${limit}
      `;
      let totalWallets = 0;
      for (const row of targetWallets) {
        const wid = String(row.wallet_id);
        await db.begin(async (tx) => {
          const wRows = await tx`
            SELECT id, project_id, currency, policy,
              available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
              debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
              credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
            FROM public.wallets WHERE id = ${wid} FOR NO KEY UPDATE
          `;
          if (!wRows.length) return;
          const wallet = toWalletRow(wRows[0]!);
          const lRows = await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots WHERE wallet_id = ${wid} AND available_atoms > 0 AND expires_at <= now()
            ORDER BY id FOR NO KEY UPDATE
          `;
          const lots = lRows.map((r: Record<string, unknown>) => toLotRow(r));
          const journals: JournalLine[] = [];
          const expired = expireLotsInMemory(wallet, lots, Date.now(), journals);
          if (expired > 0n) {
            totalWallets++;
            await flushDirtyWallets(tx, [wallet]);
            await flushDirtyLots(tx, lots);
            await insertJournalLines(tx, journals);
          }
        });
      }
      return totalWallets;
    },

    async collectAllDebts(limit = 50): Promise<number> {
      const rows = await db`
        SELECT id FROM public.wallets
        WHERE debt_atoms > 0 AND available_atoms > 0
        ORDER BY id LIMIT ${limit}
      `;
      let total = 0;
      for (const r of rows) {
        total += await db.begin((tx) => collectDebtInTx(tx, String(r.id), Date.now()));
      }
      return total;
    },

    async sweepTimedOutHolds(limit = 100): Promise<number> {
      const expired = orm
        .select({ requestId: billingHolds.requestId })
        .from(billingHolds)
        .where(and(eq(billingHolds.state, "HELD"), lte(billingHolds.expiresAt, sql`now()`)))
        .orderBy(asc(billingHolds.expiresAt))
        .limit(limit);
      const rows = await orm
        .update(billingHolds)
        .set({ state: "UNKNOWN" })
        .where(inArray(billingHolds.requestId, expired))
        .returning({ requestId: billingHolds.requestId });
      return rows.length;
    },

    async sweepExpiredLeases(limit = 100): Promise<number> {
      let swept = 0;
      const expiredWalletLeases = await db`
        SELECT id, wallet_id, currency
        FROM public.wallet_leases
        WHERE state = 'ACTIVE' AND expires_at <= now()
        ORDER BY expires_at
        LIMIT ${limit}
      `;
      for (const row of expiredWalletLeases) {
        const lid = String(row.id);
        await closeWalletLeaseInPg({
          leaseId: lid,
          walletId: String(row.wallet_id),
          projectId: "",
          currency: String(row.currency),
          remainingInPg: 0n,
          freeInMemory: 0n,
          earliestLotExpiresAtMs: null,
          expiresAtMs: 0,
          inFlightHolds: 0,
          closing: true,
          lastUsedAtMs: 0,
        });
        swept++;
      }

      const expiredSupplierLeases = await db`
        SELECT id, account_id, currency, cell
        FROM public.supplier_leases
        WHERE state = 'ACTIVE' AND expires_at <= now()
        ORDER BY expires_at
        LIMIT ${limit}
      `;
      for (const row of expiredSupplierLeases) {
        const lid = String(row.id);
        await closeSupplierLeaseInPg({
          leaseId: lid,
          accountId: String(row.account_id),
          currency: String(row.currency),
          cell: Number(row.cell),
          remainingInPg: 0n,
          freeInMemory: 0n,
          expiresAtMs: 0,
          inFlightHolds: 0,
          closing: true,
          lastUsedAtMs: 0,
        });
        swept++;
      }
      return swept;
    },

    async releaseIdleLeases(): Promise<void> {
      await maintainLeases(true);
    },

    async drain(): Promise<void> {
      draining = true;
      if (maintenanceTimer) {
        clearInterval(maintenanceTimer);
        maintenanceTimer = null;
      }
      await fastHoldQueue.drain();
      await acquireQueue.drain();
      await commitQueue.drain();
      await maintainLeases(true);
    },

    async getWallet(projectId: string, currency: string): Promise<WalletSnapshot | null> {
      const rows = await orm
        .select()
        .from(wallets)
        .where(and(eq(wallets.projectId, projectId), eq(wallets.currency, currency)));
      const w = rows[0];
      if (!w) return null;
      return {
        id: w.id,
        project_id: w.projectId,
        currency: w.currency,
        policy: w.policy,
        available_atoms: w.availableAtoms,
        reserved_atoms: w.reservedAtoms,
        spent_atoms: w.spentAtoms,
        expired_atoms: w.expiredAtoms,
        debt_atoms: w.debtAtoms,
        credit_limit_atoms: w.creditLimitAtoms,
        credit_reserved_atoms: w.creditReservedAtoms,
        credit_policy_version: w.creditPolicyVersion,
        manual_frozen: w.manualFrozen,
        evidence_disputed: w.evidenceDisputed,
        freeze_reason: w.freezeReason,
      };
    },

    async getSupplierBalance(
      accountId: string,
      currency: string,
    ): Promise<{ balance_atoms: string; reserved_atoms: string; spent_atoms: string }> {
      const [rootRows, cellRows] = await Promise.all([
        orm
          .select({
            balanceAtoms: supplierRootBalances.balanceAtoms,
            spentAtoms: supplierRootBalances.spentAtoms,
          })
          .from(supplierRootBalances)
          .where(
            and(
              eq(supplierRootBalances.accountId, accountId),
              eq(supplierRootBalances.currency, currency),
            ),
          ),
        orm
          .select({
            bal: sql<string>`coalesce(sum(${supplierCells.balanceAtoms}), 0)::text`,
            res: sql<string>`coalesce(sum(${supplierCells.reservedAtoms}), 0)::text`,
            spent: sql<string>`coalesce(sum(${supplierCells.spentAtoms}), 0)::text`,
          })
          .from(supplierCells)
          .where(and(eq(supplierCells.accountId, accountId), eq(supplierCells.currency, currency))),
      ]);
      const rootBal = BigInt(rootRows[0]?.balanceAtoms ?? "0");
      const rootSpent = BigInt(rootRows[0]?.spentAtoms ?? "0");
      const cellBal = BigInt(cellRows[0]?.bal ?? "0");
      const cellRes = BigInt(cellRows[0]?.res ?? "0");
      const cellSpent = BigInt(cellRows[0]?.spent ?? "0");
      return {
        balance_atoms: (rootBal + cellBal).toString(),
        reserved_atoms: cellRes.toString(),
        spent_atoms: (rootSpent + cellSpent).toString(),
      };
    },

    async claimOutboxEvents(
      consumerName: string,
      limit = 100,
    ): Promise<{ lease_token: string; events: OutboxEventRecord[] }> {
      const leaseToken = crypto.randomUUID();
      return orm.transaction(async (tx) => {
        await tx
          .insert(outboxConsumers)
          .values({ name: consumerName, ackedSeq: 0n })
          .onConflictDoNothing({ target: outboxConsumers.name });
        const cRows = await tx
          .select({ ackedSeq: outboxConsumers.ackedSeq })
          .from(outboxConsumers)
          .where(eq(outboxConsumers.name, consumerName))
          .for("update");
        const acked = cRows[0]!.ackedSeq;
        await tx
          .update(outboxConsumers)
          .set({
            leaseToken,
            leasedUntil: sql`now() + interval '30 seconds'`,
            updatedAt: sql`now()`,
          })
          .where(eq(outboxConsumers.name, consumerName));
        const evRows = await tx
          .select()
          .from(outboxEvents)
          .where(gt(outboxEvents.seq, acked))
          .orderBy(asc(outboxEvents.seq))
          .limit(limit);
        return {
          lease_token: leaseToken,
          events: evRows.map((r) => ({
            seq: r.seq.toString(),
            event_id: r.eventId,
            kind: r.kind,
            request_id: r.requestId,
            project_id: r.projectId,
            payload: r.payload,
            created_at: new Date(r.createdAt).toISOString(),
          })),
        };
      });
    },

    async ackOutboxEvents(
      consumerName: string,
      leaseToken: string,
      throughSeq: string,
    ): Promise<boolean> {
      const rows = await orm
        .update(outboxConsumers)
        .set({
          ackedSeq: sql`greatest(${outboxConsumers.ackedSeq}, ${throughSeq}::bigint)`,
          updatedAt: sql`now()`,
        })
        .where(
          and(eq(outboxConsumers.name, consumerName), eq(outboxConsumers.leaseToken, leaseToken)),
        )
        .returning({ ackedSeq: outboxConsumers.ackedSeq });
      return rows.length > 0;
    },

    async auditConservation(): Promise<ConservationAudit> {
      const [unbalanced, brokenLots, brokenLeases, walletMismatch, holds] = await Promise.all([
        db`
          SELECT count(*)::int AS n FROM (
            SELECT journal_id FROM public.ledger_entries
            GROUP BY journal_id HAVING count(*) < 2 OR sum(atoms) <> 0
          ) bad
        `,
        db`
          SELECT count(*)::int AS n FROM public.wallet_lots
          WHERE issued_atoms <> available_atoms + reserved_atoms + spent_atoms + expired_atoms
        `,
        db`
          SELECT count(*)::int AS n FROM public.wallet_leases wl
          WHERE wl.state = 'ACTIVE' AND wl.remaining_atoms <> coalesce((
            SELECT sum((elem->>'amount_atoms')::numeric)
            FROM jsonb_array_elements(wl.lot_allocations) elem
          ), 0)
        `,
        db`
          SELECT count(*)::int AS n FROM public.wallets w
          LEFT JOIN (
            SELECT wallet_id,
              coalesce(sum(available_atoms), 0) AS avail,
              coalesce(sum(reserved_atoms), 0) AS res,
              coalesce(sum(spent_atoms), 0) AS spent,
              coalesce(sum(expired_atoms), 0) AS exp
            FROM public.wallet_lots GROUP BY wallet_id
          ) l ON l.wallet_id = w.id
          WHERE w.available_atoms <> coalesce(l.avail, 0)
             OR w.reserved_atoms <> coalesce(l.res, 0)
             OR w.spent_atoms <> coalesce(l.spent, 0)
             OR w.expired_atoms <> coalesce(l.exp, 0)
        `,
        db`
          SELECT
            count(*) FILTER (WHERE state = 'HELD')::int AS open_holds,
            count(*) FILTER (WHERE state = 'UNKNOWN')::int AS unknown_holds
          FROM public.billing_holds
        `,
      ]);
      const u = Number(unbalanced[0]!.n);
      const b = Number(brokenLots[0]!.n) + Number(brokenLeases[0]!.n);
      const m = Number(walletMismatch[0]!.n);
      return {
        ok: u === 0 && b === 0 && m === 0,
        unbalanced_journals: u,
        broken_lots: b,
        wallet_mismatches: m,
        open_holds: Number(holds[0]!.open_holds),
        unknown_holds: Number(holds[0]!.unknown_holds),
      };
    },
  };
}
