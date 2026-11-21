import type { SQL } from "bun";
import { and, asc, eq, gt, inArray, lte, sql } from "drizzle-orm";
import { calculateCost, canonical, type Pricing, type Usage } from "@gateway/kernel";
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

async function flushDirtyWallets(tx: SQL, wallets: Iterable<WalletRow>): Promise<void> {
  const dirty = [...wallets].filter((w) => w.dirty);
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
        });
    }
  }
}

export type LedgerOptions = {
  batchWindowMs?: number;
  maxBatchSize?: number;
  maxBatchConcurrency?: number;
};

export function createLedgerEngine(db: SQL, options: LedgerOptions = {}) {
  const orm = createDrizzle(db);
  const batchWindowMs = options.batchWindowMs ?? 2;
  const maxBatchSize = options.maxBatchSize ?? 64;
  const maxBatchConcurrency = options.maxBatchConcurrency ?? 8;

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

      const cellRowsRaw = supplierAccounts.length
        ? await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ANY(${supplierAccounts}::uuid[])
            ORDER BY account_id, currency, cell
            FOR NO KEY UPDATE
          `
        : [];
      const cellsByAcctCur = new Map<string, SupplierCellRow[]>();
      const allCells: SupplierCellRow[] = [];
      for (const r of cellRowsRaw) {
        const c = toCellRow(r);
        allCells.push(c);
        const k = `${c.account_id}:${c.currency}`;
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

        const wallet = walletsByKey.get(walletKey(item.project_id, item.currency));
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

        const acctCells =
          cellsByAcctCur.get(`${item.supplier_account_id}:${item.supplier_currency}`) ?? [];
        let chosenCell: SupplierCellRow | undefined;
        if (acctCells.length > 0) {
          const start = preferredCell(item.attempt_id);
          for (let offset = 0; offset < acctCells.length; offset++) {
            const idx = (start + offset) % acctCells.length;
            const candidate = acctCells[idx]!;
            if (candidate.balance_atoms - candidate.reserved_atoms >= supplierAtoms) {
              chosenCell = candidate;
              break;
            }
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

      await flushDirtyWallets(tx, walletsById.values());
      await flushDirtyLots(tx, allLots);
      await flushDirtyCells(tx, allCells);
      await insertJournalLines(tx, journals);

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

    return results;
  }

  async function runCommitBatch(items: CommitTerminalInput[]): Promise<CommitTerminalResult[]> {
    const results = Array.from<CommitTerminalResult>({ length: items.length });
    const requestIds = [...new Set(items.map((i) => i.request_id))];

    await db.begin(async (tx) => {
      const nowMs = Date.now();
      const holdRows = await tx`
        SELECT request_id, project_id, wallet_id, operation, model, held_atoms::text, credit_atoms::text,
          lot_allocations, supplier_holds, state
        FROM public.billing_holds
        WHERE request_id = ANY(${requestIds}::uuid[])
        ORDER BY request_id
        FOR UPDATE
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
          state: String(r.state),
        });
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

      const supplierAccountIds = [
        ...new Set(
          [...holdsByReq.values()].flatMap((h) => h.supplier_holds.map((s) => s.account_id)),
        ),
      ].sort();
      const cellRowsRaw = supplierAccountIds.length
        ? await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ANY(${supplierAccountIds}::uuid[])
            ORDER BY account_id, currency, cell
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
      }[] = [];
      const outboxBatch: {
        event_id: string;
        kind: string;
        request_id: string;
        project_id: string;
        payload: Record<string, unknown>;
      }[] = [];

      for (let i = 0; i < items.length; i++) {
        const item = items[i]!;
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
          supplierCostAtoms = BigInt(calculateCost(item.usage, item.supplier_pricing));
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

        if (custLines.length >= 2) {
          const jid = crypto.randomUUID();
          const kind =
            billableCustomerAtoms === 0n
              ? "EXPIRE"
              : hold.state === "UNKNOWN"
                ? "SETTLE"
                : "SETTLE";
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

        const updatedSupplierHolds: SupplierHoldEntry[] = [];
        for (const sh of hold.supplier_holds) {
          if (sh.attempt_id === item.selected_attempt_id && sh.state === "HELD") {
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
            lot_allocations, plugin_audit, charge_policy, outcome
          )
          SELECT
            u.id, u.request_id, u.project_id, u.wallet_id, u.selected_attempt_id, u.channel_id,
            u.model, u.operation, u.currency, u.billing_price_id, u.cost_version_id, u.usage::jsonb, u.usage_digest,
            u.customer_atoms, u.funded_atoms, u.debt_atoms, u.supplier_cost_atoms, u.supplier_currency,
            u.lot_allocations::jsonb, u.plugin_audit::jsonb, u.charge_policy, u.outcome
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
            ${newCharges.map((c) => c.outcome)}::text[]
          ) AS u(
            id, request_id, project_id, wallet_id, selected_attempt_id, channel_id,
            model, operation, currency, billing_price_id, cost_version_id, usage, usage_digest,
            customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
            lot_allocations, plugin_audit, charge_policy, outcome
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

    return results;
  }

  const acquireQueue = new MicroBatchQueue(
    runAcquireBatch,
    maxBatchSize,
    batchWindowMs,
    maxBatchConcurrency,
  );
  const commitQueue = new MicroBatchQueue(
    runCommitBatch,
    maxBatchSize,
    batchWindowMs,
    maxBatchConcurrency,
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
      return db.begin(async (tx) => {
        const nowMs = Date.now();
        const holdRows = await tx`
          SELECT request_id, project_id, wallet_id, operation, model, held_atoms::text, credit_atoms::text,
            lot_allocations, supplier_holds, state
          FROM public.billing_holds WHERE request_id = ${input.request_id} FOR UPDATE
        `;
        if (!holdRows.length || holdRows[0]!.state !== "HELD") {
          return {
            ok: false,
            code: "hold_not_active",
            message: "Billing hold is no longer active",
          };
        }
        const hold = holdRows[0]!;
        const supplierHolds = [...((hold.supplier_holds ?? []) as SupplierHoldEntry[])];
        const walletId = String(hold.wallet_id);
        let heldAtoms = BigInt(String(hold.held_atoms));
        let creditAtoms = BigInt(String(hold.credit_atoms));
        const lotAllocs = [...((hold.lot_allocations ?? []) as HoldLotAllocation[])];

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
            if (existing)
              existing.amount_atoms = (BigInt(existing.amount_atoms) + step.take).toString();
            else lotAllocs.push({ lot_id: step.lot.id, amount_atoms: step.take.toString() });
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
    },

    async releaseHold(requestId: string): Promise<boolean> {
      return db.begin(async (tx) => {
        const nowMs = Date.now();
        const rows = await tx`
          SELECT request_id, wallet_id, held_atoms::text, credit_atoms::text, lot_allocations, supplier_holds, state
          FROM public.billing_holds WHERE request_id = ${requestId} FOR UPDATE
        `;
        if (!rows.length) return false;
        const hold = rows[0]!;
        if (hold.state === "RELEASED") return true;
        if (hold.state !== "HELD" && hold.state !== "UNKNOWN") return false;

        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets WHERE id = ${hold.wallet_id} FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const lotAllocs = (hold.lot_allocations ?? []) as HoldLotAllocation[];
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

        const heldAtoms = BigInt(String(hold.held_atoms));
        const creditAtoms = BigInt(String(hold.credit_atoms));
        wallet.reserved_atoms -= heldAtoms - creditAtoms;
        wallet.credit_reserved_atoms -= creditAtoms;
        wallet.available_atoms += returnedAvail;
        wallet.expired_atoms += expiredHold;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;

        const supHolds = (hold.supplier_holds ?? []) as SupplierHoldEntry[];
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
    },

    async markUnknown(requestId: string): Promise<boolean> {
      const rows = await orm
        .update(billingHolds)
        .set({ state: "UNKNOWN" })
        .where(and(eq(billingHolds.requestId, requestId), eq(billingHolds.state, "HELD")))
        .returning({ requestId: billingHolds.requestId });
      return rows.length > 0;
    },

    async creditWallet(input: CreditWalletInput): Promise<{
      lot_id: string;
      wallet_id: string;
      replayed: boolean;
      available_atoms: string;
      debt_collected_charges: number;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n)
        throw new Error("Credit amount must be a positive integer");
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
        for (let c = 0; c < CELL_COUNT; c++) {
          await tx`
            INSERT INTO public.supplier_cells (account_id, currency, cell, balance_atoms)
            VALUES (${input.account_id}, ${input.currency}, ${c}, 0)
            ON CONFLICT (account_id, currency, cell) DO NOTHING
          `;
        }
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
      await db`
        INSERT INTO public.billing_resolutions (id, request_id, action, billing_price_id, usage, reason, actor)
        VALUES (${crypto.randomUUID()}, ${input.request_id}, ${input.action}, ${input.billing_price_id ?? null}, ${input.usage ?? {}}::jsonb, ${input.reason}, ${input.actor})
      `;
      if (input.action === "RELEASE") {
        await this.releaseHold(input.request_id);
        return { replayed: false, state: "RELEASED" };
      }
      if (!input.billing_price_id) throw new Error("billing_price_id required for SETTLE");
      const priceRows = await db`
        SELECT id, currency, basis, rates FROM public.price_versions WHERE id = ${input.billing_price_id}
      `;
      if (!priceRows.length) throw new Error("Billing price version not found");
      const pr = priceRows[0]!;
      const holdRows = await db`
        SELECT supplier_holds FROM public.billing_holds WHERE request_id = ${input.request_id}
      `;
      const sh = ((holdRows[0]?.supplier_holds ?? []) as SupplierHoldEntry[])[0];
      const supPricing: Pricing = {
        currency: sh?.currency ?? String(pr.currency),
        rates: input.supplier_cost_atoms
          ? { requests: { numerator: input.supplier_cost_atoms, denominator: "1" } }
          : {},
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
      });
      if (!commitRes.ok) throw new Error(commitRes.message);
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
      const [unbalanced, brokenLots, walletMismatch, holds] = await Promise.all([
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
      const b = Number(brokenLots[0]!.n);
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
