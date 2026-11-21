import type { SQL } from "bun";
import { canonical } from "@gateway/kernel";
import type { WalletPolicy } from "./types.ts";

export const MAX_ATOMS = 10n ** 38n - 1n;
export const CELL_COUNT = 16;
export type WalletRow = {
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

export type LotRow = {
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

export type SupplierCellRow = {
  account_id: string;
  currency: string;
  cell: number;
  balance_atoms: bigint;
  reserved_atoms: bigint;
  spent_atoms: bigint;
  revision: bigint;
  dirty: boolean;
};

export type JournalLine = {
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

export function postgresState(error: unknown): string | undefined {
  return error instanceof Error && "errno" in error && typeof error.errno === "string"
    ? error.errno
    : undefined;
}

export function parseAtoms(value: string): bigint | null {
  if (!/^(0|[1-9][0-9]{0,37})$/.test(value)) return null;
  const n = BigInt(value);
  if (n < 0n || n > MAX_ATOMS) return null;
  return n;
}

export function parseSignedAtoms(value: string): bigint | null {
  if (!/^-?(0|[1-9][0-9]{0,37})$/.test(value)) return null;
  const n = BigInt(value);
  if (n < -MAX_ATOMS || n > MAX_ATOMS) return null;
  return n;
}

export function sha256Hex(value: unknown): string {
  return new Bun.CryptoHasher("sha256").update(canonical(value)).digest("hex");
}

export function computeFreezeReason(w: WalletRow): string | null {
  if (w.evidence_disputed) return "EVIDENCE_CONFLICT";
  if (w.manual_frozen) return "MANUAL";
  if (w.debt_atoms > w.credit_limit_atoms) return "DEBT_EXCEEDS_CREDIT_LIMIT";
  if (w.debt_atoms + w.credit_reserved_atoms > w.credit_limit_atoms) return "CREDIT_LIMIT_EXCEEDED";
  return null;
}

export function sortLots(lots: LotRow[], policy: WalletPolicy): LotRow[] {
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

export function lotEligible(lot: LotRow, model: string, operation: string, nowMs: number): boolean {
  if (lot.available_atoms <= 0n) return false;
  if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) return false;
  if (lot.models.length > 0 && !lot.models.includes(model)) return false;
  if (lot.operations.length > 0 && !lot.operations.includes(operation)) return false;
  return true;
}

export function preferredCell(seed: string): number {
  let h = 2166136261;
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0) % CELL_COUNT;
}

export function cellKey(accountId: string, currency: string, cell: number): string {
  return `${accountId}:${currency}:${cell}`;
}

export function walletKey(projectId: string, currency: string): string {
  return `${projectId}:${currency}`;
}

export function supplierKey(accountId: string, currency: string): string {
  return `${accountId}:${currency}`;
}

export function toWalletRow(r: Record<string, unknown>): WalletRow {
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

export function toLotRow(r: Record<string, unknown>): LotRow {
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

export function toCellRow(r: Record<string, unknown>): SupplierCellRow {
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

export async function insertJournalLines(tx: SQL, lines: JournalLine[]): Promise<void> {
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

export async function flushDirtyWallets(tx: SQL, walletRows: Iterable<WalletRow>): Promise<void> {
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

export async function flushDirtyLots(tx: SQL, lots: Iterable<LotRow>): Promise<void> {
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

export async function flushDirtyCells(tx: SQL, cells: Iterable<SupplierCellRow>): Promise<void> {
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

export function expireLotsInMemory(
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
