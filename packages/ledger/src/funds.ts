import type { SQL } from "bun";
import { canonical } from "@gateway/kernel";
import type { WalletPolicy } from "./types.ts";

export const MAX_ATOMS = 10n ** 38n - 1n;
export const CELL_COUNT = 16;
export type WalletRow = {
  id: string;
  billing_account_id: string;
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

const TRANSIENT_PG_STATES = new Set(["40P01", "40001", "55P03"]);

export function isTransientPgConcurrencyError(error: unknown): boolean {
  const code = postgresState(error);
  return code !== undefined && TRANSIENT_PG_STATES.has(code);
}

export function holdTemporarilyUnavailable() {
  return {
    ok: false,
    code: "ledger_busy",
    message: "Funds reservation is temporarily busy",
  } as const;
}

export async function withPgConcurrencyRetry<T>(
  run: () => Promise<T>,
  maxAttempts = 4,
): Promise<T> {
  let attempt = 0;
  for (;;) {
    try {
      return await run();
    } catch (error) {
      attempt++;
      if (attempt >= maxAttempts || !isTransientPgConcurrencyError(error)) {
        throw error;
      }
      await Bun.sleep(10 * attempt + Math.floor(Math.random() * 10));
    }
  }
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

const usageDigestCache = new Map<string, string>();
const SIMPLE_ASCII_KEY = /^[a-z0-9_]+$/;
const SIMPLE_ASCII_VAL = /^[0-9.]+$/;

function isSimpleAsciiNum(val: unknown): val is string {
  if (typeof val !== "string" || val.length === 0) return false;
  for (let i = 0; i < val.length; i++) {
    const c = val.charCodeAt(i);
    if ((c < 48 || c > 57) && c !== 46) return false;
  }
  return true;
}

function fastFlatCanonical(value: unknown): string | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
  const obj = value as Record<string, unknown>;
  let count = 0;
  let stdCount = 0;
  for (const k in obj) {
    count++;
    if (
      k === "cached_input_tokens" ||
      k === "input_tokens" ||
      k === "output_tokens" ||
      k === "reasoning_tokens"
    ) {
      stdCount++;
    }
  }
  if (count === 0) return "{}";
  if (count === stdCount) {
    let out = "{";
    let first = true;
    let stdSeen = 0;
    const c = obj.cached_input_tokens;
    if (c !== undefined) {
      if (!isSimpleAsciiNum(c)) return null;
      out += `"cached_input_tokens":"${c}"`;
      first = false;
      stdSeen++;
    }
    const i = obj.input_tokens;
    if (i !== undefined) {
      if (!isSimpleAsciiNum(i)) return null;
      out += `${first ? "" : ","}"input_tokens":"${i}"`;
      first = false;
      stdSeen++;
    }
    const o = obj.output_tokens;
    if (o !== undefined) {
      if (!isSimpleAsciiNum(o)) return null;
      out += `${first ? "" : ","}"output_tokens":"${o}"`;
      first = false;
      stdSeen++;
    }
    const r = obj.reasoning_tokens;
    if (r !== undefined) {
      if (!isSimpleAsciiNum(r)) return null;
      out += `${first ? "" : ","}"reasoning_tokens":"${r}"`;
      stdSeen++;
    }
    if (stdSeen !== stdCount) return null;
    return `${out}}`;
  }
  if (count > 12) return null;
  const keys = Object.keys(obj);
  if (keys.length > 1) keys.sort();
  let out = "{";
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const v = obj[k];
    if (typeof v !== "string" || !SIMPLE_ASCII_KEY.test(k) || !SIMPLE_ASCII_VAL.test(v)) {
      return null;
    }
    if (i > 0) out += ",";
    out += `"${k}":"${v}"`;
  }
  return `${out}}`;
}

export function sha256Hex(value: unknown): string {
  const fast = fastFlatCanonical(value);
  if (fast !== null) {
    const hit = usageDigestCache.get(fast);
    if (hit !== undefined) return hit;
    const digest = new Bun.CryptoHasher("sha256").update(fast).digest("hex");
    if (usageDigestCache.size >= 2048) {
      const oldest = usageDigestCache.keys().next().value;
      if (oldest !== undefined) usageDigestCache.delete(oldest);
    }
    usageDigestCache.set(fast, digest);
    return digest;
  }
  return new Bun.CryptoHasher("sha256").update(canonical(value)).digest("hex");
}

export function computeFreezeReason(w: WalletRow): string | null {
  if (w.evidence_disputed) return "EVIDENCE_CONFLICT";
  if (w.manual_frozen) return "MANUAL";
  if (w.debt_atoms > w.credit_limit_atoms) return "DEBT_EXCEEDS_CREDIT_LIMIT";
  if (w.debt_atoms + w.credit_reserved_atoms > w.credit_limit_atoms) return "CREDIT_LIMIT_EXCEEDED";
  return null;
}

export function walletAdmissionError(
  w: WalletRow,
): { code: "wallet_frozen" | "insufficient_balance"; message: string } | null {
  const freeze = w.freeze_reason ?? computeFreezeReason(w);
  if (freeze !== null) {
    return { code: "wallet_frozen", message: `Wallet frozen: ${freeze}` };
  }
  if (w.available_atoms + w.reserved_atoms < w.debt_atoms) {
    return { code: "insufficient_balance", message: "Wallet has a negative settled balance" };
  }
  return null;
}

export function canReserveWalletCredit(w: WalletRow, additional: bigint, existing = 0n): boolean {
  return (
    w.debt_atoms + w.credit_reserved_atoms + additional <= w.credit_limit_atoms &&
    (additional === 0n || w.credit_reserved_atoms === existing)
  );
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

export const SETTLEMENT_LANES = 4;
export const SUPPLIER_CELLS_PER_LANE = CELL_COUNT / SETTLEMENT_LANES;
export const OUTBOX_SHARDS = 16;

function hexNibble(code: number): number {
  if (code >= 48 && code <= 57) return code - 48;
  if (code >= 97 && code <= 102) return code - 87;
  if (code >= 65 && code <= 70) return code - 55;
  return -1;
}

export function outboxShard(billingAccountId: string | null | undefined): number {
  if (!billingAccountId || billingAccountId.length < 36) return 0;
  const first = hexNibble(billingAccountId.charCodeAt(0));
  const last = hexNibble(billingAccountId.charCodeAt(35));
  if (first < 0 || last < 0) return 0;
  return (first ^ last) & (OUTBOX_SHARDS - 1);
}

export function walletLane(billingAccountId: string): number {
  return outboxShard(billingAccountId) % SETTLEMENT_LANES;
}

export function cellKey(accountId: string, currency: string, cell: number): string {
  return `${accountId}:${currency}:${cell}`;
}

export function walletKey(billingAccountId: string, currency: string): string {
  return `${billingAccountId}:${currency}`;
}

export function supplierKey(accountId: string, currency: string): string {
  return `${accountId}:${currency}`;
}

export function toWalletRow(r: Record<string, unknown>): WalletRow {
  return {
    id: String(r.id),
    billing_account_id: String(r.billing_account_id),
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
