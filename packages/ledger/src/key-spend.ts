import type { SQL } from "bun";
import type { ApiKeySpendSnapshot, SpendLimitWindow } from "./types.ts";

export type KeySpendRow = {
  id: string;
  project_id: string;
  spend_limit_atoms: bigint | null;
  spend_limit_window: SpendLimitWindow;
  spend_window_start_ms: number;
  spent_atoms: bigint;
  reserved_atoms: bigint;
  dirty: boolean;
};

export function computeSpendWindowStartMs(
  window: SpendLimitWindow,
  nowMs: number,
  storedStartMs: number,
): number {
  if (window === "LIFETIME") return storedStartMs;
  const d = new Date(nowMs);
  if (window === "DAILY") {
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  }
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
}

export function rolloverKeySpendInMemory(row: KeySpendRow, nowMs: number): void {
  const targetStart = computeSpendWindowStartMs(
    row.spend_limit_window,
    nowMs,
    row.spend_window_start_ms,
  );
  if (targetStart > row.spend_window_start_ms) {
    row.spend_window_start_ms = targetStart;
    row.spent_atoms = 0n;
    row.dirty = true;
  }
}

export function toKeySpendRow(raw: Record<string, unknown>): KeySpendRow {
  return {
    id: String(raw.id),
    project_id: String(raw.project_id),
    spend_limit_atoms:
      raw.spend_limit_atoms === null || raw.spend_limit_atoms === undefined
        ? null
        : BigInt(String(raw.spend_limit_atoms)),
    spend_limit_window: String(raw.spend_limit_window ?? "LIFETIME") as SpendLimitWindow,
    spend_window_start_ms: Date.parse(String(raw.spend_window_start)),
    spent_atoms: BigInt(String(raw.spent_atoms ?? "0")),
    reserved_atoms: BigInt(String(raw.reserved_atoms ?? "0")),
    dirty: false,
  };
}

export async function flushDirtyKeySpends(tx: SQL, rows: Iterable<KeySpendRow>): Promise<void> {
  const dirty: KeySpendRow[] = [];
  for (const r of rows) {
    if (r.dirty) dirty.push(r);
  }
  if (dirty.length === 0) return;
  await tx`
    UPDATE public.api_keys AS k SET
      spend_window_start = u.spend_window_start,
      spent_atoms = u.spent_atoms,
      reserved_atoms = u.reserved_atoms
    FROM unnest(
      ${dirty.map((r) => r.id)}::uuid[],
      ${dirty.map((r) => new Date(r.spend_window_start_ms).toISOString())}::timestamptz[],
      ${dirty.map((r) => r.spent_atoms.toString())}::numeric[],
      ${dirty.map((r) => r.reserved_atoms.toString())}::numeric[]
    ) AS u(id, spend_window_start, spent_atoms, reserved_atoms)
    WHERE k.id = u.id
  `;
}

export async function releaseKeyReservationInTx(
  tx: SQL,
  apiKeyId: string | null | undefined,
  keyReservedAtoms: bigint,
): Promise<void> {
  if (!apiKeyId || keyReservedAtoms <= 0n) return;
  await tx`
    UPDATE public.api_keys
    SET reserved_atoms = GREATEST(0, reserved_atoms - ${keyReservedAtoms.toString()}::numeric)
    WHERE id = ${apiKeyId}
  `;
}

export async function getApiKeySpend(
  db: SQL,
  keyId: string,
  nowMs = Date.now(),
): Promise<ApiKeySpendSnapshot | null> {
  const rows = await db`
    SELECT id, project_id, spend_limit_atoms::text, spend_limit_window,
      spend_window_start, spent_atoms::text, reserved_atoms::text
    FROM public.api_keys
    WHERE id = ${keyId}
  `;
  if (!rows.length) return null;
  const row = toKeySpendRow(rows[0]!);
  rolloverKeySpendInMemory(row, nowMs);
  const remaining =
    row.spend_limit_atoms === null
      ? null
      : row.spend_limit_atoms > row.spent_atoms + row.reserved_atoms
        ? (row.spend_limit_atoms - row.spent_atoms - row.reserved_atoms).toString()
        : "0";
  return {
    id: row.id,
    project_id: row.project_id,
    spend_limit_atoms: row.spend_limit_atoms === null ? null : row.spend_limit_atoms.toString(),
    spend_limit_window: row.spend_limit_window,
    spend_window_start: new Date(row.spend_window_start_ms).toISOString(),
    spent_atoms: row.spent_atoms.toString(),
    reserved_atoms: row.reserved_atoms.toString(),
    remaining_atoms: remaining,
  };
}

export async function resetApiKeySpend(
  db: SQL,
  keyId: string,
): Promise<ApiKeySpendSnapshot | null> {
  const rows = await db`
    UPDATE public.api_keys
    SET spent_atoms = 0, spend_window_start = now()
    WHERE id = ${keyId}
    RETURNING id, project_id, spend_limit_atoms::text, spend_limit_window,
      spend_window_start, spent_atoms::text, reserved_atoms::text
  `;
  if (!rows.length) return null;
  const row = toKeySpendRow(rows[0]!);
  const remaining =
    row.spend_limit_atoms === null
      ? null
      : row.spend_limit_atoms > row.reserved_atoms
        ? (row.spend_limit_atoms - row.reserved_atoms).toString()
        : "0";
  return {
    id: row.id,
    project_id: row.project_id,
    spend_limit_atoms: row.spend_limit_atoms === null ? null : row.spend_limit_atoms.toString(),
    spend_limit_window: row.spend_limit_window,
    spend_window_start: new Date(row.spend_window_start_ms).toISOString(),
    spent_atoms: "0",
    reserved_atoms: row.reserved_atoms.toString(),
    remaining_atoms: remaining,
  };
}
