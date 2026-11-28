import { type KeySpendRow, toKeySpendRow, rolloverKeySpendInMemory } from "./key-spend.ts";
import {
  type WalletRow,
  toWalletRow,
  walletKey,
  type LotRow,
  toLotRow,
  sortLots,
  SUPPLIER_CELLS_PER_LANE,
  CELL_COUNT,
  preferredCell,
  walletLane,
  type SupplierCellRow,
  toCellRow,
  supplierKey,
} from "./funds.ts";
import type { SupplierHoldEntry, AcquireHoldInput } from "./types.ts";
import type { SQL } from "bun";

async function lockFundedSupplierCell(
  tx: SQL,
  item: AcquireHoldInput,
  supplierAtoms: bigint | null,
) {
  if (supplierAtoms === null) return [];
  const select = tx`
    SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text,
      spent_atoms::text, revision::text
    FROM public.supplier_cells
    WHERE account_id = ${item.supplier_account_id} AND currency = ${item.supplier_currency}
      AND balance_atoms - reserved_atoms >= ${supplierAtoms.toString()}::numeric
    ORDER BY (cell - ${preferredCell(item.attempt_id)} + ${CELL_COUNT}) % ${CELL_COUNT}
    LIMIT 1 FOR NO KEY UPDATE
  `;
  const available = await tx`${select} SKIP LOCKED`;
  return available.length ? available : await select;
}

export async function lockTransactionalHoldState({
  keyIds,
  tx,
  batchStartedAt,
  billingAccountIds,
  supplierAccounts,
  batchCellCursor,
  cellOffsets,
  items,
  retryContended,
  parsed,
  idempotencyKeys,
}: {
  keyIds: string[];
  tx: SQL;
  batchStartedAt: number;
  billingAccountIds: string[];
  supplierAccounts: string[];
  batchCellCursor: () => number;
  cellOffsets: Buffer<ArrayBufferLike>;
  items: AcquireHoldInput[];
  retryContended: boolean;
  parsed: { item: AcquireHoldInput; customerAtoms: bigint | null; supplierAtoms: bigint | null }[];
  idempotencyKeys: string[];
}) {
  const keySpendsById = new Map<string, KeySpendRow>();
  if (keyIds.length > 0) {
    const [pgNowRow] = await tx`SELECT floor(extract(epoch from now()) * 1000)::bigint AS ms`;
    const pgNowMs = pgNowRow ? Number(pgNowRow.ms) : batchStartedAt;
    const lockedKeys = await tx`
          SELECT id, billing_account_id, spend_limit_atoms::text, spend_limit_window,
            spend_window_start, spent_atoms::text, reserved_atoms::text
          FROM public.api_keys
          WHERE id = ANY(${keyIds}::uuid[])
          ORDER BY id
          FOR NO KEY UPDATE
        `;
    for (const r of lockedKeys) {
      const row = toKeySpendRow(r);
      rolloverKeySpendInMemory(row, pgNowMs);
      keySpendsById.set(row.id, row);
    }
  }

  const lockedWallets = await tx`
        SELECT id, billing_account_id, currency, policy,
          available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
          debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
          credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
        FROM public.wallets
        WHERE billing_account_id = ANY(${billingAccountIds}::uuid[])
        ORDER BY id
        FOR NO KEY UPDATE
      `;
  const walletsByKey = new Map<string, WalletRow>();
  const walletsById = new Map<string, WalletRow>();
  for (const r of lockedWallets) {
    const w = toWalletRow(r);
    walletsByKey.set(walletKey(w.billing_account_id, w.currency), w);
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

  const cursor = batchCellCursor() % SUPPLIER_CELLS_PER_LANE;
  const neededLanes = [...new Set(items.map((item) => walletLane(item.billing_account_id)))].sort(
    (a, b) => a - b,
  );
  const laneOffsets = neededLanes.map(
    (lane) => (cellOffsets[lane]! + cursor) % SUPPLIER_CELLS_PER_LANE,
  );
  const lockLaneCells = async (skipLocked: boolean) =>
    skipLocked
      ? tx`
          SELECT c.account_id, c.currency, c.cell, c.balance_atoms::text, c.reserved_atoms::text, c.spent_atoms::text, c.revision::text
          FROM (
            SELECT DISTINCT account_id, currency
            FROM public.supplier_cells
            WHERE account_id = ANY(${supplierAccounts}::uuid[])
            ORDER BY account_id, currency
          ) pairs
          CROSS JOIN unnest(${neededLanes}::smallint[], ${laneOffsets}::smallint[]) AS ln(lane, cell_offset)
          CROSS JOIN LATERAL (
            SELECT c.account_id, c.currency, c.cell, c.balance_atoms, c.reserved_atoms, c.spent_atoms, c.revision
            FROM public.supplier_cells c
            WHERE c.account_id = pairs.account_id
              AND c.currency = pairs.currency
              AND c.cell >= ln.lane * ${SUPPLIER_CELLS_PER_LANE}
              AND c.cell < (ln.lane + 1) * ${SUPPLIER_CELLS_PER_LANE}
            ORDER BY
              (c.balance_atoms - c.reserved_atoms <= 0),
              (
                SELECT count(*)
                FROM public.supplier_leases sl
                WHERE sl.account_id = c.account_id
                  AND sl.currency = c.currency
                  AND sl.cell = c.cell
                  AND sl.state = 'ACTIVE'
                  AND sl.expires_at > clock_timestamp()
              ) ASC,
              (c.reserved_atoms > 0) ASC,
              c.revision ASC,
              (((c.cell % ${SUPPLIER_CELLS_PER_LANE}) - ln.cell_offset + ${SUPPLIER_CELLS_PER_LANE}) % ${SUPPLIER_CELLS_PER_LANE}) ASC
            LIMIT 1
            FOR NO KEY UPDATE OF c SKIP LOCKED
          ) c
          ORDER BY c.account_id, c.currency, c.cell
        `
      : tx`
          SELECT c.account_id, c.currency, c.cell, c.balance_atoms::text, c.reserved_atoms::text, c.spent_atoms::text, c.revision::text
          FROM (
            SELECT DISTINCT account_id, currency
            FROM public.supplier_cells
            WHERE account_id = ANY(${supplierAccounts}::uuid[])
            ORDER BY account_id, currency
          ) pairs
          CROSS JOIN unnest(${neededLanes}::smallint[], ${laneOffsets}::smallint[]) AS ln(lane, cell_offset)
          CROSS JOIN LATERAL (
            SELECT c.account_id, c.currency, c.cell, c.balance_atoms, c.reserved_atoms, c.spent_atoms, c.revision
            FROM public.supplier_cells c
            WHERE c.account_id = pairs.account_id
              AND c.currency = pairs.currency
              AND c.cell >= ln.lane * ${SUPPLIER_CELLS_PER_LANE}
              AND c.cell < (ln.lane + 1) * ${SUPPLIER_CELLS_PER_LANE}
            ORDER BY
              (c.balance_atoms - c.reserved_atoms <= 0),
              (
                SELECT count(*)
                FROM public.supplier_leases sl
                WHERE sl.account_id = c.account_id
                  AND sl.currency = c.currency
                  AND sl.cell = c.cell
                  AND sl.state = 'ACTIVE'
                  AND sl.expires_at > clock_timestamp()
              ) ASC,
              (c.reserved_atoms > 0) ASC,
              c.revision ASC,
              (((c.cell % ${SUPPLIER_CELLS_PER_LANE}) - ln.cell_offset + ${SUPPLIER_CELLS_PER_LANE}) % ${SUPPLIER_CELLS_PER_LANE}) ASC
            LIMIT 1
            FOR NO KEY UPDATE OF c
          ) c
          ORDER BY c.account_id, c.currency, c.cell
        `;
  let cellRowsRaw;
  if (retryContended) {
    cellRowsRaw = await lockFundedSupplierCell(tx, parsed[0]!.item, parsed[0]!.supplierAtoms);
  } else {
    cellRowsRaw = await lockLaneCells(true);
    if (cellRowsRaw.length === 0 && supplierAccounts.length > 0) {
      cellRowsRaw = await lockLaneCells(false);
    }
  }
  const targetCells = [...new Set(cellRowsRaw.map((r: Record<string, unknown>) => Number(r.cell)))];
  const cellsByAcctCur = new Map<string, SupplierCellRow[]>();
  const allCells: SupplierCellRow[] = [];
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
          SELECT request_id, billing_account_id, operation, idempotency_key, wallet_id, fingerprint,
            held_atoms::text, credit_atoms::text, state, supplier_holds
          FROM public.billing_holds
          WHERE billing_account_id = ANY(${billingAccountIds}::uuid[])
            AND idempotency_key = ANY(${idempotencyKeys}::text[])
        `;
    for (const r of rows) {
      existingIdem.set(`${r.billing_account_id}:${r.operation}:${r.idempotency_key}`, {
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

  return {
    walletsById,
    lotsByWallet,
    walletsByKey,
    activeAccounts,
    keySpendsById,
    cellsByAcctCur,
    targetCells,
    allCells,
    allLots,
    existingIdem,
  };
}
