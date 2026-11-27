import type { SwitchSupplierInput, SwitchSupplierResult, SupplierHoldEntry } from "./types.ts";
import { MicroBatchQueue } from "./micro-batch.ts";
import { type SupplierCellRow, toCellRow, preferredCell } from "./funds.ts";
import type { SQL } from "bun";
import type { HoldTransitionLeaseHooks } from "./hold-transitions.ts";

export function createSupplierSwitchQueues({
  db,
  leases,
  executeSingleSwitch,
  maxBatchSize,
  batchWindowMs,
}: {
  db: SQL;
  leases: HoldTransitionLeaseHooks;
  executeSingleSwitch: (
    input: SwitchSupplierInput,
    nextSup: bigint,
    reqCust: bigint | null,
  ) => Promise<SwitchSupplierResult>;
  maxBatchSize: number;
  batchWindowMs: number;
}) {
  type FastSwitchJob = { input: SwitchSupplierInput; nextSup: bigint };
  const fastSwitchQueue = new MicroBatchQueue<FastSwitchJob, SwitchSupplierResult>(
    async (jobs) => {
      const out = Array.from<SwitchSupplierResult | Promise<SwitchSupplierResult>>({
        length: jobs.length,
      });
      const seenReq = new Set<string>();
      const uniqueJobs: { index: number; job: FastSwitchJob }[] = [];
      const fallbackIndexes: number[] = [];
      for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i]!;
        if (seenReq.has(job.input.request_id)) {
          fallbackIndexes.push(i);
          continue;
        }
        seenReq.add(job.input.request_id);
        uniqueJobs.push({ index: i, job });
      }

      const reqIds = uniqueJobs.map((u) => u.job.input.request_id).sort();
      const acctIds = [...new Set(uniqueJobs.map((u) => u.job.input.next_account_id))].sort();
      const rebound: { requestId: string; nextAttemptId: string; nextChannelId: string }[] = [];

      if (uniqueJobs.length > 0) {
        await db.begin(async (tx) => {
          const [snap] = await tx<
            {
              holds: {
                request_id: string;
                held_atoms: string;
                supplier_holds: SupplierHoldEntry[];
                state: string;
              }[];
              accounts: { id: string; state: string }[];
            }[]
          >`
            WITH
              h AS MATERIALIZED (
                SELECT request_id, held_atoms::text, supplier_holds, state
                FROM public.billing_holds
                WHERE request_id = ANY(${reqIds}::uuid[])
                ORDER BY request_id
                FOR NO KEY UPDATE
              ),
              a AS MATERIALIZED (
                SELECT id, state
                FROM public.upstream_accounts
                WHERE (SELECT count(*) FROM h) >= 0 AND id = ANY(${acctIds}::uuid[])
                ORDER BY id
                FOR SHARE
              )
            SELECT
              COALESCE((SELECT jsonb_agg(h) FROM h), '[]'::jsonb) AS holds,
              COALESCE((SELECT jsonb_agg(a) FROM a), '[]'::jsonb) AS accounts
          `;
          const holdByReq = new Map((snap?.holds ?? []).map((h) => [String(h.request_id), h]));
          const activeAccounts = new Set(
            (snap?.accounts ?? []).filter((a) => a.state === "ACTIVE").map((a) => String(a.id)),
          );
          const updates: {
            request_id: string;
            supplier_holds: string;
            expires_at: string | null;
          }[] = [];
          for (const { index, job } of uniqueJobs) {
            const { input, nextSup } = job;
            const hold = holdByReq.get(input.request_id);
            if (!hold || hold.state !== "HELD") {
              out[index] = {
                ok: false,
                code: "hold_not_active",
                message: "Billing hold is no longer active",
              };
              continue;
            }
            const supplierHolds = [...((hold.supplier_holds ?? []) as SupplierHoldEntry[])];
            const failedIdx = supplierHolds.findIndex(
              (sh) => sh.attempt_id === input.failed_attempt_id && sh.state === "HELD",
            );
            if (failedIdx < 0) {
              fallbackIndexes.push(index);
              continue;
            }
            const oldSh = supplierHolds[failedIdx]!;
            if (
              oldSh.account_id !== input.next_account_id ||
              oldSh.currency !== input.next_currency ||
              BigInt(oldSh.held_atoms) !== nextSup
            ) {
              fallbackIndexes.push(index);
              continue;
            }
            if (!activeAccounts.has(input.next_account_id)) {
              out[index] = {
                ok: false,
                code: "supplier_unavailable",
                message: "Fallback supplier account is not active",
              };
              continue;
            }
            supplierHolds[failedIdx] = { ...oldSh, state: "RELEASED" };
            supplierHolds.push({
              attempt_id: input.next_attempt_id,
              ...(input.next_supplier_cost_version_id
                ? { cost_version_id: input.next_supplier_cost_version_id }
                : {}),
              ...(input.next_customer_price_version_id
                ? {
                    customer_price_version_id: input.next_customer_price_version_id,
                    ...(input.next_customer_price_adjustment
                      ? { customer_price_adjustment: input.next_customer_price_adjustment }
                      : {}),
                  }
                : {}),
              account_id: input.next_account_id,
              channel_id: input.next_channel_id,
              currency: input.next_currency,
              cell: oldSh.cell,
              held_atoms: nextSup.toString(),
              state: "HELD",
            });
            updates.push({
              request_id: input.request_id,
              supplier_holds: JSON.stringify(supplierHolds),
              expires_at: input.expires_at ?? null,
            });
            rebound.push({
              requestId: input.request_id,
              nextAttemptId: input.next_attempt_id,
              nextChannelId: input.next_channel_id,
            });
            out[index] = {
              ok: true,
              next_cell: oldSh.cell,
              held_atoms: String(hold.held_atoms),
            };
          }
          if (updates.length > 0) {
            await tx`
              UPDATE public.billing_holds AS h
              SET supplier_holds = u.supplier_holds::jsonb,
                  expires_at = GREATEST(h.expires_at, COALESCE(u.expires_at, h.expires_at))
              FROM unnest(
                ${updates.map((u) => u.request_id)}::uuid[],
                ${updates.map((u) => u.supplier_holds)}::text[],
                ${updates.map((u) => u.expires_at)}::timestamptz[]
              ) AS u(request_id, supplier_holds, expires_at)
              WHERE h.request_id = u.request_id
            `;
          }
        });
        for (const r of rebound) {
          leases.rebindHoldAttempt(r.requestId, r.nextAttemptId, r.nextChannelId);
        }
      }

      for (const idx of fallbackIndexes) {
        const job = jobs[idx]!;
        out[idx] = executeSingleSwitch(job.input, job.nextSup, null);
      }
      return out;
    },
    Math.max(maxBatchSize, 256),
    batchWindowMs,
    4,
  );

  const unknownSwitchQueue = new MicroBatchQueue<FastSwitchJob, SwitchSupplierResult>(
    async (jobs) => {
      const out = Array.from<SwitchSupplierResult | Promise<SwitchSupplierResult>>({
        length: jobs.length,
      });
      const seenReq = new Set<string>();
      const uniqueJobs: { index: number; job: FastSwitchJob }[] = [];
      const fallbackIndexes: number[] = [];
      for (let i = 0; i < jobs.length; i++) {
        const job = jobs[i]!;
        if (seenReq.has(job.input.request_id)) {
          fallbackIndexes.push(i);
          continue;
        }
        seenReq.add(job.input.request_id);
        uniqueJobs.push({ index: i, job });
      }

      const reqIds = uniqueJobs.map((u) => u.job.input.request_id).sort();
      const acctIds = [...new Set(uniqueJobs.map((u) => u.job.input.next_account_id))].sort();
      const detachedSuppliers: { requestId: string; slId: string | null; supAtoms: bigint }[] = [];

      if (uniqueJobs.length > 0) {
        await db.begin(async (tx) => {
          const holdRows = await tx`
            SELECT request_id, held_atoms::text, supplier_holds, supplier_lease_id, state
            FROM public.billing_holds
            WHERE request_id = ANY(${reqIds}::uuid[])
            ORDER BY request_id
            FOR NO KEY UPDATE
          `;
          const holdByReq = new Map<
            string,
            {
              request_id: string;
              held_atoms: string;
              supplier_holds: SupplierHoldEntry[];
              supplier_lease_id: string | null;
              state: string;
            }
          >();
          const slIdSet = new Set<string>();
          for (const r of holdRows) {
            const slId = r.supplier_lease_id ? String(r.supplier_lease_id) : null;
            if (r.state === "HELD" && slId) slIdSet.add(slId);
            holdByReq.set(String(r.request_id), {
              request_id: String(r.request_id),
              held_atoms: String(r.held_atoms),
              supplier_holds: (r.supplier_holds ?? []) as SupplierHoldEntry[],
              supplier_lease_id: slId,
              state: String(r.state),
            });
          }

          const slIds = [...slIdSet].sort();
          const slById = new Map<
            string,
            { id: string; remaining: bigint; state: string; dirty: boolean }
          >();
          if (slIds.length > 0) {
            const slRows = await tx`
              SELECT id, remaining_atoms::text, state
              FROM public.supplier_leases
              WHERE id = ANY(${slIds}::uuid[])
              ORDER BY id
              FOR NO KEY UPDATE
            `;
            for (const r of slRows) {
              slById.set(String(r.id), {
                id: String(r.id),
                remaining: BigInt(String(r.remaining_atoms)),
                state: String(r.state),
                dirty: false,
              });
            }
          }

          const acctRows = await tx`
            SELECT id, state FROM public.upstream_accounts
            WHERE id = ANY(${acctIds}::uuid[])
            ORDER BY id
            FOR SHARE
          `;
          const activeAccounts = new Set<string>();
          for (const r of acctRows) {
            if (r.state === "ACTIVE") activeAccounts.add(String(r.id));
          }

          const cellRowsRaw = await tx`
            SELECT account_id, currency, cell, balance_atoms::text, reserved_atoms::text, spent_atoms::text, revision::text
            FROM public.supplier_cells
            WHERE account_id = ANY(${acctIds}::uuid[])
            ORDER BY account_id, currency, cell
            FOR NO KEY UPDATE
          `;
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

          const holdUpdates: {
            request_id: string;
            supplier_holds: string;
            clear_sl: boolean;
            expires_at: string | null;
          }[] = [];
          for (const { index, job } of uniqueJobs) {
            const { input, nextSup } = job;
            const hold = holdByReq.get(input.request_id);
            if (!hold || hold.state !== "HELD") {
              out[index] = {
                ok: false,
                code: "hold_not_active",
                message: "Billing hold is no longer active",
              };
              continue;
            }
            if (!activeAccounts.has(input.next_account_id)) {
              out[index] = {
                ok: false,
                code: "supplier_unavailable",
                message: "Fallback supplier account is not active",
              };
              continue;
            }
            const supplierHolds = [...hold.supplier_holds];
            const failedIdx = supplierHolds.findIndex(
              (sh) => sh.attempt_id === input.failed_attempt_id && sh.state === "HELD",
            );
            const acctCells =
              cellsByAcctCur.get(`${input.next_account_id}:${input.next_currency}`) ?? [];
            const start = preferredCell(input.next_attempt_id);
            let chosen: SupplierCellRow | undefined;
            for (let offset = 0; offset < acctCells.length; offset++) {
              const cand = acctCells[(start + offset) % acctCells.length]!;
              if (cand.balance_atoms - cand.reserved_atoms >= nextSup) {
                chosen = cand;
                break;
              }
            }
            if (!chosen) {
              out[index] = {
                ok: false,
                code: "supplier_insufficient_balance",
                message: "Fallback supplier lacks sufficient balance",
              };
              continue;
            }

            let clearSl = false;
            if (hold.supplier_lease_id) {
              const sl = slById.get(hold.supplier_lease_id);
              if (sl && sl.state === "ACTIVE") {
                const supAtoms = supplierHolds.reduce(
                  (sum, s) =>
                    s.state === "HELD" || s.state === "UNKNOWN" ? sum + BigInt(s.held_atoms) : sum,
                  0n,
                );
                sl.remaining = sl.remaining >= supAtoms ? sl.remaining - supAtoms : 0n;
                sl.dirty = true;
                clearSl = true;
                detachedSuppliers.push({
                  requestId: input.request_id,
                  slId: sl.id,
                  supAtoms,
                });
              } else {
                detachedSuppliers.push({
                  requestId: input.request_id,
                  slId: null,
                  supAtoms: 0n,
                });
              }
            } else {
              detachedSuppliers.push({
                requestId: input.request_id,
                slId: null,
                supAtoms: 0n,
              });
            }

            if (failedIdx >= 0) {
              supplierHolds[failedIdx] = {
                ...supplierHolds[failedIdx]!,
                ...(input.failed_provider_request_id
                  ? { provider_request_id: input.failed_provider_request_id }
                  : {}),
                state: "UNKNOWN",
              };
            }
            chosen.reserved_atoms += nextSup;
            chosen.revision += 1n;
            chosen.dirty = true;
            supplierHolds.push({
              attempt_id: input.next_attempt_id,
              ...(input.next_supplier_cost_version_id
                ? { cost_version_id: input.next_supplier_cost_version_id }
                : {}),
              ...(input.next_customer_price_version_id
                ? {
                    customer_price_version_id: input.next_customer_price_version_id,
                    ...(input.next_customer_price_adjustment
                      ? { customer_price_adjustment: input.next_customer_price_adjustment }
                      : {}),
                  }
                : {}),
              account_id: input.next_account_id,
              channel_id: input.next_channel_id,
              currency: input.next_currency,
              cell: chosen.cell,
              held_atoms: nextSup.toString(),
              state: "HELD",
            });
            holdUpdates.push({
              request_id: input.request_id,
              supplier_holds: JSON.stringify(supplierHolds),
              clear_sl: clearSl,
              expires_at: input.expires_at ?? null,
            });
            out[index] = {
              ok: true,
              next_cell: chosen.cell,
              held_atoms: hold.held_atoms,
            };
          }

          const dirtySl = [...slById.values()].filter((s) => s.dirty);
          const dirtyCells = allCells.filter((c) => c.dirty);
          if (dirtySl.length > 0 || dirtyCells.length > 0 || holdUpdates.length > 0) {
            await tx`
              WITH
                upd_sl AS (
                  UPDATE public.supplier_leases AS l SET
                    remaining_atoms = u.remaining_atoms,
                    updated_at = now()
                  FROM unnest(
                    ${dirtySl.map((s) => s.id)}::uuid[],
                    ${dirtySl.map((s) => s.remaining.toString())}::numeric[]
                  ) AS u(id, remaining_atoms)
                  WHERE l.id = u.id
                ),
                upd_c AS (
                  UPDATE public.supplier_cells AS c SET
                    reserved_atoms = u.reserved_atoms,
                    revision = u.revision
                  FROM unnest(
                    ${dirtyCells.map((c) => c.account_id)}::uuid[],
                    ${dirtyCells.map((c) => c.currency)}::text[],
                    ${dirtyCells.map((c) => c.cell)}::smallint[],
                    ${dirtyCells.map((c) => c.reserved_atoms.toString())}::numeric[],
                    ${dirtyCells.map((c) => c.revision.toString())}::bigint[]
                  ) AS u(account_id, currency, cell, reserved_atoms, revision)
                  WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
                ),
                upd_h AS (
                  UPDATE public.billing_holds AS h SET
                    supplier_holds = u.supplier_holds::jsonb,
                    supplier_lease_id = CASE WHEN u.clear_sl THEN NULL ELSE h.supplier_lease_id END,
                    expires_at = GREATEST(h.expires_at, COALESCE(u.expires_at, h.expires_at))
                  FROM unnest(
                    ${holdUpdates.map((h) => h.request_id)}::uuid[],
                    ${holdUpdates.map((h) => h.supplier_holds)}::text[],
                    ${holdUpdates.map((h) => h.clear_sl)}::boolean[],
                    ${holdUpdates.map((h) => h.expires_at)}::timestamptz[]
                  ) AS u(request_id, supplier_holds, clear_sl, expires_at)
                  WHERE h.request_id = u.request_id
                )
              SELECT 1
            `;
          }
        });
        for (const d of detachedSuppliers) {
          leases.notifySupplierDetachedLocal(d.requestId, d.slId, d.supAtoms);
        }
      }

      for (const idx of fallbackIndexes) {
        const job = jobs[idx]!;
        out[idx] = executeSingleSwitch(job.input, job.nextSup, null);
      }
      return out;
    },
    Math.max(maxBatchSize, 256),
    batchWindowMs,
    1,
  );

  return { fastSwitchQueue, unknownSwitchQueue };
}
