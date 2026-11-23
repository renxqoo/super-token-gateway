import type { SQL } from "bun";
import { carveFromLeaseLots, earliestExpiryOf, type LeasedHoldMeta } from "./leases.ts";
import type {
  ChargeLotAllocation,
  CommitTerminalInput,
  CommitTerminalResult,
  LeaseLotAllocation,
} from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { sha256Hex, cellKey, type JournalLine } from "./funds.ts";

const UUID_POOL = Buffer.allocUnsafe(4096);
let uuidHex = "";
let uuidHexOffset = 8192;

function fastUuid(): string {
  if (uuidHexOffset + 32 > 8192) {
    crypto.getRandomValues(UUID_POOL);
    for (let i = 0; i < 4096; i += 16) {
      UUID_POOL[i + 6] = (UUID_POOL[i + 6]! & 0x0f) | 0x40;
      UUID_POOL[i + 8] = (UUID_POOL[i + 8]! & 0x3f) | 0x80;
    }
    uuidHex = UUID_POOL.toString("hex");
    uuidHexOffset = 0;
  }
  const o = uuidHexOffset;
  uuidHexOffset += 32;
  return `${uuidHex.slice(o, o + 8)}-${uuidHex.slice(o + 8, o + 12)}-${uuidHex.slice(o + 12, o + 16)}-${uuidHex.slice(o + 16, o + 20)}-${uuidHex.slice(o + 20, o + 32)}`;
}

export type FastCommitCandidate = {
  index: number;
  item: CommitTerminalInput;
  meta: LeasedHoldMeta;
  customerAtoms: bigint;
  supplierAtoms: bigint;
};

export async function commitLeasedBatch(
  db: SQL,
  leases: LeaseManager,
  candidates: FastCommitCandidate[],
  results: CommitTerminalResult[],
): Promise<{ index: number; item: CommitTerminalInput }[]> {
  const fallback: { index: number; item: CommitTerminalInput }[] = [];
  if (candidates.length === 0) return fallback;

  const nowMs = Date.now();
  const nextExpMs = nowMs + leases.ttlMs;
  const renewThresholdMs = nowMs + Math.floor(leases.ttlMs / 2);
  const nowIso = new Date(nowMs).toISOString();
  const nextExpIso = new Date(nextExpMs).toISOString();
  const renewThresholdIso = new Date(renewThresholdMs).toISOString();

  const serialized = candidates.map((cand) => ({
    cand,
    chargeId: fastUuid(),
    eventId: fastUuid(),
    usageDigest: sha256Hex(cand.item.usage),
    customerAtomsText: cand.customerAtoms.toString(),
    supplierAtomsText: cand.supplierAtoms.toString(),
  }));

  const fastPayload = JSON.stringify(
    serialized.map((s) => ({
      charge_id: s.chargeId,
      event_id: s.eventId,
      request_id: s.cand.item.request_id,
      project_id: s.cand.meta.projectId,
      wallet_id: s.cand.meta.walletId,
      api_key_id: s.cand.item.api_key_id ?? s.cand.meta.apiKeyId,
      pricing_override_id: s.cand.item.pricing_override_id ?? s.cand.meta.pricingOverrideId,
      discount_numerator: s.cand.item.discount_numerator ?? s.cand.meta.discountNumerator,
      discount_denominator: s.cand.item.discount_denominator ?? s.cand.meta.discountDenominator,
      wallet_lease_id: s.cand.meta.walletLeaseId,
      supplier_lease_id: s.cand.meta.supplierLeaseId,
      selected_attempt_id: s.cand.item.selected_attempt_id,
      channel_id: s.cand.item.channel_id,
      model: s.cand.meta.model,
      operation: s.cand.meta.operation,
      currency: s.cand.meta.currency,
      billing_price_id: s.cand.item.billing_price_id,
      cost_version_id: s.cand.item.cost_version_id,
      usage: s.cand.item.usage,
      usage_digest: s.usageDigest,
      customer_atoms: s.customerAtomsText,
      supplier_atoms: s.supplierAtomsText,
      supplier_currency: s.cand.item.supplier_currency,
      plugin_audit: s.cand.item.plugin_audit ?? [],
      charge_policy: s.cand.item.charge_policy ?? "CHARGE_CUSTOMER",
    })),
  );

  const [fastRow] = await db<
    {
      res: {
        fast: boolean;
        wl_expiries?: Record<string, number>;
        sl_expiries?: Record<string, number>;
      };
    }[]
  >`
    SELECT public.commit_leased_batch(
      ${fastPayload}::text::jsonb,
      ${nextExpIso}::timestamptz,
      ${renewThresholdIso}::timestamptz,
      ${nowIso}::timestamptz
    ) AS res
  `;

  if (fastRow?.res?.fast) {
    const wlExp = fastRow.res.wl_expiries ?? {};
    const slExp = fastRow.res.sl_expiries ?? {};
    const postNow = Date.now();
    for (const s of serialized) {
      results[s.cand.index] = {
        ok: true,
        replayed: false,
        charge_id: s.chargeId,
        outcome: "SETTLED",
        customer_atoms: s.customerAtomsText,
        funded_atoms: s.customerAtomsText,
        debt_atoms: "0",
        supplier_cost_atoms: s.supplierAtomsText,
      };
      const wlExpMs = Number(wlExp[s.cand.meta.walletLeaseId] ?? 0);
      const slExpMs = Number(slExp[s.cand.meta.supplierLeaseId] ?? 0);
      leases.settleHold(
        s.cand.meta,
        s.cand.customerAtoms,
        s.cand.supplierAtoms,
        wlExpMs,
        slExpMs,
        postNow,
      );
    }
    return fallback;
  }

  const reqIds = [...new Set(candidates.map((c) => c.item.request_id))].sort();
  const wLeaseIds = [...new Set(candidates.map((c) => c.meta.walletLeaseId))].sort();
  const sLeaseIds = [...new Set(candidates.map((c) => c.meta.supplierLeaseId))].sort();
  const walletIds = [...new Set(candidates.map((c) => c.meta.walletId))].sort();

  const slowSerialized = serialized.map((s) => ({
    ...s,
    usageJson: JSON.stringify(s.cand.item.usage),
    pluginAuditJson: JSON.stringify(s.cand.item.plugin_audit ?? []),
  }));
  const committed: FastCommitCandidate[] = [];
  let slowWlExp: Record<string, number> = {};
  let slowSlExp: Record<string, number> = {};

  await db.begin(async (tx) => {
    const holdRows = await tx`
        SELECT request_id, state, wallet_lease_id, supplier_lease_id, key_reserved_atoms::text
        FROM public.billing_holds
        WHERE request_id = ANY(${reqIds}::uuid[])
        ORDER BY request_id
        FOR NO KEY UPDATE
      `;
    const holdByReq = new Map<
      string,
      {
        state: string;
        wallet_lease_id: string | null;
        supplier_lease_id: string | null;
        key_reserved_atoms: bigint;
      }
    >();
    for (const r of holdRows) {
      holdByReq.set(String(r.request_id), {
        state: String(r.state),
        wallet_lease_id: r.wallet_lease_id ? String(r.wallet_lease_id) : null,
        supplier_lease_id: r.supplier_lease_id ? String(r.supplier_lease_id) : null,
        key_reserved_atoms: BigInt(String(r.key_reserved_atoms ?? "0")),
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

    type PreparedFastCharge = (typeof slowSerialized)[number] & {
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

    for (const data of slowSerialized) {
      const { cand } = data;
      const h = holdByReq.get(cand.item.request_id);
      const wl = wlById.get(cand.meta.walletLeaseId);
      const sl = slById.get(cand.meta.supplierLeaseId);
      if (
        !h ||
        h.state !== "HELD" ||
        h.key_reserved_atoms !== 0n ||
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
        ...data,
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

    const [slowExpRow] = await tx<
      {
        wl_expiries: Record<string, number>;
        sl_expiries: Record<string, number>;
      }[]
    >`
        WITH
          upd_wl AS (
            UPDATE public.wallet_leases AS l SET
              lot_allocations = u.lot_allocations::jsonb,
              remaining_atoms = u.remaining_atoms,
              earliest_lot_expires_at = u.earliest_lot_expires_at,
              expires_at = CASE
                WHEN l.expires_at < ${renewThresholdIso}::timestamptz THEN GREATEST(l.expires_at, ${nextExpIso}::timestamptz)
                ELSE l.expires_at
              END,
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
            RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
          ),
          upd_sl AS (
            UPDATE public.supplier_leases AS l SET
              remaining_atoms = u.remaining_atoms,
              expires_at = CASE
                WHEN l.expires_at < ${renewThresholdIso}::timestamptz THEN GREATEST(l.expires_at, ${nextExpIso}::timestamptz)
                ELSE l.expires_at
              END,
              updated_at = now()
            FROM unnest(
              ${dirtySl.map((s) => s.id)}::uuid[],
              ${dirtySl.map((s) => s.remaining.toString())}::numeric[]
            ) AS u(id, remaining_atoms)
            WHERE l.id = u.id
            RETURNING l.id, (extract(epoch FROM l.expires_at) * 1000)::bigint AS expires_ms
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
              id, request_id, project_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
              model, operation, currency, billing_price_id, cost_version_id,
              pricing_override_id, discount_numerator, discount_denominator,
              usage, usage_digest,
              customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
              lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
            )
            SELECT
              u.id, u.request_id, u.project_id, u.wallet_id, u.api_key_id, u.selected_attempt_id, u.channel_id,
              u.model, u.operation, u.currency, u.billing_price_id, u.cost_version_id,
              u.pricing_override_id, u.discount_numerator, u.discount_denominator,
              u.usage::jsonb, u.usage_digest,
              u.customer_atoms, u.customer_atoms, 0, u.supplier_cost_atoms, u.supplier_currency,
              u.lot_allocations::jsonb, u.plugin_audit::jsonb, u.charge_policy, 'SETTLED',
              u.customer_journal_id, u.supplier_journal_id
            FROM unnest(
              ${prepared.map((p) => p.chargeId)}::uuid[],
              ${prepared.map((p) => p.cand.item.request_id)}::uuid[],
              ${prepared.map((p) => p.cand.meta.projectId)}::uuid[],
              ${prepared.map((p) => p.cand.meta.walletId)}::uuid[],
              ${prepared.map((p) => p.cand.item.api_key_id ?? p.cand.meta.apiKeyId)}::uuid[],
              ${prepared.map((p) => p.cand.item.selected_attempt_id)}::uuid[],
              ${prepared.map((p) => p.cand.item.channel_id)}::uuid[],
              ${prepared.map((p) => p.cand.meta.model)}::text[],
              ${prepared.map((p) => p.cand.meta.operation)}::text[],
              ${prepared.map((p) => p.cand.meta.currency)}::text[],
              ${prepared.map((p) => p.cand.item.billing_price_id)}::uuid[],
              ${prepared.map((p) => p.cand.item.cost_version_id)}::uuid[],
              ${prepared.map((p) => p.cand.item.pricing_override_id ?? p.cand.meta.pricingOverrideId)}::uuid[],
              ${prepared.map((p) => p.cand.item.discount_numerator ?? p.cand.meta.discountNumerator)}::numeric[],
              ${prepared.map((p) => p.cand.item.discount_denominator ?? p.cand.meta.discountDenominator)}::numeric[],
              ${prepared.map((p) => p.usageJson)}::text[],
              ${prepared.map((p) => p.usageDigest)}::text[],
              ${prepared.map((p) => p.customerAtomsText)}::numeric[],
              ${prepared.map((p) => p.supplierAtomsText)}::numeric[],
              ${prepared.map((p) => p.cand.item.supplier_currency)}::text[],
              ${prepared.map((p) => JSON.stringify(p.chargeAllocations))}::text[],
              ${prepared.map((p) => p.pluginAuditJson)}::text[],
              ${prepared.map((p) => p.cand.item.charge_policy ?? "CHARGE_CUSTOMER")}::text[],
              ${prepared.map((p) => p.customerJournalId)}::uuid[],
              ${prepared.map((p) => p.supplierJournalId)}::uuid[]
            ) AS u(
              id, request_id, project_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
              model, operation, currency, billing_price_id, cost_version_id,
              pricing_override_id, discount_numerator, discount_denominator,
              usage, usage_digest,
              customer_atoms, supplier_cost_atoms, supplier_currency,
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
          )
        SELECT
          COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_wl), '{}'::jsonb) AS wl_expiries,
          COALESCE((SELECT jsonb_object_agg(id::text, expires_ms) FROM upd_sl), '{}'::jsonb) AS sl_expiries
      `;
    if (slowExpRow) {
      slowWlExp = slowExpRow.wl_expiries ?? {};
      slowSlExp = slowExpRow.sl_expiries ?? {};
    }

    const slowOutboxJson = JSON.stringify(
      prepared.map((p) => ({
        event_id: p.eventId,
        request_id: p.cand.item.request_id,
        project_id: p.cand.meta.projectId,
        payload: {
          charge_id: p.chargeId,
          request_id: p.cand.item.request_id,
          api_key_id: p.cand.item.api_key_id ?? p.cand.meta.apiKeyId,
          attempt_id: p.cand.item.selected_attempt_id,
          channel_id: p.cand.item.channel_id,
          model: p.cand.meta.model,
          operation: p.cand.meta.operation,
          currency: p.cand.meta.currency,
          pricing_override_id: p.cand.item.pricing_override_id ?? p.cand.meta.pricingOverrideId,
          usage: p.cand.item.usage,
          customer_atoms: p.customerAtomsText,
          funded_atoms: p.customerAtomsText,
          debt_atoms: "0",
          supplier_cost_atoms: p.supplierAtomsText,
          outcome: "SETTLED",
          wallet_available_atoms: (wAvailAfter.get(p.cand.meta.walletId) ?? 0n).toString(),
        },
      })),
    );
    await tx`
            INSERT INTO public.outbox_events (event_id, kind, request_id, project_id, payload)
            SELECT u.event_id, 'billing.settled', u.request_id, u.project_id, u.payload
            FROM jsonb_to_recordset(${slowOutboxJson}::text::jsonb) AS u(
              event_id uuid, request_id uuid, project_id uuid, payload jsonb
            )
            ORDER BY public.outbox_shard(u.project_id) ASC, u.request_id ASC
      `;

    for (const p of prepared) {
      results[p.cand.index] = {
        ok: true,
        replayed: false,
        charge_id: p.chargeId,
        outcome: "SETTLED",
        customer_atoms: p.customerAtomsText,
        funded_atoms: p.customerAtomsText,
        debt_atoms: "0",
        supplier_cost_atoms: p.supplierAtomsText,
      };
    }
  });

  const postNow = Date.now();
  for (const candidate of committed) {
    leases.settleHold(
      candidate.meta,
      candidate.customerAtoms,
      candidate.supplierAtoms,
      Number(slowWlExp[candidate.meta.walletLeaseId] ?? 0),
      Number(slowSlExp[candidate.meta.supplierLeaseId] ?? 0),
      postNow,
    );
  }
  return fallback;
}
