import type { SQL } from "bun";
import type { Usage } from "@gateway/kernel";
import type { ChargeLotAllocation, CommitTerminalResult, SupplierHoldEntry } from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import {
  sha256Hex,
  computeFreezeReason,
  lotEligible,
  cellKey,
  expireLotsInMemory,
  withPgConcurrencyRetry,
  classifyLedgerStatementCancellation,
  type JournalLine,
} from "./funds.ts";
import { priceSettlement } from "./settlement-pricing.ts";
import { flushDirtyKeySpends } from "./key-spend.ts";

import {
  loadDirectSettlementState,
  type DirectSettlementEntry,
  type DetachedHold,
} from "./settlement-direct-state.ts";

export async function markAccountsReconcileRequired(
  db: SQL,
  accountIds: readonly string[],
): Promise<void> {
  if (accountIds.length === 0) return;
  const sorted = [...new Set(accountIds)].sort();
  await db.begin(async (tx) => {
    await tx`
      SELECT id FROM public.upstream_accounts
      WHERE id = ANY(${sorted}::uuid[])
      ORDER BY id
      FOR NO KEY UPDATE
    `;
    await tx`
      UPDATE public.upstream_accounts
      SET state = 'RECONCILE_REQUIRED', revision = revision + 1, updated_at = now()
      WHERE id = ANY(${sorted}::uuid[]) AND state = 'ACTIVE'
    `;
  });
}

export async function commitDirectBatch(
  db: SQL,
  notifyDetachedLocal: LeaseManager["notifyDetachedLocal"],
  entries: DirectSettlementEntry[],
  results: CommitTerminalResult[],
): Promise<void> {
  const staged: CommitTerminalResult[] = [];
  const apply = (tx: SQL) => {
    staged.length = 0;
    return applyDirectSettlements(tx, entries, staged);
  };
  const { detached, reconcileAccounts } = await db.begin(
    classifyLedgerStatementCancellation(apply),
  );
  for (const { index } of entries) {
    if (staged[index] !== undefined) results[index] = staged[index]!;
  }
  if (reconcileAccounts.length > 0) {
    await withPgConcurrencyRetry(() => markAccountsReconcileRequired(db, reconcileAccounts), 3);
  }
  for (const d of detached) {
    notifyDetachedLocal(d.requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
  }
}

export async function applyDirectSettlements(
  tx: SQL,
  directEntries: DirectSettlementEntry[],
  results: CommitTerminalResult[],
): Promise<{ detached: DetachedHold[]; reconcileAccounts: string[] }> {
  const detachedNotifications: DetachedHold[] = [];
  const nowMs = Date.now();
  const {
    holdsByReq,
    selectedAttempts,
    existingCharges,
    keySpendsById,
    walletsById,
    lotsById,
    lotsByWallet,
    cellsByKey,
  } = await loadDirectSettlementState({ tx, entries: directEntries, detachedNotifications, nowMs });

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
    billing_account_id: string;
    wallet_id: string;
    api_key_id: string | null;
    selected_attempt_id: string;
    channel_id: string;
    model: string;
    operation: string;
    currency: string;
    billing_price_id: string;
    cost_version_id: string;
    pricing_override_id: string | null;
    discount_numerator: string | null;
    discount_denominator: string | null;
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
    billing_account_id: string;
    payload: Record<string, unknown>;
  }[] = [];

  for (const entry of directEntries) {
    const { index: i, item, previouslySettledSupplierJournal } = entry;
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
    const selectedAttempt = selectedAttempts.get(entry);
    if (!selectedAttempt) {
      results[i] = {
        ok: false,
        code: "invalid_state",
        message: "Selected supplier attempt does not match a settleable hold",
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

    let computedCustomerAtoms: bigint;
    let supplierCostAtoms: bigint;
    try {
      ({ customerAtoms: computedCustomerAtoms, supplierAtoms: supplierCostAtoms } =
        priceSettlement(item));
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

    let supplierJournalId = previouslySettledSupplierJournal ?? null;
    const updatedSupplierHolds: SupplierHoldEntry[] = [];
    for (const sh of hold.supplier_holds) {
      if (sh === selectedAttempt && sh.state !== "SETTLED") {
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

    if (hold.api_key_id) {
      const kRow = keySpendsById.get(hold.api_key_id);
      if (kRow) {
        if (hold.key_reserved_atoms > 0n) {
          kRow.reserved_atoms =
            kRow.reserved_atoms > hold.key_reserved_atoms
              ? kRow.reserved_atoms - hold.key_reserved_atoms
              : 0n;
          kRow.dirty = true;
        }
        if (
          kRow.spend_limit_atoms !== null &&
          hold.created_at_ms >= kRow.spend_window_start_ms &&
          billableCustomerAtoms > 0n
        ) {
          kRow.spent_atoms += billableCustomerAtoms;
          kRow.dirty = true;
        }
      }
    }

    const effectiveApiKeyId = item.api_key_id ?? hold.api_key_id;
    const effectiveOverrideId =
      item.pricing_override_id === undefined ? hold.pricing_override_id : item.pricing_override_id;
    const effectiveDiscNum =
      item.discount_numerator === undefined ? hold.discount_numerator : item.discount_numerator;
    const effectiveDiscDen =
      item.discount_denominator === undefined
        ? hold.discount_denominator
        : item.discount_denominator;

    holdUpdates.push({
      request_id: item.request_id,
      state: "SETTLED",
      supplier_holds: updatedSupplierHolds,
    });
    newCharges.push({
      id: chargeId,
      request_id: item.request_id,
      billing_account_id: hold.billing_account_id,
      wallet_id: wallet.id,
      api_key_id: effectiveApiKeyId,
      selected_attempt_id: item.selected_attempt_id,
      channel_id: item.channel_id,
      model: hold.model,
      operation: hold.operation,
      currency: wallet.currency,
      billing_price_id: item.billing_price_id,
      cost_version_id: item.cost_version_id,
      pricing_override_id: effectiveOverrideId,
      discount_numerator: effectiveDiscNum,
      discount_denominator: effectiveDiscDen,
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
      billing_account_id: hold.billing_account_id,
      payload: {
        charge_id: chargeId,
        request_id: item.request_id,
        api_key_id: effectiveApiKeyId,
        attempt_id: item.selected_attempt_id,
        channel_id: item.channel_id,
        model: hold.model,
        operation: hold.operation,
        currency: wallet.currency,
        pricing_override_id: effectiveOverrideId,
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

  await flushDirtyKeySpends(tx, keySpendsById.values());
  const dirtyWallets = [...walletsById.values()].filter((w) => w.dirty);
  const dirtyLots = [...lotsById.values()].filter((l) => l.dirty);
  const dirtyCells = [...cellsByKey.values()].filter((c) => c.dirty);
  const outboxJson =
    outboxBatch.length > 0
      ? JSON.stringify(
          outboxBatch.map((o) => ({
            event_id: o.event_id,
            kind: o.kind,
            request_id: o.request_id,
            billing_account_id: o.billing_account_id,
            payload: o.payload,
          })),
        )
      : "[]";
  if (
    dirtyWallets.length > 0 ||
    dirtyLots.length > 0 ||
    dirtyCells.length > 0 ||
    journals.length > 0 ||
    holdUpdates.length > 0 ||
    newCharges.length > 0 ||
    outboxBatch.length > 0
  ) {
    await tx`
      WITH
        upd_w AS (
          UPDATE public.wallets AS w SET
            available_atoms = u.available_atoms,
            reserved_atoms = u.reserved_atoms,
            spent_atoms = u.spent_atoms,
            expired_atoms = u.expired_atoms,
            debt_atoms = u.debt_atoms,
            credit_reserved_atoms = u.credit_reserved_atoms,
            freeze_reason = u.freeze_reason
          FROM unnest(
            ${dirtyWallets.map((w) => w.id)}::uuid[],
            ${dirtyWallets.map((w) => w.available_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.reserved_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.spent_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.expired_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.debt_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.credit_reserved_atoms.toString())}::numeric[],
            ${dirtyWallets.map((w) => w.freeze_reason)}::text[]
          ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms, debt_atoms, credit_reserved_atoms, freeze_reason)
          WHERE w.id = u.id
        ),
        upd_l AS (
          UPDATE public.wallet_lots AS l SET
            available_atoms = u.available_atoms,
            reserved_atoms = u.reserved_atoms,
            spent_atoms = u.spent_atoms,
            expired_atoms = u.expired_atoms
          FROM unnest(
            ${dirtyLots.map((l) => l.id)}::uuid[],
            ${dirtyLots.map((l) => l.available_atoms.toString())}::numeric[],
            ${dirtyLots.map((l) => l.reserved_atoms.toString())}::numeric[],
            ${dirtyLots.map((l) => l.spent_atoms.toString())}::numeric[],
            ${dirtyLots.map((l) => l.expired_atoms.toString())}::numeric[]
          ) AS u(id, available_atoms, reserved_atoms, spent_atoms, expired_atoms)
          WHERE l.id = u.id
        ),
        upd_c AS (
          UPDATE public.supplier_cells AS c SET
            balance_atoms = u.balance_atoms,
            reserved_atoms = u.reserved_atoms,
            spent_atoms = u.spent_atoms,
            revision = u.revision
          FROM unnest(
            ${dirtyCells.map((c) => c.account_id)}::uuid[],
            ${dirtyCells.map((c) => c.currency)}::text[],
            ${dirtyCells.map((c) => c.cell)}::smallint[],
            ${dirtyCells.map((c) => c.balance_atoms.toString())}::numeric[],
            ${dirtyCells.map((c) => c.reserved_atoms.toString())}::numeric[],
            ${dirtyCells.map((c) => c.spent_atoms.toString())}::numeric[],
            ${dirtyCells.map((c) => c.revision.toString())}::bigint[]
          ) AS u(account_id, currency, cell, balance_atoms, reserved_atoms, spent_atoms, revision)
          WHERE c.account_id = u.account_id AND c.currency = u.currency AND c.cell = u.cell
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
        upd_h AS (
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
        ),
        ins_ch AS (
          INSERT INTO public.billing_charges (
            id, request_id, billing_account_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
            model, operation, currency, billing_price_id, cost_version_id,
            pricing_override_id, discount_numerator, discount_denominator,
            usage, usage_digest,
            customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
            lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
          )
          SELECT
            u.id, u.request_id, u.billing_account_id, u.wallet_id, u.api_key_id, u.selected_attempt_id, u.channel_id,
            u.model, u.operation, u.currency, u.billing_price_id, u.cost_version_id,
            u.pricing_override_id, u.discount_numerator, u.discount_denominator,
            u.usage::jsonb, u.usage_digest,
            u.customer_atoms, u.funded_atoms, u.debt_atoms, u.supplier_cost_atoms, u.supplier_currency,
            u.lot_allocations::jsonb, u.plugin_audit::jsonb, u.charge_policy, u.outcome,
            u.customer_journal_id, u.supplier_journal_id
          FROM unnest(
            ${newCharges.map((c) => c.id)}::uuid[],
            ${newCharges.map((c) => c.request_id)}::uuid[],
            ${newCharges.map((c) => c.billing_account_id)}::uuid[],
            ${newCharges.map((c) => c.wallet_id)}::uuid[],
            ${newCharges.map((c) => c.api_key_id)}::uuid[],
            ${newCharges.map((c) => c.selected_attempt_id)}::uuid[],
            ${newCharges.map((c) => c.channel_id)}::uuid[],
            ${newCharges.map((c) => c.model)}::text[],
            ${newCharges.map((c) => c.operation)}::text[],
            ${newCharges.map((c) => c.currency)}::text[],
            ${newCharges.map((c) => c.billing_price_id)}::uuid[],
            ${newCharges.map((c) => c.cost_version_id)}::uuid[],
            ${newCharges.map((c) => c.pricing_override_id)}::uuid[],
            ${newCharges.map((c) => c.discount_numerator)}::numeric[],
            ${newCharges.map((c) => c.discount_denominator)}::numeric[],
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
            id, request_id, billing_account_id, wallet_id, api_key_id, selected_attempt_id, channel_id,
            model, operation, currency, billing_price_id, cost_version_id,
            pricing_override_id, discount_numerator, discount_denominator,
            usage, usage_digest,
            customer_atoms, funded_atoms, debt_atoms, supplier_cost_atoms, supplier_currency,
            lot_allocations, plugin_audit, charge_policy, outcome, customer_journal_id, supplier_journal_id
          )
        ),
        ins_ob AS (
          INSERT INTO public.outbox_events (event_id, kind, request_id, billing_account_id, payload)
          SELECT u.event_id, u.kind, u.request_id, u.billing_account_id, u.payload
          FROM jsonb_to_recordset(${outboxJson}::text::jsonb) AS u(
            event_id uuid, kind text, request_id uuid, billing_account_id uuid, payload jsonb
          )
          ORDER BY public.outbox_shard(u.billing_account_id) ASC, u.request_id ASC
        )
      SELECT 1
    `;
  }
  return {
    detached: detachedNotifications,
    reconcileAccounts: [...reconcileAccounts].sort(),
  };
}
