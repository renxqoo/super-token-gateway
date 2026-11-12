import type { SQL } from "bun";
import { eq, sql } from "drizzle-orm";
import {
  GatewayError,
  type FinancialVoucherRegistration,
  type UpstreamRechargeInput,
  type UpstreamRechargeReceipt,
} from "@gateway/contracts";
import { createRepositories } from "../database.ts";
import { upstreamAccountBalances } from "../schema/index.ts";
import { recordUpstreamRecharge } from "./recharge.ts";
import {
  registerFinancialVoucher,
  readFinancialVoucher,
  cleanupFinancialVouchers,
} from "./vouchers.ts";
import { fundingId, fundingFailure } from "./validation.ts";

export { recordFinancialVoucher } from "./vouchers.ts";
export { recordUpstreamRecharge } from "./recharge.ts";
export function createUpstreamFundingStore(db: SQL) {
  return {
    registerVoucher(input: FinancialVoucherRegistration, actor: string) {
      return registerFinancialVoucher(db, input, actor);
    },
    voucher(id: string) {
      return readFinancialVoucher(db, id);
    },
    cleanupVouchers(limit = 100) {
      return cleanupFinancialVouchers(db, limit);
    },
    async recharge(
      accountId: string,
      input: UpstreamRechargeInput,
      actor: string,
    ): Promise<UpstreamRechargeReceipt> {
      try {
        return await db.begin((tx) => recordUpstreamRecharge(tx, accountId, input, actor));
      } catch (error) {
        fundingFailure(error);
      }
    },
    async receipt(accountId: string, externalRef: string): Promise<UpstreamRechargeReceipt> {
      if (typeof externalRef !== "string" || externalRef.length < 1 || externalRef.length > 200)
        throw new GatewayError(400, "invalid_funding_input", "Invalid recharge reference");
      try {
        const rows =
          await db`SELECT receipt FROM upstream_funding_operations WHERE account_id=${fundingId(accountId)} AND kind='RECHARGE' AND external_ref=${externalRef}`;
        if (!rows[0])
          throw new GatewayError(404, "upstream_recharge_not_found", "Recharge receipt not found");
        return rows[0].receipt as UpstreamRechargeReceipt;
      } catch (error) {
        fundingFailure(error);
      }
    },
    async balances(accountId: string) {
      try {
        return await createRepositories(db)
          .select({
            currency: upstreamAccountBalances.currency,
            balance_atoms: upstreamAccountBalances.balanceAtoms,
            reserved_atoms: upstreamAccountBalances.reservedAtoms,
            spent_atoms: upstreamAccountBalances.spentAtoms,
            adjustment_atoms: upstreamAccountBalances.adjustmentAtoms,
            revision: upstreamAccountBalances.revision,
            available_atoms: sql<string>`(${upstreamAccountBalances.balanceAtoms}-${upstreamAccountBalances.reservedAtoms})::text`,
          })
          .from(upstreamAccountBalances)
          .where(eq(upstreamAccountBalances.accountId, fundingId(accountId)))
          .orderBy(upstreamAccountBalances.currency);
      } catch (error) {
        fundingFailure(error);
      }
    },
  };
}
