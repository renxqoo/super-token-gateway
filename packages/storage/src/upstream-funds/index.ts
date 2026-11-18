import type { SQL } from "bun";
import {
  GatewayError,
  type FinancialVoucherRegistration,
  type UpstreamRechargeInput,
  type UpstreamRechargeReceipt,
} from "@gateway/contracts";
import { withDatabaseTransaction } from "../transaction.ts";
import { recordUpstreamRecharge } from "./recharge.ts";
import {
  registerFinancialVoucher,
  readFinancialVoucher,
  cleanupFinancialVouchers,
} from "./vouchers.ts";
import { fundingId, fundingFailure } from "./validation.ts";

export { recordFinancialVoucher } from "./vouchers.ts";
export { recordUpstreamRecharge } from "./recharge.ts";
export { recordUpstreamAdjustment } from "./adjustment.ts";
export { recordUpstreamReconciliation } from "./reconciliation.ts";
export { createUpstreamAccountStore } from "./accounts.ts";
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
        const account = fundingId(accountId);
        return await withDatabaseTransaction(
          db,
          { deadline: performance.now() + 4000 },
          async (tx) => {
            await tx`SELECT core_funding_control_v1(${account}::uuid,false)`;
            return recordUpstreamRecharge(tx, account, input, actor);
          },
        );
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
        return [
          ...(await db<
            {
              currency: string;
              balance_atoms: string;
              reserved_atoms: string;
              spent_atoms: string;
              adjustment_atoms: string;
              revision: string;
              available_atoms: string;
            }[]
          >`SELECT currency,balance_atoms::text,reserved_atoms::text,spent_atoms::text,adjustment_atoms::text,revision::text,(balance_atoms-reserved_atoms)::text available_atoms FROM core_upstream_balances_v1 WHERE account_id=${fundingId(accountId)} ORDER BY currency`),
        ];
      } catch (error) {
        fundingFailure(error);
      }
    },
  };
}
