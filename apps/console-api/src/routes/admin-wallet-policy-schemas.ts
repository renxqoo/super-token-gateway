import { z } from "zod";
import { currencySchema, nonNegativeAtomsSchema, positiveDecimalSchema } from "./shared.ts";

const walletPolicyEnum = z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]);

export const creditPolicySchema = z
  .object({
    currency: currencySchema.optional(),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    credit_limit_decimal: positiveDecimalSchema.optional(),
    policy: walletPolicyEnum.optional(),
  })
  .strict();

export const defaultCreditPolicySchema = z
  .object({
    default_credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    default_credit_limit_decimal: positiveDecimalSchema.optional(),
    default_wallet_policy: walletPolicyEnum.optional(),
  })
  .strict();

export const batchCreditPolicySchema = z
  .object({
    scope: z.enum(["ALL_BILLING_ACCOUNTS", "ORGANIZATION", "USER", "BILLING_ACCOUNTS"]),
    org_id: z.uuid().optional(),
    user_id: z.uuid().optional(),
    billing_account_ids: z.array(z.uuid()).min(1).max(500).optional(),
    only_default_billing_account: z.boolean().optional(),
    credit_limit_atoms: nonNegativeAtomsSchema.optional(),
    credit_limit_decimal: positiveDecimalSchema.optional(),
    policy: walletPolicyEnum.optional(),
    dry_run: z.boolean().default(false),
    reason: z.string().min(1).max(500),
  })
  .strict();
