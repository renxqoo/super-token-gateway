import { z } from "zod";
import { currencySchema, positiveAtomsSchema } from "./billing.ts";
import { digestSchema } from "./enterprise.ts";

export function normalizeFundingTimestamp(value: string): string {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/.exec(value);
  if (!match) throw new RangeError("Expected a UTC timestamp with at most six fractional digits");
  const milliseconds = Date.parse(`${match[1]}Z`);
  if (
    !Number.isFinite(milliseconds) ||
    match[1]!.startsWith("0000-") ||
    new Date(milliseconds).toISOString().slice(0, 19) !== match[1]
  )
    throw new RangeError("Invalid UTC calendar timestamp");
  return `${match[1]}.${(match[2] ?? "").padEnd(6, "0")}Z`;
}

export const fundingTimestampSchema = z.string().transform((value, context) => {
  try {
    return normalizeFundingTimestamp(value);
  } catch {
    context.addIssue({
      code: "custom",
      message: "Expected a valid UTC timestamp at microsecond precision",
    });
    return z.NEVER;
  }
});
export const upstreamRechargeSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    payment_currency: currencySchema,
    payment_atoms: positiveAtomsSchema,
    credited_currency: currencySchema,
    credited_atoms: positiveAtomsSchema,
    voucher_id: z.uuid().transform((value) => value.toLowerCase()),
    voucher_digest: digestSchema,
    completed_at: fundingTimestampSchema,
    reason: z.string().trim().min(1).max(2000),
  })
  .strict();
export const financialVoucherRegistrationSchema = z
  .object({
    content: z
      .instanceof(Uint8Array)
      .refine((value) => value.byteLength > 0 && value.byteLength <= 5242880),
    content_type: z.enum(["image/png", "image/jpeg", "application/pdf"]),
    digest: digestSchema,
    expires_at: fundingTimestampSchema,
  })
  .strict();
export type UpstreamRechargeInput = z.input<typeof upstreamRechargeSchema>;
export type FinancialVoucherRegistration = z.input<typeof financialVoucherRegistrationSchema>;
export type UpstreamRechargeReceipt = {
  operation_id: string;
  journal_id: string;
  account_id: string;
  kind: "RECHARGE";
  external_ref: string;
  payment_currency: string;
  payment_atoms: string;
  credited_currency: string;
  credited_atoms: string;
  voucher_id: string;
  voucher_digest: string;
  completed_at: string;
  reason: string;
  actor: string;
  payload_digest: string;
  balance_atoms: string;
  balance_revision: string;
};
