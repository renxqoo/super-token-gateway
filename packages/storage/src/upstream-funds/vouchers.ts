import type { SQL } from "bun";
import { eq, sql } from "drizzle-orm";
import {
  financialVoucherRegistrationSchema,
  GatewayError,
  type FinancialVoucherRegistration,
} from "@gateway/contracts";
import { createRepositories } from "../database.ts";
import { financialVouchers } from "../schema/index.ts";
import { fundingActor, fundingFailure, fundingId, fundingInput } from "./validation.ts";

function voucherSignature(content: Uint8Array, contentType: string): boolean {
  if (contentType === "image/png")
    return [137, 80, 78, 71, 13, 10, 26, 10].every((byte, index) => content[index] === byte);
  if (contentType === "image/jpeg")
    return content[0] === 255 && content[1] === 216 && content[2] === 255;
  return [37, 80, 68, 70, 45].every((byte, index) => content[index] === byte);
}
const voucherMetadata = {
  id: financialVouchers.id,
  digest: financialVouchers.digest,
  content_type: financialVouchers.contentType,
  size: sql<number>`octet_length(${financialVouchers.content})`,
  actor: financialVouchers.actor,
  expires_at: sql<string>`to_char(${financialVouchers.expiresAt} AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
  referenced: sql<boolean>`EXISTS(SELECT 1 FROM upstream_funding_operations WHERE voucher_id=financial_vouchers.id)`,
};
export async function recordFinancialVoucher(
  tx: SQL,
  input: FinancialVoucherRegistration,
  actor: string,
) {
  const parsed = fundingInput(financialVoucherRegistrationSchema, input);
  const trustedActor = fundingActor(actor);
  const content = Buffer.from(parsed.content);
  if (!voucherSignature(content, parsed.content_type))
    throw new GatewayError(
      400,
      "invalid_voucher_content",
      "Voucher signature does not match its allowed content type",
    );
  const digest = new Bun.CryptoHasher("sha256").update(content).digest("hex");
  if (digest !== parsed.digest)
    throw new GatewayError(400, "voucher_digest_mismatch", "Voucher content digest does not match");
  try {
    return await (async () => {
      let target: string;
      await tx`SELECT pg_advisory_xact_lock(hashtextextended(${`financial-voucher:${digest}`},0))`;
      const old =
        await tx`SELECT id,content_type FROM financial_vouchers WHERE digest=${digest} FOR UPDATE`;
      if (old[0]) {
        if (old[0].content_type !== parsed.content_type)
          throw new GatewayError(
            409,
            "voucher_conflict",
            "Voucher digest has a different content type",
          );
        const eligibility = (
          await tx`SELECT expires_at<=clock_timestamp() AS expired,EXISTS(SELECT 1 FROM upstream_funding_operations WHERE voucher_id=v.id) AS referenced FROM financial_vouchers v WHERE id=${old[0].id}`
        )[0]!;
        if (eligibility.expired && !eligibility.referenced)
          throw new GatewayError(
            409,
            "voucher_expired",
            "Unreferenced voucher staging has expired; explicit cleanup is required",
          );
        target = String(old[0].id);
      } else {
        const id = crypto.randomUUID();
        const inserted =
          await tx`INSERT INTO financial_vouchers(id,digest,content_type,content,actor,expires_at) SELECT ${id},${digest},${parsed.content_type},${content},${trustedActor},${parsed.expires_at}::timestamptz WHERE ${parsed.expires_at}::timestamptz>clock_timestamp() AND ${parsed.expires_at}::timestamptz<=clock_timestamp()+interval '24 hours' RETURNING id`;
        if (!inserted.length)
          throw new GatewayError(
            400,
            "invalid_voucher_expiry",
            "Voucher staging expiry must be in the next 24 hours",
          );
        target = id;
      }
      return (
        await createRepositories(tx)
          .select(voucherMetadata)
          .from(financialVouchers)
          .where(eq(financialVouchers.id, target))
      )[0]!;
    })();
  } catch (error) {
    fundingFailure(error);
  }
}
export async function registerFinancialVoucher(
  db: SQL,
  input: FinancialVoucherRegistration,
  actor: string,
) {
  return db.begin((tx) => recordFinancialVoucher(tx, input, actor));
}
export async function readFinancialVoucher(db: SQL, id: string) {
  try {
    const rows = await createRepositories(db)
      .select({ ...voucherMetadata, content: financialVouchers.content })
      .from(financialVouchers)
      .where(eq(financialVouchers.id, fundingId(id)));
    if (!rows[0]) throw new GatewayError(404, "voucher_not_found", "Financial voucher not found");
    return rows[0];
  } catch (error) {
    fundingFailure(error);
  }
}
export async function cleanupFinancialVouchers(db: SQL, limit = 100): Promise<string[]> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000)
    throw new GatewayError(
      400,
      "invalid_funding_input",
      "Voucher cleanup limit must be between 1 and 1000",
    );
  try {
    return await db.begin(async (tx) => {
      const rows =
        await tx`SELECT id FROM financial_vouchers v WHERE expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM upstream_funding_operations WHERE voucher_id=v.id) ORDER BY expires_at,id LIMIT ${limit} FOR UPDATE SKIP LOCKED`;
      if (!rows.length) return [];
      const deleted =
        await tx`DELETE FROM financial_vouchers v WHERE id=ANY(${rows.map((row: { id: string }) => row.id)}::uuid[]) AND expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM upstream_funding_operations WHERE voucher_id=v.id) RETURNING id`;
      return deleted.map((row: { id: string }) => row.id);
    });
  } catch (error) {
    fundingFailure(error);
  }
}
