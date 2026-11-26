import { eq, sql } from "drizzle-orm";
import { configEpoch, type LedgerDrizzle } from "./schema.ts";

export function catalogDigest(value: string | Uint8Array): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

type DrizzleTx = Parameters<Parameters<LedgerDrizzle["transaction"]>[0]>[0];

export async function bumpEpoch(tx: DrizzleTx): Promise<number> {
  const rows = await tx
    .update(configEpoch)
    .set({
      version: sql`${configEpoch.version} + 1`,
      updatedAt: sql`now()`,
    })
    .where(eq(configEpoch.id, 1))
    .returning({ version: configEpoch.version });
  return rows[0]!.version;
}
