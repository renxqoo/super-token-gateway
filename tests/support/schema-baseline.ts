import type { SQL } from "bun";
import manifest from "../../packages/storage/src/migrations/manifest.json";

export async function installV2Baseline(db: SQL): Promise<void> {
  const baseline = manifest.migrations[0]!;
  if (baseline.version !== 2026111202) throw new Error("Expected frozen v2 baseline");
  await db.begin(async (tx) => {
    for (const entry of baseline.files) {
      const source = await Bun.file(
        new URL(`../../packages/storage/src/${entry.path}`, import.meta.url),
      ).text();
      if (new Bun.CryptoHasher("sha256").update(source).digest("hex") !== entry.checksum)
        throw new Error("Baseline fixture checksum mismatch");
      await tx.unsafe(source);
    }
    const checksum = new Bun.CryptoHasher("sha256").update(JSON.stringify(baseline)).digest("hex");
    await tx`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${baseline.version},${checksum},${baseline}::jsonb)`;
  });
}
