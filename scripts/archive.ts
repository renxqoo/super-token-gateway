import { resolve } from "node:path";
import { createDatabase, prepareEvidenceMonth, moveEvidenceMonth } from "@gateway/storage";
import { exportEvidenceArchive, verifyEvidenceArchive } from "./evidence-archive.ts";
const args = Bun.argv.slice(2);
const option = (name: string) => {
  const at = args.indexOf(name);
  return at < 0 ? undefined : args[at + 1];
};
const verify = option("--verify");
if (verify) {
  const manifest = await verifyEvidenceArchive(resolve(verify));
  console.log(JSON.stringify({ verified: true, month: manifest.month, files: manifest.files }));
} else {
  const url = process.env.MAINTENANCE_DATABASE_URL ?? process.env.DATABASE_URL;
  if (!url) throw new Error("MAINTENANCE_DATABASE_URL or DATABASE_URL required");
  const db = createDatabase(url, 1);
  try {
    const month = option("--prepare") ?? option("--month");
    if (!month) throw new Error("Use --prepare YYYY-MM or --month YYYY-MM --output directory");
    if (option("--prepare")) await prepareEvidenceMonth(db, month, args.includes("--quiesced"));
    else {
      const output = option("--output");
      if (!output) throw new Error("Explicit archive --output directory required");
      const manifest = await exportEvidenceArchive(db, month, resolve(output));
      if (option("--tablespace")) {
        if (!args.includes("--quiesced"))
          throw new Error("Tablespace movement requires --quiesced");
        await verifyEvidenceArchive(manifest);
        await moveEvidenceMonth(db, month, option("--tablespace")!);
      }
      console.log(JSON.stringify({ manifest, onlineEvidenceRetained: true }));
    }
  } finally {
    await db.close();
  }
}
