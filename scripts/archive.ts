import { createDatabase, exportLedgerArchive, verifyLedgerArchive } from "@gateway/ledger";

const command = Bun.argv[2];
const url = process.env.DATABASE_URL;

if (command === "verify") {
  const manifestPath = Bun.argv[3];
  if (!manifestPath) throw new Error("Usage: archive.ts verify <manifest.json>");
  const verified = await verifyLedgerArchive(manifestPath);
  console.log(JSON.stringify({ verified: true, ...verified }));
  process.exit(0);
}

if (command === "export") {
  const month = Bun.argv[3];
  const directory = Bun.argv[4];
  if (!url || !month || !directory) {
    throw new Error("Usage: DATABASE_URL=... archive.ts export <YYYY-MM> <directory>");
  }
  const db = createDatabase(url, 2);
  try {
    const manifestPath = await exportLedgerArchive(db, month, directory);
    console.log(JSON.stringify({ exported: true, manifest: manifestPath }));
  } finally {
    await db.close();
  }
  process.exit(0);
}

throw new Error(
  "Usage: archive.ts export <YYYY-MM> <directory> | archive.ts verify <manifest.json>",
);
