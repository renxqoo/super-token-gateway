import { createDatabase, migrate } from "@gateway/storage";
const url = process.env.MIGRATION_DATABASE_URL ?? process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");
const db = createDatabase(url);
try {
  await migrate(db, {
    upgrade: Bun.argv.includes("--upgrade"),
    quiesced: Bun.argv.includes("--quiesced"),
  });
  console.log("Gateway schema is current");
} finally {
  await db.close();
}
