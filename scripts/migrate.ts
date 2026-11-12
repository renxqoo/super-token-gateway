import { createDatabase, migrate } from "@gateway/storage";
const url = process.env.MIGRATION_DATABASE_URL ?? process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");
if (Bun.argv.includes("--upgrade"))
  throw new Error(
    "Historical database upgrades are unsupported; explicitly select a new empty v2 database",
  );
const database = Bun.argv[Bun.argv.indexOf("--database") + 1];
if (
  !Bun.argv.includes("--database") ||
  !database ||
  decodeURIComponent(new URL(url).pathname.slice(1)) !== database
)
  throw new Error("Pass --database <exact-name> matching MIGRATION_DATABASE_URL or DATABASE_URL");
const db = createDatabase(url);
try {
  await migrate(db);
  console.log("Core v2 schema is current");
} finally {
  await db.close();
}
