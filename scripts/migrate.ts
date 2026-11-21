import { createDatabase, migrate, SCHEMA_VERSION } from "@gateway/ledger";

const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");

const db = createDatabase(url, 4);
try {
  await migrate(db);
  console.log(JSON.stringify({ migrated: true, schema_version: SCHEMA_VERSION }));
} finally {
  await db.close();
}
