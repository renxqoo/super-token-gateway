import {
  CONSOLE_SCHEMA_VERSION,
  createConsoleDatabase,
  migrateConsoleDb,
} from "@gateway/console-api";

const url = process.env.CONSOLE_DATABASE_URL;
if (!url) throw new Error("CONSOLE_DATABASE_URL is required");

const db = createConsoleDatabase(url, 4);
try {
  await migrateConsoleDb(db);
  console.log(JSON.stringify({ migrated: true, console_schema_version: CONSOLE_SCHEMA_VERSION }));
} finally {
  await db.close();
}
