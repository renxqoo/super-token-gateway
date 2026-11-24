import {
  CONSOLE_SCHEMA_VERSION,
  assertConsoleSchemaVersion,
  createConsoleDatabase,
  migrateConsoleDb,
} from "@gateway/console-api";
import { GatewayClient } from "../apps/console-api/src/gateway-client.ts";

const url = process.env.CONSOLE_MIGRATION_DATABASE_URL || process.env.CONSOLE_DATABASE_URL;
if (!url) throw new Error("CONSOLE_MIGRATION_DATABASE_URL or CONSOLE_DATABASE_URL is required");

const db = createConsoleDatabase(url, 4);
try {
  const args = Bun.argv.slice(2);
  if (args.length === 0) {
    await migrateConsoleDb(db);
    console.log(JSON.stringify({ migrated: true, console_schema_version: CONSOLE_SCHEMA_VERSION }));
  } else if (args.length === 1 && args[0] === "--bind-projects") {
    await assertConsoleSchemaVersion(db);
    const baseUrl = process.env.GATEWAY_INTERNAL_URL;
    const security = process.env.GATEWAY_SECURITY_TOKEN ?? process.env.GATEWAY_ADMIN_TOKEN;
    if (!baseUrl || !security)
      throw new Error(
        "GATEWAY_INTERNAL_URL and GATEWAY_SECURITY_TOKEN are required to bind projects",
      );
    const gateway = new GatewayClient({ baseUrl, tokens: { security } });
    let after: string | null = null;
    let checked = 0;
    let repaired = 0;
    while (true) {
      const projects: { id: string; org_id: string; gateway_project_id: string }[] = await db`
        SELECT id, org_id, gateway_project_id FROM public.projects
        WHERE gateway_synced AND (${after}::uuid IS NULL OR id > ${after}::uuid)
        ORDER BY id LIMIT 100
      `;
      if (projects.length === 0) break;
      for (const project of projects) {
        const result = await gateway.bindConsoleProject(
          project.gateway_project_id,
          project.org_id,
          project.id,
        );
        checked++;
        if (result.bound) repaired++;
      }
      after = projects.at(-1)!.id;
    }
    console.log(JSON.stringify({ console_project_bindings_verified: true, checked, repaired }));
  } else {
    throw new Error("Usage: console-migrate.js [--bind-projects]");
  }
} finally {
  await db.close();
}
