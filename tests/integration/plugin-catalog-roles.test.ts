import { expect, test } from "bun:test";
import { appendFile } from "node:fs/promises";
import { createDatabase } from "@gateway/storage";
import { withCatalog } from "../support/catalog.ts";
const registry = ".local/T04b1-resources.jsonl";
test("runtime worker and catalog roles cannot bypass controlled mutations with a caller-set GUC or assume owner", () =>
  withCatalog(async (f, db) => {
    const suffix = crypto.randomUUID().replaceAll("-", ""),
      runtime = `gateway_t04b1_runtime_${suffix}`,
      worker = `gateway_t04b1_worker_${suffix}`;
    const roles = [runtime, worker],
      password = crypto.randomUUID(),
      connections: ReturnType<typeof createDatabase>[] = [];
    const record = async (event: string, role: string) =>
      appendFile(registry, JSON.stringify({ event, role }) + "\n", { mode: 0o600 });
    const identity = (await db`SELECT current_database() AS database,current_user AS owner`)[0];
    try {
      for (const role of roles) {
        await db.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
        await record("role_created", role);
      }
      await db.unsafe(
        (await Bun.file("infra/runtime-role.sql").text())
          .replaceAll("gateway_runtime", runtime)
          .replaceAll("gateway_worker", worker),
      );
      const probes = [
        ...roles.map((role) => {
          const url = new URL(process.env.DATABASE_URL!);
          url.pathname = `/${identity.database}`;
          url.username = role;
          url.password = password;
          const connection = createDatabase(url.toString(), 1);
          connections.push(connection);
          return connection;
        }),
        f.appDb,
      ];
      for (const connection of probes) {
        await connection`SELECT set_config('gateway.catalog_mutation','on',false)`;
        expect(
          (await connection`SELECT pg_has_role(current_user,${identity.owner},'MEMBER') AS owns`)[0]
            .owns,
        ).toBe(false);
        await expect(
          connection`SELECT set_config('role',${identity.owner},false)`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          connection`UPDATE plugin_versions SET state='PUBLISHED'`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          connection`UPDATE plugin_validation_runs SET state='SUCCEEDED'`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          connection`UPDATE channel_plugin_bindings SET config_revision=999`,
        ).rejects.toMatchObject({ errno: "42501" });
        await expect(
          connection`UPDATE channels SET binding_id=NULL,config_revision=999`,
        ).rejects.toMatchObject({ errno: "42501" });
      }
      for (const connection of connections)
        await expect(
          connection`SELECT plugin_catalog_command_v1('plugin_publish_v1','bootstrap:forged',${crypto.randomUUID()}::uuid,'{}',NULL,NULL,NULL,NULL,NULL,false)`,
        ).rejects.toMatchObject({ errno: "42501" });
    } finally {
      await Promise.all(connections.map((c) => c.close()));
      for (const role of roles) {
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
        await record("role_dropped", role);
      }
    }
  }));
