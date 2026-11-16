import { expect, test } from "bun:test";
import { withCatalog } from "../support/catalog.ts";
test("the catalog role cannot bypass controlled mutations with a caller-set GUC or assume owner", () =>
  withCatalog(async (f, db) => {
    const owner = (await db`SELECT current_user AS owner`)[0].owner;
    const connection = f.appDb;
    await connection`SELECT set_config('gateway.catalog_mutation','on',false)`;
    expect(
      (await connection`SELECT pg_has_role(current_user,${owner},'MEMBER') AS owns`)[0].owns,
    ).toBe(false);
    await expect(connection`SELECT set_config('role',${owner},false)`).rejects.toMatchObject({
      errno: "42501",
    });
    await expect(connection`UPDATE plugin_versions SET state='PUBLISHED'`).rejects.toMatchObject({
      errno: "42501",
    });
    await expect(
      connection`UPDATE plugin_validation_runs SET state='SUCCEEDED'`,
    ).rejects.toMatchObject({ errno: "42501" });
    await expect(
      connection`UPDATE channel_plugin_bindings SET config_revision=999`,
    ).rejects.toMatchObject({ errno: "42501" });
    await expect(
      connection`UPDATE channels SET binding_id=NULL,config_revision=999`,
    ).rejects.toMatchObject({ errno: "42501" });
  }));
