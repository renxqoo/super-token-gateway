import { expect, test } from "bun:test";
import { migrate, Store } from "@gateway/storage";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withDatabase } from "../support/database.ts";
test("actual v3 through financial schema upgrade preserves historical channels audits costs and immutable packages", () =>
  withDatabase(
    async (db) => {
      for (const entry of manifest.migrations.slice(0, 2)) {
        for (const file of entry.files)
          await db.unsafe(
            await Bun.file(
              new URL(`../../packages/storage/src/${file.path}`, import.meta.url),
            ).text(),
          );
        const checksum = new Bun.CryptoHasher("sha256").update(JSON.stringify(entry)).digest("hex");
        await db`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${entry.version},${checksum},${entry}::jsonb)`;
      }
      const id = crypto.randomUUID(),
        store = new Store(db);
      await store.createProvider(
        id,
        {
          name: "retained-v3-channel",
          kind: "openai",
          base_url: "http://127.0.0.1:1/v1",
          concurrency: 1,
          rpm: 1,
        },
        "retained-encrypted-secret",
      );
      const cost = crypto.randomUUID();
      await db`INSERT INTO cost_versions(id,currency,rates) VALUES(${cost},'USD','{}')`;
      await db`INSERT INTO audit_events(id,action,resource_id) VALUES(${crypto.randomUUID()},'retained_v3_audit','retained')`;
      const digest = "a".repeat(64);
      await db`INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by) VALUES(${digest},${Buffer.from("historical-v3-package")},${{ historical: true }},'historical','historical','historical')`;
      const before = (
        await db`SELECT id,brand_id,account_id,secret,config_revision::text FROM channels`
      )[0];
      await migrate(db);
      expect(await db`SELECT version FROM schema_migrations ORDER BY version`).toEqual([
        { version: 2026111202 },
        { version: 2026111203 },
        { version: 2026111204 },
        { version: 2026111205 },
        { version: 2026111206 },
        { version: 2026111207 },
        { version: 2026111208 },
      ]);
      expect(
        (await db`SELECT id,brand_id,account_id,secret,config_revision::text FROM channels`)[0],
      ).toEqual(before);
      expect(
        (await db`SELECT actor,details FROM audit_events WHERE action='retained_v3_audit'`)[0],
      ).toEqual({ actor: null, details: null });
      expect((await db`SELECT ceilings FROM cost_versions WHERE id=${cost}`)[0].ceilings).toEqual(
        {},
      );
      const pkg = (
        await db`SELECT package_bytes,release_descriptor FROM plugin_packages WHERE digest=${digest}`
      )[0];
      expect(Buffer.from(pkg.package_bytes).toString()).toBe("historical-v3-package");
      expect(pkg.release_descriptor).toBeNull();
      expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(0);
      await migrate(db);
      expect((await db`SELECT count(*)::int AS n FROM schema_migrations`)[0].n).toBe(7);
    },
    { registry: ".local/T04b1-resources.jsonl" },
  ));
