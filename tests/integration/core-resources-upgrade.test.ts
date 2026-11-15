import { expect, test } from "bun:test";
import { migrate, createCatalogStore } from "@gateway/storage";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withDatabase } from "../support/database.ts";

test("the resources append pins legacy resource rows to their channel's current plugin binding and leaves unbound channels unpinned", () =>
  withDatabase(async (db) => {
    const target = manifest.migrations.findIndex((entry) => entry.version === 2026111218);
    for (const entry of manifest.migrations.slice(0, target)) {
      for (const file of entry.files)
        await db.unsafe(
          await Bun.file(
            new URL(`../../packages/storage/src/${file.path}`, import.meta.url),
          ).text(),
        );
      const checksum = new Bun.CryptoHasher("sha256").update(JSON.stringify(entry)).digest("hex");
      await db`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${entry.version},${checksum},${entry}::jsonb)`;
    }
    const store = createCatalogStore(db),
      bound = crypto.randomUUID(),
      unbound = crypto.randomUUID();
    for (const id of [bound, unbound])
      await store.createProvider(
        id,
        { name: id, kind: "openai", base_url: "https://synthetic.test/v1", concurrency: 1, rpm: 1 },
        "retained-encrypted-secret",
      );
    const channel = (await db`SELECT account_id,brand_id FROM channels WHERE id=${bound}`)[0] as {
      account_id: string;
      brand_id: string;
    };
    const digest = "b".repeat(64),
      version = crypto.randomUUID(),
      binding = crypto.randomUUID(),
      project = crypto.randomUUID();
    await db`INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by) VALUES(${digest},${Buffer.from("synthetic-package")},${{ synthetic: true }},'synthetic','synthetic','synthetic')`;
    await db`INSERT INTO plugin_versions(id,name,version,package_digest,abi_version,config_schema_digest) VALUES(${version},'synthetic','1.0.0',${digest},1,${"c".repeat(64)})`;
    await db`INSERT INTO channel_plugin_bindings(id,channel_id,plugin_version_id,account_id,brand_id,config_revision,config,config_digest,actor) VALUES(${binding},${bound},${version},${channel.account_id},${channel.brand_id},1,'{}',${"d".repeat(64)},'synthetic')`;
    await db`UPDATE channels SET binding_id=${binding},config_revision=1 WHERE id=${bound}`;
    await db`INSERT INTO projects(id,name,billing_currency,rpm,tpm,concurrency) VALUES(${project},'legacy-resources','USD',1,1,1)`;
    for (const [id, provider] of [
      ["response_legacybound", bound],
      ["response_legacyunbound", unbound],
    ])
      await db`INSERT INTO resources(id,project_id,provider_id,remote_id,kind,model) VALUES(${id},${project},${provider},${`remote-${id}`},'response','legacy')`;
    await migrate(db);
    expect(await db`SELECT id,binding_id::text FROM resources ORDER BY id`).toEqual([
      { id: "response_legacybound", binding_id: binding },
      { id: "response_legacyunbound", binding_id: null },
    ]);
  }));
