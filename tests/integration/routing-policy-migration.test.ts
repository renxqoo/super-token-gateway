import { expect, test } from "bun:test";
import { assertSchemaVersion, migrate, SCHEMA_VERSION } from "@gateway/ledger";
import {
  CONSOLE_SCHEMA_VERSION,
  createConsoleDatabase,
  migrateConsoleDb,
} from "@gateway/console-api";
import gatewayManifest from "../../packages/ledger/src/migrations/manifest.json";
import consoleManifest from "../../apps/console-api/src/db/migrations/manifest.json";
import { withLedgerDb } from "./ledger-fixture.ts";

const policy = {
  operations: ["chat"],
  mode: "observe",
  first_content_target_ms: 1200,
  supplier_reserve_limit: { currency: "USD", atoms: "300" },
};
const normalized = { ...policy, execution_policy: "protected" };
const explicit = { ...policy, mode: "active", execution_policy: "route" };
const stored = [
  { name: "disabled", policy: false },
  { name: "explicit", policy: explicit },
  { name: "inherited", policy: null },
  { name: "protected", policy },
];

test("Gateway upgrades stored model policies without rewriting revisions or switching protections", () =>
  withLedgerDb(
    async (db) => {
      await db.begin(async (tx) => {
        const root = new URL("../../packages/ledger/src/database.ts", import.meta.url);
        for (const migration of gatewayManifest.migrations.filter(
          ({ version }) => version <= 2026112804,
        )) {
          for (const { path } of migration.files) {
            await tx.unsafe(await Bun.file(new URL(path, root)).text());
          }
          const checksum = new Bun.CryptoHasher("sha256")
            .update(JSON.stringify(migration))
            .digest("hex");
          await tx`INSERT INTO public.schema_migrations(version, checksum, manifest)
            VALUES (${migration.version}, ${checksum}, ${migration}::jsonb)`;
        }
        for (const item of stored) {
          await tx`INSERT INTO public.models(name, operations, max_output_tokens, adaptive_routing, source_revision)
            VALUES (${item.name}, ARRAY['chat'], 100, ${JSON.stringify(item.policy)}::text::jsonb, 5)`;
        }
      });
      const [before] =
        await db`SELECT version::float8 AS version, lineage_token FROM public.config_epoch`;
      await migrate(db);
      expect(await assertSchemaVersion(db)).toBe(SCHEMA_VERSION);
      const [after] =
        await db`SELECT version::float8 AS version, lineage_token FROM public.config_epoch`;
      expect(after!.version).toBe(before!.version + 1);
      expect(after!.lineage_token).not.toBe(before!.lineage_token);
      expect(
        await db`
        SELECT name, adaptive_routing AS policy, source_revision FROM public.models ORDER BY name
      `,
      ).toEqual([
        { name: "disabled", policy: false, source_revision: 5 },
        { name: "explicit", policy: explicit, source_revision: 5 },
        { name: "inherited", policy: null, source_revision: 5 },
        { name: "protected", policy: normalized, source_revision: 5 },
      ]);
      await expect(db`
        UPDATE public.models SET adaptive_routing = ${JSON.stringify(policy)}::text::jsonb WHERE name = 'protected'
      `).rejects.toThrow(/models_adaptive_execution_policy_check/);
      await migrate(db);
      expect(
        await db`SELECT version::float8 AS version, lineage_token FROM public.config_epoch`,
      ).toEqual([after]);
    },
    async () => {},
  ));

test("Console migrates an outstanding model sync to the same explicit contract", () =>
  withLedgerDb(
    async (_, url) => {
      const db = createConsoleDatabase(url, 2);
      try {
        await db.begin(async (tx) => {
          const root = new URL("../../apps/console-api/src/db/migrate.ts", import.meta.url);
          await tx`CREATE TABLE public.console_schema_migrations (
            version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
          )`;
          for (const migration of consoleManifest.migrations.filter(
            ({ version }) => version <= 2026112802,
          )) {
            await tx.unsafe(await Bun.file(new URL(migration.path, root)).text());
            await tx`INSERT INTO public.console_schema_migrations (version, checksum)
              VALUES (${migration.version}, ${migration.checksum})`;
          }
          for (const item of stored) {
            await tx`INSERT INTO public.model_catalog
              (name, display_name, operations, adaptive_routing, desired_revision, synced_revision)
              VALUES (${item.name}, ${item.name}, ARRAY['chat'], ${JSON.stringify(item.policy)}::text::jsonb, 7, 5)`;
          }
        });
        expect(await migrateConsoleDb(db)).toBe(CONSOLE_SCHEMA_VERSION);
        const models = await db`
          SELECT name, adaptive_routing AS policy, desired_revision, synced_revision, sync_status
          FROM public.model_catalog ORDER BY name
        `;
        expect(models).toEqual([
          {
            name: "disabled",
            policy: false,
            desired_revision: 7,
            synced_revision: 5,
            sync_status: "PENDING",
          },
          {
            name: "explicit",
            policy: explicit,
            desired_revision: 7,
            synced_revision: 5,
            sync_status: "PENDING",
          },
          {
            name: "inherited",
            policy: null,
            desired_revision: 7,
            synced_revision: 5,
            sync_status: "PENDING",
          },
          {
            name: "protected",
            policy: normalized,
            desired_revision: 7,
            synced_revision: 5,
            sync_status: "PENDING",
          },
        ]);
        await expect(db`
          UPDATE public.model_catalog SET adaptive_routing = ${JSON.stringify(policy)}::text::jsonb WHERE name = 'protected'
        `).rejects.toThrow(/model_catalog_adaptive_execution_policy_check/);
      } finally {
        await db.close();
      }
    },
    async () => {},
  ));
