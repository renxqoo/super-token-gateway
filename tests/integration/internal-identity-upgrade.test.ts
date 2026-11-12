import { expect, test } from "bun:test";
import { migrate } from "@gateway/storage";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withDatabase } from "../support/database.ts";
test("v4 identity append preserves legacy null owners and rejects new unclassified execution commands", () =>
  withDatabase(
    async (db) => {
      for (const migration of manifest.migrations.slice(0, 3)) {
        for (const entry of migration.files)
          await db.unsafe(
            await Bun.file(
              new URL(`../../packages/storage/src/${entry.path}`, import.meta.url),
            ).text(),
          );
        const checksum = new Bun.CryptoHasher("sha256")
          .update(JSON.stringify(migration))
          .digest("hex");
        await db`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${migration.version},${checksum},${migration}::jsonb)`;
      }
      const id = crypto.randomUUID(),
        command = crypto.randomUUID();
      await db`INSERT INTO execution_commands(id,command_id,kind,caller_identity,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state) VALUES(${id},${command},'historical_unclassified','legacy:owner',${crypto.randomUUID()},repeat('c',64),clock_timestamp()+interval '1 hour','legacy',1,'COMMITTED')`;
      const before = (await db`SELECT * FROM execution_commands WHERE id=${id}`)[0];
      await migrate(db);
      const after = (await db`SELECT * FROM execution_commands WHERE id=${id}`)[0];
      const { owner_session_id, receipt, context_id, ...legacy } = after;
      expect(owner_session_id).toBeNull();
      expect(context_id).toBeNull();
      expect(receipt).toEqual({});
      expect(legacy).toEqual(before);
      await expect(
        db`INSERT INTO execution_commands(id,command_id,kind,caller_identity,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state) VALUES(${crypto.randomUUID()},${crypto.randomUUID()},'historical_unclassified','legacy:owner',${crypto.randomUUID()},repeat('d',64),clock_timestamp()+interval '1 hour','forged-legacy',1,'COMMITTED')`,
      ).rejects.toMatchObject({ errno: "P0403" });
      await expect(
        db`UPDATE execution_commands SET caller_identity='new:owner' WHERE id=${id}`,
      ).rejects.toThrow();
      expect((await db`SELECT count(*)::int AS n FROM internal_startup_sessions`)[0].n).toBe(0);
      expect((await db`SELECT count(*)::int AS n FROM schema_migrations`)[0].n).toBe(
        manifest.migrations.length,
      );
    },
    { registry: ".local/identity-resources.jsonl" },
  ));
