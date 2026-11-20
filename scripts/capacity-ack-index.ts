import { Database } from "bun:sqlite";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";

export async function createCapacityAckIndex(directory: string) {
  const scratch = await mkdtemp(join(directory, ".ack-index-"));
  let db: Database | undefined;
  let closing: Promise<void> | undefined;
  let size = 0;

  function close() {
    return (closing ??= (async () => {
      try {
        db?.close();
      } finally {
        await rm(scratch, { recursive: true, force: true });
      }
    })());
  }

  function page(ids: string[]) {
    if (closing || ids.length > 8192 || ids.some((id) => !/^[A-Za-z0-9_-]{1,100}$/.test(id)))
      throw new Error("Invalid ACK index page");
    return JSON.stringify(ids);
  }

  try {
    db = new Database(join(scratch, "observed.sqlite"), { create: true });
    db.run("PRAGMA cache_size=-2048");
    db.run("PRAGMA mmap_size=0");
    db.run(
      "CREATE TABLE observed(id TEXT PRIMARY KEY, first_ack_ms INTEGER NOT NULL) WITHOUT ROWID",
    );
    const missing = db.query<{ id: string }, [string]>(
      "SELECT DISTINCT value AS id FROM json_each(?) AS batch WHERE NOT EXISTS (SELECT 1 FROM observed WHERE observed.id=batch.value)",
    );
    const insert = db.query<{ id: string }, [string, number]>(
      "INSERT INTO observed(id,first_ack_ms) SELECT value,?2 FROM json_each(?1) WHERE true ON CONFLICT(id) DO NOTHING RETURNING id",
    );
    return {
      get size() {
        return size;
      },
      unseen(ids: string[]) {
        return missing.all(page(ids)).map((row) => row.id);
      },
      add(ids: string[], at: number) {
        if (!Number.isSafeInteger(at) || at < 0) throw new Error("Invalid ACK observation time");
        const fresh = insert.all(page(ids), at).map((row) => row.id);
        size += fresh.length;
        return fresh;
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}
