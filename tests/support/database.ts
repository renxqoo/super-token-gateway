import { createDatabase } from "@gateway/storage";
import type { SQL } from "bun";
import { appendFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
export async function withDatabase(
  run: (db: SQL, name: string) => Promise<void>,
  options: { max?: number; registry?: string } = {},
): Promise<void> {
  const base = process.env.DATABASE_URL;
  if (!base) throw new Error("DATABASE_URL required");
  const name = `gateway_test_foundation_${crypto.randomUUID().replaceAll("-", "")}`;
  const owner = createDatabase(base, 1);
  let created = false;
  let db: SQL | undefined;
  const record = async (event: string) => {
    if (options.registry) {
      await mkdir(dirname(options.registry), { recursive: true, mode: 0o700 });
      await appendFile(
        options.registry,
        `${JSON.stringify({ event, name, at: new Date().toISOString() })}\n`,
        { mode: 0o600 },
      );
    }
  };
  try {
    await owner.unsafe(`CREATE DATABASE ${name}`);
    created = true;
    await record("created");
    const url = new URL(base);
    url.pathname = `/${name}`;
    db = createDatabase(url.toString(), options.max ?? 1);
    await run(db, name);
  } finally {
    try {
      await db?.close();
      if (created) {
        await owner.unsafe(`DROP DATABASE ${name} WITH (FORCE)`);
        await record("dropped");
      }
    } finally {
      await owner.close();
    }
  }
}
