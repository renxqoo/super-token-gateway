import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createGzip, createGunzip } from "node:zlib";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { join, dirname } from "node:path";
import { z } from "zod";
import type { SQL } from "bun";
import { evidenceMonth, SCHEMA_VERSION } from "@gateway/storage";

const manifestSchema = z
  .object({
    version: z.literal(1),
    schema: z.literal(SCHEMA_VERSION),
    month: z.string(),
    semantics: z.literal("repeatable_read_snapshot_online_evidence_retained"),
    captured_at: z.string().datetime(),
    snapshot: z.string(),
    files: z
      .array(
        z
          .object({
            kind: z.enum(["usage", "journal"]),
            filename: z.enum(["usage.ndjson.gz", "journal.ndjson.gz"]),
            rows: z.number().int().nonnegative(),
            sha256: z.string().regex(/^[a-f0-9]{64}$/),
            content_sha256: z.string().regex(/^[a-f0-9]{64}$/),
          })
          .strict(),
      )
      .length(2),
  })
  .strict();
function instantMicros(value: string): bigint {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00:00)$/.exec(value);
  if (!match) throw new Error("Archive timestamp must retain UTC microseconds");
  const millis = Date.parse(`${match[1]}Z`);
  if (!Number.isFinite(millis) || new Date(millis).toISOString().slice(0, 19) !== match[1])
    throw new Error("Invalid archive timestamp");
  return BigInt(millis) * 1000n + BigInt((match[2] ?? "").padEnd(6, "0"));
}
async function fileHash(path: string): Promise<string> {
  const hash = new Bun.CryptoHasher("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}
export async function exportEvidenceArchive(
  db: SQL,
  month: string,
  directory: string,
): Promise<string> {
  const range = evidenceMonth(month);
  if (new Date(range.end).getTime() > Date.now())
    throw new Error("Only completed UTC months can be archived");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const manifest: z.infer<typeof manifestSchema> = {
    version: 1,
    schema: SCHEMA_VERSION,
    month,
    semantics: "repeatable_read_snapshot_online_evidence_retained",
    captured_at: new Date().toISOString(),
    snapshot: "",
    files: [],
  };
  await db.begin(async (tx) => {
    await tx`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`;
    await tx`SET LOCAL TIME ZONE 'UTC'`;
    await tx`SET LOCAL statement_timeout=0`;
    manifest.snapshot = String((await tx`SELECT pg_export_snapshot() AS id`)[0].id);
    const versions = await tx`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`;
    if (versions.length !== 1 || versions[0].version !== SCHEMA_VERSION)
      throw new Error("Archive schema mismatch");
    for (const [kind, table] of [
      ["usage", "usage_events"],
      ["journal", "money_journal"],
    ] as const) {
      const hash = new Bun.CryptoHasher("sha256");
      let rows = 0,
        afterTime: string | null = null,
        afterId: string | null = null;
      const filename = `${kind}.ndjson.gz` as "usage.ndjson.gz" | "journal.ndjson.gz";
      async function* records() {
        while (true) {
          const batch = await tx.unsafe(
            `SELECT to_jsonb(e)::text AS record,e.created_at::text AS time,e.id::text AS id FROM public.${table} e WHERE e.created_at>=$1::timestamptz AND e.created_at<$2::timestamptz AND ($3::timestamptz IS NULL OR (e.created_at,e.id)>($3::timestamptz,$4::uuid)) ORDER BY e.created_at,e.id LIMIT 256`,
            [range.start, range.end, afterTime, afterId],
          );
          if (!batch.length) break;
          for (const row of batch) {
            const line = `${row.record}\n`;
            hash.update(line);
            rows++;
            yield line;
          }
          afterTime = String(batch.at(-1).time);
          afterId = String(batch.at(-1).id);
        }
      }
      await pipeline(
        Readable.from(records()),
        createGzip(),
        createWriteStream(join(directory, filename), { flags: "wx", mode: 0o600 }),
      );
      manifest.files.push({
        kind,
        filename,
        rows,
        content_sha256: hash.digest("hex"),
        sha256: await fileHash(join(directory, filename)),
      });
    }
  });
  const path = join(directory, "manifest.json");
  await writeFile(path, JSON.stringify(manifest, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  await verifyEvidenceArchive(path);
  return path;
}
export async function verifyEvidenceArchive(path: string): Promise<z.infer<typeof manifestSchema>> {
  const metadata = await readFile(path, "utf8");
  if (metadata.length > 16384) throw new Error("Archive manifest exceeds budget");
  const manifest = manifestSchema.parse(JSON.parse(metadata));
  const range = evidenceMonth(manifest.month);
  if (new Set(manifest.files.map((file) => file.kind)).size !== 2)
    throw new Error("Archive kinds must be unique");
  for (const file of manifest.files) {
    if (file.filename !== `${file.kind}.ndjson.gz`)
      throw new Error("Archive file identity mismatch");
    const source = join(dirname(path), file.filename);
    if ((await fileHash(source)) !== file.sha256)
      throw new Error("Archive compressed checksum mismatch");
    const hash = new Bun.CryptoHasher("sha256");
    const input = createReadStream(source).pipe(createGunzip());
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let text = "",
      rows = 0,
      lastTime: bigint | undefined = undefined,
      lastId = "";
    for await (const raw of input) {
      hash.update(raw);
      text += decoder.decode(raw, { stream: true });
      let newline: number;
      while ((newline = text.indexOf("\n")) >= 0) {
        if (newline > 1048576) throw new Error("Archive record exceeds budget");
        const row = JSON.parse(text.slice(0, newline)) as { id?: unknown; created_at?: unknown };
        const id = z.uuid().parse(row.id),
          time = instantMicros(z.string().parse(row.created_at));
        if (
          time < instantMicros(range.start) ||
          time >= instantMicros(range.end) ||
          (lastTime !== undefined && time < lastTime) ||
          (time === lastTime && id <= lastId)
        )
          throw new Error("Archive record identity, order or month mismatch");
        rows++;
        lastTime = time;
        lastId = id;
        text = text.slice(newline + 1);
      }
      if (text.length > 1048576) throw new Error("Archive record exceeds budget");
    }
    text += decoder.decode();
    if (text.length || rows !== file.rows || hash.digest("hex") !== file.content_sha256)
      throw new Error("Archive content checksum or count mismatch");
  }
  return manifest;
}
