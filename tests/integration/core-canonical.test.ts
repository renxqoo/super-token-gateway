import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { migrate } from "@gateway/storage";
import { withDatabase } from "../support/database.ts";

test("database canonical key ordering preserves every ASCII code unit and long protocol field names", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const inputs = [
      "",
      ...Array.from({ length: 127 }, (_, n) => String.fromCharCode(n + 1)),
      "credential_ciphertext_sha256",
      "authorization_plan_digest",
      "ascii_key_".repeat(1000),
    ];
    const rows = await db<
      { value: string; bytes: string }[]
    >`SELECT value,encode(core_utf16_sort_v1(value),'hex') bytes FROM unnest(${inputs}::text[]) value`;
    expect(rows).toHaveLength(inputs.length);
    for (const row of rows)
      expect(row.bytes).toBe(Buffer.from(row.value, "utf16le").swap16().toString("hex"));
  }));

test("database canonical mixed Unicode ordering uses UTF-16 rather than UTF-8, locale or JSONB storage order", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const input = {
      "\uffff": 7,
      "\ue000": 6,
      "😀": 5,
      "𐀀": 4,
      "\ud7ff": 3,
      é: 2,
      a: 1,
      "\u0001": 0,
    };
    const expected = '{"\\u0001":0,"a":1,"é":2,"\ud7ff":3,"𐀀":4,"😀":5,"\ue000":6,"\uffff":7}';
    const [row] =
      await db`SELECT core_canonical_v1(${input}::jsonb) canonical,core_digest_v1(${input}::jsonb) digest`;
    expect(row.canonical).toBe(expected);
    expect(row.digest).toBe(createHash("sha256").update(expected).digest("hex"));
  }));

test("database canonical preserves nested structures, empty values, escaped strings and persisted numeric spelling", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const expected =
      '{"a":[null,true,false,{},[],{"n":1.250,"text":"a b\\t\\n\\\"\\\\é中😀"}],"z":-12.50}';
    const [row] =
      await db`SELECT core_canonical_v1('{"z":-12.50,"a":[null,true,false,{},[],{"text":"a b\\t\\n\\\"\\\\é中😀","n":1.250}]}'::jsonb) canonical`;
    expect(row.canonical).toBe(expected);
    for (const [literal, canonical] of [
      ["null", "null"],
      ["true", "true"],
      ["false", "false"],
      ["[]", "[]"],
      ["{}", "{}"],
      ["-0.000", "0.000"],
    ])
      expect(
        (await db.unsafe("SELECT core_canonical_v1($1::text::jsonb) value", [literal]))[0].value,
      ).toBe(canonical);
  }));

test("database canonical preserves SQL NULL and its ASCII fast path is independent of string escape settings", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const definitions = (
      await db<
        { def: string }[]
      >`SELECT pg_get_functiondef(oid) AS def FROM pg_proc WHERE oid IN ('public.core_utf16_sort_v1(text)'::regprocedure,'public.core_canonical_v1(jsonb)'::regprocedure) ORDER BY proname`
    )
      .map((row) => `${row.def};`)
      .join("\n");
    for (const setting of ["off", "on"]) {
      await db.begin(async (tx) => {
        await tx`SELECT set_config('standard_conforming_strings',${setting},true)`;
        await tx.unsafe(definitions);
        const input = { z: "a\\b\t\n", a: [null, false, {}, []], é: 1, "😀": 2 },
          expected = '{"a":[null,false,{},[]],"z":"a\\\\b\\t\\n","é":1,"😀":2}';
        const [row] =
          await tx`SELECT core_canonical_v1(${input}::jsonb) canonical,encode(core_utf16_sort_v1('credential_ciphertext_sha256'),'hex') sort,core_canonical_v1(NULL::jsonb) sql_null,core_digest_v1(NULL::jsonb) null_digest`;
        expect(row.canonical).toBe(expected);
        expect(row.sort).toBe(
          Buffer.from("credential_ciphertext_sha256", "utf16le").swap16().toString("hex"),
        );
        expect(row.sql_null).toBeNull();
        expect(row.null_digest).toBeNull();
      });
    }
    await expect(db`SELECT core_utf16_sort_v1(NULL::text)`).rejects.toMatchObject({
      errno: "22004",
    });
  }));
