import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { migrate } from "@gateway/storage";
import { withDatabase } from "../support/database.ts";

async function costPrice(
  db: SQL,
  rates: Record<string, { numerator: string; denominator: string }>,
) {
  const id = crypto.randomUUID();
  await db`INSERT INTO cost_versions(id,currency,rates) VALUES(${id},'USD',${rates}::jsonb)`;
  return id;
}

async function price(db: SQL, id: string, usage: Record<string, string>) {
  return (
    await db`SELECT core_funding.price_v1(${usage}::jsonb,p.dimensions,p.weights,p.denominator)::text amount FROM core_funding.cost_prices p WHERE p.price_id=${id}`
  )[0].amount as string;
}

test("compiled version prices preserve per-request ceiling, exposure subtraction, decimals, zero dimensions and the 38-digit boundary", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const third = { numerator: "1", denominator: "3" };
    const two = await costPrice(db, { input_tokens: third, output_tokens: third });
    expect(await price(db, two, { input_tokens: "1", output_tokens: "1" })).toBe("1");
    const perRequest = await costPrice(db, { input_tokens: third });
    expect(
      await db`SELECT core_funding.price_v1(u,p.dimensions,p.weights,p.denominator)::text amount FROM core_funding.cost_prices p CROSS JOIN unnest(${[{ input_tokens: "1" }, { input_tokens: "1" }]}::jsonb[]) u WHERE p.price_id=${perRequest}`,
    ).toEqual([{ amount: "1" }, { amount: "1" }]);
    expect(
      await db`SELECT core_funding.price_v1('{"input_tokens":"1","output_tokens":"1"}'::jsonb,p.dimensions,p.weights,p.denominator)-core_funding.price_v1('{"input_tokens":"1"}'::jsonb,p.dimensions,p.weights,p.denominator) AS reserve FROM core_funding.cost_prices p WHERE p.price_id=${two}`,
    ).toEqual([{ reserve: "0" }]);
    expect(
      (
        await db`SELECT core_funding.exposure_v1(${two}::uuid,'{"input_tokens":"1","output_tokens":"1"}'::jsonb,'{"input_tokens":"1"}'::jsonb,ARRAY['input_tokens'],'{}'::jsonb) value`
      )[0].value,
    ).toEqual({
      cost_atoms: "1",
      reserve_atoms: "0",
      base_reserve_atoms: "0",
      complete: false,
      overrun: false,
      risk_dimensions: [],
    });
    expect(
      (
        await db`SELECT core_funding.exposure_v1(${two}::uuid,'{"input_tokens":"1","output_tokens":"1"}'::jsonb,'{}'::jsonb,ARRAY[]::text[],'{"input_tokens":"4","output_tokens":"7"}'::jsonb) value`
      )[0].value,
    ).toEqual({
      cost_atoms: "0",
      reserve_atoms: "4",
      base_reserve_atoms: "1",
      complete: false,
      overrun: false,
      risk_dimensions: ["input_tokens", "output_tokens"],
    });
    await expect(
      db`SELECT core_funding.exposure_v1(${two}::uuid,'{"input_tokens":"1","output_tokens":"1"}'::jsonb,'{}'::jsonb,NULL::text[],'{}'::jsonb)`,
    ).rejects.toThrow("supplier_exposure_invalid");
    const free = await costPrice(db, {});
    expect(
      (
        await db`SELECT core_funding.exposure_v1(${free}::uuid,'{}'::jsonb,'{}'::jsonb,ARRAY[]::text[],'{}'::jsonb) value`
      )[0].value,
    ).toEqual({
      cost_atoms: "0",
      reserve_atoms: "0",
      base_reserve_atoms: "0",
      complete: true,
      overrun: false,
      risk_dimensions: [],
    });
    const decimal = await costPrice(db, {
      audio_seconds: { numerator: "99999999999999999999999999999999999999", denominator: "1" },
    });
    expect(await price(db, decimal, { audio_seconds: "0.000000000000000001" })).toBe(
      "100000000000000000000",
    );
    const wide = await costPrice(db, {
      historic_a: { numerator: "1", denominator: "99999999999999999999999999999999999999" },
      historic_b: { numerator: "1", denominator: "99999999999999999999999999999999999997" },
    });
    expect(await price(db, wide, { historic_a: "1", historic_b: "1" })).toBe("1");
    expect(await price(db, two, { input_tokens: "0.5", output_tokens: "0.5" })).toBe("1");
    const zero = await costPrice(db, { input_tokens: { numerator: "0", denominator: "7" } });
    expect(await price(db, zero, { input_tokens: "5" })).toBe("0");
    expect(
      (
        await db`SELECT core_funding.exposure_v1(${zero}::uuid,'{"input_tokens":"1"}'::jsonb,'{"input_tokens":"2"}'::jsonb,ARRAY['input_tokens'],'{}'::jsonb) value`
      )[0].value,
    ).toEqual({
      cost_atoms: "0",
      reserve_atoms: "0",
      base_reserve_atoms: "0",
      complete: true,
      overrun: true,
      risk_dimensions: [],
    });
    await expect(price(db, zero, { input_tokens: "invalid" })).rejects.toThrow(
      "invalid price input",
    );
    await expect(price(db, decimal, { audio_seconds: "2" })).rejects.toThrow(
      "price exceeds amount range",
    );
    const billing = crypto.randomUUID();
    await db`INSERT INTO billing_prices(id,currency,rates,ceilings,basis) VALUES(${billing},'USD','{"requests":{"numerator":"3","denominator":"2"}}'::jsonb,'{"requests":"1"}'::jsonb,'REQUEST_COMPLETION')`;
    expect(
      await db`SELECT core_funding.price_v1('{"requests":"1"}'::jsonb,p.dimensions,p.weights,p.denominator)::text amount FROM core_funding.billing_prices p WHERE p.price_id=${billing}`,
    ).toEqual([{ amount: "2" }]);
  }));

test("compiled prices are immutable and invalid source prices cannot leave partial rows", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const first = await costPrice(db, { input_tokens: { numerator: "1", denominator: "3" } });
    const current = await costPrice(db, { input_tokens: { numerator: "5", denominator: "3" } });
    expect(await price(db, first, { input_tokens: "1" })).toBe("1");
    expect(await price(db, current, { input_tokens: "1" })).toBe("2");
    await expect(
      db`UPDATE core_funding.cost_prices SET weights=ARRAY[999::numeric] WHERE price_id=${first}`,
    ).rejects.toThrow("immutable record");
    expect(await price(db, first, { input_tokens: "1" })).toBe("1");
    const invalid = crypto.randomUUID();
    const failure =
      await db`INSERT INTO cost_versions(id,currency,rates) VALUES(${invalid},'USD','{"input_tokens":{"numerator":"1","denominator":"0"}}'::jsonb)`.catch(
        (error: unknown) => error,
      );
    expect(String(failure)).toContain(
      `price_program_compile_failed:cost_versions:${invalid}:invalid price input`,
    );
    expect(await db`SELECT id FROM cost_versions WHERE id=${invalid}`).toEqual([]);
    expect(
      await db`SELECT price_id FROM core_funding.cost_prices WHERE price_id=${invalid}`,
    ).toEqual([]);
  }));
