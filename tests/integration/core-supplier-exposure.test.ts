import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { migrate } from "@gateway/storage";
import { withDatabase } from "../support/database.ts";

const rate = (numerator: string, denominator = "1") => ({ numerator, denominator });

async function exposure(
  db: SQL,
  input: {
    rates: Record<string, { numerator: string; denominator: string }>;
    estimated: Record<string, string>;
    usage: Record<string, string>;
    finals: string[];
    risk?: Record<string, string>;
  },
) {
  return (
    await db`SELECT core_supplier_exposure_v1(${input.rates}::jsonb,${input.estimated}::jsonb,${input.usage}::jsonb,${input.finals}::text[],${input.risk ?? {}}::jsonb) value`
  )[0].value as {
    cost_atoms: string;
    reserve_atoms: string;
    base_reserve_atoms: string;
    complete: boolean;
    overrun: boolean;
    risk_dimensions: string[];
  };
}

test("supplier exposure follows the independent manual examples for known cost and remaining reserve", () =>
  withDatabase(
    async (db) => {
      await migrate(db);
      const a = {
        rates: { requests: rate("2"), input_tokens: rate("1", "3"), output_tokens: rate("1", "3") },
        estimated: { requests: "1", input_tokens: "60", output_tokens: "30" },
      };
      expect(await exposure(db, { ...a, usage: {}, finals: [] })).toMatchObject({
        cost_atoms: "0",
        reserve_atoms: "32",
        complete: false,
      });
      expect(
        await exposure(db, {
          ...a,
          usage: { requests: "1", input_tokens: "30" },
          finals: ["requests"],
        }),
      ).toMatchObject({ cost_atoms: "2", reserve_atoms: "30", complete: false, overrun: false });
      expect(
        await exposure(db, {
          ...a,
          usage: { requests: "1", input_tokens: "31", output_tokens: "2" },
          finals: ["requests", "input_tokens", "output_tokens"],
        }),
      ).toMatchObject({ cost_atoms: "13", reserve_atoms: "0", complete: true, overrun: false });

      const b = {
        rates: { input_tokens: rate("1"), output_tokens: rate("1") },
        estimated: { input_tokens: "20", output_tokens: "20" },
      };
      expect(
        await exposure(db, { ...b, usage: { input_tokens: "30" }, finals: ["input_tokens"] }),
      ).toMatchObject({ cost_atoms: "30", reserve_atoms: "20", overrun: true, complete: false });
      expect(
        await exposure(db, {
          ...b,
          usage: { input_tokens: "30", output_tokens: "25" },
          finals: ["input_tokens", "output_tokens"],
        }),
      ).toMatchObject({ cost_atoms: "55", reserve_atoms: "0", overrun: true, complete: true });

      const c = {
        rates: { requests: rate("20"), output_tokens: rate("1") },
        estimated: { requests: "1", output_tokens: "60" },
      };
      expect(
        await exposure(db, { ...c, usage: { requests: "1" }, finals: ["requests"] }),
      ).toMatchObject({ cost_atoms: "20", reserve_atoms: "60" });
      expect(
        await exposure(db, {
          ...c,
          usage: { requests: "1", output_tokens: "30" },
          finals: ["requests", "output_tokens"],
        }),
      ).toMatchObject({ cost_atoms: "50", reserve_atoms: "0", complete: true });

      const d = {
        rates: { input_tokens: rate("1"), output_tokens: rate("1") },
        estimated: { input_tokens: "20", output_tokens: "20" },
      };
      expect(
        await exposure(db, {
          ...d,
          usage: { input_tokens: "25" },
          finals: [],
          risk: { input_tokens: "25" },
        }),
      ).toEqual({
        cost_atoms: "0",
        reserve_atoms: "45",
        base_reserve_atoms: "40",
        complete: false,
        overrun: false,
        risk_dimensions: ["input_tokens"],
      });
      expect(
        await exposure(db, {
          ...d,
          usage: { input_tokens: "10", output_tokens: "15" },
          finals: ["input_tokens", "output_tokens"],
          risk: { input_tokens: "25" },
        }),
      ).toEqual({
        cost_atoms: "25",
        reserve_atoms: "0",
        base_reserve_atoms: "0",
        complete: true,
        overrun: false,
        risk_dimensions: [],
      });

      const thirds = {
        rates: { input_tokens: rate("1", "3"), output_tokens: rate("1", "3") },
        estimated: { input_tokens: "1", output_tokens: "1" },
      };
      expect(await exposure(db, { ...thirds, usage: {}, finals: [] })).toMatchObject({
        reserve_atoms: "1",
      });
      expect(
        await exposure(db, {
          rates: { audio_ms: rate("8", "1000") },
          estimated: { audio_ms: "125" },
          usage: { audio_ms: "125" },
          finals: ["audio_ms"],
        }),
      ).toMatchObject({ cost_atoms: "1", reserve_atoms: "0" });
      expect(
        await exposure(db, {
          rates: { requests: rate("9007199254740993") },
          estimated: { requests: "1" },
          usage: { requests: "1" },
          finals: ["requests"],
        }),
      ).toMatchObject({ cost_atoms: "9007199254740993" });

      for (const invalid of [
        { ...b, estimated: { input_tokens: "20" }, usage: {}, finals: [] },
        { ...b, usage: {}, finals: ["input_tokens"] },
      ]) {
        const failure = await exposure(db, invalid).catch((error) => error);
        expect(failure).toBeInstanceOf(Error);
      }
    },
    { registry: ".local/T04b2b-resources.jsonl" },
  ));
