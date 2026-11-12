import { sql } from "drizzle-orm";
import { customType } from "drizzle-orm/pg-core";

export const jsonb = customType<{ data: unknown; driverData: unknown }>({
  dataType: () => "jsonb",
  toDriver: (value) => (value === null ? null : Bun.sql.json(value)),
});

export const jsonNull = sql`'null'::jsonb`;
