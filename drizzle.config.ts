import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "postgresql",
  schema: "./packages/ledger/src/schema.ts",
  out: "./packages/ledger/src/migrations",
  strict: true,
});
