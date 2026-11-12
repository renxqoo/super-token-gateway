import { defineConfig } from "drizzle-kit";
export default defineConfig({
  dialect: "postgresql",
  schema: "./packages/storage/src/schema/index.ts",
  out: "./packages/storage/src/migrations",
  strict: true,
});
