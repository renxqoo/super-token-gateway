import { afterAll, beforeAll } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

export let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;

export const adminSession = { access: "", refresh: "", userId: "" };

export function registerAuthFixture() {
  beforeAll(async () => {
    h = await createConsoleTestHarness({
      consoleConfigOverrides: {
        clientCorsOrigins: ["https://client.test"],
        adminCorsOrigins: ["https://admin.internal"],
      },
      enableWorker: false,
    });
  });

  afterAll(async () => {
    await h?.close();
  });
}
