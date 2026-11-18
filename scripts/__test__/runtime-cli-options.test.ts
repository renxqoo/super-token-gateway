import { expect, test } from "bun:test";
import { resolve } from "node:path";

for (const timeout of ["0", "60001", "NaN", "1.5"])
  test(`owner runtime CLI rejects invalid timeout ${timeout} before connecting or reporting success`, async () => {
    const url = "postgres://fixture@127.0.0.1:1/runtime_cli_options";
    const child = Bun.spawn(
      [
        process.execPath,
        resolve("scripts/approve-plugin-runtime.ts"),
        "--database",
        "runtime_cli_options",
        "--package-digest",
        "a".repeat(64),
        "--kind",
        "wasm",
        "--artifact-digest",
        "b".repeat(64),
        "--disable",
        "--actor",
        "unit-owner",
        "--reason",
        "Invalid deadline must fail before database access",
        `--timeout-ms=${timeout}`,
      ],
      {
        env: { PATH: process.env.PATH, DATABASE_URL: url, MIGRATION_DATABASE_URL: url },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const stdout = new Response(child.stdout).text();
    const stderr = new Response(child.stderr).text();
    try {
      expect(await child.exited).not.toBe(0);
      expect(await stdout).toBe("");
      expect(await stderr).toContain("RUNTIME_APPROVAL_BUDGET_INVALID");
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await child.exited;
      await Promise.allSettled([stdout, stderr]);
    }
  });
