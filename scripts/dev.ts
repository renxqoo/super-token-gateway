import { spawn } from "node:child_process";

const children = [
  spawn(process.execPath, ["apps/mock-providers/src/main.ts"], {
    stdio: "inherit",
    env: process.env,
  }),
  spawn(process.execPath, ["apps/gateway/src/main.ts", "--mode=all-in-one"], {
    stdio: "inherit",
    env: process.env,
  }),
];

function shutdown(): void {
  for (const child of children) child.kill("SIGTERM");
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

await Promise.all(
  children.map(
    (child) =>
      new Promise<void>((resolve) => {
        child.on("exit", () => resolve());
      }),
  ),
);
