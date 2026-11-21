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

const exits = children.map(
  (child) =>
    new Promise<number>((resolve) => {
      child.once("exit", (code) => resolve(code ?? 0));
      child.once("error", (error) => {
        console.error(error.message);
        resolve(1);
      });
    }),
);
const firstExit = await Promise.race(exits);
shutdown();
await Promise.all(exits);
process.exitCode = firstExit;
