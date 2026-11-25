import { writeFile } from "node:fs/promises";
import type { InternalCredentials } from "../apps/gateway/src/internal-auth.ts";

const localRoles = ["ops", "catalog", "security", "finance", "auditor", "events"] as const;

export function localInternalCredentials(
  token: string,
  consumers: string[] = ["default", "console_api"],
): InternalCredentials {
  return [
    {
      id: "local-operator",
      token_sha256: new Bun.CryptoHasher("sha256").update(token).digest("hex"),
      roles: [...localRoles],
      consumers,
    },
  ];
}

export function localConsoleTokens(token: string) {
  return {
    ops: token,
    catalog: token,
    security: token,
    finance: token,
    auditor: token,
    events: token,
  };
}

export function localConsoleTokenEnvironment(token: string): Record<string, string> {
  return Object.fromEntries(
    localRoles.map((role) => ["GATEWAY_" + role.toUpperCase() + "_TOKEN", token]),
  );
}

export async function writeLocalInternalAuth(path: string | URL, token: string) {
  await writeFile(path, JSON.stringify(localInternalCredentials(token)), {
    mode: 0o600,
    flag: "wx",
  });
}
