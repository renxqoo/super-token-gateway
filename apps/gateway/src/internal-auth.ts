import { readFileSync } from "node:fs";
import { z } from "zod";
import { secureEqual } from "@gateway/runtime";

const roles = ["ops", "catalog", "security", "finance", "auditor", "events"] as const;
export type InternalRole = (typeof roles)[number];

const credentialList = z
  .array(
    z.strictObject({
      id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.@-]{0,119}$/),
      token_sha256: z.string().regex(/^[a-f0-9]{64}$/),
      roles: z.array(z.enum(roles)).min(1).max(roles.length),
      consumers: z.array(z.string().min(1).max(120)).max(100).default([]),
    }),
  )
  .min(1)
  .max(256);

export type InternalCredentials = z.output<typeof credentialList>;
export type InternalPrincipal = {
  id: string;
  roles: readonly InternalRole[];
  consumers: readonly string[];
  legacy: boolean;
};

export function loadInternalCredentials(file: string): InternalCredentials {
  try {
    const credentials = credentialList.parse(JSON.parse(readFileSync(file, "utf8")));
    if (
      new Set(credentials.map((c) => c.id)).size !== credentials.length ||
      new Set(credentials.map((c) => c.token_sha256)).size !== credentials.length ||
      credentials.some(
        (c) => c.id === "legacy-admin" || c.roles.includes("events") !== c.consumers.length > 0,
      )
    ) {
      throw new Error("Invalid credential assignment");
    }
    return credentials;
  } catch {
    throw new Error(
      "INTERNAL_AUTH_FILE must contain valid distinct credentials and role assignments",
    );
  }
}

export function authenticateInternal(
  authorization: string | undefined,
  credentials: InternalCredentials | undefined,
  legacyToken: string | undefined,
): InternalPrincipal | undefined {
  const token = /^Bearer ([^\s,]+)$/i.exec(authorization ?? "")?.[1];
  if (!token || token.length > 256) return;
  if (!credentials) {
    if (legacyToken && secureEqual(token, legacyToken)) {
      return { id: "legacy-admin", roles, consumers: [], legacy: true };
    }
    return;
  }
  if (!/^[A-Za-z0-9_-]{43,128}$/.test(token)) return;
  const hash = new Bun.CryptoHasher("sha256").update(token).digest("hex");
  let principal: InternalPrincipal | undefined;
  for (const credential of credentials) {
    if (secureEqual(hash, credential.token_sha256)) {
      principal = {
        id: credential.id,
        roles: credential.roles,
        consumers: credential.consumers,
        legacy: false,
      };
    }
  }
  return principal;
}
