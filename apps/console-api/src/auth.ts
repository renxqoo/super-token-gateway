import { timingSafeEqual } from "node:crypto";
import { isIP } from "node:net";
import { and, eq, gt, isNull } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  consoleAuditLogs,
  orgMembers,
  projects,
  sessions,
  users,
  type ConsoleDrizzle,
  type OrgRole,
} from "./db/schema.ts";

export type AuthUser = {
  id: string;
  email: string;
  displayName: string;
  isPlatformAdmin: boolean;
  status: "ACTIVE" | "SUSPENDED";
  sessionId: string;
};

export function sha256Hex(value: string): string {
  return new Bun.CryptoHasher("sha256").update(value).digest("hex");
}

export function generateOpaqueToken(prefix: string): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return `${prefix}${Buffer.from(bytes).toString("hex")}`;
}

export function constantTimeEquals(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

export async function hashPassword(password: string): Promise<string> {
  return Bun.password.hash(password, {
    algorithm: "argon2id",
    memoryCost: 65536,
    timeCost: 2,
  });
}

export async function verifyPassword(password: string, hash: string): Promise<boolean> {
  return Bun.password.verify(password, hash);
}

export function maskSecret(secret: string): string {
  const trimmed = secret.trim();
  if (!trimmed) return "";
  if (trimmed.length <= 8) return `${trimmed.slice(0, 2)}***`;
  return `${trimmed.slice(0, 6)}***${trimmed.slice(-4)}`;
}

export function isValidCidr(raw: string): boolean {
  const trimmed = raw.trim();
  const slash = trimmed.indexOf("/");
  const ipPart = slash === -1 ? trimmed : trimmed.slice(0, slash);
  const prefixPart = slash === -1 ? null : trimmed.slice(slash + 1);
  const family = isIP(ipPart);
  if (family === 0) return false;
  if (prefixPart === null) return true;
  if (!/^\d{1,3}$/.test(prefixPart)) return false;
  const bits = Number(prefixPart);
  const maxBits = family === 4 ? 32 : 128;
  return Number.isInteger(bits) && bits >= 0 && bits <= maxBits;
}

export function isSafePublicWebhookUrl(raw: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return false;
  }
  if (parsed.protocol !== "https:") return false;
  const host = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (!host || host === "localhost" || host.endsWith(".local") || host.endsWith(".internal")) {
    return false;
  }
  if (isIP(host) !== 0) {
    if (
      host === "::1" ||
      host.startsWith("127.") ||
      host.startsWith("10.") ||
      host.startsWith("169.254.") ||
      host.startsWith("192.168.") ||
      host.startsWith("0.") ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(host) ||
      host.startsWith("fc") ||
      host.startsWith("fd") ||
      host.startsWith("fe80")
    ) {
      return false;
    }
  }
  return true;
}

export class LoginRateLimiter {
  private readonly buckets = new Map<string, number[]>();

  constructor(
    private readonly maxAttempts = 10,
    private readonly windowMs = 15 * 60_000,
    private readonly maxKeys = 10_000,
  ) {}

  private prune(key: string, now: number): number[] {
    const existing = this.buckets.get(key);
    if (!existing) return [];
    const cutoff = now - this.windowMs;
    const kept = existing.filter((ts) => ts > cutoff);
    if (kept.length === 0) this.buckets.delete(key);
    else this.buckets.set(key, kept);
    return kept;
  }

  private pruneExpiredAll(now: number): void {
    const cutoff = now - this.windowMs;
    for (const [k, arr] of this.buckets) {
      const kept = arr.filter((ts) => ts > cutoff);
      if (kept.length === 0) this.buckets.delete(k);
      else this.buckets.set(k, kept);
    }
  }

  check(key: string, now = Date.now()): void {
    const kept = this.prune(key, now);
    if (kept.length >= this.maxAttempts) {
      throw new GatewayError(429, "too_many_attempts", "Too many failed login attempts");
    }
  }

  recordFailure(key: string, now = Date.now()): void {
    if (this.buckets.size >= this.maxKeys && !this.buckets.has(key)) {
      this.pruneExpiredAll(now);
      if (this.buckets.size >= this.maxKeys) {
        for (const [k, arr] of this.buckets) {
          if (arr.length < this.maxAttempts) {
            this.buckets.delete(k);
            break;
          }
        }
      }
      if (this.buckets.size >= this.maxKeys) return;
    }
    const kept = this.prune(key, now);
    kept.push(now);
    this.buckets.set(key, kept);
  }

  reset(key: string): void {
    this.buckets.delete(key);
  }
}

export function extractBearerToken(headerValue: string | undefined): string | null {
  if (!headerValue) return null;
  if (!headerValue.toLowerCase().startsWith("bearer ")) return null;
  const token = headerValue.slice(7).trim();
  return token || null;
}

export async function authenticateSession(
  orm: ConsoleDrizzle,
  authHeader: string | undefined,
): Promise<AuthUser> {
  const token = extractBearerToken(authHeader);
  if (!token) throw new GatewayError(401, "unauthorized", "Missing authentication token");
  const digest = sha256Hex(token);
  const nowIso = new Date().toISOString();
  const rows = await orm
    .select({
      sessionId: sessions.id,
      userId: users.id,
      email: users.email,
      displayName: users.displayName,
      isPlatformAdmin: users.isPlatformAdmin,
      status: users.status,
    })
    .from(sessions)
    .innerJoin(users, eq(users.id, sessions.userId))
    .where(
      and(
        eq(sessions.tokenHash, digest),
        isNull(sessions.revokedAt),
        gt(sessions.expiresAt, nowIso),
      ),
    )
    .limit(1);
  const row = rows[0];
  if (!row) throw new GatewayError(401, "unauthorized", "Invalid or expired session");
  if (row.status !== "ACTIVE") {
    throw new GatewayError(403, "account_suspended", "User account is suspended");
  }
  return {
    id: row.userId,
    email: row.email,
    displayName: row.displayName,
    isPlatformAdmin: row.isPlatformAdmin,
    status: row.status,
    sessionId: row.sessionId,
  };
}

export async function requireOrgMember(
  orm: ConsoleDrizzle,
  userId: string,
  orgId: string,
  allowedRoles?: readonly OrgRole[],
): Promise<OrgRole> {
  const rows = await orm
    .select({ role: orgMembers.role })
    .from(orgMembers)
    .where(and(eq(orgMembers.orgId, orgId), eq(orgMembers.userId, userId)))
    .limit(1);
  const member = rows[0];
  if (!member) throw new GatewayError(404, "not_found", "Organization not found");
  if (allowedRoles && !allowedRoles.includes(member.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient organization role");
  }
  return member.role;
}

export async function requireProjectAccess(
  orm: ConsoleDrizzle,
  userId: string,
  projectId: string,
  allowedRoles?: readonly OrgRole[],
) {
  const rows = await orm
    .select({
      project: projects,
      role: orgMembers.role,
    })
    .from(projects)
    .innerJoin(orgMembers, and(eq(orgMembers.orgId, projects.orgId), eq(orgMembers.userId, userId)))
    .where(eq(projects.id, projectId))
    .limit(1);
  const row = rows[0];
  if (!row) throw new GatewayError(404, "not_found", "Project not found");
  if (allowedRoles && !allowedRoles.includes(row.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient role for project operation");
  }
  return row;
}

export async function recordAuditLog(
  orm: ConsoleDrizzle,
  input: {
    actor?: AuthUser | null;
    orgId?: string | null;
    action: string;
    targetType: string;
    targetId: string;
    metadata?: Record<string, unknown>;
  },
): Promise<void> {
  await orm.insert(consoleAuditLogs).values({
    id: crypto.randomUUID(),
    actorUserId: input.actor?.id ?? null,
    actorEmail: input.actor?.email ?? null,
    orgId: input.orgId ?? null,
    action: input.action,
    targetType: input.targetType,
    targetId: input.targetId,
    metadata: input.metadata ?? {},
  });
}
