import { timingSafeEqual } from "node:crypto";
import { lookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { and, eq, gt, isNull } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  consoleAuditLogs,
  organizations,
  orgMembers,
  projects,
  sessions,
  users,
  type ConsoleDrizzle,
  type OrgKind,
  type OrgRole,
  type SessionScope,
} from "./db/schema.ts";

export type AuthUser = {
  id: string;
  email: string;
  displayName: string;
  isPlatformAdmin: boolean;
  status: "ACTIVE" | "SUSPENDED";
  sessionId: string;
  sessionScope: SessionScope;
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

let dummyHashPromise: Promise<string> | null = null;
export async function burnDummyPasswordVerify(password: string): Promise<void> {
  dummyHashPromise ??= hashPassword("console-dummy-timing-guard-0000");
  const dummy = await dummyHashPromise;
  await verifyPassword(password, dummy).catch(() => false);
}

export function maskSecret(secret: string): string {
  const trimmed = secret.trim();
  if (!trimmed) return "";
  if (trimmed.length <= 8) return `${trimmed.slice(0, 2)}***`;
  return `${trimmed.slice(0, 6)}***${trimmed.slice(-4)}`;
}

export function normalizeIpAddress(raw: string): string {
  const trimmed = raw
    .trim()
    .toLowerCase()
    .replace(/^\[|\]$/g, "");
  if (trimmed.startsWith("::ffff:")) {
    const rest = trimmed.slice(7);
    if (isIP(rest) === 4) return rest;
    const hexMatch = /^([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(rest);
    if (hexMatch) {
      const hi = Number.parseInt(hexMatch[1]!, 16);
      const lo = Number.parseInt(hexMatch[2]!, 16);
      return `${(hi >> 8) & 0xff}.${hi & 0xff}.${(lo >> 8) & 0xff}.${lo & 0xff}`;
    }
  }
  return trimmed;
}

export function normalizeAndValidateCidr(raw: string): string | null {
  const trimmed = raw.trim();
  const parts = trimmed.split("/");
  if (parts.length > 2) return null;
  const address = normalizeIpAddress(parts[0] ?? "");
  const family = isIP(address);
  if (family === 0) return null;
  const maxBits = family === 4 ? 32 : 128;
  if (parts[1] === undefined) return `${address}/${maxBits}`;
  if (!/^\d{1,3}$/.test(parts[1])) return null;
  const bits = Number(parts[1]);
  if (!Number.isInteger(bits) || bits < 0 || bits > maxBits) return null;
  return `${address}/${bits}`;
}

export function isValidCidr(raw: string): boolean {
  return normalizeAndValidateCidr(raw) !== null;
}

const blockedV4 = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const) {
  blockedV4.addSubnet(address, prefix, "ipv4");
}

const blockedV6 = new BlockList();
for (const [address, prefix] of [
  ["2001::", 23],
  ["2001:2::", 48],
  ["2001:db8::", 32],
  ["2001:10::", 28],
  ["2001:20::", 28],
  ["2002::", 16],
  ["3fff::", 20],
] as const) {
  blockedV6.addSubnet(address, prefix, "ipv6");
}

const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");

export function isPublicIpAddress(raw: string): boolean {
  const address = normalizeIpAddress(raw);
  const family = isIP(address);
  if (family === 4) return !blockedV4.check(address, "ipv4");
  if (family === 6) return globalV6.check(address, "ipv6") && !blockedV6.check(address, "ipv6");
  return false;
}

const cidrMatcherCache = new Map<string, BlockList>();

export function inCidrList(rawAddress: string, cidrs: readonly string[]): boolean {
  const address = normalizeIpAddress(rawAddress);
  const family = isIP(address);
  if (family === 0 || cidrs.length === 0) return false;
  const key = cidrs.join(",");
  let matcher = cidrMatcherCache.get(key);
  if (!matcher) {
    matcher = new BlockList();
    for (const cidr of cidrs) {
      const normalized = normalizeAndValidateCidr(cidr);
      if (!normalized) continue;
      const [base, prefix] = normalized.split("/");
      matcher.addSubnet(base!, Number(prefix), isIP(base!) === 4 ? "ipv4" : "ipv6");
    }
    if (cidrMatcherCache.size > 1000) cidrMatcherCache.clear();
    cidrMatcherCache.set(key, matcher);
  }
  return matcher.check(address, family === 4 ? "ipv4" : "ipv6");
}

export function resolveClientIp(
  peerIp: string | undefined,
  forwardedHeader: string | undefined,
  trustedProxyCidrs: readonly string[],
): string | null {
  if (!peerIp) return null;
  const peer = normalizeIpAddress(peerIp);
  if (isIP(peer) === 0) return null;
  if (trustedProxyCidrs.length === 0 || !inCidrList(peer, trustedProxyCidrs) || !forwardedHeader) {
    return peer;
  }
  const chain = forwardedHeader
    .split(",")
    .map((part) => normalizeIpAddress(part))
    .filter(Boolean);
  if (chain.length === 0 || chain.length > 32 || chain.some((ip) => isIP(ip) === 0)) {
    return peer;
  }
  let current = peer;
  for (let i = chain.length - 1; i >= 0; i--) {
    if (!inCidrList(current, trustedProxyCidrs)) break;
    current = chain[i]!;
  }
  return current;
}

export function isSafePublicWebhookUrl(raw: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return false;
  }
  if (parsed.protocol !== "https:") return false;
  if (parsed.username || parsed.password) return false;
  const rawHost = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, "");
  const host = rawHost.replace(/\.+$/, "");
  if (
    !host ||
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host.endsWith(".local") ||
    host.endsWith(".internal")
  ) {
    return false;
  }
  const normalizedIp = normalizeIpAddress(host);
  if (isIP(normalizedIp) !== 0) {
    return isPublicIpAddress(normalizedIp);
  }
  return true;
}

export async function assertPublicWebhookDns(
  rawUrl: string,
  lookupImpl: typeof lookup = lookup,
): Promise<string[]> {
  if (!isSafePublicWebhookUrl(rawUrl)) {
    throw new GatewayError(
      400,
      "unsafe_webhook_url",
      "Webhook URL must target a public HTTPS host",
    );
  }
  const parsed = new URL(rawUrl);
  const host = parsed.hostname
    .toLowerCase()
    .replace(/^\[|\]$/g, "")
    .replace(/\.+$/, "");
  const normalizedIp = normalizeIpAddress(host);
  if (isIP(normalizedIp) !== 0) {
    if (!isPublicIpAddress(normalizedIp)) {
      throw new GatewayError(400, "unsafe_webhook_url", "Webhook URL resolves to a non-public IP");
    }
    return [normalizedIp];
  }
  const records = await lookupImpl(host, { all: true, verbatim: true });
  if (!records || records.length === 0) {
    throw new GatewayError(502, "webhook_dns_failed", "Webhook hostname did not resolve");
  }
  const resolved: string[] = [];
  for (const rec of records) {
    const ip = normalizeIpAddress(rec.address);
    if (!isPublicIpAddress(ip)) {
      throw new GatewayError(
        400,
        "unsafe_webhook_url",
        "Webhook hostname resolved to non-public IP",
      );
    }
    resolved.push(ip);
  }
  return resolved;
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

  private makeCapacity(key: string, now: number): boolean {
    if (this.buckets.size < this.maxKeys || this.buckets.has(key)) return true;
    this.pruneExpiredAll(now);
    if (this.buckets.size < this.maxKeys) return true;
    for (const [k, arr] of this.buckets) {
      if (arr.length < this.maxAttempts) {
        this.buckets.delete(k);
        return true;
      }
    }
    return false;
  }

  check(key: string, now = Date.now()): void {
    const kept = this.prune(key, now);
    if (kept.length >= this.maxAttempts) {
      throw new GatewayError(429, "too_many_attempts", "Too many failed login attempts");
    }
    if (!this.buckets.has(key) && this.buckets.size >= this.maxKeys) {
      if (!this.makeCapacity(key, now)) {
        throw new GatewayError(429, "too_many_attempts", "Authentication rate limit capacity full");
      }
    }
  }

  recordFailure(key: string, now = Date.now()): void {
    if (!this.makeCapacity(key, now)) {
      throw new GatewayError(429, "too_many_attempts", "Authentication rate limit capacity full");
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
  expectedScope: SessionScope,
): Promise<AuthUser> {
  const token = extractBearerToken(authHeader);
  if (!token) throw new GatewayError(401, "unauthorized", "Missing authentication token");
  const digest = sha256Hex(token);
  const now = new Date();
  const rows = await orm
    .select({
      sessionId: sessions.id,
      sessionScope: sessions.scope,
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
        eq(sessions.scope, expectedScope),
        isNull(sessions.revokedAt),
        gt(sessions.expiresAt, now),
      ),
    )
    .limit(1);
  const row = rows[0];
  if (!row) throw new GatewayError(401, "unauthorized", "Invalid or expired session");
  if (row.status !== "ACTIVE") {
    throw new GatewayError(403, "account_suspended", "User account is suspended");
  }
  if (expectedScope === "ADMIN" && !row.isPlatformAdmin) {
    throw new GatewayError(403, "forbidden", "Platform administrator privileges required");
  }
  return {
    id: row.userId,
    email: row.email,
    displayName: row.displayName,
    isPlatformAdmin: row.isPlatformAdmin,
    status: row.status,
    sessionId: row.sessionId,
    sessionScope: row.sessionScope,
  };
}

export async function requireOrgMember(
  orm: ConsoleDrizzle,
  userId: string,
  orgId: string,
  allowedRoles?: readonly OrgRole[],
): Promise<{ role: OrgRole; orgKind: OrgKind }> {
  const rows = await orm
    .select({
      role: orgMembers.role,
      orgKind: organizations.kind,
      orgStatus: organizations.status,
    })
    .from(orgMembers)
    .innerJoin(organizations, eq(organizations.id, orgMembers.orgId))
    .where(and(eq(orgMembers.orgId, orgId), eq(orgMembers.userId, userId)))
    .limit(1);
  const member = rows[0];
  if (!member) throw new GatewayError(404, "not_found", "Organization not found");
  if (member.orgStatus !== "ACTIVE") {
    throw new GatewayError(403, "org_suspended", "Organization is suspended");
  }
  if (allowedRoles && !allowedRoles.includes(member.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient organization role");
  }
  return { role: member.role, orgKind: member.orgKind };
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
      orgKind: organizations.kind,
      orgStatus: organizations.status,
    })
    .from(projects)
    .innerJoin(organizations, eq(organizations.id, projects.orgId))
    .innerJoin(orgMembers, and(eq(orgMembers.orgId, projects.orgId), eq(orgMembers.userId, userId)))
    .where(eq(projects.id, projectId))
    .limit(1);
  const row = rows[0];
  if (!row) throw new GatewayError(404, "not_found", "Project not found");
  if (row.orgStatus !== "ACTIVE") {
    throw new GatewayError(403, "org_suspended", "Organization is suspended");
  }
  if (allowedRoles && !allowedRoles.includes(row.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient role for project operation");
  }
  return { project: row.project, role: row.role, orgKind: row.orgKind };
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
