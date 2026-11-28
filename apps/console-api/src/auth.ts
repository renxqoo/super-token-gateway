import { createHmac, timingSafeEqual } from "node:crypto";
import { lookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { and, eq, gt, isNull, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  consoleAuditLogs,
  organizations,
  orgMembers,
  billingAccounts,
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

const registrationTokenTtlMs = 15 * 60_000;

function registrationSignature(email: string, expiresAt: number, secret: string): string {
  return createHmac("sha256", Buffer.from(secret, "hex"))
    .update(`${expiresAt}\n${email}`)
    .digest("hex");
}

export function issueRegistrationToken(email: string, secret: string) {
  const expiresAt = Date.now() + registrationTokenTtlMs;
  return {
    registration_token: `${expiresAt}.${registrationSignature(email, expiresAt, secret)}`,
    expires_at: new Date(expiresAt).toISOString(),
  };
}

export function verifyRegistrationToken(email: string, token: string, secret: string): boolean {
  const match = /^(\d{13})\.([a-f0-9]{64})$/.exec(token);
  if (!match) return false;
  const expiresAt = Number(match[1]);
  const now = Date.now();
  return (
    expiresAt > now &&
    constantTimeEquals(match[2]!, registrationSignature(email, expiresAt, secret))
  );
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
  if (trimmed.length <= 2) return "***";
  if (trimmed.length <= 12) return `${trimmed.slice(0, 2)}***`;
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
  lookupImpl: (hostname: string) => Promise<{ address: string }[]> = (hostname) =>
    lookup(hostname, { all: true }),
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
  const records = await lookupImpl(host);
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

export async function fetchPublicWebhook(
  rawUrl: string,
  init: RequestInit,
  send: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response> = fetch,
  resolve: typeof assertPublicWebhookDns = assertPublicWebhookDns,
): Promise<Response> {
  const signal = init.signal;
  signal?.throwIfAborted();
  const pending = resolve(rawUrl);
  const addresses = signal
    ? await new Promise<string[]>((resolveAddresses, reject) => {
        const abort = () => reject(signal.reason);
        signal.addEventListener("abort", abort, { once: true });
        pending
          .then(resolveAddresses, reject)
          .finally(() => signal.removeEventListener("abort", abort));
      })
    : await pending;
  signal?.throwIfAborted();
  const host = new URL(rawUrl);
  const originalHostname = host.hostname.replace(/^\[|\]$/g, "");
  const address = addresses.find((ip) => isIP(ip) === 4) ?? addresses[0]!;
  const headers = new Headers(init.headers);
  headers.set("host", host.host);
  host.hostname = isIP(address) === 6 ? `[${address}]` : address;
  const pinnedInit: BunFetchRequestInit = {
    ...init,
    headers,
    redirect: "manual",
    proxy: false,
    tls: {
      rejectUnauthorized: true,
      ...(isIP(originalHostname) === 0 ? { serverName: originalHostname } : {}),
    },
  };
  return send(host, pinnedInit);
}

export class LoginRateLimiter {
  private readonly buckets = new Map<string, number[]>();
  private readonly activeAttempts = new Map<string, number>();
  private readonly maxAttempts: number;
  private readonly windowMs: number;
  private readonly maxKeys: number;
  private readonly maxPasswordWork: number;
  private activePasswordWork = 0;

  constructor(
    options: {
      maxAttempts?: number;
      windowMs?: number;
      maxKeys?: number;
      maxPasswordWork?: number;
    } = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 10;
    this.windowMs = options.windowMs ?? 15 * 60_000;
    this.maxKeys = options.maxKeys ?? 10_000;
    this.maxPasswordWork = options.maxPasswordWork ?? 4;
  }

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
      if (arr.length < this.maxAttempts && !this.activeAttempts.has(k)) {
        this.buckets.delete(k);
        return true;
      }
    }
    return false;
  }

  check(key: string, now = Date.now()): void {
    const kept = this.prune(key, now);
    if (kept.length + (this.activeAttempts.get(key) ?? 0) >= this.maxAttempts) {
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

  async runPasswordWork<T>(work: () => Promise<T>): Promise<T> {
    if (this.activePasswordWork >= this.maxPasswordWork) {
      throw new GatewayError(429, "too_many_attempts", "Authentication is busy");
    }
    this.activePasswordWork++;
    try {
      return await work();
    } finally {
      this.activePasswordWork--;
    }
  }

  async verifyAttempt<T>(
    keys: string[],
    verify: () => Promise<T | undefined>,
  ): Promise<T | undefined> {
    return this.runPasswordWork(async () => {
      for (const key of keys) this.check(key);
      for (const key of keys) this.activeAttempts.set(key, (this.activeAttempts.get(key) ?? 0) + 1);
      try {
        const result = await verify();
        if (result === undefined) for (const key of keys) this.recordFailure(key);
        return result;
      } finally {
        for (const key of keys) {
          const remaining = this.activeAttempts.get(key)! - 1;
          if (remaining === 0) this.activeAttempts.delete(key);
          else this.activeAttempts.set(key, remaining);
        }
      }
    });
  }
}

export function extractBearerToken(headerValue: string | undefined): string | null {
  if (!headerValue) return null;
  if (!headerValue.toLowerCase().startsWith("bearer ")) return null;
  const token = headerValue.slice(7).trim();
  return token || null;
}

export function assertAccountCanUseScope(
  user: Pick<AuthUser, "status" | "isPlatformAdmin">,
  scope: SessionScope,
): void {
  if (user.status !== "ACTIVE") {
    throw new GatewayError(403, "account_suspended", "User account is suspended");
  }
  if (scope === "ADMIN" && !user.isPlatformAdmin) {
    throw new GatewayError(403, "forbidden", "Platform administrator privileges required");
  }
  if (scope === "CLIENT" && user.isPlatformAdmin) {
    throw new GatewayError(
      403,
      "forbidden",
      "Platform administrator accounts must authenticate on the Admin API",
    );
  }
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
  assertAccountCanUseScope(row, expectedScope);
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
      inferenceReady: sql<boolean>`${organizations.inferenceRevision} = ${organizations.inferenceSyncedRevision}`,
    })
    .from(orgMembers)
    .innerJoin(organizations, eq(organizations.id, orgMembers.orgId))
    .where(and(eq(orgMembers.orgId, orgId), eq(orgMembers.userId, userId)))
    .limit(1);
  const member = rows[0];
  if (!member) throw new GatewayError(404, "not_found", "Organization not found");
  if (member.orgStatus !== "ACTIVE" || !member.inferenceReady) {
    throw new GatewayError(403, "org_suspended", "Organization is suspended");
  }
  if (allowedRoles && !allowedRoles.includes(member.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient organization role");
  }
  return { role: member.role, orgKind: member.orgKind };
}

export const defaultBillingAccountId = sql<string | null>`(
  SELECT ${billingAccounts.id}
  FROM ${billingAccounts}
  WHERE ${billingAccounts.orgId} = ${organizations.id}
  ORDER BY ${billingAccounts.createdAt}, ${billingAccounts.id}
  LIMIT 1
)`;

export async function requireBillingAccountAccess(
  orm: Pick<ConsoleDrizzle, "select">,
  userId: string,
  billingAccountId: string,
  allowedRoles?: readonly OrgRole[],
  lockMembership = false,
) {
  const query = orm
    .select({
      billingAccount: billingAccounts,
      role: orgMembers.role,
      orgKind: organizations.kind,
      orgStatus: organizations.status,
      inferenceReady: sql<boolean>`${organizations.inferenceRevision} = ${organizations.inferenceSyncedRevision}`,
    })
    .from(billingAccounts)
    .innerJoin(organizations, eq(organizations.id, billingAccounts.orgId))
    .innerJoin(
      orgMembers,
      and(eq(orgMembers.orgId, billingAccounts.orgId), eq(orgMembers.userId, userId)),
    )
    .where(eq(billingAccounts.id, billingAccountId))
    .limit(1);
  const [row] = await (lockMembership ? query.for("share", { of: orgMembers }) : query);
  if (!row) throw new GatewayError(404, "not_found", "Billing account not found");
  if (row.orgStatus !== "ACTIVE" || !row.inferenceReady) {
    throw new GatewayError(403, "org_suspended", "Organization is suspended");
  }
  if (allowedRoles && !allowedRoles.includes(row.role)) {
    throw new GatewayError(403, "forbidden", "Insufficient role for billing account operation");
  }
  return { billingAccount: row.billingAccount, role: row.role, orgKind: row.orgKind };
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
