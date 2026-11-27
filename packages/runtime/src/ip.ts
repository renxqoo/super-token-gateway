import { BlockList, isIP } from "node:net";
import { GatewayError } from "@gateway/contracts";
export function normalizeAddress(address: string): string {
  return address.startsWith("::ffff:") && isIP(address.slice(7)) === 4 ? address.slice(7) : address;
}
export function validateCidr(value: string): string {
  const parts = value.split("/");
  const address = normalizeAddress(parts[0] ?? "");
  const family = isIP(address);
  const bits = family === 4 ? 32 : 128;
  const prefix = parts[1] === undefined ? bits : Number(parts[1]);
  if (
    !family ||
    parts.length > 2 ||
    !Number.isInteger(prefix) ||
    prefix < 0 ||
    prefix > bits ||
    (parts[1] !== undefined && !/^\d{1,3}$/.test(parts[1]))
  )
    throw new GatewayError(400, "invalid_cidr", "IP ranges require valid IPv4 or IPv6 CIDRs");
  return `${address}/${prefix}`;
}
const matchers = new Map<string, BlockList>();
export function inCidrs(address: string, ranges: readonly string[]): boolean {
  address = normalizeAddress(address);
  const family = isIP(address);
  if (!family) return false;
  const key = ranges.join(",");
  let matcher = matchers.get(key);
  if (!matcher) {
    matcher = new BlockList();
    for (const range of ranges) {
      const [base, prefix] = validateCidr(range).split("/");
      matcher.addSubnet(base!, Number(prefix), isIP(base!) === 4 ? "ipv4" : "ipv6");
    }
    if (matchers.size > 10000) matchers.clear();
    matchers.set(key, matcher);
  }
  return matcher.check(address, family === 4 ? "ipv4" : "ipv6");
}
export function clientAddress(
  peer: string | undefined,
  forwarded: string | null,
  trusted: readonly string[],
): string | undefined {
  if (!peer) return undefined;
  peer = normalizeAddress(peer);
  if (!isIP(peer)) return undefined;
  if (!trusted.length || !inCidrs(peer, trusted) || !forwarded) return peer;
  const chain = forwarded.split(",").map((v) => normalizeAddress(v.trim()));
  if (chain.length > 32 || chain.some((v) => !isIP(v)))
    throw new GatewayError(400, "invalid_forwarded_ip", "Invalid trusted proxy address chain");
  let current = peer;
  for (let i = chain.length - 1; i >= 0; i--) {
    if (!inCidrs(current, trusted)) break;
    current = chain[i]!;
  }
  return current;
}

const blocked = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.168.0.0", 16],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const)
  blocked.addSubnet(address, prefix, "ipv4");

const blockedV6 = new BlockList();
for (const [address, prefix] of [
  ["2001::", 23],
  ["2001:2::", 48],
  ["2001:db8::", 32],
  ["2001:10::", 28],
  ["2001:20::", 28],
  ["2002::", 16],
  ["3fff::", 20],
] as const)
  blockedV6.addSubnet(address, prefix, "ipv6");

const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");

export function isPublicAddress(address: string): boolean {
  if (isIP(address) === 4) return !blocked.check(address, "ipv4");
  return (
    isIP(address) === 6 && globalV6.check(address, "ipv6") && !blockedV6.check(address, "ipv6")
  );
}

export function validateBaseUrl(
  value: string,
  allowPrivate: boolean,
  privateCidrs: readonly string[] = [],
): URL {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !["https:", "http:"].includes(url.protocol)
  ) {
    throw new GatewayError(400, "unsafe_upstream", "Invalid upstream URL");
  }
  const literal = url.hostname.replace(/^\[|\]$/g, "");
  const permittedPrivate = isIP(literal) && inCidrs(literal, privateCidrs);
  if (!allowPrivate && !permittedPrivate && url.protocol !== "https:")
    throw new GatewayError(400, "unsafe_upstream", "Upstream requires HTTPS");
  const host = url.hostname.replace(/^\[|\]$/g, "");
  if (
    !allowPrivate &&
    !permittedPrivate &&
    (host === "localhost" || (isIP(host) && !isPublicAddress(host)))
  ) {
    throw new GatewayError(400, "unsafe_upstream", "Private upstream addresses are forbidden");
  }
  return url;
}
