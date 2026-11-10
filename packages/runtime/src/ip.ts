import { BlockList, isIP } from "node:net";
import { GatewayError } from "@gateway/kernel";
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
