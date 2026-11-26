import { batchRenewSha } from "./cluster-redis.ts";

export function safetyMarginFor(leaseTtlMs: number): number {
  return Math.min(1000, Math.max(15, Math.floor(leaseTtlMs / 6)));
}

export function jitteredRenewDelay(renewEveryMs: number): number {
  if (renewEveryMs < 200) return renewEveryMs;
  const factor = 0.85 + Math.random() * 0.3;
  return Math.max(50, Math.floor(renewEveryMs * factor));
}

export function buildPermitRenewalArgs(
  permits: readonly { concKey: string; member: string; leaseTtlStr: string }[],
): string[] {
  const args = [batchRenewSha, String(permits.length)];
  for (const permit of permits) args.push(permit.concKey);
  for (const permit of permits) args.push(permit.member, permit.leaseTtlStr);
  return args;
}
