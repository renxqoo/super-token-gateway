import { operations } from "@gateway/kernel";
import { z } from "zod";

const quantity = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
const scopeSchema = z
  .strictObject({
    channel_id: z.uuid().transform((id) => id.toLowerCase()),
    model: z.string().min(1).max(160),
    operation: z.enum(operations),
    currency: z.string().regex(/^[A-Z]{3,8}$/),
    max_input_tokens: quantity.optional(),
    max_output_tokens: quantity.optional(),
    max_customer_atoms: quantity.optional(),
    trend: z
      .strictObject({
        minimum_samples: z.int().min(1).max(20_000).default(20),
        baseline_windows: z.int().min(1).max(48).default(6),
        consecutive_windows: z.int().min(1).max(12).default(3),
        increase_bps: z.int().min(1).max(100_000).default(5_000),
        minimum_delta: quantity.default("100"),
      })
      .optional(),
  })
  .refine(
    (scope) =>
      scope.max_input_tokens !== undefined ||
      scope.max_output_tokens !== undefined ||
      scope.max_customer_atoms !== undefined ||
      scope.trend !== undefined,
    "Each scope requires an enabled rule",
  );

export type UsageMonitorScope = z.output<typeof scopeSchema>;

export function scopeKey(
  scope: Pick<UsageMonitorScope, "channel_id" | "model" | "operation" | "currency">,
) {
  return JSON.stringify([scope.channel_id, scope.model, scope.operation, scope.currency]);
}

const policySchema = z
  .strictObject({
    effective_from: z.iso.datetime(),
    window_ms: z.int().min(60_000).max(3_600_000).default(300_000),
    initial_grace_ms: z.int().min(0).max(3_600_000).default(60_000),
    final_grace_ms: z.int().min(1).max(86_400_000).default(900_000),
    catchup_windows: z.int().min(1).max(288).default(24),
    max_rows: z.int().min(1).max(20_000).default(4_000),
    scopes: z.array(scopeSchema).min(1).max(32),
  })
  .superRefine((policy, ctx) => {
    if (Date.parse(policy.effective_from) % policy.window_ms !== 0) {
      ctx.addIssue({
        code: "custom",
        path: ["effective_from"],
        message: "Must align to a UTC window",
      });
    }
    if (policy.final_grace_ms <= policy.initial_grace_ms) {
      ctx.addIssue({
        code: "custom",
        path: ["final_grace_ms"],
        message: "Must exceed initial grace",
      });
    }
    if (new Set(policy.scopes.map(scopeKey)).size !== policy.scopes.length) {
      ctx.addIssue({ code: "custom", path: ["scopes"], message: "Scope keys must be distinct" });
    }
  });

export const usageMonitorIntervalMs = 15_000;
export type UsageMonitorPolicy = z.output<typeof policySchema>;
export type UsageMonitorConfig = {
  policy: UsageMonitorPolicy;
  notifications: boolean;
  intervalMs: number;
};

export function parseUsageMonitorPolicy(value: unknown): UsageMonitorPolicy {
  return policySchema.parse(value);
}

export function versionUsageMonitorPolicy(policy: UsageMonitorPolicy): string {
  return new Bun.CryptoHasher("sha256").update(JSON.stringify(policy)).digest("hex");
}
