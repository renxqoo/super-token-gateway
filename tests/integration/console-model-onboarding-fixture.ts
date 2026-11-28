import { expect } from "bun:test";
import type { ModelOnboardingResult } from "@gateway/contracts";
import {
  adminAccess,
  channelId,
  customerPriceId,
  h,
  supplierPriceId,
} from "./console-sync-fixture.ts";

export const onboarding = "/admin/v1/models/onboarding";

export async function draft(name: string) {
  const epoch = await h.adminApi<{ config_epoch: number }>("GET", onboarding, adminAccess);
  expect(epoch.status).toBe(200);
  return {
    id: crypto.randomUUID(),
    expected_epoch: epoch.data.config_epoch,
    model: {
      name,
      display_name: "Onboarding test model",
      operations: ["chat"],
      max_output_tokens: 100,
    },
    billing_price_id: customerPriceId,
    deployments: [10, 0].map((priority) => ({
      id: crypto.randomUUID(),
      channel_id: channelId,
      upstream_model: "gpt-4o",
      cost_version_id: supplierPriceId,
      priority,
    })),
  };
}

export function statusPath(input: Awaited<ReturnType<typeof draft>>) {
  return "/admin/v1/models/" + encodeURIComponent(input.model.name) + "/onboarding/" + input.id;
}

export async function waitForCommit(input: Awaited<ReturnType<typeof draft>>) {
  const deadline = performance.now() + 5000;
  let result = await h.adminApi<ModelOnboardingResult>("GET", statusPath(input), adminAccess);
  while (result.status === 202 && performance.now() < deadline) {
    await Bun.sleep(20);
    result = await h.adminApi<ModelOnboardingResult>("GET", statusPath(input), adminAccess);
  }
  expect(result.status, JSON.stringify(result.data)).toBe(200);
  return result.data;
}
