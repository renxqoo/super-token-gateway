import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { createAsyncTasksRepository } from "@gateway/ledger";
import {
  createTeamBillingAccount,
  h,
  adminAccess,
  customerAccess,
  videoChannelId,
} from "./console-billing-fixture.ts";

export function registerVideoTaskCases() {
  test("a failed video poll preserves the task and charge until a later successful poll confirms the refund", async () => {
    const accountId = await createTeamBillingAccount("Video Poll Recovery", "video-poll-recovery");
    const topup = await h.adminApi(
      "POST",
      `/admin/v1/finance/billing-accounts/${accountId}/topups`,
      adminAccess,
      { external_ref: "video-poll-recovery", amount_atoms: "10000" },
    );
    expect(topup.status).toBe(200);
    const key = await h.clientApi<{ key: string }>(
      "POST",
      `/api/v1/billing-accounts/${accountId}/api-keys`,
      customerAccess,
      { name: "Video poll recovery", operations: ["videos"] },
    );
    expect(key.status).toBe(201);
    const headers = { authorization: `Bearer ${key.data.key}`, "content-type": "application/json" };
    const submitted = await fetch(`${h.gatewayPublicUrl}/v1/videos`, {
      method: "POST",
      headers,
      body: JSON.stringify({ model: "bill-video", prompt: "clouds" }),
    });
    expect(submitted.status).toBe(200);
    const { gateway_task_id: taskId, task_id: upstreamId } = (await submitted.json()) as {
      gateway_task_id: string;
      task_id: string;
    };
    const repository = createAsyncTasksRepository(h.gwDb);
    const original = await repository.getTask(taskId);
    expect(original).toMatchObject({ refund_eligibility: "PENDING", refund_id: null });
    const wallet = () =>
      h.clientApi<{ available_atoms: string }>(
        "GET",
        `/api/v1/billing-accounts/${accountId}/wallet`,
        customerAccess,
      );
    expect((await wallet()).data.available_atoms).toBe("5000");

    const poll = () => fetch(`${h.gatewayPublicUrl}/v1/videos/${taskId}`, { headers });
    const providerBody = { status: "Fail", error_code: 5001 };
    h.videoStates.set(upstreamId, { body: providerBody, httpStatus: 503 });
    try {
      const unavailable = await poll();
      expect(unavailable.status).toBe(502);
      expect(await unavailable.json()).toMatchObject({ error: { code: "upstream_poll_failed" } });
      expect(await repository.getTask(taskId)).toMatchObject({
        status: original!.status,
        refund_eligibility: "PENDING",
        refund_id: null,
      });
      expect((await wallet()).data.available_atoms).toBe("5000");

      h.videoStates.set(upstreamId, { body: providerBody });
      const recovered = await poll();
      expect(recovered.status).toBe(200);
      expect(await recovered.json()).toMatchObject({
        status: "FAILED",
        failure_category: "UPSTREAM_SYSTEM",
        refund_eligibility: "AUTO_REFUNDED",
      });
      expect((await wallet()).data.available_atoms).toBe("10000");
    } finally {
      h.videoStates.delete(upstreamId);
    }
  });

  test("video tasks stay private to the issuing Key and accessible after rotation", async () => {
    const accountId = await createTeamBillingAccount("Video Task Access", "video-task-access");
    const keysPath = `/api/v1/billing-accounts/${accountId}/api-keys`;
    const topup = await h.adminApi(
      "POST",
      `/admin/v1/finance/billing-accounts/${accountId}/topups`,
      adminAccess,
      { external_ref: "video-task-access", amount_atoms: "10000" },
    );
    expect(topup.status).toBe(200);
    async function newKey(name: string, role: "ADMIN" | "INVOKER" = "INVOKER") {
      const result = await h.clientApi<{ id: string; key: string }>(
        "POST",
        keysPath,
        customerAccess,
        { name, role, operations: ["videos"] },
      );
      expect(result.status).toBe(201);
      return result.data;
    }
    const owner = await newKey("Creator");
    const sibling = await newKey("Other application");
    const admin = await newKey("Account administrator", "ADMIN");
    const submitted = await fetch(`${h.gatewayPublicUrl}/v1/videos`, {
      method: "POST",
      headers: { authorization: `Bearer ${owner.key}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "bill-video", prompt: "clouds" }),
    });
    expect(submitted.status).toBe(200);
    const { gateway_task_id: taskId } = (await submitted.json()) as { gateway_task_id: string };
    expect(taskId).toMatch(/^vid_/);
    const tasksPath = `${h.gatewayPublicUrl}/v1/videos/${taskId}`;
    async function requestTask(key: string, cancel = false) {
      const response = await fetch(tasksPath + (cancel ? "/cancel" : ""), {
        method: cancel ? "POST" : "GET",
        headers: { authorization: `Bearer ${key}` },
      });
      await response.body?.cancel();
      return response.status;
    }
    expect(await requestTask(sibling.key)).toBe(404);
    expect(await requestTask(sibling.key, true)).toBe(404);
    expect(await requestTask(admin.key)).toBe(200);

    const rotated = await h.clientApi<{ key: string }>(
      "POST",
      `${keysPath}/${owner.id}/rotate`,
      customerAccess,
    );
    expect(rotated.status).toBe(201);
    expect(await requestTask(owner.key)).toBe(401);
    expect(await requestTask(rotated.data.key)).toBe(200);
    expect(await requestTask(rotated.data.key, true)).toBe(200);
  });

  test("symptom: a stale Gateway can read or cancel video tasks after a Key policy is tightened", async () => {
    const accountId = await createTeamBillingAccount("Video Task Policies", "video-task-policies");
    const keysPath = `/api/v1/billing-accounts/${accountId}/api-keys`;
    const cases = [
      { policy: { models: ["different-model"] }, method: "GET", error: "model_not_allowed" },
      { policy: { operations: ["chat"] }, method: "GET", error: "operation_not_allowed" },
      { policy: { allowed_cidrs: ["192.0.2.0/24"] }, method: "POST", error: "ip_not_allowed" },
    ];
    const repository = createAsyncTasksRepository(h.gwDb);
    const requests = await Promise.all(
      cases.map(async ({ policy, method, error }) => {
        const key = await h.clientApi<{ id: string; key: string }>(
          "POST",
          keysPath,
          customerAccess,
          {
            name: error,
          },
        );
        expect(key.status).toBe(201);
        const taskId = `vid_${key.data.id}`;
        await repository.createTask({
          id: taskId,
          request_id: crypto.randomUUID(),
          charge_id: null,
          billing_account_id: accountId,
          api_key_id: key.data.id,
          channel_id: videoChannelId,
          model: "bill-video",
          operation: "videos",
          upstream_task_id: taskId,
          status: method === "POST" ? "QUEUED" : "SUCCEEDED",
        });
        return { policy, method, error, key: key.data, taskId };
      }),
    );
    const replica = createGatewayRuntime({
      config: { ...h.gatewayConfig, snapshotPollMs: 0 },
      db: h.gwDb,
      wasmExecutor: null,
    });
    try {
      await replica.start();
      const epoch = replica.snapshot.get().epoch;
      for (const { key, policy } of requests) {
        const updated = await h.clientApi("PATCH", `${keysPath}/${key.id}`, customerAccess, policy);
        expect(updated.status).toBe(200);
      }
      expect(replica.snapshot.get().epoch).toBe(epoch);
      const results = [];
      for (const { key, taskId, method } of requests) {
        const suffix = method === "POST" ? "/cancel" : "";
        const response = await replica.fetchPublic(
          new Request(`http://test/v1/videos/${taskId}${suffix}`, {
            method,
            headers: { authorization: `Bearer ${key.key}` },
          }),
          { getPeerAddress: () => "127.0.0.1" },
        );
        results.push({ status: response.status, body: await response.json() });
      }
      expect(results).toMatchObject(
        requests.map(({ error }) => ({ status: 403, body: { error: { code: error } } })),
      );
      expect(await repository.getTask(requests[2]!.taskId)).toMatchObject({
        status: "QUEUED",
        user_cancelled: false,
      });
    } finally {
      await replica.close();
    }
  });
}
