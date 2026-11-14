import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { syntheticAdminKey } from "../support/catalog.ts";
import { publisherUpload } from "../support/plugin-package.ts";
import { chatReplies, withGatewayStack } from "../support/gateway-process.ts";

async function servedDigests(db: SQL) {
  return (
    await db`SELECT snapshot->'dispatch_projection'->'candidate_ref'->>'package_digest' AS digest FROM execution_permits ORDER BY created_at`
  ).map((row: { digest: string }) => row.digest);
}

test(
  "two running Gateways serve a plugin uploaded and published online, stop routing to it once disabled, and recover after rollback without restarts",
  () =>
    withGatewayStack({ reply: () => chatReplies.json("ok") }, async (stack) => {
      const { f, db, setup } = stack;
      const catalog = f.catalog;
      const second = await stack.startGateway({}, "gatewayB");
      const step = (revision: string) => ({
        command_id: crypto.randomUUID(),
        expected_revision: revision,
        reason: "Synthetic online release",
      });
      expect(await servedDigests(db)).toEqual([]);
      const before = await stack.send();
      expect(before.status).toBe(200);
      const [original] = await servedDigests(db);
      const built = await publisherUpload(
        catalog.publisher,
        catalog.release.descriptor.runtime_digest,
        { id: "synthetic.chat", version: "3.0.0" },
      );
      const uploaded = await catalog.request("/plugins", {
        command_id: crypto.randomUUID(),
        package_base64: built.package_base64,
        release: built.release,
        reason: "Synthetic online release",
      });
      expect(uploaded.status).toBe(201);
      const id = String(uploaded.value.version_id),
        digest = String(uploaded.value.package_digest);
      expect(digest).not.toBe(original);
      await catalog.request(`/plugins/${id}/validate`, step(String(uploaded.value.revision)));
      const validated = await catalog.request(`/plugins/${id}`);
      expect(validated.value.state).toBe("VALIDATED");
      const published = await catalog.request(
        `/plugins/${id}/publish`,
        step(String(validated.value.revision)),
      );
      expect(published.value.state).toBe("PUBLISHED");
      const config = {
        model_map: { [setup.model]: "synthetic" },
        max_output_tokens: 100,
        max_input_tokens: 1000,
        auth: "bearer",
      };
      const rebind = (versionId: string, expected: string) =>
        catalog.request(
          `/providers/${setup.provider.id}/plugin-binding`,
          {
            ...catalog.binding(versionId),
            expected_revision: expected,
            account_id: setup.provider.account_id,
            brand_id: setup.provider.brand_id,
            secret_reference: `channel:${setup.provider.id}`,
            config,
          },
          syntheticAdminKey,
          "PUT",
        );
      expect((await rebind(id, "1")).status).toBe(201);
      for (const via of [stack.gateway, second]) {
        const response = await stack.send({ via });
        expect(response.status).toBe(200);
        expect(
          ((await response.json()) as { choices: { message: { content: string } }[] }).choices[0]!
            .message.content,
        ).toBe("ok");
      }
      expect((await servedDigests(db)).slice(1)).toEqual([digest, digest]);
      const disabled = await catalog.request(
        `/plugins/${id}/disable`,
        step(String(published.value.revision)),
      );
      expect(disabled.value.state).toBe("DISABLED");
      const calls = stack.received.length;
      for (const via of [stack.gateway, second]) {
        const response = await stack.send({ via });
        expect(response.status).toBe(503);
        expect(((await response.json()) as { error: { code: string } }).error.code).toBe(
          "no_route",
        );
      }
      expect(stack.received.length).toBe(calls);
      expect((await rebind(setup.publication.version_id, "2")).status).toBe(201);
      for (const via of [stack.gateway, second])
        expect((await stack.send({ via })).status).toBe(200);
      expect((await servedDigests(db)).slice(-2)).toEqual([original, original]);
    }),
  120000,
);
