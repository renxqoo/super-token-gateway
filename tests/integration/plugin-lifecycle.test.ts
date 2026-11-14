import { expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { verifier } from "@gateway/runtime/crypto";
import { withCatalog, syntheticAdminKey, syntheticPepper } from "../support/catalog.ts";
import { publisherUpload } from "../support/plugin-package.ts";

const upload = (built: { package_base64: string; release: unknown }) => ({
  command_id: crypto.randomUUID(),
  package_base64: built.package_base64,
  release: built.release,
  reason: "Synthetic publisher upload",
});
const step = (revision: string) => ({
  command_id: crypto.randomUUID(),
  expected_revision: revision,
  reason: "Synthetic lifecycle step",
});

test(
  "a trusted publisher uploads a new plugin online that validates, publishes and binds, and lists with its binding history",
  () =>
    withCatalog(async (f, db) => {
      const built = await publisherUpload(f.publisher, f.release.descriptor.runtime_digest, {
        id: "synthetic.chat",
        version: "2.1.0",
      });
      const input = upload(built);
      const created = await f.request("/plugins", input);
      expect(created.status).toBe(201);
      expect(created.value.state).toBe("UPLOADED");
      const id = String(created.value.version_id);
      expect((await f.request("/plugins", input)).value).toEqual(created.value);
      const validated = await f.request(
        `/plugins/${id}/validate`,
        step(String(created.value.revision)),
      );
      expect(validated.status).toBe(200);
      const truth = await f.request(`/plugins/${id}`);
      expect(truth.value.state).toBe("VALIDATED");
      const published = await f.request(
        `/plugins/${id}/publish`,
        step(String(truth.value.revision)),
      );
      expect(published.status).toBe(200);
      expect(published.value.state).toBe("PUBLISHED");
      const bound = await f.request(
        `/providers/${f.provider.id}/plugin-binding`,
        f.binding(id),
        syntheticAdminKey,
        "PUT",
      );
      expect(bound.status).toBe(201);
      expect((await f.request(`/providers/${f.provider.id}/plugin-binding`)).value).toMatchObject({
        executable: true,
        trust_state: "TRUSTED",
        plugin_version_id: id,
      });
      const listed = (await f.request("/plugins")).value.data as Record<string, unknown>[];
      expect(listed.map((v) => [v.name, v.version, v.state])).toEqual([
        ["synthetic.chat", "2.1.0", "PUBLISHED"],
      ]);
      const history = (await f.request(`/providers/${f.provider.id}/plugin-bindings`)).value
        .data as Record<string, unknown>[];
      expect(history.map((b) => [b.plugin_version_id, b.config_revision, b.current])).toEqual([
        [id, "1", true],
      ]);
      expect(
        (
          await db`SELECT action FROM audit_events WHERE action IN ('plugin_uploaded','plugin_published','plugin_bound') ORDER BY created_at`
        ).map((r: { action: string }) => r.action),
      ).toEqual(["plugin_uploaded", "plugin_published", "plugin_bound"]);
    }),
  60000,
);

test("uploads outside the publisher namespace, from unknown, revoked or expired publishers, or with tampered releases are refused without a version", () =>
  withCatalog(async (f, db) => {
    const runtime = f.release.descriptor.runtime_digest;
    const stranger = generateKeyPairSync("ed25519");
    const cases = [
      await publisherUpload(f.publisher, runtime, { id: "gateway.openai-chat", version: "9.0.0" }),
      await publisherUpload(f.publisher, runtime, { id: "other.chat", version: "1.0.0" }),
      await publisherUpload(stranger, runtime, { id: "synthetic.chat", version: "1.0.0" }),
      await publisherUpload(
        f.publisher,
        runtime,
        { id: "synthetic.chat", version: "1.0.0" },
        "synthetic-catalog-test",
      ),
      await publisherUpload(f.publisher, "0".repeat(64), {
        id: "synthetic.chat",
        version: "1.0.0",
      }),
    ];
    for (const built of cases)
      expect((await f.request("/plugins", upload(built))).status).toBe(400);
    const good = await publisherUpload(f.publisher, runtime, {
      id: "synthetic.chat",
      version: "1.0.0",
    });
    const other = await publisherUpload(f.publisher, runtime, {
      id: "synthetic.chat",
      version: "1.0.1",
    });
    expect(
      (await f.request("/plugins", { ...upload(good), package_base64: other.package_base64 }))
        .status,
    ).toBe(400);
    expect((await f.request("/plugins", { ...upload(good), release: f.release })).status).toBe(400);
    for (const change of [
      { publishers: [{ ...f.policy.publishers[0]!, revoked: true }] },
      { publishers: [{ ...f.policy.publishers[0]!, expires_at: "2020-06-01T00:00:00.000Z" }] },
      { publishers: [] },
    ]) {
      await writeFile(f.policyPath, JSON.stringify({ ...f.policy, ...change }));
      await f.reload();
      expect((await f.request("/plugins", upload(good))).status).toBe(400);
    }
    expect((await db`SELECT count(*)::int AS n FROM plugin_versions`)[0].n).toBe(0);
  }));

test("upload requires plugins:upload and the bundled install permission alone cannot register a publisher release", () =>
  withCatalog(async (f, db) => {
    const admin = crypto.randomUUID(),
      token = "synthetic-plugin-uploader-key";
    await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${admin},'Synthetic uploader',${["plugins:install-bundled", "plugins:read"]})`;
    await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${admin},${verifier(token, syntheticPepper)},'synthetic')`;
    const built = await publisherUpload(f.publisher, f.release.descriptor.runtime_digest, {
      id: "synthetic.chat",
      version: "1.0.0",
    });
    expect((await f.request("/plugins", upload(built), token)).status).toBe(403);
    expect((await f.request("/plugins", upload(built), "")).status).toBe(401);
    await expect(
      f.application.upload(
        { identity: "bootstrap:forged", permissions: ["plugins:upload"] },
        async () => upload(built),
      ),
    ).rejects.toThrow("permission");
    await db`UPDATE technical_administrators SET permissions=${["plugins:upload"]} WHERE id=${admin}`;
    expect((await f.request("/plugins", upload(built), token)).status).toBe(201);
    expect((await f.request("/plugins", {}, token)).status).toBe(400);
  }));

test(
  "disable and quarantine remove a version from execution and new bindings, keep history, and rollback rebinds an earlier version",
  () =>
    withCatalog(async (f, db) => {
      const runtime = f.release.descriptor.runtime_digest;
      async function release(version: string) {
        const created = await f.request(
          "/plugins",
          upload(await publisherUpload(f.publisher, runtime, { id: "synthetic.chat", version })),
        );
        const id = String(created.value.version_id);
        await f.request(`/plugins/${id}/validate`, step(String(created.value.revision)));
        const truth = await f.request(`/plugins/${id}`);
        const published = await f.request(
          `/plugins/${id}/publish`,
          step(String(truth.value.revision)),
        );
        expect(published.value.state).toBe("PUBLISHED");
        return { id, revision: String(published.value.revision) };
      }
      const first = await release("1.0.0"),
        second = await release("1.1.0");
      const bindFirst = f.binding(first.id);
      expect(
        (
          await f.request(
            `/providers/${f.provider.id}/plugin-binding`,
            bindFirst,
            syntheticAdminKey,
            "PUT",
          )
        ).status,
      ).toBe(201);
      const bindSecond = f.binding(second.id, { expected_revision: "1" });
      expect(
        (
          await f.request(
            `/providers/${f.provider.id}/plugin-binding`,
            bindSecond,
            syntheticAdminKey,
            "PUT",
          )
        ).value.config_revision,
      ).toBe("2");
      const disable = step(second.revision);
      const disabled = await f.request(`/plugins/${second.id}/disable`, disable);
      expect(disabled.status).toBe(200);
      expect(disabled.value).toMatchObject({ state: "DISABLED", quarantined: false });
      expect((await f.request(`/plugins/${second.id}/disable`, disable)).value).toEqual(
        disabled.value,
      );
      expect(
        (await f.request(`/plugins/${second.id}/disable`, { ...disable, reason: "changed" }))
          .status,
      ).toBe(409);
      expect(
        (await f.request(`/plugins/${second.id}/disable`, step(String(disabled.value.revision))))
          .status,
      ).toBe(409);
      expect((await f.request(`/providers/${f.provider.id}/plugin-binding`)).value.executable).toBe(
        false,
      );
      expect(
        (
          await f.request(
            `/providers/${f.provider.id}/plugin-binding`,
            f.binding(second.id, { expected_revision: "2" }),
            syntheticAdminKey,
            "PUT",
          )
        ).status,
      ).toBe(409);
      const rollback = await f.request(
        `/providers/${f.provider.id}/plugin-binding`,
        f.binding(first.id, { expected_revision: "2" }),
        syntheticAdminKey,
        "PUT",
      );
      expect(rollback.status).toBe(201);
      expect(rollback.value.config_revision).toBe("3");
      expect((await f.request(`/providers/${f.provider.id}/plugin-binding`)).value).toMatchObject({
        executable: true,
        plugin_version_id: first.id,
      });
      const history = (await f.request(`/providers/${f.provider.id}/plugin-bindings`)).value
        .data as Record<string, unknown>[];
      expect(history.map((b) => [b.plugin_version_id, b.config_revision, b.current])).toEqual([
        [first.id, "3", true],
        [second.id, "2", false],
        [first.id, "1", false],
      ]);
      const quarantined = await f.request(`/plugins/${first.id}/quarantine`, step(first.revision));
      expect(quarantined.value).toMatchObject({ state: "PUBLISHED", quarantined: true });
      expect((await f.request(`/providers/${f.provider.id}/plugin-binding`)).value.executable).toBe(
        false,
      );
      expect(
        (
          await f.request(
            `/plugins/${first.id}/quarantine`,
            step(String(quarantined.value.revision)),
          )
        ).status,
      ).toBe(409);
      expect((await db`SELECT count(*)::int AS n FROM plugin_packages`)[0].n).toBe(2);
      expect(
        (
          await db`SELECT action FROM audit_events WHERE action IN ('plugin_disabled','plugin_quarantined') ORDER BY created_at`
        ).map((r: { action: string }) => r.action),
      ).toEqual(["plugin_disabled", "plugin_quarantined"]);
      const reader = crypto.randomUUID(),
        token = "synthetic-plugin-reader-key";
      await db`INSERT INTO technical_administrators(id,name,permissions) VALUES(${reader},'Synthetic reader',${["plugins:read"]})`;
      await db`INSERT INTO administrator_credentials(id,administrator_id,verifier,prefix) VALUES(${crypto.randomUUID()},${reader},${verifier(token, syntheticPepper)},'synthetic')`;
      expect((await f.request(`/plugins/${second.id}/quarantine`, step("5"), token)).status).toBe(
        403,
      );
      expect((await f.request("/plugins", undefined, token)).status).toBe(200);
    }),
  90000,
);

test("a second upload while one is still streaming and a third concurrent validation are refused with catalog_busy without new versions or runs", () =>
  withCatalog(async (f, db) => {
    const runtime = f.release.descriptor.runtime_digest;
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    const first = upload(
      await publisherUpload(f.publisher, runtime, { id: "synthetic.chat", version: "1.0.0" }),
    );
    const encoded = new TextEncoder().encode(JSON.stringify(first));
    const streaming = f.http.request("http://catalog.test/plugins", {
      method: "POST",
      headers: { authorization: `Bearer ${syntheticAdminKey}`, "content-type": "application/json" },
      body: new ReadableStream({
        async start(controller) {
          controller.enqueue(encoded.subarray(0, 100));
          await held;
          controller.enqueue(encoded.subarray(100));
          controller.close();
        },
      }),
    });
    await Bun.sleep(50);
    const second = upload(
      await publisherUpload(f.publisher, runtime, { id: "synthetic.chat", version: "1.0.1" }),
    );
    const refused = await f.request("/plugins", second);
    expect(refused.status).toBe(429);
    expect((refused.value.error as { code: string }).code).toBe("catalog_busy");
    release();
    expect((await streaming).status).toBe(201);
    const versions = [String((await f.request("/plugins", second)).value.version_id)];
    for (const version of ["1.0.2", "1.0.3"])
      versions.push(
        String(
          (
            await f.request(
              "/plugins",
              upload(
                await publisherUpload(f.publisher, runtime, { id: "synthetic.chat", version }),
              ),
            )
          ).value.version_id,
        ),
      );
    const results = await Promise.all(
      versions.map((id) => f.request(`/plugins/${id}/validate`, step("0"))),
    );
    expect(results.map((r) => r.status).sort()).toEqual([200, 200, 429]);
    expect((await db`SELECT count(*)::int AS n FROM plugin_validation_runs`)[0].n).toBe(2);
    expect((await db`SELECT count(*)::int AS n FROM plugin_versions`)[0].n).toBe(4);
  }));
