import { readFileSync, statSync } from "node:fs";
import type { SQL } from "bun";
import { resolve } from "node:path";
import { z } from "zod";
import { createPluginCatalogStore, type CatalogReceipt } from "@gateway/storage";
import { canonical } from "@gateway/kernel";
import { GatewayError } from "@gateway/contracts";
import { sha256, validateConfig } from "@gateway/plugin-sdk";
import { createPlatformAuthenticator, type PlatformActor } from "../platform/authentication.ts";
import { bundledReleaseIds, releasePolicySchema, type BundledReleaseId } from "@gateway/plugin-sdk";
import { verifyRelease } from "./releases.ts";
import { validateCatalogPackage } from "./validator.ts";
import { resolvePluginExecutable } from "@gateway/plugin-host";
export const installBundledReleaseSchema = z
  .object({
    command_id: z.uuid(),
    release_id: z.enum(bundledReleaseIds),
    reason: z.string().min(1).max(500),
  })
  .strict();
export const uploadPluginSchema = z
  .object({
    command_id: z.uuid(),
    package_base64: z
      .string()
      .min(1)
      .max(Math.ceil((44 << 20) / 3) * 4)
      .regex(/^[A-Za-z0-9+/]+={0,2}$/),
    release: z.record(z.string(), z.unknown()),
    reason: z.string().min(1).max(500),
  })
  .strict();
export const bindPluginSchema = z
  .object({
    command_id: z.uuid(),
    version_id: z.uuid(),
    expected_revision: z.string().regex(/^(0|[1-9][0-9]{0,37})$/),
    account_id: z.uuid(),
    brand_id: z.uuid(),
    secret_reference: z.string().max(100),
    config: z.record(z.string(), z.unknown()),
    reason: z.string().min(1).max(500),
  })
  .strict();
export const versionCommandSchema = z
  .object({
    command_id: z.uuid(),
    expected_revision: z.string().regex(/^(0|[1-9][0-9]{0,37})$/),
    reason: z.string().min(1).max(500),
  })
  .strict();
export const recoveryCommandSchema = z
  .object({
    command_id: z.uuid(),
    run_id: z.uuid(),
    lease_epoch: z.string().regex(/^[1-9][0-9]{0,37}$/),
    reason: z.string().min(1).max(500),
  })
  .strict();
export function catalogStageCommand(commandId: string, stage: string): string {
  const hash = sha256(`gateway-plugin-catalog-v1\0${commandId}\0${stage}`);
  return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-4${hash.slice(13, 16)}-a${hash.slice(17, 20)}-${hash.slice(20, 32)}`;
}
async function boundedFile(path: string, max: number) {
  const file = Bun.file(path);
  if (file.size > max)
    throw new GatewayError(413, "catalog_artifact_budget", "Catalog artifact exceeds byte budget");
  return file.bytes();
}
export function createCatalogApplication(options: {
  db: SQL;
  adminKey: string;
  pepper: string;
  policyFile: string;
  releaseDirectory: string;
  helperExecutable: string;
}) {
  const store = createPluginCatalogStore(options.db),
    auth = createPlatformAuthenticator(store, options),
    owner = crypto.randomUUID();
  const validationsByCommand = new Map<
    string,
    { digest: string; promise: Promise<CatalogReceipt> }
  >();
  const pendingValidations = new Set<Promise<CatalogReceipt>>();
  let uploading = false;
  const shutdownControllers = new Set<AbortController>();
  const pinned = resolvePluginExecutable(resolve(options.helperExecutable));
  const helperExecutable = pinned.executable;
  const policyFile = resolve(options.policyFile);
  const policyStat = statSync(policyFile);
  if (!policyStat.isFile() || policyStat.size > 1 << 20)
    throw new Error("A bounded deployment release policy file is required");
  const policySnapshot = releasePolicySchema.parse(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(readFileSync(policyFile))),
  );
  let closed = false;
  function available() {
    if (closed) throw new GatewayError(503, "catalog_closing", "Catalog is shutting down");
  }
  async function trusted(bytes: Uint8Array, signed: unknown) {
    return verifyRelease(bytes, signed, policySnapshot, pinned.digest);
  }
  async function persisted(id: string) {
    const row = await store.version(z.uuid().parse(id));
    const release = await trusted(new Uint8Array(row.package_bytes), {
      descriptor: row.release_descriptor,
      signature: { algorithm: "Ed25519", key_id: row.release_key_id, value: row.release_signature },
    });
    return { row, release };
  }
  function descriptorInputs(d: Record<string, unknown>) {
    return {
      package_digest: d.package_digest,
      policy_digest: d.validation_policy_digest,
      runtime_digest: d.runtime_digest,
      fixture_digest: d.fixture_digest,
      config_schema_digest: d.config_schema_digest,
    };
  }
  function registration(
    actor: PlatformActor,
    permission: "plugins:install-bundled" | "plugins:upload",
    commandId: string,
    bytes: Uint8Array,
    release: Awaited<ReturnType<typeof trusted>>,
    reason: string,
  ) {
    return store.command(
      "plugin_register_v1",
      auth.authorization(actor, permission),
      z.uuid().parse(commandId),
      {
        package_base64: Buffer.from(bytes).toString("base64"),
        package_digest: release.pkg.digest,
        manifest: release.pkg.manifest,
        config_schema_digest: release.signed.descriptor.config_schema_digest,
        signature: release.packageSignature.value,
        signing_key_id: release.packageSignature.key_id,
        release_descriptor: release.signed.descriptor,
        release_signature: release.signed.signature.value,
        release_key_id: release.signed.signature.key_id,
        reason,
      },
      undefined,
      release.trustExpiresAt,
    );
  }
  async function register(
    actor: PlatformActor,
    commandId: string,
    reason: string,
    releaseId: BundledReleaseId,
  ) {
    available();
    auth.require(actor, "plugins:install-bundled");
    const directory = resolve(options.releaseDirectory, z.enum(bundledReleaseIds).parse(releaseId));
    const bytes = await boundedFile(resolve(directory, "package.json"), 44 << 20);
    const signed = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(
        await boundedFile(resolve(directory, "release.json"), 256 << 10),
      ),
    );
    const release = await trusted(bytes, signed);
    return registration(actor, "plugins:install-bundled", commandId, bytes, release, reason);
  }
  async function accept(actor: PlatformActor, parsed: z.infer<typeof uploadPluginSchema>) {
    const bytes = new Uint8Array(Buffer.from(parsed.package_base64, "base64"));
    const release = await trusted(bytes, parsed.release);
    if (!release.signed.descriptor.source.startsWith("upload:"))
      throw new GatewayError(
        400,
        "untrusted_plugin_release",
        "Plugin release does not satisfy the configured trust policy",
      );
    return registration(actor, "plugins:upload", parsed.command_id, bytes, release, parsed.reason);
  }
  async function transition(
    actor: PlatformActor,
    kind: "plugin_disable_v1" | "plugin_quarantine_v1",
    id: string,
    input: unknown,
  ) {
    available();
    auth.require(actor, "plugins:publish");
    const parsed = versionCommandSchema.parse(input);
    return store.command(kind, auth.authorization(actor, "plugins:publish"), parsed.command_id, {
      version_id: z.uuid().parse(id),
      expected_revision: parsed.expected_revision,
      reason: parsed.reason,
    });
  }
  async function runValidation(
    actor: PlatformActor,
    id: string,
    parsed: z.infer<typeof versionCommandSchema>,
    signal: AbortSignal,
  ) {
    const identity = auth.require(actor, "plugins:validate"),
      row = await store.version(z.uuid().parse(id));
    const body = {
      version_id: id,
      ...descriptorInputs(row.release_descriptor ?? {}),
      expected_revision: parsed.expected_revision,
      reason: parsed.reason,
    };
    const previous = await store.receipt("plugin_validate_v1", identity, parsed.command_id, body);
    if (previous) {
      const run = await store.run(z.uuid().parse(previous.run_id));
      if (run.state === "SUCCEEDED" || run.state === "FAILED") {
        const finished = await store.receipt(
          "plugin_finish_v1",
          identity,
          catalogStageCommand(parsed.command_id, "finish"),
        );
        return finished ?? { ...previous, state: run.state, result: run.result };
      }
      throw new GatewayError(
        409,
        "validation_owned",
        "Persisted validation requires its active worker or cleanup recovery",
      );
    }
    const { release } = await persisted(id);
    const started = await store.command(
      "plugin_validate_v1",
      auth.authorization(actor, "plugins:validate"),
      parsed.command_id,
      body,
      owner,
      release.trustExpiresAt,
    );
    const runId = z.uuid().parse(started.run_id),
      run = await store.run(runId);
    if (run.lease_owner !== owner || run.state !== "RUNNING")
      throw new GatewayError(
        409,
        "validation_owned",
        "Validation is owned by another worker or requires cleanup recovery",
      );
    const result = await validateCatalogPackage(release.pkg, helperExecutable, {
      signal,
      onResource: (resource) =>
        store
          .command(
            "plugin_resources_v1",
            { identity },
            catalogStageCommand(parsed.command_id, "resources"),
            {
              version_id: id,
              run_id: runId,
              lease_epoch: run.lease_epoch,
              pid: resource.pid,
              connection: resource.connection,
              reason: parsed.reason,
            },
            owner,
          )
          .then(() => undefined),
    });
    return store.command(
      "plugin_finish_v1",
      { identity },
      catalogStageCommand(parsed.command_id, "finish"),
      {
        version_id: id,
        run_id: runId,
        lease_epoch: run.lease_epoch,
        result,
        reason: parsed.reason,
      },
      owner,
    );
  }
  function busy() {
    return new GatewayError(
      429,
      "catalog_busy",
      "Catalog is processing the maximum number of plugin operations",
    );
  }
  function validate(
    actor: PlatformActor,
    id: string,
    input: z.infer<typeof versionCommandSchema>,
    signal?: AbortSignal,
  ) {
    available();
    const identity = auth.require(actor, "plugins:validate"),
      parsed = versionCommandSchema.parse(input),
      key = `${identity}\0${parsed.command_id}`,
      digest = sha256(canonical({ version_id: id, ...parsed }));
    const existing = validationsByCommand.get(key);
    if (existing) {
      if (existing.digest !== digest)
        throw new GatewayError(
          409,
          "catalog_conflict",
          "Catalog command conflicts with active validation",
        );
      return existing.promise;
    }
    if (pendingValidations.size >= 2) throw busy();
    const controller = new AbortController();
    shutdownControllers.add(controller);
    const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal,
      operation = runValidation(actor, id, parsed, combined);
    pendingValidations.add(operation);
    const promise = operation.finally(() => {
      pendingValidations.delete(operation);
      shutdownControllers.delete(controller);
      validationsByCommand.delete(key);
    });
    validationsByCommand.set(key, { digest, promise });
    return promise;
  }
  async function publish(
    actor: PlatformActor,
    id: string,
    input: z.infer<typeof versionCommandSchema>,
  ) {
    available();
    const identity = auth.require(actor, "plugins:publish"),
      parsed = versionCommandSchema.parse(input),
      row = await store.version(z.uuid().parse(id));
    const body = {
      version_id: id,
      ...descriptorInputs(row.release_descriptor ?? {}),
      expected_revision: parsed.expected_revision,
      reason: parsed.reason,
    };
    const replay = await store.receipt("plugin_publish_v1", identity, parsed.command_id, body);
    if (replay) return replay;
    const { release } = await persisted(id);
    return store.command(
      "plugin_publish_v1",
      auth.authorization(actor, "plugins:publish"),
      parsed.command_id,
      body,
      undefined,
      release.trustExpiresAt,
    );
  }

  return {
    authenticate: auth.authenticate,
    async close() {
      closed = true;
      for (const controller of shutdownControllers) controller.abort();
      await Promise.allSettled(pendingValidations);
    },
    register,
    validate,
    publish,
    async upload(actor: PlatformActor, read: () => Promise<unknown>) {
      available();
      auth.require(actor, "plugins:upload");
      if (uploading) throw busy();
      uploading = true;
      try {
        return await accept(actor, uploadPluginSchema.parse(await read()));
      } finally {
        uploading = false;
      }
    },
    disable: (actor: PlatformActor, id: string, input: unknown) =>
      transition(actor, "plugin_disable_v1", id, input),
    quarantine: (actor: PlatformActor, id: string, input: unknown) =>
      transition(actor, "plugin_quarantine_v1", id, input),
    async versions(actor: PlatformActor) {
      auth.require(actor, "plugins:read");
      return store.versions();
    },
    async bindings(actor: PlatformActor, channel: string) {
      auth.require(actor, "plugins:read");
      return store.bindings(z.uuid().parse(channel));
    },
    async installBundled(actor: PlatformActor, input: unknown, signal?: AbortSignal) {
      const parsed = installBundledReleaseSchema.parse(input);
      auth.require(actor, "plugins:install-bundled");
      auth.require(actor, "plugins:validate");
      auth.require(actor, "plugins:publish");
      const original = await store.receipt(
        "plugin_publish_v1",
        auth.require(actor, "plugins:publish"),
        catalogStageCommand(parsed.command_id, "publish"),
      );
      if (original) {
        const command = catalogStageCommand(parsed.command_id, "publish");
        const firstPublication =
          original.command_id === command &&
          original.actor === auth.require(actor, "plugins:publish");
        return publish(actor, original.version_id, {
          command_id: command,
          expected_revision: (
            BigInt(String(original.revision)) - (firstPublication ? 1n : 0n)
          ).toString(),
          reason: parsed.reason,
        });
      }
      const uploaded = await register(
          actor,
          catalogStageCommand(parsed.command_id, "register"),
          parsed.reason,
          parsed.release_id,
        ),
        id = uploaded.version_id;
      const current = await store.truth(id);
      if (
        current.state === "UPLOADED" ||
        current.state === "VALIDATING" ||
        current.state === "REJECTED"
      ) {
        const validated = await validate(
          actor,
          id,
          {
            command_id: catalogStageCommand(parsed.command_id, "validate"),
            expected_revision: uploaded.revision!,
            reason: parsed.reason,
          },
          signal,
        );
        if (validated.state !== "VALIDATED" && validated.state !== "SUCCEEDED")
          throw new GatewayError(
            409,
            "plugin_validation_failed",
            "Plugin validation did not complete successfully",
          );
      }
      const validated = await store.truth(id),
        publicationCommand = catalogStageCommand(parsed.command_id, "publish"),
        publication = validated.publication as Record<string, unknown> | null;
      const originalCommand =
        publication?.command_id === publicationCommand &&
        publication.actor === auth.require(actor, "plugins:publish");
      return publish(actor, id, {
        command_id: publicationCommand,
        expected_revision:
          validated.state === "PUBLISHED" && originalCommand
            ? (BigInt(validated.revision) - 1n).toString()
            : validated.revision,
        reason: parsed.reason,
      });
    },
    async bind(actor: PlatformActor, channelId: string, input: unknown) {
      available();
      const identity = auth.require(actor, "plugins:bind"),
        parsed = bindPluginSchema.parse(input),
        body = {
          ...parsed,
          version_id: parsed.version_id,
          channel_id: z.uuid().parse(channelId),
          config: parsed.config,
        };
      const replay = await store.receipt("plugin_bind_v1", identity, parsed.command_id, body);
      if (replay) return replay;
      const { release } = await persisted(parsed.version_id);
      let config: Record<string, unknown>;
      try {
        config = validateConfig(release.pkg.configSchema, parsed.config);
      } catch {
        throw new GatewayError(
          400,
          "invalid_plugin_config",
          "Plugin configuration does not match its signed schema",
        );
      }
      return store.command(
        "plugin_bind_v1",
        auth.authorization(actor, "plugins:bind"),
        parsed.command_id,
        { ...body, config },
        undefined,
        release.trustExpiresAt,
      );
    },
    async recover(actor: PlatformActor, id: string, input: unknown) {
      available();
      const identity = auth.require(actor, "plugins:recover");
      const parsed = recoveryCommandSchema.parse(input);
      const claimed = await store.command(
        "plugin_recover_v1",
        auth.authorization(actor, "plugins:recover"),
        parsed.command_id,
        { version_id: z.uuid().parse(id), ...parsed },
        owner,
      );
      const run = await store.run(parsed.run_id);
      if (run.version_id !== id)
        throw new GatewayError(409, "validation_owned", "Cleanup belongs to another version");
      if (run.state === "FAILED" && run.error_code === "OWNER_LOST")
        return store.command(
          "plugin_finish_v1",
          { identity },
          catalogStageCommand(parsed.command_id, "cleanup"),
          {
            version_id: id,
            run_id: run.id,
            lease_epoch: run.lease_epoch,
            result: { cases: 0, steps: 0, cleanup_confirmed: true, error_code: "OWNER_LOST" },
            reason: parsed.reason,
          },
          owner,
        );
      if (run.lease_owner !== owner || run.state !== "CLEANUP_PENDING") return claimed;
      if (!run.resource_pid || !run.resource_connection) return claimed;
      try {
        process.kill(-Number(run.resource_pid), 0);
        return claimed;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") return claimed;
      }
      return store.command(
        "plugin_finish_v1",
        { identity },
        catalogStageCommand(parsed.command_id, "cleanup"),
        {
          version_id: id,
          run_id: run.id,
          lease_epoch: run.lease_epoch,
          result: { cases: 0, steps: 0, cleanup_confirmed: true, error_code: "OWNER_LOST" },
          reason: parsed.reason,
        },
        owner,
      );
    },
    async truth(actor: PlatformActor, id: string) {
      auth.require(actor, "plugins:read");
      return store.truth(z.uuid().parse(id));
    },
    async binding(actor: PlatformActor, id: string) {
      auth.require(actor, "plugins:read");
      const binding = await store.binding(z.uuid().parse(id));
      if (!binding.executable) return { ...binding, trust_state: "UNBOUND_OR_UNPUBLISHED" };
      try {
        await persisted(binding.plugin_version_id);
        return { ...binding, trust_state: "TRUSTED" };
      } catch {
        return { ...binding, executable: false, trust_state: "UNTRUSTED" };
      }
    },
  };
}
export type CatalogApplication = ReturnType<typeof createCatalogApplication>;
