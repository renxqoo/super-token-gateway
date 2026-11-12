import type { SQL } from "bun";
import { GatewayError } from "@gateway/contracts";
export type CatalogKind =
  | "plugin_register_v1"
  | "plugin_validate_v1"
  | "plugin_finish_v1"
  | "plugin_publish_v1"
  | "plugin_bind_v1"
  | "plugin_recover_v1"
  | "plugin_resources_v1";
export type CatalogAuthorization =
  | { identity: "bootstrap:ADMIN_KEY"; bootstrap: true }
  | { identity: string; credentialId: string; administratorId: string; verifier: string };
export type CatalogReceipt = Record<string, unknown> & {
  version_id: string;
  state?: string;
  revision?: string;
};
export function createPluginCatalogStore(db: SQL) {
  return {
    async authenticate(
      verifier: string,
    ): Promise<
      | { actor: string; permissions: string[]; credential_id: string; administrator_id: string }
      | undefined
    > {
      return (
        await db<
          {
            actor: string;
            permissions: string[];
            credential_id: string;
            administrator_id: string;
          }[]
        >`SELECT * FROM verify_platform_credential_v1(${verifier})`
      )[0];
    },
    async command<Kind extends CatalogKind>(
      kind: Kind,
      authorization: Kind extends "plugin_finish_v1" | "plugin_resources_v1"
        ? { identity: string }
        : CatalogAuthorization,
      commandId: string,
      body: Record<string, unknown>,
      owner?: string,
      trustExpiresAt?: string,
    ): Promise<CatalogReceipt> {
      try {
        const credential =
          "credentialId" in authorization
            ? (authorization as Extract<CatalogAuthorization, { credentialId: string }>)
            : undefined;
        const rows =
          await db`SELECT plugin_catalog_command_v1(${kind},${authorization.identity},${commandId}::uuid,${body}::jsonb,${owner ?? null}::uuid,${trustExpiresAt ?? null}::timestamptz,${credential?.credentialId ?? null}::uuid,${credential?.administratorId ?? null}::uuid,${credential?.verifier ?? null},${"bootstrap" in authorization && authorization.bootstrap === true}) AS receipt`;
        return rows[0].receipt as CatalogReceipt;
      } catch (error) {
        const code =
          (error as { errno?: string; code?: string }).errno ?? (error as { code?: string }).code;
        if (code === "P0409" || code === "23505")
          throw new GatewayError(
            409,
            "catalog_conflict",
            "Catalog command conflicts with committed state",
          );
        if (code === "P0403")
          throw new GatewayError(403, "catalog_scope_denied", "Catalog ownership denied");
        if (code === "P0002")
          throw new GatewayError(404, "catalog_not_found", "Catalog object not found");
        if (code === "P0422" || code === "23514" || code === "22P02")
          throw new GatewayError(400, "invalid_catalog_input", "Invalid catalog command");
        throw error;
      }
    },
    async receipt(
      kind: CatalogKind,
      actor: string,
      commandId: string,
      body?: Record<string, unknown>,
    ): Promise<CatalogReceipt | undefined> {
      try {
        const rows =
          await db`SELECT plugin_catalog_receipt_v1(${kind},${actor},${commandId}::uuid,${body ?? null}::jsonb) AS receipt`;
        return rows[0].receipt ?? undefined;
      } catch (error) {
        if ((error as { errno?: string }).errno === "P0409")
          throw new GatewayError(
            409,
            "catalog_conflict",
            "Catalog command conflicts with committed state",
          );
        throw error;
      }
    },
    async version(id: string) {
      const rows =
        await db`SELECT v.*,p.package_bytes,p.release_descriptor,p.release_signature,p.release_key_id FROM plugin_versions v JOIN plugin_packages p ON p.digest=v.package_digest WHERE v.id=${id}::uuid`;
      if (!rows[0]) throw new GatewayError(404, "catalog_not_found", "Plugin version not found");
      return rows[0];
    },
    async run(id: string) {
      const rows =
        await db`SELECT *,lease_epoch::text FROM plugin_validation_runs WHERE id=${id}::uuid`;
      if (!rows[0]) throw new GatewayError(404, "catalog_not_found", "Validation run not found");
      return rows[0];
    },
    async truth(id: string) {
      const rows =
        await db`SELECT v.id,v.name,v.version,v.package_digest,v.abi_version,v.config_schema_digest,v.state,v.quarantined,v.revision::text,v.validation_result,r.receipt AS publication FROM plugin_versions v LEFT JOIN plugin_publication_receipts r ON r.version_id=v.id WHERE v.id=${id}::uuid`;
      if (!rows[0]) throw new GatewayError(404, "catalog_not_found", "Plugin version not found");
      return rows[0];
    },
    async binding(id: string) {
      const rows =
        await db`SELECT c.id,c.brand_id,c.account_id,c.binding_id,c.config_revision::text,b.plugin_version_id,b.config,b.config_digest,v.package_digest,(b.id IS NOT NULL AND v.state='PUBLISHED' AND NOT v.quarantined AND p.id IS NOT NULL) AS executable FROM channels c LEFT JOIN channel_plugin_bindings b ON b.id=c.binding_id LEFT JOIN plugin_versions v ON v.id=b.plugin_version_id LEFT JOIN plugin_publication_receipts p ON p.version_id=v.id WHERE c.id=${id}::uuid`;
      if (!rows[0]) throw new GatewayError(404, "catalog_not_found", "Provider not found");
      return rows[0];
    },
  };
}
