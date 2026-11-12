import { GatewayError } from "@gateway/contracts";
import { secureEqual, verifier } from "@gateway/runtime/crypto";
import type { CatalogAuthorization, createPluginCatalogStore } from "@gateway/storage";
const permissionNames = [
  "catalog:manage",
  "funding:read",
  "funding:recharge",
  "customer:fund",
  "plugins:read",
  "plugins:install-bundled",
  "plugins:validate",
  "plugins:publish",
  "plugins:bind",
  "plugins:recover",
] as const;
export type PlatformPermission = (typeof permissionNames)[number];
export type PlatformActor = Readonly<{ identity: string; permissions: readonly string[] }>;
export function createPlatformAuthenticator(
  store: ReturnType<typeof createPluginCatalogStore>,
  config: { adminKey: string; pepper: string },
) {
  const verified = new WeakMap<PlatformActor, CatalogAuthorization>();
  return {
    async authenticate(authorization: string | undefined): Promise<PlatformActor> {
      const token = authorization?.match(/^Bearer ([^\s]{1,16384})$/)?.[1];
      if (!token) throw new GatewayError(401, "unauthenticated", "Platform credential required");
      const proof = verifier(token, config.pepper);
      const bootstrap = secureEqual(token, config.adminKey);
      const administrator = bootstrap ? undefined : await store.authenticate(proof);
      if (!bootstrap && !administrator)
        throw new GatewayError(401, "unauthenticated", "Platform credential required");
      const actor = Object.freeze({
        identity: administrator?.actor ?? "bootstrap:ADMIN_KEY",
        permissions: Object.freeze([...(administrator?.permissions ?? permissionNames)]),
      });
      verified.set(
        actor,
        Object.freeze(
          administrator
            ? {
                identity: actor.identity,
                credentialId: administrator.credential_id,
                administratorId: administrator.administrator_id,
                verifier: proof,
              }
            : { identity: "bootstrap:ADMIN_KEY", bootstrap: true },
        ),
      );
      return actor;
    },
    authorization(actor: PlatformActor, permission: PlatformPermission): CatalogAuthorization {
      const authorization = verified.get(actor);
      if (!authorization || !actor.permissions.includes(permission))
        throw new GatewayError(403, "platform_scope_denied", "Platform permission required");
      return authorization;
    },
    require(actor: PlatformActor, permission: PlatformPermission) {
      if (!verified.has(actor) || !actor.permissions.includes(permission))
        throw new GatewayError(403, "platform_scope_denied", "Platform permission required");
      return actor.identity;
    },
  };
}
