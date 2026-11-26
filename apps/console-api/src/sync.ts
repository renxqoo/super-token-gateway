import type { SQL } from "bun";
import type { ConsoleDrizzle } from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";
import { createConsoleCatalogSync } from "./sync-catalog.ts";
import { createConsoleAdmissionSync } from "./sync-admission.ts";
import { createConsoleReconciler } from "./sync-reconciler.ts";
import { createEntityLocker } from "./sync-state.ts";

export function createConsoleSync(db: SQL, orm: ConsoleDrizzle, gateway: GatewayClient) {
  const withEntityLock = createEntityLocker(db);
  const admission = createConsoleAdmissionSync(orm, gateway, withEntityLock);
  const catalog = createConsoleCatalogSync(
    orm,
    gateway,
    withEntityLock,
    admission.ensureBillingAccountSynced,
  );
  return {
    admission,
    catalog,
    reconcilePending: createConsoleReconciler(db, orm, admission, catalog),
  };
}

export type ConsoleSync = ReturnType<typeof createConsoleSync>;
