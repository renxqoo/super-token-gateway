import { Hono } from "hono";
import { createAdminCatalogRoutes } from "./admin-catalog.ts";
import { createAdminFinanceRoutes } from "./admin-finance.ts";
import { createAdminOpsRoutes } from "./admin-ops.ts";
import { requirePlatformAdmin, type ConsoleDeps, type ConsoleEnv } from "./shared.ts";

const PROTECTED_PREFIXES = [
  "/upstream-accounts",
  "/upstream-accounts/*",
  "/channels",
  "/channels/*",
  "/pricing-templates",
  "/pricing-templates/*",
  "/models",
  "/models/*",
  "/routes",
  "/routes/*",
  "/pricing-overrides",
  "/pricing-overrides/*",
  "/sync/*",
  "/finance/*",
  "/plugins",
  "/plugins/*",
  "/plugin-releases",
  "/plugin-releases/*",
  "/providers/*",
  "/audit-logs",
  "/users",
  "/users/*",
  "/orgs/*",
  "/billing-accounts",
  "/billing-accounts/*",
];

export function createAdminRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const adminOnly = requirePlatformAdmin(deps);
  for (const pattern of PROTECTED_PREFIXES) {
    app.use(pattern, adminOnly);
  }
  app.route("/", createAdminCatalogRoutes(deps));
  app.route("/", createAdminFinanceRoutes(deps));
  app.route("/", createAdminOpsRoutes(deps));
  return app;
}
