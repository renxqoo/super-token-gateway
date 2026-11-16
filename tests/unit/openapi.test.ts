import { expect, test } from "bun:test";
import { SQL } from "bun";
import { createGatewayApp } from "../../apps/gateway/src/app.ts";
import { createCoreAccountApplication } from "../../apps/core-api/src/public/account.ts";
import { createCoreManagementApplication } from "../../apps/core-api/src/platform/management.ts";
import { catalogRoutes } from "../../apps/core-api/src/plugins/http.ts";

type Operation = { method: string; path: string };
type RouteTable = { routes: { method: string; path: string }[] };

function operations(app: RouteTable, prefix = ""): Operation[] {
  return app.routes
    .filter((route) => route.method !== "ALL")
    .map((route) => ({ method: route.method.toLowerCase(), path: `${prefix}${route.path}` }));
}

function serves(route: Operation, documented: Operation) {
  if (route.method !== documented.method) return false;
  const served = route.path.split("/"),
    described = documented.path.split("/");
  return (
    served.length === described.length &&
    served.every((segment, index) => {
      const other = described[index]!;
      if (segment.startsWith(":")) return other.length > 0;
      return !other.startsWith("{") && segment === other;
    })
  );
}

async function contract() {
  const document = (await Bun.file("docs/openapi.json").json()) as {
    paths: Record<string, Record<string, unknown>>;
  };
  const documented = Object.entries(document.paths).flatMap(([path, methods]) =>
    Object.keys(methods).map((method) => ({ method, path })),
  );
  const db = new SQL("postgres://unused:unused@127.0.0.1:1/unused", { max: 1 });
  const secret = "synthetic-platform-secret-0123456789";
  const v2 = [
    ...operations(createGatewayApp({} as never)),
    ...operations(createCoreAccountApplication({ db, pepper: secret, trustedProxyCidrs: [] })),
    ...operations(catalogRoutes({} as never), "/admin"),
    ...operations(
      createCoreManagementApplication({
        db,
        adminKey: secret,
        pepper: secret,
        encryptionKey: "44".repeat(32),
        queue: { stats: async () => ({}), retry: async () => {} } as never,
      }),
      "/admin",
    ),
  ];
  await db.close();
  return { documented, v2 };
}

test("every route the v2 Gateway and Core serve is documented in the published OpenAPI with its method", async () => {
  const { documented, v2 } = await contract();
  const undocumented = v2.filter((route) => !documented.some((entry) => serves(route, entry)));
  expect(undocumented).toEqual([]);
  expect(v2).toContainEqual({ method: "post", path: "/v1/music/generations" });
  expect(v2).toContainEqual({ method: "post", path: "/admin/upstream-accounts/:id/recharges" });
});

test("every documented OpenAPI operation is served by the v2 Gateway or Core", async () => {
  const { documented, v2 } = await contract();
  const unserved = documented.filter((entry) => !v2.some((route) => serves(route, entry)));
  expect(unserved).toEqual([]);
});
