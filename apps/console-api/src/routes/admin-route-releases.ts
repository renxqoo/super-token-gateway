import { Hono } from "hono";
import {
  modelOnboardingPreviewSchema,
  modelOnboardingSchema,
  routeReleaseSchema,
  routeRollbackSchema,
} from "@gateway/contracts";
import { parseJsonBody, parseUuid, type ConsoleDeps, type ConsoleEnv } from "./shared.ts";

export function createAdminRouteReleaseRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const model = "/models/:model";
  const releases = model + "/route-releases";

  const onboardingPath = "/models/onboarding";
  const onboarding = deps.sync.routeReleases.onboarding;
  app.get(onboardingPath, async (c) => c.json(await onboarding.epoch()));

  app.post(onboardingPath + "/validate", async (c) =>
    c.json(await onboarding.validate(await parseJsonBody(c, modelOnboardingSchema))),
  );

  app.post(onboardingPath + "/preview", async (c) =>
    c.json(await onboarding.preview(await parseJsonBody(c, modelOnboardingPreviewSchema))),
  );

  app.post(onboardingPath, async (c) => {
    const input = await parseJsonBody(c, modelOnboardingSchema);
    const result = await onboarding.publish(input, c.get("user"));
    return c.json(result, result.state === "COMMITTED" ? 200 : 202);
  });

  app.get(model + "/onboarding/:id", async (c) => {
    const id = parseUuid(c.req.param("id"), "id").toLowerCase();
    const result = await onboarding.get(c.req.param("model")!, id);
    if (!result) return c.json({ id, state: "PENDING" }, 202);
    return c.json(result, result.state === "COMMITTED" ? 200 : 202);
  });

  app.get(model + "/routing", async (c) =>
    c.json(await deps.sync.routeReleases.view(c.req.param("model")!)),
  );

  app.post(releases + "/validate", async (c) =>
    c.json(
      await deps.sync.routeReleases.validate(
        c.req.param("model")!,
        await parseJsonBody(c, routeReleaseSchema),
      ),
    ),
  );

  app.post(releases, async (c) =>
    c.json(
      await deps.sync.routeReleases.publish(
        c.req.param("model")!,
        await parseJsonBody(c, routeReleaseSchema),
        c.get("user"),
      ),
    ),
  );

  app.get(releases + "/:id", async (c) => {
    const id = parseUuid(c.req.param("id"), "id").toLowerCase();
    const result = await deps.sync.routeReleases.get(c.req.param("model")!, id);
    return result ? c.json(result) : c.json({ id, state: "PENDING" }, 202);
  });

  app.post(releases + "/:id/rollback", async (c) =>
    c.json(
      await deps.sync.routeReleases.publish(
        c.req.param("model")!,
        {
          ...(await parseJsonBody(c, routeRollbackSchema)),
          rollback_of: parseUuid(c.req.param("id"), "id").toLowerCase(),
        },
        c.get("user"),
      ),
    ),
  );

  return app;
}
