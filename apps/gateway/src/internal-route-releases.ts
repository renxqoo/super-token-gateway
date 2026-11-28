import { GatewayError, routeReleaseSchema, routeRollbackSchema } from "@gateway/contracts";
import { createRouteReleaseRepository } from "@gateway/ledger";
import { parseUuid } from "./internal-request.ts";
import type { InternalApiDeps, InternalJsonRoute, InternalRoute } from "./internal-route-types.ts";

export function registerInternalRouteReleases(
  deps: InternalApiDeps,
  route: InternalRoute,
  jsonRoute: InternalJsonRoute,
  onConfigUpdated: (epoch: number) => Promise<void>,
) {
  const modelPath = "/internal/v1/models/:model";
  const releasePath = modelPath + "/route-releases";
  const repository = (model: string) => createRouteReleaseRepository(deps.db, model);

  async function report(
    model: string,
    repo: ReturnType<typeof repository>,
    release: NonNullable<Awaited<ReturnType<typeof repo.get>>>,
  ) {
    return {
      ...release,
      model_name: model,
      local_applied_epoch: deps.snapshot.get().epoch,
      nodes: await repo.confirmations(release),
    };
  }

  route("GET", modelPath + "/routing", "catalog", async (c) =>
    c.json(await repository(c.req.param("model")!).view()),
  );

  jsonRoute("POST", releasePath + "/validate", "catalog", routeReleaseSchema, async (c, body) => {
    const changes = await repository(c.req.param("model")!).validate(
      body.expected_epoch,
      body.targets,
    );
    return c.json({ config_epoch: body.expected_epoch, changes });
  });

  jsonRoute("POST", releasePath, "catalog", routeReleaseSchema, async (c, body) => {
    const model = c.req.param("model")!;
    const repo = repository(model);
    const release = await repo.publish(body, c.get("principal").id);
    await onConfigUpdated(release.config_epoch);
    return c.json(await report(model, repo, release));
  });

  route("GET", releasePath + "/:releaseId", "catalog", async (c) => {
    const model = c.req.param("model")!;
    const repo = repository(model);
    const release = await repo.get(parseUuid(c.req.param("releaseId"), "releaseId"));
    if (!release) throw new GatewayError(404, "not_found", "Route release not found");
    return c.json(await report(model, repo, release));
  });

  jsonRoute(
    "POST",
    releasePath + "/:releaseId/rollback",
    "catalog",
    routeRollbackSchema,
    async (c, body) => {
      const model = c.req.param("model")!;
      const repo = repository(model);
      const release = await repo.publish(
        {
          ...body,
          rollback_of: parseUuid(c.req.param("releaseId"), "releaseId").toLowerCase(),
        },
        c.get("principal").id,
      );
      await onConfigUpdated(release.config_epoch);
      return c.json(await report(model, repo, release));
    },
  );
}
