import { and, asc, eq, inArray, isNull, lte, sql } from "drizzle-orm";
import {
  GatewayError,
  type GatewayRouteReleaseResult,
  type GatewayModelOnboardingResult,
  type ModelOnboardingInput,
  type RouteReleaseIntent,
  type RouteReleaseResult,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256Hex, type AuthUser } from "./auth.ts";
import {
  consoleAuditLogs,
  routeReleaseRequests,
  routes,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";
import { createModelOnboardingClient } from "./model-onboarding-client.ts";
import {
  discardModelOnboarding,
  onboardingConflict,
  promoteModelOnboarding,
  reserveModelOnboarding,
} from "./model-onboarding-projection.ts";
import { createRouteReleaseClient } from "./route-release-client.ts";

type RequestRow = typeof routeReleaseRequests.$inferSelect;
type ExpectedTarget = { route_id: string; expected_revision: number };

function publicResult(result: GatewayRouteReleaseResult): RouteReleaseResult {
  const { request_digest: _, ...record } = result;
  return record;
}

function conflict(message: string): never {
  throw new GatewayError(409, "route_release_conflict", message);
}

export function createConsoleRouteReleases(orm: ConsoleDrizzle, gateway: GatewayClient) {
  const client = createRouteReleaseClient(gateway);
  const modelClient = createModelOnboardingClient(gateway);

  async function begin(
    model: string,
    intent: RequestRow["intent"],
    actor: AuthUser,
  ): Promise<RequestRow> {
    const draft = "deployments" in intent ? intent : null;
    const identity = draft
      ? { ...draft, deployments: [...draft.deployments].sort((a, b) => a.id.localeCompare(b.id)) }
      : "targets" in intent
        ? {
            model,
            ...intent,
            targets: [...intent.targets].sort((a, b) => a.route_id.localeCompare(b.route_id)),
          }
        : { model, ...intent };
    const digest = sha256Hex(canonical(identity));
    const [existing] = await orm
      .select()
      .from(routeReleaseRequests)
      .where(eq(routeReleaseRequests.id, intent.id));
    if (existing) {
      assertSameRequest(existing, digest, actor.id);
      return existing;
    }

    const targets: ExpectedTarget[] =
      "targets" in intent
        ? intent.targets
        : "rollback_of" in intent
          ? (await client.get(model, intent.rollback_of)).changes.map(({ after }) => ({
              route_id: after.id,
              expected_revision: after.source_revision,
            }))
          : [];

    return orm.transaction(async (tx) => {
      const [created] = await tx
        .insert(routeReleaseRequests)
        .values({
          id: intent.id,
          modelName: model,
          actorUserId: actor.id,
          actorEmail: actor.email,
          digest,
          intent,
        })
        .onConflictDoNothing()
        .returning();
      if (!created) {
        const [previous] = await tx
          .select()
          .from(routeReleaseRequests)
          .where(eq(routeReleaseRequests.id, intent.id));
        assertSameRequest(previous!, digest, actor.id);
        return previous!;
      }

      if (draft) await reserveModelOnboarding(tx, draft);
      for (const target of [...targets].sort((a, b) => a.route_id.localeCompare(b.route_id))) {
        const [marked] = await tx
          .update(routes)
          .set({ pendingReleaseId: intent.id })
          .where(
            and(
              eq(routes.id, target.route_id),
              eq(routes.modelName, model),
              eq(routes.desiredRevision, target.expected_revision),
              eq(routes.syncedRevision, target.expected_revision),
              eq(routes.syncStatus, "SYNCED"),
              isNull(routes.pendingReleaseId),
            ),
          )
          .returning({ id: routes.id });
        if (!marked) conflict("A route has changed, is not synced, or has another pending release");
      }
      return created;
    });
  }

  function assertSameRequest(row: RequestRow, digest: string, actorId: string) {
    if (row.digest !== digest || row.actorUserId !== actorId) {
      conflict("Release identifier already belongs to another request");
    }
  }

  function checkResult(row: RequestRow, result: GatewayRouteReleaseResult) {
    if (
      row.digest !== result.request_digest ||
      row.id !== result.id ||
      row.modelName !== result.model_name
    ) {
      conflict("Gateway release does not match the Console request");
    }
  }

  async function saveResult(row: RequestRow, result: GatewayRouteReleaseResult) {
    checkResult(row, result);
    await orm.transaction(async (tx) => {
      const [pending] = await tx
        .update(routeReleaseRequests)
        .set({ state: "COMMITTED" })
        .where(and(eq(routeReleaseRequests.id, row.id), eq(routeReleaseRequests.state, "PENDING")))
        .returning({ id: routeReleaseRequests.id });
      if (!pending) return;

      for (const { before, after } of result.changes) {
        const [applied] = await tx
          .update(routes)
          .set({
            priority: after.priority,
            weight: after.weight,
            syncedRevision: after.source_revision,
            desiredRevision: after.source_revision,
            syncStatus: "SYNCED",
            pendingReleaseId: null,
            lastSyncError: null,
            updatedAt: sql`now()`,
          })
          .where(
            and(
              eq(routes.id, after.id),
              eq(routes.pendingReleaseId, row.id),
              eq(routes.desiredRevision, before.source_revision),
              eq(routes.syncedRevision, before.source_revision),
            ),
          )
          .returning({ id: routes.id });
        if (!applied) conflict("Console route projection crossed a pending release");
      }

      await tx.insert(consoleAuditLogs).values({
        id: crypto.randomUUID(),
        actorUserId: row.actorUserId,
        actorEmail: row.actorEmail,
        action: result.rollback_of ? "admin.route_release.rollback" : "admin.route_release.publish",
        targetType: "route_release",
        targetId: row.id,
        metadata: {
          model_name: row.modelName,
          config_epoch: result.config_epoch,
          rollback_of: result.rollback_of,
        },
      });
    });
  }

  async function projectExternalRelease(result: GatewayRouteReleaseResult) {
    const ids = result.changes.map(({ after }) => after.id);
    await orm.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(routes)
        .where(and(eq(routes.modelName, result.model_name), inArray(routes.id, ids)))
        .orderBy(asc(routes.id))
        .for("update");
      if (rows.length !== ids.length) return;
      const current = new Map(rows.map((route) => [route.id, route]));
      if (
        !result.changes.every(({ before }) => {
          const route = current.get(before.id)!;
          return (
            route.syncStatus === "SYNCED" &&
            route.pendingReleaseId === null &&
            route.desiredRevision === before.source_revision &&
            route.syncedRevision === before.source_revision &&
            route.priority === before.priority &&
            route.weight === before.weight
          );
        })
      )
        return;
      for (const { after } of result.changes) {
        await tx
          .update(routes)
          .set({
            priority: after.priority,
            weight: after.weight,
            syncedRevision: after.source_revision,
            desiredRevision: after.source_revision,
            updatedAt: sql`now()`,
          })
          .where(eq(routes.id, after.id));
      }
    });
  }

  function rejectedOnboarding(row: RequestRow): never {
    if (row.rejection) {
      const { status, code, message } = row.rejection;
      throw new GatewayError(status, code, message);
    }
    onboardingConflict("Onboarding was rejected. Use a new identifier");
  }

  async function reject(row: RequestRow, err?: GatewayError) {
    await orm.transaction(async (tx) => {
      const [pending] = await tx
        .update(routeReleaseRequests)
        .set({
          state: "REJECTED",
          ...(err
            ? { rejection: { status: err.status, code: err.code, message: err.message } }
            : {}),
        })
        .where(and(eq(routeReleaseRequests.id, row.id), eq(routeReleaseRequests.state, "PENDING")))
        .returning({ id: routeReleaseRequests.id });
      if (!pending) return;
      if ("deployments" in row.intent) {
        await discardModelOnboarding(tx, row);
      } else {
        await tx
          .update(routes)
          .set({ pendingReleaseId: null })
          .where(eq(routes.pendingReleaseId, row.id));
      }
    });
  }

  async function submit(row: RequestRow, signal?: AbortSignal): Promise<RouteReleaseResult> {
    if (row.state === "REJECTED")
      conflict("Release was rejected. Validate again with a new identifier");
    if (row.state === "COMMITTED") {
      const result = await client.get(row.modelName, row.id);
      checkResult(row, result);
      return publicResult(result);
    }
    const intent = row.intent;
    if ("deployments" in intent) conflict("Release identifier is reserved for model onboarding");
    let result: GatewayRouteReleaseResult;
    try {
      if ("rollback_of" in intent) {
        const { rollback_of, ...input } = intent;
        result = await client.rollback(row.modelName, rollback_of, input, signal);
      } else {
        result = await client.publish(row.modelName, intent, signal);
      }
    } catch (err) {
      if (
        err instanceof GatewayError &&
        ["route_release_conflict", "invalid_route_release", "not_found"].includes(err.code)
      )
        await reject(row);
      throw err;
    }
    await saveResult(row, result);
    return publicResult(result);
  }

  async function finishOnboarding(
    row: RequestRow,
    input: ModelOnboardingInput,
    initial: GatewayModelOnboardingResult,
    signal?: AbortSignal,
  ) {
    const { projected, result } = await promoteModelOnboarding(
      orm,
      row,
      input,
      initial,
      (id) => gateway.getPriceVersion(id, signal),
      () => modelClient.get(row.modelName, row.id, signal),
    );
    const { request_digest: _, ...record } = result;
    return { ...record, state: projected ? ("COMMITTED" as const) : ("PENDING" as const) };
  }

  async function submitOnboarding(
    row: RequestRow,
    input: ModelOnboardingInput,
    signal?: AbortSignal,
  ) {
    if (row.state === "REJECTED") rejectedOnboarding(row);
    let result: GatewayModelOnboardingResult;
    try {
      result =
        row.state === "COMMITTED"
          ? await modelClient.get(row.modelName, row.id)
          : await modelClient.publish(input, signal);
    } catch (err) {
      if (
        row.state === "PENDING" &&
        err instanceof GatewayError &&
        ["model_onboarding_conflict", "invalid_model_onboarding"].includes(err.code)
      ) {
        await reject(row, err);
      }
      throw err;
    }
    return finishOnboarding(row, input, result, signal);
  }

  return {
    view: client.view,
    validate: client.validate,

    onboarding: {
      epoch: modelClient.epoch,
      validate: modelClient.validate,
      preview: modelClient.preview,

      async publish(input: ModelOnboardingInput, actor: AuthUser) {
        return submitOnboarding(await begin(input.model.name, input, actor), input);
      },

      async get(model: string, id: string) {
        const [row] = await orm
          .select()
          .from(routeReleaseRequests)
          .where(and(eq(routeReleaseRequests.id, id), eq(routeReleaseRequests.modelName, model)));
        if (!row || !("deployments" in row.intent)) {
          throw new GatewayError(404, "not_found", "Model onboarding not found");
        }
        if (row.state === "REJECTED") rejectedOnboarding(row);
        let result: GatewayModelOnboardingResult;
        try {
          result = await modelClient.get(model, id);
        } catch (err) {
          if (err instanceof GatewayError && err.status === 404 && row.state === "PENDING")
            return null;
          throw err;
        }
        return finishOnboarding(row, row.intent, result);
      },
    },

    async publish(model: string, intent: RouteReleaseIntent, actor: AuthUser) {
      return submit(await begin(model, intent, actor));
    },

    async get(model: string, id: string) {
      const [row] = await orm
        .select()
        .from(routeReleaseRequests)
        .where(and(eq(routeReleaseRequests.id, id), eq(routeReleaseRequests.modelName, model)));
      if (row && "deployments" in row.intent) {
        throw new GatewayError(404, "not_found", "Route release not found");
      }
      let result: GatewayRouteReleaseResult;
      try {
        result = await client.get(model, id);
      } catch (err) {
        if (err instanceof GatewayError && err.status === 404) {
          if (row?.state === "PENDING") return null;
          if (row?.state === "REJECTED") {
            conflict("Release was rejected. Validate again with a new identifier");
          }
        }
        throw err;
      }
      if (row?.state === "REJECTED") {
        if (row.digest !== result.request_digest) await projectExternalRelease(result);
        conflict("Release was rejected. Validate again with a new identifier");
      }
      if (row) await saveResult(row, result);
      else await projectExternalRelease(result);
      return publicResult(result);
    },

    async reconcilePending(signal?: AbortSignal) {
      const pending = await orm
        .select()
        .from(routeReleaseRequests)
        .where(
          and(
            eq(routeReleaseRequests.state, "PENDING"),
            lte(routeReleaseRequests.nextAttemptAt, new Date()),
          ),
        )
        .orderBy(asc(routeReleaseRequests.nextAttemptAt), asc(routeReleaseRequests.id))
        .limit(10);
      let synced = 0;
      let failed = 0;
      for (const row of pending) {
        if (signal?.aborted) break;
        const [claimed] = await orm
          .update(routeReleaseRequests)
          .set({ nextAttemptAt: sql`now() + interval '30 seconds'` })
          .where(
            and(
              eq(routeReleaseRequests.id, row.id),
              eq(routeReleaseRequests.state, "PENDING"),
              lte(routeReleaseRequests.nextAttemptAt, new Date()),
            ),
          )
          .returning({ id: routeReleaseRequests.id });
        if (!claimed) continue;
        try {
          if ("deployments" in row.intent) {
            const status = await modelClient
              .get(row.modelName, row.id, signal)
              .catch((err: unknown) => {
                if (err instanceof GatewayError && err.status === 404) return null;
                throw err;
              });
            const result =
              status?.request_digest === row.digest
                ? await finishOnboarding(row, row.intent, status, signal)
                : await submitOnboarding(row, row.intent, signal);
            if (result.state === "COMMITTED") synced++;
          } else {
            await submit(row, signal);
            synced++;
          }
        } catch {
          failed++;
        }
      }
      return { synced, failed };
    },
  };
}
