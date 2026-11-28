import type { SQL } from "bun";
import { canonical, DomainError } from "@gateway/kernel";
import type { PluginReleaseChange, PluginReleaseTarget } from "./types.ts";

import { createConfigReleaseStore, type ConfigReleaseRecord } from "./config-releases.ts";

export type PluginReleaseRecord = ConfigReleaseRecord<PluginReleaseChange>;

export type PluginRevocationRecord = {
  plugin_id: string;
  actor: string;
  reason: string;
  config_epoch: number;
  created_at: string;
};

function conflict(message: string): never {
  throw new DomainError("plugin_release_conflict", message);
}

async function lockReleaseRevisions(tx: SQL, targets: readonly PluginReleaseTarget[]) {
  const required = new Map<string, "PROVIDER" | "CONTENT" | null>();
  const explicit = new Set<string>();
  for (const target of targets) {
    if ("channel_id" in target) required.set(target.provider_plugin_id, "PROVIDER");
    else if ("route_id" in target) {
      for (const step of target.plugin_pipeline) {
        if (step.enabled !== false) required.set(step.plugin_id, "CONTENT");
      }
    } else {
      explicit.add(target.plugin_id);
      if (!required.has(target.plugin_id)) required.set(target.plugin_id, null);
    }
  }
  for (const id of [...required.keys()].sort()) {
    const capability = required.get(id);
    const [revision] = await tx`
      SELECT capability FROM public.plugins WHERE id = ${id} FOR UPDATE
    `;
    if (!revision || (capability && revision.capability !== capability)) {
      conflict("Plugin revision is missing or has the wrong capability");
    }
    const [revocation] = await tx`SELECT 1 FROM public.plugin_revocations WHERE plugin_id = ${id}`;
    if (revocation) conflict("Revoked revisions cannot be published or rolled back");
    if (!explicit.has(id)) {
      await tx`UPDATE public.plugins SET enabled = true WHERE id = ${id} AND NOT enabled`;
    }
  }
}

function orderedChanges(changes: PluginReleaseChange[]) {
  const order = ({ after }: PluginReleaseChange) =>
    "plugin_id" in after ? (after.available ? 0 : 2) : 1;
  return [...changes].sort(
    (a, b) => order(a) - order(b) || canonical(a.before).localeCompare(canonical(b.before)),
  );
}

async function applyBindings(tx: SQL, changes: readonly PluginReleaseChange[]): Promise<void> {
  for (const { before, after } of changes) {
    if ("channel_id" in before && "channel_id" in after && before.channel_id === after.channel_id) {
      const [row] = await tx`
        UPDATE public.channels
        SET provider_plugin_id = ${after.provider_plugin_id}, config_revision = config_revision + 1, updated_at = now()
        WHERE id = ${before.channel_id} AND provider_plugin_id = ${before.provider_plugin_id}
        RETURNING id
      `;
      if (!row) conflict("Channel binding has changed");
    } else if ("route_id" in before && "route_id" in after && before.route_id === after.route_id) {
      const [row] = await tx`
        UPDATE public.routes SET plugin_pipeline = ${JSON.stringify(after.plugin_pipeline)}::text::jsonb
        WHERE id = ${before.route_id} AND plugin_pipeline = ${JSON.stringify(before.plugin_pipeline)}::text::jsonb
        RETURNING id
      `;
      if (!row) conflict("Route binding has changed");
    } else if (
      "plugin_id" in before &&
      "plugin_id" in after &&
      before.plugin_id === after.plugin_id
    ) {
      if (!after.available) {
        const [inUse] = await tx`
          SELECT 1 FROM public.channels WHERE enabled AND provider_plugin_id = ${after.plugin_id}
          UNION ALL
          SELECT 1 FROM public.routes r, jsonb_array_elements(r.plugin_pipeline) AS step
          WHERE r.enabled AND step->>'plugin_id' = ${after.plugin_id} AND step->>'enabled' IS DISTINCT FROM 'false'
          LIMIT 1
        `;
        if (inUse) conflict("Plugin revision is still bound. Publish new bindings first.");
      }
      const [row] = await tx`
        UPDATE public.plugins SET enabled = ${after.available}
        WHERE id = ${after.plugin_id} AND enabled = ${before.available} RETURNING id
      `;
      if (!row) conflict("Plugin revision availability has changed");
    } else {
      conflict("Release target has changed");
    }
  }
}

export function createPluginReleaseRepository(db: SQL) {
  const store = createConfigReleaseStore<PluginReleaseChange>(
    db,
    { kind: "PLUGIN", model_name: null },
    "plugin_release_conflict",
  );
  return {
    get: store.get,
    confirmations: store.confirmations,

    revoke(
      pluginId: string,
      actor: string,
      reason: string,
    ): Promise<PluginRevocationRecord | null> {
      return db.begin(async (tx) => {
        const [revision] =
          await tx`SELECT id FROM public.plugins WHERE id = ${pluginId} FOR UPDATE`;
        if (!revision) return null;
        const [existing] = await tx<PluginRevocationRecord[]>`
          SELECT * FROM public.plugin_revocations WHERE plugin_id = ${pluginId}
        `;
        if (existing) return { ...existing, config_epoch: Number(existing.config_epoch) };
        await tx`UPDATE public.plugins SET enabled = false WHERE id = ${pluginId}`;
        const [epoch] = await tx`
          UPDATE public.config_epoch SET version = version + 1, updated_at = now()
          WHERE id = 1 RETURNING version
        `;
        const [revocation] = await tx<PluginRevocationRecord[]>`
          INSERT INTO public.plugin_revocations (plugin_id, actor, reason, config_epoch)
          VALUES (${pluginId}, ${actor}, ${reason}, ${epoch!.version})
          RETURNING *
        `;
        return { ...revocation!, config_epoch: Number(revocation!.config_epoch) };
      });
    },

    publish(input: {
      id: string;
      actor: string;
      request_digest: string;
      expected_epoch: number;
      changes: PluginReleaseChange[];
      rollback_of: string | null;
    }): Promise<PluginReleaseRecord> {
      return db.begin(async (tx) => {
        const previous = await store.lockAndReplay(tx, input);
        if (previous) return previous;
        if (input.rollback_of) {
          const [superseded] = await tx`
            SELECT 1 FROM public.config_releases original
            JOIN public.config_releases newer ON newer.config_epoch > original.config_epoch
            CROSS JOIN LATERAL jsonb_array_elements(original.changes) old_change
            CROSS JOIN LATERAL jsonb_array_elements(newer.changes) new_change
            WHERE original.kind = 'PLUGIN' AND newer.kind = 'PLUGIN' AND original.id = ${input.rollback_of}
              AND (
                old_change->'after'->>'channel_id' = new_change->'after'->>'channel_id'
                OR old_change->'after'->>'route_id' = new_change->'after'->>'route_id'
                OR old_change->'after'->>'plugin_id' = new_change->'after'->>'plugin_id'
              )
            LIMIT 1
          `;
          if (superseded) conflict("A later release has changed the binding");
        }
        const changes = orderedChanges(input.changes);
        await lockReleaseRevisions(
          tx,
          changes.map(({ after }) => after),
        );
        await applyBindings(tx, changes);
        return store.record(tx, input);
      });
    },
  };
}
