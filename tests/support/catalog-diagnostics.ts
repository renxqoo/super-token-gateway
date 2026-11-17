import type { SQL } from "bun";

export async function reportCatalogFailure(db: SQL, error: unknown) {
  if ((error as { code?: unknown } | null)?.code !== "plugin_validation_failed") return;
  const query = db`SELECT current_database() database,coalesce(jsonb_agg(v),'[]'::jsonb) validations FROM (SELECT id,state,error_code,result,resource_pid,resource_connection FROM plugin_validation_runs WHERE error_code IS NOT NULL ORDER BY started_at DESC,id DESC LIMIT 4) v`;
  const timer = setTimeout(() => query.cancel(), 200);
  try {
    const [diagnostics] = await query;
    console.error(JSON.stringify({ event: "plugin_validation_failed", ...diagnostics }));
  } catch {
  } finally {
    clearTimeout(timer);
  }
}
