\set ON_ERROR_STOP on
BEGIN;

DO $$
BEGIN
  IF to_regclass('public.console_schema_migrations') IS NULL
     OR to_regclass('public.schema_migrations') IS NOT NULL THEN
    RAISE EXCEPTION 'Apply Console migrations to the dedicated Console database first';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stg_console') THEN
    CREATE ROLE stg_console NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  ELSIF EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'stg_console' AND
      (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
  ) OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = 'stg_console'::regrole) THEN
    RAISE EXCEPTION 'unsafe preexisting stg_console role';
  END IF;
END;
$$;

SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO stg_console', current_database()) \gexec
REVOKE ALL ON SCHEMA public FROM PUBLIC, stg_console;
GRANT USAGE ON SCHEMA public TO stg_console;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC, stg_console;

GRANT SELECT ON public.console_schema_migrations TO stg_console;
GRANT SELECT, INSERT, UPDATE ON public.users, public.organizations, public.org_invitations,
  public.billing_accounts, public.usage_hourly_rollups, public.usage_daily_rollups,
  public.alert_deliveries, public.platform_credit_defaults TO stg_console;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.sessions, public.org_members, public.api_key_records,
  public.upstream_accounts, public.channels, public.pricing_templates, public.model_catalog,
  public.routes, public.pricing_overrides, public.alert_rules, public.alert_eval_queue TO stg_console;
GRANT SELECT, INSERT ON public.price_version_snapshots, public.usage_events, public.console_audit_logs TO stg_console;
GRANT SELECT, INSERT, UPDATE ON public.usage_monitor_windows,
  public.usage_monitor_window_stats, public.usage_monitor_findings, public.route_release_requests TO stg_console;
COMMIT;
