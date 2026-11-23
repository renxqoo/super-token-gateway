\set ON_ERROR_STOP on
BEGIN;
SELECT format('ALTER DATABASE %I OWNER TO %I', current_database(), :'migration_role') \gexec
ALTER SCHEMA public OWNER TO pg_database_owner;
SELECT format('ALTER TABLE public.%I OWNER TO %I', c.relname, :'migration_role')
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
WHERE c.relkind IN ('r', 'p') AND (c.relname IN (
  'schema_migrations', 'config_epoch', 'projects', 'api_keys', 'upstream_accounts', 'plugins',
  'plugin_releases', 'plugin_revocations', 'gateway_config_nodes', 'plugin_release_acks',
  'channels', 'models', 'price_versions', 'project_pricing_overrides', 'async_tasks', 'routes', 'wallets', 'wallet_lots', 'supplier_root_balances',
  'supplier_cells', 'wallet_leases', 'supplier_leases', 'billing_holds', 'billing_charges',
  'ledger_entries', 'ledger_entries_default', 'funding_operations', 'billing_resolutions',
  'outbox_events', 'outbox_consumers', 'outbox_consumer_shards', 'admin_audit_events', 'admin_audit_pending'
) OR EXISTS (
  SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid AND i.inhparent = 'public.ledger_entries'::regclass
))
\gexec
SELECT format('ALTER FUNCTION public.%I(%s) OWNER TO %I', p.proname, pg_get_function_identity_arguments(p.oid), :'migration_role')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public' AND p.proname IN (
  'prepare_ledger_month', 'immutable_record', 'check_ledger_journal_balance', 'order_outbox_publish',
  'operations_health', 'refresh_config_epoch_lineage', 'reject_revoked_plugin_activation',
  'keep_plugin_revision', 'outbox_shard', 'commit_leased_batch'
)
\gexec
COMMIT;
