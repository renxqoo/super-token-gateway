\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
  role_name TEXT;
BEGIN
  FOREACH role_name IN ARRAY ARRAY['stg_data', 'stg_control', 'stg_worker', 'stg_audit'] LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', role_name);
    ELSIF EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = role_name AND
        (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)
    ) OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = to_regrole(role_name)::oid) THEN
      RAISE EXCEPTION 'unsafe preexisting capability role %', role_name;
    END IF;
  END LOOP;
END;
$$;

SELECT format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO stg_data, stg_control, stg_worker, stg_audit', current_database()) \gexec
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO stg_data, stg_control, stg_worker, stg_audit;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC, stg_data, stg_control, stg_worker, stg_audit;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC, stg_data, stg_control, stg_worker, stg_audit;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, stg_data, stg_control, stg_worker, stg_audit;

GRANT SELECT ON public.schema_migrations TO stg_data, stg_control, stg_worker, stg_audit;
GRANT SELECT ON public.config_epoch, public.billing_accounts, public.api_keys, public.upstream_accounts,
  public.plugins, public.channels, public.models, public.price_versions, public.billing_account_pricing_overrides,
  public.pricing_modifiers, public.fx_rate_versions, public.routes TO stg_data, stg_control;
GRANT SELECT ON public.console_organization_admission TO stg_data, stg_control;
GRANT INSERT, UPDATE ON public.console_organization_admission TO stg_control;
GRANT SELECT, INSERT, UPDATE ON public.async_tasks TO stg_data, stg_worker;
GRANT SELECT, UPDATE ON public.async_tasks TO stg_control;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.async_task_admissions TO stg_data;
GRANT SELECT, DELETE ON public.async_task_admissions TO stg_worker;
GRANT UPDATE (expires_at) ON public.async_task_admissions TO stg_worker;
GRANT SELECT ON public.async_tasks TO stg_audit;
GRANT SELECT ON public.wallets, public.wallet_lots, public.supplier_cells, public.wallet_leases,
  public.supplier_leases, public.billing_holds, public.billing_charges, public.ledger_entries
  TO stg_data, stg_control, stg_worker;
GRANT SELECT ON public.supplier_root_balances TO stg_control, stg_worker;
GRANT SELECT ON public.outbox_events, public.outbox_consumers, public.outbox_consumer_shards TO stg_control, stg_worker;
GRANT SELECT ON public.plugin_revocations TO stg_data, stg_control;
GRANT SELECT ON public.gateway_config_nodes, public.plugin_release_acks TO stg_data, stg_control, stg_worker;
GRANT SELECT (config_epoch) ON public.plugin_releases TO stg_data;
GRANT SELECT (expected_nodes) ON public.plugin_releases TO stg_worker;
GRANT INSERT, UPDATE ON public.gateway_config_nodes TO stg_data;
GRANT DELETE ON public.gateway_config_nodes TO stg_worker;
GRANT INSERT ON public.plugin_release_acks TO stg_data;
GRANT INSERT ON public.plugin_revocations TO stg_control;
GRANT SELECT ON public.funding_operations, public.billing_resolutions, public.plugin_releases TO stg_control;
GRANT SELECT ON public.billing_charges, public.ledger_entries, public.admin_audit_events,
  public.price_versions, public.pricing_modifiers, public.fx_rate_versions TO stg_audit;

GRANT INSERT ON public.wallet_leases, public.supplier_leases, public.billing_holds TO stg_data;
GRANT UPDATE (available_atoms, reserved_atoms, spent_atoms, expired_atoms, debt_atoms,
  credit_reserved_atoms, evidence_disputed, freeze_reason) ON public.wallets TO stg_data, stg_worker;
GRANT UPDATE (available_atoms, reserved_atoms, spent_atoms, expired_atoms)
  ON public.wallet_lots TO stg_data, stg_worker;
GRANT UPDATE (balance_atoms, reserved_atoms, spent_atoms, revision)
  ON public.supplier_cells TO stg_data, stg_worker;
GRANT UPDATE (state, remaining_atoms, spent_atoms, spent_lot_id, lot_allocations, earliest_lot_expires_at, expires_at, updated_at)
  ON public.wallet_leases TO stg_data, stg_control, stg_worker;
GRANT UPDATE (state, remaining_atoms, spent_atoms, expires_at, updated_at)
  ON public.supplier_leases TO stg_data, stg_control, stg_worker;
GRANT UPDATE (state, held_atoms, credit_atoms, key_reserved_atoms, lot_allocations, supplier_holds, wallet_lease_id,
  supplier_lease_id, expires_at, closed_at) ON public.billing_holds TO stg_data, stg_control;
GRANT UPDATE (state, lot_allocations, wallet_lease_id, supplier_lease_id)
  ON public.billing_holds TO stg_worker;
GRANT UPDATE (state, revision, updated_at) ON public.upstream_accounts TO stg_data;
GRANT UPDATE ON public.supplier_root_balances TO stg_control, stg_worker;
GRANT UPDATE ON public.billing_charges TO stg_control;
GRANT UPDATE (funded_atoms, debt_atoms, lot_allocations, outcome) ON public.billing_charges TO stg_worker;
GRANT INSERT ON public.billing_charges TO stg_data, stg_control;
GRANT INSERT (journal_id, ordinal, side, entity_id, request_id, reference, kind, currency, account, atoms)
  ON public.ledger_entries TO stg_data, stg_control, stg_worker;
GRANT INSERT (event_id, kind, request_id, billing_account_id, payload) ON public.outbox_events TO stg_data, stg_control;
GRANT USAGE ON SEQUENCE public.outbox_events_seq_seq TO stg_data, stg_control;
GRANT INSERT, UPDATE ON public.outbox_consumers, public.outbox_consumer_shards TO stg_control, stg_worker;

GRANT UPDATE (spend_window_start, spent_atoms, reserved_atoms) ON public.api_keys TO stg_data;
GRANT INSERT, UPDATE ON public.billing_accounts, public.api_keys, public.upstream_accounts, public.plugins,
  public.channels, public.models, public.routes, public.billing_account_pricing_overrides, public.pricing_modifiers,
  public.wallets, public.wallet_lots,
  public.supplier_root_balances, public.supplier_cells TO stg_control;
GRANT UPDATE ON public.config_epoch TO stg_control;
GRANT INSERT ON public.price_versions, public.fx_rate_versions, public.funding_operations, public.billing_resolutions,
  public.plugin_releases, public.admin_audit_events TO stg_control;
GRANT INSERT, DELETE ON public.admin_audit_pending TO stg_control;
GRANT SELECT (request_id) ON public.admin_audit_pending TO stg_control;
GRANT EXECUTE ON FUNCTION public.prepare_ledger_month(DATE) TO stg_worker;
GRANT EXECUTE ON FUNCTION public.operations_health(TEXT) TO stg_worker;
GRANT EXECUTE ON FUNCTION public.outbox_shard(UUID) TO stg_data, stg_control, stg_worker;
REVOKE ALL ON FUNCTION public.publish_outbox_batch(UUID[], TEXT[], UUID[], UUID[], JSONB[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.publish_outbox_batch(UUID[], TEXT[], UUID[], UUID[], JSONB[]) TO stg_data;
GRANT EXECUTE ON FUNCTION public.commit_leased_batch(
  UUID[], UUID[], UUID[], UUID[], UUID[], UUID[], UUID[], NUMERIC[], NUMERIC[],
  UUID[], UUID[], UUID[], UUID[], TEXT[], TEXT[], TEXT[], UUID[], UUID[],
  TEXT[], TEXT[], NUMERIC[], NUMERIC[], TEXT[], TEXT[], TEXT[],
  TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ
) TO stg_data;
GRANT EXECUTE ON FUNCTION public.acquire_leased_holds(
  TEXT, UUID[], UUID[], BOOLEAN,
  UUID[], UUID[], UUID[], UUID[], UUID[], NUMERIC[], NUMERIC[],
  TEXT[], TEXT[], TEXT[], NUMERIC[], TEXT[], UUID[], UUID[], TIMESTAMPTZ[]
) TO stg_data;

GRANT SELECT ON public.billing_reconciliation_receipts, public.billing_reconciliation_budgets,
  public.billing_reconciliation_policies, public.billing_reconciliation_waivers TO stg_control, stg_worker;
GRANT INSERT ON public.billing_reconciliation_receipts, public.billing_reconciliation_budgets,
  public.billing_reconciliation_policies TO stg_control;
GRANT INSERT ON public.billing_reconciliation_receipts TO stg_worker;
GRANT SELECT, INSERT ON public.billing_reconciliation_polls TO stg_worker;
GRANT UPDATE (next_at, poll_count) ON public.billing_reconciliation_polls TO stg_worker;
GRANT UPDATE (state, result, reviewed_at) ON public.billing_reconciliation_receipts TO stg_control, stg_worker;
GRANT UPDATE (customer_usage, customer_actor, review_history) ON public.billing_reconciliation_receipts TO stg_control;
GRANT UPDATE (limit_atoms) ON public.billing_reconciliation_budgets TO stg_control;
GRANT UPDATE (enabled, include_historical, eligible_since, age_seconds, max_customer_atoms, limit_atoms)
  ON public.billing_reconciliation_policies TO stg_control;
GRANT INSERT ON public.billing_reconciliation_waivers TO stg_worker;
GRANT UPDATE (used_atoms) ON public.billing_reconciliation_budgets, public.billing_reconciliation_policies TO stg_worker;
GRANT SELECT ON public.price_versions, public.pricing_modifiers, public.fx_rate_versions,
  public.api_keys, public.upstream_accounts, public.config_epoch TO stg_worker;
GRANT UPDATE (state, revision, updated_at) ON public.upstream_accounts TO stg_worker;
GRANT UPDATE (version) ON public.config_epoch TO stg_worker;
GRANT UPDATE (supplier_holds, closed_at) ON public.billing_holds TO stg_worker;
GRANT UPDATE (reconciliation_started_at) ON public.billing_holds TO stg_data, stg_worker;
GRANT UPDATE (spend_window_start, spent_atoms, reserved_atoms) ON public.api_keys TO stg_worker;
GRANT INSERT ON public.billing_charges TO stg_worker;
GRANT INSERT (event_id, kind, request_id, billing_account_id, payload) ON public.outbox_events TO stg_worker;
GRANT USAGE ON SEQUENCE public.outbox_events_seq_seq TO stg_worker;
COMMIT;
