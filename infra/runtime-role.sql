-- Execute as the schema owner. Gateway and worker use different LOGIN credentials.
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='gateway_runtime') THEN CREATE ROLE gateway_runtime NOLOGIN;END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='gateway_worker') THEN CREATE ROLE gateway_worker NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO gateway_runtime,gateway_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gateway_runtime,gateway_worker;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO gateway_runtime,gateway_worker;
GRANT INSERT,UPDATE ON projects,api_keys,providers,models,routes,organizations,principals,event_consumers,event_deliveries,resources TO gateway_runtime;
GRANT INSERT ON cost_versions,billing_prices,billing_tariffs,billing_resolutions,audit_events TO gateway_runtime;
GRANT UPDATE ON billing_tariffs TO gateway_runtime;
GRANT INSERT ON attempts TO gateway_runtime;
GRANT UPDATE ON gateway_requests TO gateway_runtime;
GRANT UPDATE(policy,manual_frozen,freeze_reason,evidence_disputed) ON wallets TO gateway_runtime;
GRANT UPDATE(next_reconcile_at,reconcile_until) ON billing_holds TO gateway_runtime;
GRANT EXECUTE ON FUNCTION credit_wallet(uuid,text,text,numeric,text,integer,timestamptz,text[],text[]),admit_gateway_batch(jsonb),refund_billing_charge(uuid,numeric,text,text),adjust_billing_charge(uuid,numeric,text,text) TO gateway_runtime;
-- Only a worker can persist queued usage and execute automatic financial settlement.
GRANT EXECUTE ON FUNCTION persist_usage_batch(jsonb),settle_requests(uuid[],uuid) TO gateway_worker;
GRANT UPDATE(reconcile_token,reconcile_until) ON billing_holds TO gateway_worker;
GRANT INSERT,UPDATE ON resources TO gateway_worker;
-- Revoke helper entrypoints for every untrusted caller, including the HTTP runtime.
REVOKE ALL ON FUNCTION record_usage_evidence(uuid,uuid,text,text,text,jsonb,numeric,boolean,text,text),commit_usage_evidence(uuid,uuid,text,text,text,jsonb,numeric,boolean,text,text,text),admit_initial_request(jsonb),collect_wallet_debt(uuid,integer),expire_wallet(uuid) FROM PUBLIC,gateway_runtime,gateway_worker;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON usage_events,usage_conflicts,wallet_disputes,wallet_lots,money_journal,billing_charges,charge_allocations,hold_allocations,funding_operations,billing_refunds,billing_adjustments FROM gateway_runtime,gateway_worker;

GRANT EXECUTE ON FUNCTION gateway_token_summary(uuid) TO gateway_runtime,gateway_worker;

GRANT EXECUTE ON FUNCTION exact_billing_price(jsonb,jsonb) TO gateway_runtime;
