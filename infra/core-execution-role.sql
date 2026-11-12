DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_execution_api') THEN CREATE ROLE core_execution_api NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_context_v1(jsonb,uuid,jsonb),public.core_preflight_v1(jsonb,uuid,uuid),public.core_quota_v1(jsonb,uuid,text,jsonb) TO core_execution_api;
REVOKE ALL ON execution_contexts,execution_commands,execution_permits,execution_quota_leases,upstream_holds,upstream_account_balances,channels,api_keys,wallets FROM core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_lookup_v1(jsonb,jsonb) TO core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_quota_close_v1(jsonb,jsonb,jsonb) TO core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_execution_batch_v1(jsonb) TO core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_quota_recovery_v1(jsonb,jsonb) TO core_execution_api;
GRANT EXECUTE ON FUNCTION public.core_abort_recovery_v1(jsonb,jsonb,jsonb) TO core_execution_api;
