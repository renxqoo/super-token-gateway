DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_settlement_worker') THEN CREATE ROLE core_settlement_worker NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_settlement_worker;
GRANT SELECT(version) ON schema_migrations TO core_settlement_worker;
GRANT SELECT(session_id,public_key_spki,public_key_sha256) ON internal_startup_sessions TO core_settlement_worker;
GRANT EXECUTE ON FUNCTION public.persist_core_evidence_groups_v1(jsonb) TO core_settlement_worker;
GRANT EXECUTE ON FUNCTION public.core_reconcile_holds_v1(integer) TO core_settlement_worker;
GRANT SELECT(attempt_id,state,granted_at,expires_at) ON execution_permits TO core_settlement_worker;
