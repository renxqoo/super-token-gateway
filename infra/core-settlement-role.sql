DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_settlement_worker') THEN CREATE ROLE core_settlement_worker NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_settlement_worker;
GRANT SELECT(version) ON schema_migrations TO core_settlement_worker;
GRANT SELECT(session_id,public_key_spki,public_key_sha256) ON internal_startup_sessions TO core_settlement_worker;
GRANT EXECUTE ON FUNCTION public.persist_core_evidence_groups_v1(jsonb) TO core_settlement_worker;
