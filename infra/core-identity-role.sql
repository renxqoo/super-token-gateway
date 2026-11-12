DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_identity_api') THEN CREATE ROLE core_identity_api NOLOGIN;END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_identity_verifier') THEN CREATE ROLE core_identity_verifier NOLOGIN;END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_identity_deployer') THEN CREATE ROLE core_identity_deployer NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_identity_api,core_identity_verifier,core_identity_deployer;
GRANT EXECUTE ON FUNCTION public.internal_identity_read_peer_v1(text),public.internal_identity_read_session_v1(uuid),public.internal_identity_read_registration_v1(uuid),public.internal_identity_challenge_v1(jsonb,uuid,bytea,bytea,text,text,bytea,integer),public.internal_identity_register_v1(jsonb,uuid,uuid,text,integer),public.internal_identity_current_v1(jsonb) TO core_identity_api;
GRANT EXECUTE ON FUNCTION public.internal_identity_read_peer_v1(text),public.internal_identity_read_session_v1(uuid) TO core_identity_verifier;
GRANT EXECUTE ON FUNCTION public.internal_identity_provision_v1(text,text,text,text,text,timestamptz,timestamptz),public.internal_identity_change_v1(text,text,text,uuid,text) TO core_identity_deployer;
REVOKE ALL ON internal_service_instances,internal_service_certificates,internal_startup_registrations,internal_startup_sessions FROM core_identity_api,core_identity_verifier,core_identity_deployer;
