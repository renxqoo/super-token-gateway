CREATE FUNCTION public.internal_identity_notify_v2() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM pg_notify('stg_identity','');
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION public.internal_identity_notify_v2() FROM PUBLIC;
ALTER FUNCTION public.internal_identity_notify_v2() OWNER TO core_identity_owner;
CREATE TRIGGER identity_instance_notify_v2 AFTER INSERT OR UPDATE ON public.internal_service_instances FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();
CREATE TRIGGER identity_certificate_notify_v2 AFTER INSERT OR UPDATE ON public.internal_service_certificates FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();
CREATE TRIGGER identity_session_notify_v2 AFTER INSERT OR UPDATE ON public.internal_startup_sessions FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();
