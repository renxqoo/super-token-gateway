CREATE FUNCTION public.core_plugin_package_v1(p jsonb,package_digest text) RETURNS bytea LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE result bytea;BEGIN
 IF p->>'service' IS DISTINCT FROM 'gateway' OR p->>'command_type' IS DISTINCT FROM 'plugin_package_v1' OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/plugin-package' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_current_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'));
 IF package_digest IS NULL OR package_digest !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT k.package_bytes INTO result FROM public.plugin_packages k WHERE k.digest=package_digest AND EXISTS(SELECT 1 FROM public.plugin_versions v WHERE v.package_digest=k.digest AND v.state='PUBLISHED' AND NOT v.quarantined);
 IF result IS NULL THEN RAISE EXCEPTION 'plugin_package_unavailable' USING ERRCODE='P0404';END IF;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION public.core_plugin_package_v1(jsonb,text) FROM PUBLIC;
