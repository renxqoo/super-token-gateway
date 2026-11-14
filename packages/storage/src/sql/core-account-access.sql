CREATE FUNCTION public.core_account_auth_v1(proof text,ip text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;pr public.projects%ROWTYPE;o public.organizations%ROWTYPE;u public.principals%ROWTYPE;BEGIN
 IF proof IS NULL OR proof='' THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 SELECT * INTO k FROM public.api_keys WHERE verifier=proof;
 IF k.id IS NULL THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 SELECT * INTO pr FROM public.projects WHERE id=k.project_id;
 IF pr.organization_id IS NOT NULL THEN SELECT * INTO o FROM public.organizations WHERE id=pr.organization_id;END IF;
 IF k.principal_id IS NOT NULL THEN SELECT * INTO u FROM public.principals WHERE id=k.principal_id;END IF;
 IF (k.enabled AND pr.enabled AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp()) AND (pr.organization_id IS NULL OR o.enabled) AND (k.principal_id IS NULL OR u.enabled AND u.organization_id=pr.organization_id AND u.role=ANY(CASE k.role WHEN 'ADMIN' THEN ARRAY['OWNER','ADMIN'] WHEN 'INVOKER' THEN ARRAY['OWNER','ADMIN','MEMBER'] WHEN 'BILLING' THEN ARRAY['OWNER','ADMIN','BILLING'] ELSE ARRAY['OWNER','ADMIN','MEMBER','BILLING','VIEWER'] END))) IS NOT TRUE THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 IF cardinality(k.allowed_cidrs)>0 AND (ip IS NULL OR NOT EXISTS(SELECT 1 FROM unnest(k.allowed_cidrs) c WHERE ip::inet <<= c::inet)) THEN RETURN jsonb_build_object('status','ip_denied');END IF;
 RETURN jsonb_build_object('status','authenticated','project_id',pr.id,'key_id',k.id,'role',k.role,'models',to_jsonb(k.models),'operations',to_jsonb(k.operations));
END $$;
REVOKE ALL ON FUNCTION public.core_account_auth_v1(text,text) FROM PUBLIC;
