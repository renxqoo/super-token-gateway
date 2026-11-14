CREATE OR REPLACE FUNCTION public.core_platform_authorize_v1(proof jsonb,permission text) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE c public.administrator_credentials%ROWTYPE;a public.technical_administrators%ROWTYPE;BEGIN
 IF permission IS NULL OR permission NOT IN ('catalog:manage','funding:read','funding:recharge','customer:fund','customer:read','customer:adjust','funding:adjust') OR jsonb_typeof(proof) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 IF proof=jsonb_build_object('identity','bootstrap:ADMIN_KEY','bootstrap',true) THEN RETURN 'bootstrap:ADMIN_KEY';END IF;
 IF proof-ARRAY['identity','credentialId','administratorId','verifier']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['identity','credentialId','administratorId','verifier'])f WHERE jsonb_typeof(proof->f) IS DISTINCT FROM 'string' OR proof->>f='') THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.administrator_credentials WHERE id=(proof->>'credentialId')::uuid FOR SHARE;
 SELECT * INTO a FROM public.technical_administrators WHERE id=(proof->>'administratorId')::uuid FOR SHARE;
 IF c.id IS NULL OR a.id IS NULL OR c.administrator_id IS DISTINCT FROM a.id OR proof->>'identity' IS DISTINCT FROM 'technical-admin:'||a.id::text OR c.verifier IS DISTINCT FROM proof->>'verifier' OR (c.enabled AND a.enabled AND permission=ANY(a.permissions) AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())) IS NOT TRUE THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 RETURN proof->>'identity';
END $$;
REVOKE ALL ON FUNCTION public.core_platform_authorize_v1(jsonb,text) FROM PUBLIC;
