CREATE OR REPLACE FUNCTION public.core_preflight_v1(p jsonb,context_id uuid,attempt_id uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 ctx public.execution_contexts%ROWTYPE;
 permit public.execution_permits%ROWTYPE;
 leases jsonb;
 requested_request uuid;
 execution_closed boolean;
BEGIN
 PERFORM public.core_caller_v1(p);
 IF attempt_id IS NULL THEN
  EXECUTE 'SELECT * FROM public.execution_contexts WHERE id=$1' INTO ctx USING context_id;
  IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
  IF p->>'command_type'<>'execution_quota_v1' THEN PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
  RETURN ctx.snapshot||jsonb_build_object('leases','[]'::jsonb,'permit_state',NULL,'permit_epoch',NULL,'execution_closed',NULL);
 END IF;

 EXECUTE 'SELECT attempt_id,context_id,owner_session_id,incarnation_id,owner_key_sha256,gateway_instance_id,owner_service,certificate_sha256,permit_epoch,state FROM public.execution_permits WHERE attempt_id=$1'
  INTO permit.attempt_id,permit.context_id,permit.owner_session_id,permit.incarnation_id,permit.owner_key_sha256,permit.gateway_instance_id,permit.owner_service,permit.certificate_sha256,permit.permit_epoch,permit.state
  USING attempt_id;
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid
  OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid
  OR permit.owner_key_sha256 IS DISTINCT FROM p->>'key_sha256'
  OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id'
  OR permit.owner_service IS DISTINCT FROM p->>'service'
  OR permit.certificate_sha256 IS DISTINCT FROM p->>'certificate_sha256' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 EXECUTE 'SELECT owner_session_id,key_id,ip,operation,model FROM public.execution_contexts WHERE id=$1'
  INTO ctx.owner_session_id,ctx.key_id,ctx.ip,ctx.operation,ctx.model USING permit.context_id;
 IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF p->>'command_type'<>'execution_quota_v1' THEN PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
 EXECUTE 'SELECT request_id FROM public.attempts WHERE id=$1' INTO requested_request USING attempt_id;
 EXECUTE 'SELECT coalesce(jsonb_agg(jsonb_build_object(''state'',l.state,''attempt_id'',l.attempt_id,''permit_epoch'',ep.permit_epoch::text,''descriptor'',l.descriptor) ORDER BY l.lease_id),''[]''::jsonb)
  FROM public.execution_quota_leases l JOIN LATERAL(SELECT permit_epoch FROM public.execution_permits WHERE attempt_id=l.attempt_id OFFSET 0) ep ON true WHERE l.request_id=$1' INTO leases USING requested_request;
 EXECUTE 'SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=$1' INTO execution_closed USING requested_request;
 RETURN jsonb_build_object('leases',leases,'permit_state',permit.state,'permit_epoch',permit.permit_epoch::text,'execution_closed',execution_closed);
END $$;

CREATE OR REPLACE FUNCTION core_funding.customer_authorities_v1(items jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 before_keys public.api_keys[];
 before_projects public.projects[];
 ks public.api_keys[];
 ps public.projects[];
 os public.organizations[];
 us public.principals[];
 result jsonb;
BEGIN
 IF jsonb_array_length(items)<2 THEN RETURN '{"customers":{},"catalog":{},"memo":false}'::jsonb;END IF;
 WITH inputs AS MATERIALIZED (
  SELECT value->'projection'->>'command_type' kind,value->'data' data FROM jsonb_array_elements(items)
 ), attempt_keys AS (
  SELECT DISTINCT (data->>'attempt_id')::uuid id FROM inputs WHERE kind IN('execution_begin_v1','execution_abort_v1')
 ), contexts AS (
  SELECT (data->>'context_id')::uuid id FROM inputs WHERE kind IN('execution_admit_v1','execution_next_v1')
  UNION
  SELECT ep.context_id FROM attempt_keys t JOIN LATERAL (
   SELECT context_id FROM public.execution_permits WHERE attempt_id=t.id OFFSET 0
  ) ep ON true
 ), keys AS (
  SELECT DISTINCT c.key_id FROM contexts x JOIN LATERAL (
   SELECT key_id FROM public.execution_contexts WHERE id=x.id OFFSET 0
  ) c ON true
 )
 SELECT coalesce(array_agg(k.row ORDER BY x.key_id),'{}'::public.api_keys[]) INTO before_keys
 FROM keys x JOIN LATERAL (SELECT a AS row FROM public.api_keys a WHERE a.id=x.key_id OFFSET 0) k ON true;

 IF cardinality(before_keys)=0 THEN RETURN '{"customers":{},"catalog":{}}'::jsonb;END IF;
 SELECT coalesce(array_agg(p ORDER BY p.id),'{}'::public.projects[]) INTO before_projects FROM public.projects p WHERE p.id=ANY(ARRAY(SELECT DISTINCT k.project_id FROM unnest(before_keys) k));
 SELECT coalesce(array_agg(x.r),'{}'::public.organizations[]) INTO os FROM(SELECT o r FROM public.organizations o WHERE o.id=ANY(ARRAY(SELECT DISTINCT p.organization_id FROM unnest(before_projects) p)) ORDER BY o.id FOR SHARE OF o NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.principals[]) INTO us FROM(SELECT u r FROM public.principals u WHERE u.id=ANY(ARRAY(SELECT DISTINCT k.principal_id FROM unnest(before_keys) k)) ORDER BY u.id FOR SHARE OF u NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.projects[]) INTO ps FROM(SELECT p r FROM public.projects p WHERE p.id=ANY(ARRAY(SELECT DISTINCT k.project_id FROM unnest(before_keys) k)) ORDER BY p.id FOR SHARE OF p NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.api_keys[]) INTO ks FROM(SELECT k r FROM public.api_keys k WHERE k.id=ANY(ARRAY(SELECT b.id FROM unnest(before_keys) b)) ORDER BY k.id FOR SHARE OF k NOWAIT)x;
 IF EXISTS(SELECT 1 FROM unnest(ks) k JOIN unnest(before_keys) b ON b.id=k.id WHERE (k.project_id,k.principal_id) IS DISTINCT FROM (b.project_id,b.principal_id))
  OR EXISTS(SELECT 1 FROM unnest(ps) p JOIN unnest(before_projects) b ON b.id=p.id WHERE p.organization_id IS DISTINCT FROM b.organization_id) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 SELECT jsonb_build_object('customers',coalesce(jsonb_object_agg(k.id::text,jsonb_build_object('key',to_jsonb(k),'project',to_jsonb(p),'organization',to_jsonb(o),'principal',to_jsonb(u))),'{}'::jsonb),'catalog','{}'::jsonb)
 INTO result FROM unnest(ks) k LEFT JOIN unnest(ps) p ON p.id=k.project_id LEFT JOIN unnest(os) o ON o.id=p.organization_id LEFT JOIN unnest(us) u ON u.id=k.principal_id;
 IF pg_column_size(result)>8388608 THEN RETURN '{"customers":{},"catalog":{}}'::jsonb;END IF;
 RETURN result;
END $$;
