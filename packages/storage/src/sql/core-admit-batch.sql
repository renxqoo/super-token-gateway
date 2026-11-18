CREATE FUNCTION core_funding.reserve_batch_v1(account uuid,cur text,attempt uuid,amount numeric,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE c core_funding.balance_cells%ROWTYPE;entry jsonb;k text;preferred integer=get_byte(uuid_send(attempt),15)%16;BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(pending->'cells') IS DISTINCT FROM 'object' OR amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT state.* INTO c FROM core_funding.balance_cells state
 WHERE state.account_id=account AND state.currency=cur
  AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
  AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit
 ORDER BY (state.cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE OF state SKIP LOCKED;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM core_funding.balance_cells state
   WHERE state.account_id=account AND state.currency=cur
    AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
    AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
  RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';
 END IF;
 k=account::text||':'||cur||':'||c.cell::text;entry=pending->'cells'->k;
 IF entry IS NULL THEN entry=jsonb_build_object('base',to_jsonb(c),'next',to_jsonb(c));ELSE c=jsonb_populate_record(NULL::core_funding.balance_cells,entry->'next');END IF;
 pending=jsonb_set(pending,ARRAY['cells',k],jsonb_build_object('base',entry->'base','next',(entry->'next')||jsonb_build_object('reserved_atoms',c.reserved_atoms+amount,'revision',c.revision+1)));
 RETURN jsonb_build_object('cell',c.cell,'pending',pending);
END $$;

CREATE FUNCTION core_funding.flush_admit_v1(pending jsonb) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE inputs jsonb;results jsonb;allowed jsonb;updated integer;expected integer;BEGIN
 IF pending->'admissions'='{}'::jsonb THEN RETURN;END IF;
 SELECT jsonb_agg(a.value->'customer' ORDER BY (a.value->>'sequence')::integer),count(*) INTO inputs,expected FROM jsonb_each(pending->'admissions') a;
 results=public.allocate_customer_batch_v1(inputs);
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(inputs) WITH ORDINALITY i(value,n) WHERE results->((n-1)::text)->>'created' IS DISTINCT FROM 'true' OR results->((n-1)::text)->>'attempt_id' IS DISTINCT FROM i.value->>'attempt_id') THEN RAISE EXCEPTION 'admit_batch_allocation' USING ERRCODE='P0B01';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'admissions') a
  JOIN public.execution_contexts c ON c.id=(a.value->>'context_id')::uuid
  LEFT JOIN public.api_keys k ON k.id=c.key_id LEFT JOIN public.projects p ON p.id=k.project_id LEFT JOIN public.organizations o ON o.id=p.organization_id LEFT JOIN public.principals u ON u.id=k.principal_id
  WHERE core_funding.auth_decision_v1(k,p,o,u,c.ip,c.operation,c.model,clock_timestamp()) IS DISTINCT FROM a.value->'authorization'
   OR c.expires_at<=clock_timestamp() OR c.owner_session_id IS DISTINCT FROM (a.value->>'owner_session_id')::uuid
   OR (a.value->>'command_deadline')::timestamptz<=clock_timestamp()
   OR (a.value->'authority_times'->>'session_expires_at')::timestamptz<=clock_timestamp()
   OR (a.value->'authority_times'->>'certificate_not_before')::timestamptz>clock_timestamp()
   OR (a.value->'authority_times'->>'certificate_expires_at')::timestamptz<=clock_timestamp()) THEN RAISE EXCEPTION 'admit_batch_recheck' USING ERRCODE='P0B01';END IF;
 allowed=core_execution_runtime.allowed_v1((SELECT jsonb_agg(DISTINCT jsonb_build_object('ref',jsonb_build_object('package_digest',a.value->>'package_digest'))) FROM jsonb_each(pending->'admissions') a));
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'admissions') a WHERE (allowed->(a.value->>'package_digest') @> jsonb_build_array(a.value->'execution')) IS DISTINCT FROM true) THEN RAISE EXCEPTION 'admit_batch_recheck' USING ERRCODE='P0B01';END IF;
 UPDATE public.gateway_requests q SET state='ADMITTED',execution_budget=a.value->'execution_budget' FROM jsonb_each(pending->'admissions') a WHERE q.id=(a.value->'customer'->>'id')::uuid;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>expected THEN RAISE EXCEPTION 'admit_batch_request_missing' USING ERRCODE='P0B01';END IF;
 UPDATE public.attempts at SET binding_id=(a.value->>'binding_id')::uuid FROM jsonb_each(pending->'admissions') a WHERE at.id=a.key::uuid AND at.request_id=(a.value->'customer'->>'id')::uuid;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>expected THEN RAISE EXCEPTION 'admit_batch_attempt_missing' USING ERRCODE='P0B01';END IF;
 INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt)
  SELECT id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt FROM jsonb_to_recordset(pending->'commands') x(id uuid,command_id uuid,kind text,caller_identity text,owner_session_id uuid,caller_incarnation uuid,body_digest text,deadline_at timestamptz,trace_id text,contract_version integer,state text,request_id uuid,attempt_id uuid,context_id uuid,receipt jsonb);
 INSERT INTO public.execution_permits(attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest)
  SELECT ep.attempt_id,ep.command_id,ep.gateway_instance_id,ep.owner_session_id,ep.owner_key_sha256,ep.owner_service,ep.certificate_sha256,ep.incarnation_id,ep.permit_epoch,ep.binding_id,ep.dispatched_digest,ep.state,ep.expires_at,ep.route_id,ep.context_id,ep.snapshot,ep.manifest,ep.manifest_digest,ep.candidate_digest,ep.authorization_plan_digest,ep.prep_session_id,ep.prep_binding_digest
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_populate_record(NULL::public.execution_permits,a.value->'permit') ep;
 INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version,funding_cell,current_reserved_atoms)
  SELECT h.attempt_id,h.account_id,h.channel_id,h.currency,h.estimated_atoms,h.cost_version_id,h.state,h.estimated_quantities,h.estimated_quantities_digest,h.estimate_manifest_digest,h.estimate_algorithm_version,h.funding_cell,h.current_reserved_atoms
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_populate_record(NULL::public.upstream_holds,a.value->'hold') h;
 PERFORM core_funding.flush_v1(pending->'funding');
 INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,descriptor)
  SELECT l.lease_id,l.attempt_id,l.command_id,l.scope_kind,l.scope_id,l.request_id,l.state,l.descriptor
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_to_recordset(a.value->'leases') l(lease_id text,attempt_id uuid,command_id uuid,scope_kind text,scope_id text,request_id uuid,state text,descriptor jsonb);
 INSERT INTO public.audit_events(id,action,resource_id,actor,details)
  SELECT id,action,resource_id,actor,details FROM jsonb_to_recordset(pending->'audits') x(id uuid,action text,resource_id text,actor text,details jsonb);
END $$;

CREATE OR REPLACE FUNCTION core_funding.run_execution_v1(items jsonb,time_bounds jsonb[],group_indices integer[],item_errors jsonb[],group_errors jsonb[],bulk boolean,proof_bounds numeric[]) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE item jsonb;computed jsonb;applied jsonb;result jsonb;results jsonb='[]';cache jsonb;pending jsonb;group_index integer;item_index integer=0;failure text;message text;key text;attempt_key text;request_key text;intent_key text;kind text;BEGIN
 BEGIN
  BEGIN
   cache=core_funding.customer_authorities_v1((SELECT coalesce(jsonb_agg(value),'[]') FROM jsonb_array_elements(items) WITH ORDINALITY i(value,n) WHERE item_errors[n] IS NULL AND group_errors[group_indices[n]] IS NULL));
  EXCEPTION WHEN OTHERS THEN cache='{"customers":{},"catalog":{}}'::jsonb;END;
  pending=CASE WHEN bulk THEN '{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb END;
  FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
   item_index=item_index+1;group_index=group_indices[item_index];
   key=NULL;attempt_key=NULL;request_key=NULL;intent_key=NULL;kind=item->'projection'->>'command_type';
   BEGIN
    key='command:'||(item->'projection'->>'service')||':'||(item->'projection'->>'instance_id')||':'||(item->'projection'->>'command_type')||':'||((item->'envelope'->>'command_id')::uuid)::text;
    IF kind='execution_begin_v1' THEN attempt_key=((item->'data'->>'attempt_id')::uuid)::text;
    ELSIF kind='execution_admit_v1' THEN
     attempt_key=((item->'data'->'dispatch_projection'->>'proposed_attempt_id')::uuid)::text;
     request_key='request:'||((item->'data'->'dispatch_projection'->>'proposed_request_id')::uuid)::text;
     IF item->'data'->>'intent_digest' IS NOT NULL THEN intent_key='intent:'||((item->'data'->>'project_id')::uuid)::text||':'||(item->'data'->>'intent_digest');END IF;
    END IF;
   EXCEPTION WHEN invalid_text_representation THEN key=NULL;attempt_key=NULL;request_key=NULL;intent_key=NULL;END;
   IF bulk AND pending->'grants'<>'{}'::jsonb AND (item->'projection'->>'command_type' IS DISTINCT FROM 'execution_begin_v1' OR attempt_key IS NULL OR key IS NULL OR pending->'grants'?attempt_key OR pending->'keys'?key) THEN
    PERFORM core_funding.flush_begin_v1(pending);pending='{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb;
   END IF;
   IF bulk AND pending->'admissions'<>'{}'::jsonb AND (kind IS DISTINCT FROM 'execution_admit_v1' OR attempt_key IS NULL OR request_key IS NULL OR key IS NULL OR pending->'admissions'?attempt_key OR pending->'keys'?request_key OR pending->'keys'?key OR coalesce(pending->'keys'?intent_key,false)) THEN
    PERFORM core_funding.flush_admit_v1(pending);pending='{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb;
   END IF;
   result=coalesce(item_errors[item_index],group_errors[group_index]);
   IF result IS NULL THEN
    BEGIN
     computed=item->'computed';
     IF item->'projection'->>'command_type'='execution_begin_v1' THEN computed=computed||jsonb_build_object('proof_remaining_ms',coalesce(floor(proof_bounds[item_index]-extract(epoch FROM clock_timestamp())*1000),-1),'proof_deadline_ms',proof_bounds[item_index]);END IF;
     applied=core_funding.execution_item_v1(item->'projection',item->'envelope',item->'data',computed,time_bounds[group_index],cache,pending);
     result=applied->'result';cache=applied->'cache';
     IF bulk THEN pending=applied->'pending';END IF;
    EXCEPTION WHEN OTHERS THEN
     GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
     result=jsonb_build_object('error',CASE WHEN failure='P0F01' THEN 'funding_slow' WHEN failure IN ('55P03','P0503','40P01') THEN 'identity_busy' WHEN message IN ('command_conflict','customer_forbidden','internal_identity_revoked','internal_session_invalid','internal_peer_forbidden','internal_owner_mismatch','context_invalid','internal_deadline') THEN message ELSE 'identity_store_unavailable' END);
    END;
   END IF;
   results=results||jsonb_build_array(result);
  END LOOP;
  IF bulk THEN PERFORM core_funding.flush_begin_v1(pending);PERFORM core_funding.flush_admit_v1(pending);END IF;
 EXCEPTION WHEN OTHERS THEN
  IF NOT bulk THEN RAISE;END IF;
  RETURN core_funding.run_execution_v1(items,time_bounds,group_indices,item_errors,group_errors,false,proof_bounds);
 END;
 RETURN results;
END $$;


CREATE OR REPLACE FUNCTION core_funding.execution_item_v1(p jsonb,e jsonb,d jsonb,computed jsonb,authority_times jsonb,cache jsonb,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
<<execution>>
DECLARE ctx public.execution_contexts%ROWTYPE;cmd public.execution_commands%ROWTYPE;q public.gateway_requests%ROWTYPE;permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;h public.upstream_holds%ROWTYPE;funding_cell smallint;
 kind text=p->>'command_type';caller text=p->>'service'||':'||(p->>'instance_id');keys text[];k text;auth jsonb;candidates jsonb;c jsonb;projection jsonb;ref jsonb;manifest jsonb;quote jsonb;estimated jsonb='{}';dim text;bound numeric;qty numeric;amount numeric;response jsonb;result jsonb;plan jsonb;command_pk uuid=gen_random_uuid();qid uuid;aid uuid;ordinal integer=1;account uuid;wallet uuid;descriptor jsonb;failure text;rejection text;fresh boolean=false;started timestamptz=clock_timestamp();rebuilt jsonb;
 saved_cache jsonb=cache;saved_pending jsonb=pending;customer jsonb;catalog jsonb;allowed jsonb;allocation jsonb;funding jsonb;admission jsonb;journal uuid;
BEGIN
 IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR e-ARRAY['command_id','body_digest','caller_incarnation','deadline_at','trace_id','contract_version']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['command_id','body_digest','caller_incarnation','deadline_at','trace_id'])f WHERE jsonb_typeof(e->f) IS DISTINCT FROM 'string') OR jsonb_typeof(e->'contract_version') IS DISTINCT FROM 'number' OR e->>'contract_version' IS DISTINCT FROM '1' OR (e->>'command_id',e->>'caller_incarnation',e->>'deadline_at',e->>'trace_id') IS DISTINCT FROM (p->>'command_id',p->>'incarnation_id',p->>'deadline_at',p->>'trace_id') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 IF public.core_digest_v1(jsonb_build_object('kind',kind,'contract_version',1,'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'deadline_at',p->>'deadline_at','data',d)) IS DISTINCT FROM e->>'body_digest' THEN RAISE EXCEPTION 'internal_proof_invalid' USING ERRCODE='P0401';END IF;
 keys=ARRAY['command:'||caller||':'||kind||':'||(e->>'command_id')];
 IF kind IN ('execution_admit_v1','execution_next_v1') THEN keys=keys||ARRAY['uuid-request:'||(d->'dispatch_projection'->>'proposed_request_id'),'uuid-attempt:'||(d->'dispatch_projection'->>'proposed_attempt_id')];END IF;
 IF kind='execution_admit_v1' AND d->>'intent_digest' IS NOT NULL THEN keys=array_append(keys,'intent:'||(d->>'project_id')||':'||(d->>'intent_digest'));END IF;
 FOR k IN SELECT value FROM unnest(keys) x(value) ORDER BY value COLLATE "C" LOOP IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.execution.v1:'||k,0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;END LOOP;
 PERFORM public.core_caller_projection_v2(p);
 IF jsonb_typeof(authority_times) IS DISTINCT FROM 'object' OR EXISTS(SELECT 1 FROM unnest(ARRAY['session_expires_at','certificate_not_before','certificate_expires_at']) f WHERE jsonb_typeof(authority_times->f) IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF (authority_times->>'certificate_not_before')::timestamptz>clock_timestamp() OR (authority_times->>'certificate_expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF (authority_times->>'session_expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF kind IN ('execution_admit_v1','execution_next_v1') THEN
  SELECT * INTO ctx FROM public.execution_contexts WHERE id=(d->>'context_id')::uuid;
 ELSE
  IF kind='execution_query_v1' THEN
   SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=d->>'kind' AND command_id=(d->>'command_id')::uuid;
   IF NOT FOUND THEN RETURN jsonb_build_object('result',jsonb_build_object('receipt',jsonb_build_object('command_state','NOT_FOUND')),'cache',cache,'pending',pending);END IF;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=cmd.attempt_id;
  ELSE SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid;END IF;
  SELECT * INTO ctx FROM public.execution_contexts WHERE id=CASE WHEN kind='execution_query_v1' THEN cmd.context_id ELSE permit.context_id END;
 END IF;
 IF ctx.id IS NULL THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF NOT (kind='execution_query_v1' AND cmd.kind='execution_abort_recovery_v1') THEN
  customer=cache->'customers'->ctx.key_id::text;
  IF customer IS NULL THEN auth=public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);
  ELSE auth=core_funding.auth_decision_v1(jsonb_populate_record(NULL::public.api_keys,NULLIF(customer->'key','null'::jsonb)),jsonb_populate_record(NULL::public.projects,NULLIF(customer->'project','null'::jsonb)),jsonb_populate_record(NULL::public.organizations,NULLIF(customer->'organization','null'::jsonb)),jsonb_populate_record(NULL::public.principals,NULLIF(customer->'principal','null'::jsonb)),ctx.ip,ctx.operation,ctx.model,clock_timestamp());END IF;
 END IF;
 IF kind='execution_query_v1' THEN
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',cmd.receipt||CASE WHEN permit.attempt_id IS NOT NULL THEN jsonb_build_object('state',permit.state) ELSE '{}'::jsonb END),'cache',cache,'pending',pending);
 END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=execution.kind AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',cmd.receipt),'cache',cache,'pending',pending);
 END IF;
 BEGIN
  IF kind IN ('execution_admit_v1','execution_next_v1') THEN
   IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR ctx.expires_at<=clock_timestamp() OR ctx.snapshot->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' OR ctx.snapshot->'scopes' IS DISTINCT FROM auth->'scopes' THEN RAISE EXCEPTION 'context_stale' USING ERRCODE='P0422';END IF;
   IF computed->>'rejection' IN ('candidate_stale','cost_unbounded','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request','cost_ceiling') THEN RAISE EXCEPTION '%',computed->>'rejection' USING ERRCODE='P0422';END IF;
   catalog=core_funding.catalog_v1(cache,ctx.model,ctx.operation,auth->>'customer_currency');cache=catalog->'cache';candidates=catalog->'frame'->'candidates';allowed=catalog->'frame'->'runtimes';
   projection=d->'dispatch_projection';ref=projection->'candidate_ref';manifest=d->'manifest';
   SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=ref;
    IF c IS NOT NULL AND allowed IS NULL THEN allowed=core_execution_runtime.allowed_v1(jsonb_build_array(c));END IF;
   IF projection->'version' IS DISTINCT FROM '2'::jsonb OR jsonb_typeof(projection) IS DISTINCT FROM 'object' OR projection-ARRAY['version','proposed_request_id','proposed_attempt_id','origin','method','operation','request_target_sha256','final_headers_digest','final_body_sha256','candidate_ref','auth','credential_ciphertext_sha256','billable_manifest_sha256','deadline_at','execution']<>'{}'::jsonb OR (SELECT count(*) FROM jsonb_object_keys(projection))<>15 OR jsonb_typeof(manifest) IS DISTINCT FROM 'object' OR manifest-ARRAY['version','operation','model','upstream_model','stream','input_bytes','ceilings','quantities','input_characters','images','resource_ids']<>'{}'::jsonb OR jsonb_typeof(manifest->'quantities') IS DISTINCT FROM 'object' OR jsonb_typeof(manifest->'resource_ids') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0422';END IF;
   IF c IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ctx.snapshot->'candidates') WHERE value=c) OR projection->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'model' IS DISTINCT FROM ctx.model OR manifest->>'upstream_model' IS DISTINCT FROM c->>'upstream_model' OR projection->>'origin' IS DISTINCT FROM c->>'origin' OR projection->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' OR projection->>'billable_manifest_sha256' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.billable-manifest','1',manifest)) OR d->>'dispatched_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.dispatch-projection','2',projection)) OR projection->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR (projection->'auth'->>'type') IS DISTINCT FROM c->'config'->>'auth' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF jsonb_typeof(projection->'execution') IS DISTINCT FROM 'object' OR (projection->'execution')-ARRAY['version','kind','artifact_digest']<>'{}'::jsonb OR projection->'execution'->'version' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(projection->'execution'->'kind') IS DISTINCT FROM 'string' OR projection->'execution'->>'kind' NOT IN ('wasm','builtin-worker') OR jsonb_typeof(projection->'execution'->'artifact_digest') IS DISTINCT FROM 'string' OR projection->'execution'->>'artifact_digest'!~'^[a-f0-9]{64}$' OR (ctx.snapshot->'execution_runtimes'->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true OR (allowed->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF d->>'prep_binding_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.prep-binding','2',jsonb_build_object('execution',projection->'execution','session',jsonb_build_object('id',d->>'prep_session_id','tenant',ctx.project_id,'attempt',projection->>'proposed_attempt_id','incarnation',p->>'incarnation_id','plugin',ref->>'package_digest','config',public.core_digest_v1(c->'config')),'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id'),'request_id',projection->>'proposed_request_id','operation',projection->>'operation','binding_id',ref->>'binding_id','config_revision',ref->>'config_revision','config_digest',ref->>'config_digest'))) THEN RAISE EXCEPTION 'prep_binding_invalid' USING ERRCODE='P0422';END IF;
   IF jsonb_typeof(c->'cost_rates') IS DISTINCT FROM 'object' OR c->'cost_rates'='{}'::jsonb THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
   FOR dim IN SELECT jsonb_object_keys(c->'cost_rates') LOOP
    IF jsonb_typeof(c->'cost_ceilings'->dim) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
    bound=(c->'cost_ceilings'->>dim)::numeric;qty=coalesce((manifest->'quantities'->>dim)::numeric,(manifest->'ceilings'->>dim)::numeric,bound);
    IF manifest->'quantities'?dim AND manifest->'ceilings'?dim AND (manifest->'quantities'->>dim)::numeric>(manifest->'ceilings'->>dim)::numeric THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
    IF qty<0 OR qty>bound*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) OR dim='requests' AND qty<1 THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
    estimated=estimated||jsonb_build_object(dim,qty::text);
   END LOOP;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'input_tokens' THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'output_tokens' OR (manifest->'ceilings'->>'output_tokens')::numeric>(SELECT max_output_tokens FROM public.models WHERE name=ctx.model)*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) THEN RAISE EXCEPTION 'output_bound' USING ERRCODE='P0422';END IF;
   IF c->'config'?'max_output_tokens' AND (manifest->'ceilings'->>'output_tokens')::numeric>(c->'config'->>'max_output_tokens')::numeric*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) OR c->'config'?'max_input_tokens' AND (manifest->'ceilings'->>'input_tokens')::numeric>(c->'config'->>'max_input_tokens')::numeric*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
   amount=core_funding.cost_v1((ref->>'cost_version_id')::uuid,estimated);
   IF computed->'estimated_quantities' IS DISTINCT FROM estimated OR computed->>'supplier_atoms' IS DISTINCT FROM amount::text THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
   quote=computed->'quote';
   IF quote->>'currency' IS DISTINCT FROM auth->>'customer_currency' OR NOT quote->'prices'? (ref->>'channel_id') OR quote->'prices'->>(ref->>'channel_id') IS DISTINCT FROM ref->>'customer_price_id' THEN RAISE EXCEPTION 'quote_invalid' USING ERRCODE='P0422';END IF;
   qid=(projection->>'proposed_request_id')::uuid;aid=(projection->>'proposed_attempt_id')::uuid;
   SELECT account_id INTO account FROM public.channels WHERE id=(ref->>'channel_id')::uuid;
   PERFORM core_funding.share_v1(ARRAY[account]);
   SELECT id INTO wallet FROM public.wallets WHERE project_id=ctx.project_id AND currency=auth->>'customer_currency' FOR NO KEY UPDATE NOWAIT;
   IF wallet IS NULL THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
    funding=core_funding.reserve_batch_v1(account,c->>'cost_currency',aid,amount,pending->'funding');funding_cell=(funding->>'cell')::smallint;pending=jsonb_set(pending,ARRAY['funding'],funding->'pending');
   ELSE funding_cell=core_funding.reserve_v1(account,c->>'cost_currency',aid,amount);END IF;
   IF kind='execution_admit_v1' THEN
    IF d->>'project_id' IS DISTINCT FROM ctx.project_id::text OR d->>'key_id' IS DISTINCT FROM ctx.key_id::text OR d->>'fingerprint' IS DISTINCT FROM ctx.fingerprint OR d->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0422';END IF;
    IF d->>'intent_digest' IS NOT NULL THEN
     SELECT * INTO q FROM public.gateway_requests WHERE project_id=ctx.project_id AND intent_digest=d->>'intent_digest';
     IF FOUND THEN qid=q.id;aid=NULL;RAISE EXCEPTION '%',CASE WHEN q.fingerprint=ctx.fingerprint THEN 'already_accepted' ELSE 'idempotency_conflict' END USING ERRCODE='P0409';END IF;
    END IF;
    IF EXISTS(SELECT 1 FROM public.gateway_requests WHERE id=qid) OR EXISTS(SELECT 1 FROM public.attempts WHERE id=aid) THEN RAISE EXCEPTION 'proposed_id_conflict' USING ERRCODE='P0409';END IF;
    allocation=jsonb_build_object('id',qid,'attempt_id',aid,'project_id',ctx.project_id,'key_id',ctx.key_id,'operation',ctx.operation,'model',ctx.model,'digest',d->>'intent_digest','fingerprint',ctx.fingerprint,'quote',quote,'route_id',ref->>'route_id','cost_pricing_id',ref->>'cost_version_id');
    IF pending IS NULL THEN
     result=public.allocate_customer_batch_v1(jsonb_build_array(allocation))->'0';
    IF result?'error' THEN
     IF result->>'error'='P0402' THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
     IF result->>'error'='55P03' THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
     RAISE EXCEPTION 'allocator_failed';
    END IF;
    IF (result->>'created')::boolean IS DISTINCT FROM true OR result->>'attempt_id' IS DISTINCT FROM aid::text THEN RAISE EXCEPTION 'allocator_failed';END IF;
    UPDATE public.gateway_requests SET state='ADMITTED',execution_budget=jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id') WHERE id=qid;
    UPDATE public.attempts SET binding_id=(ref->>'binding_id')::uuid WHERE id=aid;
    ELSE
     admission=jsonb_build_object('sequence',(SELECT count(*) FROM jsonb_each(pending->'admissions')),'customer',allocation,'binding_id',ref->>'binding_id','context_id',ctx.id,'owner_session_id',p->>'session_id','authorization',auth,'authority_times',authority_times,'command_deadline',p->>'deadline_at','package_digest',ref->>'package_digest','execution',projection->'execution','execution_budget',jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'));
    END IF;
   ELSE
    IF qid IS DISTINCT FROM (d->>'request_id')::uuid THEN RAISE EXCEPTION 'request_mismatch' USING ERRCODE='P0409';END IF;
    SELECT * INTO q FROM public.gateway_requests WHERE id=qid FOR UPDATE NOWAIT;
    PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
    PERFORM id FROM public.attempts WHERE request_id=qid ORDER BY attempts.ordinal,attempts.id FOR NO KEY UPDATE NOWAIT;
    PERFORM ep.attempt_id FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid ORDER BY ep.attempt_id FOR NO KEY UPDATE OF ep NOWAIT;
    PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=(d->>'previous_attempt_id')::uuid FOR NO KEY UPDATE NOWAIT;
    ordinal=coalesce((SELECT max(attempts.ordinal) FROM public.attempts WHERE request_id=qid),0)+1;
    IF q.execution_closed_at IS NOT NULL THEN RAISE EXCEPTION 'execution_closed' USING ERRCODE='P0409';END IF;
    IF (computed->>'token_count')::numeric+coalesce((SELECT sum((l.descriptor->>'tokens')::numeric) FROM public.execution_quota_leases l WHERE request_id=qid AND scope_kind='channel'),0)>(q.execution_budget->>'token_budget')::numeric THEN RAISE EXCEPTION 'retry_quota_exceeded' USING ERRCODE='P0422';END IF;
    IF q.fingerprint IS DISTINCT FROM ctx.fingerprint OR q.operation IS DISTINCT FROM ctx.operation OR q.model IS DISTINCT FROM ctx.model OR q.execution_budget->>'incarnation_id' IS DISTINCT FROM p->>'incarnation_id' OR manifest->'resource_ids' IS DISTINCT FROM (SELECT ep.manifest->'resource_ids' FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid AND at.ordinal=1) THEN RAISE EXCEPTION 'request_mismatch' USING ERRCODE='P0409';END IF;
    IF q.key_id IS DISTINCT FROM ctx.key_id OR q.project_id IS DISTINCT FROM ctx.project_id OR q.execution_budget->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR q.selected_attempt_id IS NOT NULL OR q.billing_outcome NOT IN ('PENDING','UNKNOWN') OR ordinal IS DISTINCT FROM (d->>'expected_ordinal')::integer OR ordinal>(q.execution_budget->>'max_attempts')::integer OR (q.execution_budget->>'deadline_at')::timestamptz< (p->>'deadline_at')::timestamptz OR NOT EXISTS(SELECT 1 FROM public.attempts WHERE id=(d->>'previous_attempt_id')::uuid AND request_id=qid AND attempts.ordinal=execution.ordinal-1) OR d->>'visible_output' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'retry_conflict' USING ERRCODE='P0409';END IF;
    IF (q.billing_quote->'prices'->>(ref->>'channel_id')) IS DISTINCT FROM ref->>'customer_price_id' OR (quote->>'amount_atoms')::numeric>(q.billing_quote->>'amount_atoms')::numeric THEN RAISE EXCEPTION 'retry_quote_exceeded' USING ERRCODE='P0422';END IF;
    IF EXISTS(SELECT 1 FROM public.attempts WHERE id=aid) THEN RAISE EXCEPTION 'proposed_id_conflict' USING ERRCODE='P0409';END IF;
    UPDATE public.execution_permits SET state='UNKNOWN' WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='SEND_GRANTED';
    UPDATE public.upstream_holds SET state='UNKNOWN' WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='HELD' AND EXISTS(SELECT 1 FROM public.execution_permits WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='UNKNOWN');
    INSERT INTO public.attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,binding_id,state) VALUES(aid,qid,ordinal,(ref->>'channel_id')::uuid,c->>'upstream_model',(ref->>'cost_version_id')::uuid,(ref->>'customer_price_id')::uuid,(ref->>'binding_id')::uuid,'DISPATCHING');
    quote=q.billing_quote;
   END IF;
   plan=jsonb_build_object('version',2,'owner',jsonb_build_object('service','gateway','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'attempt_id',aid,'permit_epoch','1','dispatched_digest',d->>'dispatched_digest','dispatch_projection',projection);
   response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'ordinal',ordinal,'state','RESERVED','permit_epoch','1','deadline_at',p->>'deadline_at','customer_atoms',quote->>'amount_atoms','customer_currency',quote->>'currency','supplier_atoms',amount::text,'supplier_currency',c->>'cost_currency','authorization_plan',plan,'authorization_plan_digest',public.core_digest_v1(jsonb_build_array('stg.internal.host-authorization-plan','2',plan)));
   response=response||jsonb_build_object('quota_lease_ids',(SELECT jsonb_agg(lease_id ORDER BY lease_id) FROM (SELECT value->>'lease_id' AS lease_id FROM jsonb_array_elements(computed->'leases') UNION SELECT lease_id FROM public.execution_quota_leases WHERE request_id=qid AND scope_kind<>'channel') leases));
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
    admission=admission||jsonb_build_object('permit',jsonb_build_object('attempt_id',aid,'command_id',command_pk,'gateway_instance_id',p->>'instance_id','owner_session_id',p->>'session_id','owner_key_sha256',p->>'key_sha256','owner_service','gateway','certificate_sha256',p->>'certificate_sha256','incarnation_id',p->>'incarnation_id','permit_epoch',1,'binding_id',ref->>'binding_id','dispatched_digest',d->>'dispatched_digest','state','RESERVED','expires_at',p->>'deadline_at','route_id',ref->>'route_id','context_id',ctx.id,'snapshot',plan,'manifest',manifest,'manifest_digest',projection->>'billable_manifest_sha256','candidate_digest',ref->>'candidate_digest','authorization_plan_digest',response->>'authorization_plan_digest','prep_session_id',d->>'prep_session_id','prep_binding_digest',d->>'prep_binding_digest'),'hold',jsonb_build_object('attempt_id',aid,'account_id',account,'channel_id',ref->>'channel_id','currency',c->>'cost_currency','estimated_atoms',amount,'cost_version_id',ref->>'cost_version_id','state','HELD','estimated_quantities',estimated,'estimated_quantities_digest',public.core_digest_v1(estimated),'estimate_manifest_digest',projection->>'billable_manifest_sha256','estimate_algorithm_version',1,'funding_cell',funding_cell,'current_reserved_atoms',amount),'leases','[]'::jsonb);
    pending=jsonb_set(pending,ARRAY['commands'],(pending->'commands')||jsonb_build_array(jsonb_build_object('id',command_pk,'command_id',e->>'command_id','kind',kind,'caller_identity',caller,'owner_session_id',p->>'session_id','caller_incarnation',p->>'incarnation_id','body_digest',e->>'body_digest','deadline_at',e->>'deadline_at','trace_id',e->>'trace_id','contract_version',1,'state','COMMITTED','request_id',qid,'attempt_id',aid,'context_id',ctx.id,'receipt',response)));
    pending=jsonb_set(pending,ARRAY['keys','command:'||caller||':'||kind||':'||((e->>'command_id')::uuid)::text],'true'::jsonb);
    pending=jsonb_set(pending,ARRAY['keys','request:'||qid::text],'true'::jsonb);
    IF d->>'intent_digest' IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['keys','intent:'||ctx.project_id::text||':'||(d->>'intent_digest')],'true'::jsonb);END IF;
    journal=gen_random_uuid();funding=pending->'funding';
    funding=jsonb_set(funding,ARRAY['journals'],(funding->'journals')||jsonb_build_array(jsonb_build_object('id',journal,'account_id',account,'channel_id',ref->>'channel_id','attempt_id',aid,'currency',c->>'cost_currency','kind','RESERVE','reference','reserve:'||aid)));
    funding=jsonb_set(funding,ARRAY['entries'],(funding->'entries')||jsonb_build_array(jsonb_build_object('journal_id',journal,'ordinal',0,'currency',c->>'cost_currency','account','upstream:available','atoms',-amount),jsonb_build_object('journal_id',journal,'ordinal',1,'currency',c->>'cost_currency','account','upstream:reserved','atoms',amount)));
    pending=jsonb_set(pending,ARRAY['funding'],funding);
   ELSE
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
   INSERT INTO public.execution_permits(attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest) VALUES(aid,command_pk,p->>'instance_id',(p->>'session_id')::uuid,p->>'key_sha256','gateway',p->>'certificate_sha256',(p->>'incarnation_id')::uuid,1,(ref->>'binding_id')::uuid,d->>'dispatched_digest','RESERVED',(p->>'deadline_at')::timestamptz,(ref->>'route_id')::uuid,ctx.id,plan,manifest,projection->>'billable_manifest_sha256',ref->>'candidate_digest',response->>'authorization_plan_digest',(d->>'prep_session_id')::uuid,d->>'prep_binding_digest');
   INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version,funding_cell,current_reserved_atoms) VALUES(aid,account,(ref->>'channel_id')::uuid,c->>'cost_currency',amount,(ref->>'cost_version_id')::uuid,'HELD',estimated,public.core_digest_v1(estimated),projection->>'billable_manifest_sha256',1,funding_cell,amount);
   PERFORM public.core_upstream_journal_v1(account,c->>'cost_currency',(ref->>'channel_id')::uuid,aid,'RESERVE',amount);
   END IF;
   IF jsonb_array_length(computed->'leases')<>(CASE WHEN kind='execution_admit_v1' THEN jsonb_array_length(auth->'scopes')+1 ELSE 1 END) THEN RAISE EXCEPTION 'quota_invalid';END IF;
   FOR descriptor IN SELECT value FROM jsonb_array_elements(computed->'leases') LOOP
    IF descriptor->>'command_id' IS DISTINCT FROM e->>'command_id' OR descriptor->>'body_digest' IS DISTINCT FROM e->>'body_digest' OR descriptor->>'caller' IS DISTINCT FROM caller OR descriptor->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR descriptor->>'state' IS DISTINCT FROM 'DRAFT' OR NOT (EXISTS(SELECT 1 FROM jsonb_array_elements(auth->'scopes')s WHERE s->>'kind'=descriptor->>'scope_kind' AND s->>'id'=descriptor->>'scope_id') OR descriptor->>'scope_kind'='channel' AND descriptor->>'scope_id'=ref->>'channel_id') THEN RAISE EXCEPTION 'quota_invalid';END IF;
    IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
     admission=jsonb_set(admission,ARRAY['leases'],(admission->'leases')||jsonb_build_array(jsonb_build_object('lease_id',descriptor->>'lease_id','attempt_id',aid,'command_id',command_pk,'scope_kind',descriptor->>'scope_kind','scope_id',descriptor->>'scope_id','request_id',qid,'state','ADOPTION_PENDING','descriptor',descriptor)));
    ELSE
    INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,descriptor) VALUES(descriptor->>'lease_id',aid,command_pk,descriptor->>'scope_kind',descriptor->>'scope_id',qid,'ADOPTION_PENDING',descriptor);
    END IF;
   END LOOP;
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['admissions',aid::text],admission);END IF;
  ELSIF kind IN ('execution_begin_v1','execution_abort_v1') THEN
   aid=(d->>'attempt_id')::uuid;
   IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.permit_epoch::text IS DISTINCT FROM d->>'permit_epoch' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
   IF kind='execution_begin_v1' THEN
    catalog=core_funding.catalog_v1(cache,ctx.model,ctx.operation,auth->>'customer_currency');cache=catalog->'cache';candidates=catalog->'frame'->'candidates';allowed=catalog->'frame'->'runtimes';
    SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=permit.snapshot->'dispatch_projection'->'candidate_ref';
    IF c IS NOT NULL AND allowed IS NULL THEN allowed=core_execution_runtime.allowed_v1(jsonb_build_array(c));END IF;
    IF c IS NULL OR permit.snapshot->'dispatch_projection'->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
    IF permit.snapshot->'version' IS DISTINCT FROM '2'::jsonb OR permit.snapshot->'dispatch_projection'->'version' IS DISTINCT FROM '2'::jsonb OR (allowed->(c->'ref'->>'package_digest') @> jsonb_build_array(permit.snapshot->'dispatch_projection'->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   ELSE
    PERFORM id FROM public.channels WHERE id=(SELECT provider_id FROM public.attempts WHERE id=aid) FOR SHARE NOWAIT;
    PERFORM id FROM public.upstream_accounts WHERE id=(SELECT account_id FROM public.upstream_holds WHERE attempt_id=aid) FOR SHARE NOWAIT;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=aid;
   PERFORM core_funding.share_v1(ARRAY[h.account_id]);
   SELECT id INTO wallet FROM public.wallets WHERE id=(SELECT wallet_id FROM public.billing_holds WHERE request_id=(SELECT request_id FROM public.attempts WHERE id=aid)) FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO q FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=aid) FOR UPDATE NOWAIT;qid=q.id;
   PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
   SELECT * INTO a FROM public.attempts WHERE id=aid FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
   IF permit.state<>'RESERVED' THEN RAISE EXCEPTION 'permit_not_reserved' USING ERRCODE='P0409';END IF;
   IF kind='execution_begin_v1' THEN
    IF q.execution_closed_at IS NOT NULL OR permit.expires_at<=clock_timestamp() OR permit.authorization_plan_digest IS DISTINCT FROM d->>'authorization_plan_digest' OR permit.dispatched_digest IS DISTINCT FROM d->>'dispatched_digest' OR permit.prep_session_id::text IS DISTINCT FROM d->>'prep_session_id' OR permit.prep_binding_digest IS DISTINCT FROM d->>'prep_binding_digest' OR h.state<>'HELD' OR EXISTS(SELECT 1 FROM public.wallets WHERE id=wallet AND (freeze_reason IS NOT NULL OR public.wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) IS NOT NULL)) OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=qid AND (l.scope_kind<>'channel' OR l.attempt_id=aid) AND (l.state<>'ACTIVE' OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(computed->'proofs') proof WHERE proof->>'lease_id'=l.lease_id AND (proof->>'revision')::numeric=l.revision))) OR (computed->>'proof_remaining_ms')::numeric-extract(epoch FROM (clock_timestamp()-started))*1000<=1000 OR jsonb_array_length(computed->'proofs')<>(SELECT count(*) FROM public.execution_quota_leases WHERE request_id=qid AND (scope_kind<>'channel' OR attempt_id=aid)) THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
    rebuilt='{}'::jsonb;
    FOR dim IN SELECT jsonb_object_keys(c->'cost_rates') LOOP rebuilt=rebuilt||jsonb_build_object(dim,coalesce(permit.manifest->'quantities'->>dim,permit.manifest->'ceilings'->>dim,c->'cost_ceilings'->>dim));END LOOP;
    IF rebuilt IS DISTINCT FROM h.estimated_quantities OR h.estimated_quantities IS NULL OR h.estimated_quantities_digest IS DISTINCT FROM public.core_digest_v1(h.estimated_quantities) OR h.estimate_manifest_digest IS DISTINCT FROM permit.manifest_digest OR h.estimated_atoms IS DISTINCT FROM core_funding.cost_v1(h.cost_version_id,h.estimated_quantities) THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
    IF pending IS NULL THEN UPDATE public.execution_permits SET state='SEND_GRANTED',granted_at=clock_timestamp() WHERE attempt_id=aid AND state='RESERVED';fresh=FOUND;
    ELSE
     fresh=true;
     pending=jsonb_set(pending,ARRAY['grants',aid::text],jsonb_build_object('permit',jsonb_build_object('owner_service',permit.owner_service,'gateway_instance_id',permit.gateway_instance_id,'owner_session_id',permit.owner_session_id,'owner_key_sha256',permit.owner_key_sha256,'certificate_sha256',permit.certificate_sha256,'incarnation_id',permit.incarnation_id,'permit_epoch',permit.permit_epoch,'context_id',permit.context_id,'authorization_plan_digest',permit.authorization_plan_digest,'dispatched_digest',permit.dispatched_digest,'prep_session_id',permit.prep_session_id,'prep_binding_digest',permit.prep_binding_digest),'key_id',ctx.key_id,'ip',ctx.ip,'operation',ctx.operation,'model',ctx.model,'authorization',auth,'package_digest',c->'ref'->>'package_digest','execution',permit.snapshot->'dispatch_projection'->'execution','authority_times',authority_times,'command_deadline',p->>'deadline_at','proof_deadline_ms',computed->'proof_deadline_ms'));
    END IF;
    response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'state','SEND_GRANTED','permit_epoch',permit.permit_epoch::text,'authorization_plan_digest',permit.authorization_plan_digest,'deadline_at',to_char(permit.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
   ELSE
    UPDATE public.execution_permits SET state='ABORTED' WHERE attempt_id=aid AND state='RESERVED';
    UPDATE public.attempts SET state='NOT_EXECUTED',finished_at=clock_timestamp() WHERE id=aid;
    PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
    UPDATE public.upstream_holds SET state='RELEASED',final_cost_atoms=0 WHERE attempt_id=aid;
    PERFORM core_funding.apply_v1(aid,0,0,true);
    PERFORM public.core_upstream_journal_v1(h.account_id,h.currency,h.channel_id,aid,'RELEASE',h.current_reserved_atoms);
    IF d->>'close_request'='true' THEN
     IF EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid AND (ep.state<>'ABORTED' OR ep.granted_at IS NOT NULL)) THEN RAISE EXCEPTION 'abort_closure_conflict' USING ERRCODE='P0409';END IF;
     UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()) WHERE id=qid;
    END IF;
    IF (SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=qid) AND q.selected_attempt_id IS NULL AND NOT EXISTS(SELECT 1 FROM public.attempts WHERE request_id=qid AND state<>'NOT_EXECUTED') THEN
     UPDATE public.gateway_requests SET state='FAILED',billing_outcome='FAILED' WHERE id=qid;
     result=public.settle_requests(ARRAY[qid]);
     IF result->qid::text->>'state'='ERROR' THEN RAISE EXCEPTION 'settlement_failed';END IF;
    END IF;
    UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id=qid AND (scope_kind='channel' AND attempt_id=aid OR (SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=qid)) AND state IN ('ADOPTION_PENDING','ACTIVE');
    response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'state','ABORTED','permit_epoch',permit.permit_epoch::text);
   END IF;
   IF kind='execution_begin_v1' AND pending IS NOT NULL THEN
    pending=jsonb_set(pending,ARRAY['commands'],(pending->'commands')||jsonb_build_array(jsonb_build_object('id',command_pk,'command_id',e->>'command_id','kind',kind,'caller_identity',caller,'owner_session_id',p->>'session_id','caller_incarnation',p->>'incarnation_id','body_digest',e->>'body_digest','deadline_at',e->>'deadline_at','trace_id',e->>'trace_id','contract_version',1,'state','COMMITTED','request_id',qid,'attempt_id',aid,'context_id',ctx.id,'receipt',response)));
    pending=jsonb_set(pending,ARRAY['keys','command:'||caller||':'||kind||':'||((e->>'command_id')::uuid)::text],'true'::jsonb);
   ELSE
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
   END IF;
  ELSE RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
  IF kind IN('execution_begin_v1','execution_admit_v1') AND pending IS NOT NULL THEN
   pending=jsonb_set(pending,ARRAY['audits'],(pending->'audits')||jsonb_build_array(jsonb_build_object('id',gen_random_uuid(),'action',kind,'resource_id',aid::text,'actor',caller,'details',jsonb_build_object('command_id',e->>'command_id','request_id',qid,'attempt_id',aid,'state',response->>'state'))));
  ELSE
  INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),kind,aid::text,caller,jsonb_build_object('command_id',e->>'command_id','request_id',qid,'attempt_id',aid,'state',response->>'state'));
  END IF;
 EXCEPTION WHEN SQLSTATE 'P0402' OR SQLSTATE 'P0409' OR SQLSTATE 'P0422' THEN
  GET STACKED DIAGNOSTICS rejection=MESSAGE_TEXT;
  cache=saved_cache;pending=saved_pending;
  IF rejection NOT IN ('insufficient_funds','upstream_funds_unavailable','already_accepted','idempotency_conflict','proposed_id_conflict','context_stale','context_invalid','candidate_stale','cost_unbounded','cost_ceiling','estimate_invalid','quote_invalid','request_mismatch','execution_closed','retry_quota_exceeded','retry_conflict','retry_quote_exceeded','permit_not_reserved','begin_precondition','internal_request_invalid','abort_closure_conflict','prep_binding_invalid','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request') THEN RAISE;END IF;
  response=jsonb_build_object('command_state','REJECTED','code',rejection);
  IF rejection IN ('already_accepted','idempotency_conflict') THEN response=response||jsonb_build_object('request_id',qid);END IF;
  INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,rejection_code,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'REJECTED',rejection,ctx.id,response);
  fresh=false;
 END;
 RETURN jsonb_build_object('result',jsonb_build_object('receipt',response,'fresh_transition',fresh)||CASE WHEN fresh THEN jsonb_build_object('credential_ciphertext',(SELECT secret FROM public.channels WHERE id=a.provider_id),'channel_id',a.provider_id,'snapshot',permit.snapshot) ELSE '{}'::jsonb END,'cache',cache,'pending',pending);
END $$;



DO $$ DECLARE item_owner name;BEGIN
 SELECT pg_get_userbyid(proowner) INTO item_owner FROM pg_proc WHERE oid='core_funding.run_execution_v1(jsonb,jsonb[],integer[],jsonb[],jsonb[],boolean,numeric[])'::regprocedure;
 EXECUTE format('GRANT EXECUTE ON FUNCTION core_funding.reserve_batch_v1(uuid,text,uuid,numeric,jsonb),core_funding.flush_admit_v1(jsonb),core_funding.flush_v1(jsonb) TO %I',item_owner);
END $$;
REVOKE ALL ON FUNCTION core_funding.reserve_batch_v1(uuid,text,uuid,numeric,jsonb),core_funding.flush_admit_v1(jsonb) FROM PUBLIC;
