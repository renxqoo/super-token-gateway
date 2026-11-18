CREATE FUNCTION public.allocate_customer_rows_v1(p_inputs jsonb,bind_execution boolean)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE item record;p jsonb;quote jsonb;credential api_keys%ROWTYPE;route routes%ROWTYPE;old gateway_requests%ROWTYPE;
 wallet wallets%ROWTYPE;lot wallet_lots%ROWTYPE;wallet_rows wallets[];lot_rows wallet_lots[];local_lots wallet_lots[];
 wallet_index jsonb;lot_ranges jsonb;wallet_dirty boolean[];lot_dirty boolean[];
 staged_requests gateway_requests[]='{}';staged_holds billing_holds[]='{}';staged_allocations jsonb[]='{}';staged_journals jsonb[]='{}';staged_attempts attempts[]='{}';local_allocations jsonb;
 pending_request gateway_requests%ROWTYPE;pending_hold billing_holds%ROWTYPE;pending_attempt attempts%ROWTYPE;
 result jsonb;results jsonb='{}';outcomes jsonb[]='{}';expiry_errors jsonb='{}';legs jsonb;failure text;new_id uuid;attempt_id uuid;pricing uuid;
 wi integer;first_lot integer;last_lot integer;li integer;i integer;sequence integer;
 atoms numeric;remaining numeric;take numeric;credit_part numeric;funded_part numeric;asof timestamptz;locked_wallets uuid[];
 midpoint integer;left_inputs jsonb;right_inputs jsonb;left_results jsonb;right_results jsonb;
 max_atoms constant numeric=99999999999999999999999999999999999999;
BEGIN
 bind_execution=bind_execution AND has_column_privilege(current_user,'public.gateway_requests','execution_budget','INSERT')
  AND has_column_privilege(current_user,'public.attempts','binding_id','INSERT');
 IF p_inputs IS NULL OR jsonb_typeof(p_inputs)<>'array' OR jsonb_array_length(p_inputs) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'invalid admission batch';END IF;
 SELECT coalesce(array_agg(id),'{}'::uuid[]) INTO locked_wallets FROM (SELECT w.id FROM (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target
 JOIN LATERAL(SELECT id FROM wallets WHERE project_id=target.project_id AND currency=target.currency OFFSET 0) found ON true
 JOIN wallets w ON w.id=found.id ORDER BY w.id FOR NO KEY UPDATE OF w SKIP LOCKED) acquired;
 FOR item IN SELECT w.id FROM wallets w JOIN (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target
  ON target.project_id=w.project_id AND target.currency=w.currency WHERE w.id=ANY(locked_wallets) AND w.has_expiring_lots ORDER BY w.id LOOP
  BEGIN PERFORM expire_wallet(item.id);
  EXCEPTION WHEN numeric_value_out_of_range THEN expiry_errors=jsonb_set(expiry_errors,ARRAY[item.id::text],to_jsonb('22003'::text));END;
 END LOOP;
 BEGIN
 SELECT array_agg(w ORDER BY w.id) INTO wallet_rows FROM wallets w JOIN (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target ON target.project_id=w.project_id AND target.currency=w.currency WHERE w.id=ANY(locked_wallets);
 wallet_rows=coalesce(wallet_rows,'{}'::wallets[]);
 SELECT coalesce(jsonb_object_agg((wallet_rows[n]).project_id::text||':'||(wallet_rows[n]).currency,n),'{}') INTO wallet_index FROM generate_subscripts(wallet_rows,1) input(n);
 SELECT array_agg(l ORDER BY l.wallet_id,
  CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
  CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id)
 INTO lot_rows FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.id IN(SELECT (v).id FROM unnest(wallet_rows) v) AND l.available_atoms>0;
 lot_rows=coalesce(lot_rows,'{}'::wallet_lots[]);
 SELECT coalesce(jsonb_object_agg(wallet_id,jsonb_build_array(first_index,last_index)),'{}') INTO lot_ranges FROM(
  SELECT (lot_rows[n]).wallet_id AS wallet_id,min(n) AS first_index,max(n) AS last_index FROM generate_subscripts(lot_rows,1) input(n) GROUP BY (lot_rows[n]).wallet_id
 ) ranges;
 wallet_dirty=array_fill(false,ARRAY[cardinality(wallet_rows)]);lot_dirty=array_fill(false,ARRAY[cardinality(lot_rows)]);
 FOR item IN SELECT input.value,input.ordinality,k AS credential,project.enabled AS project_enabled,r AS route,
  m.enabled AS model_enabled,pr.enabled AS provider_enabled,m.operations AS model_operations,
  price.id AS billing_price,cost.id AS cost_price
  FROM jsonb_array_elements(p_inputs) WITH ORDINALITY input
  LEFT JOIN api_keys k ON k.id=(input.value->>'key_id')::uuid
  LEFT JOIN projects project ON project.id=k.project_id
  LEFT JOIN routes r ON r.id=(input.value->>'route_id')::uuid
  LEFT JOIN models m ON m.name=r.model_name LEFT JOIN channels pr ON pr.id=r.provider_id
  LEFT JOIN billing_prices price ON price.id=(input.value->'quote'->'prices'->>r.provider_id::text)::uuid AND price.currency=input.value->'quote'->>'currency'
  LEFT JOIN cost_versions cost ON cost.id=(input.value->>'cost_pricing_id')::uuid
  ORDER BY input.ordinality LOOP
  p=item.value;quote=p->'quote';
   new_id=(p->>'id')::uuid;attempt_id=(p->>'attempt_id')::uuid; IF attempt_id IS NULL THEN RAISE EXCEPTION 'attempt identity required';END IF;
   credential=item.credential;route=item.route;
   IF credential.id IS NULL OR NOT credential.enabled OR NOT coalesce(item.project_enabled,false) OR credential.expires_at<=clock_timestamp() THEN outcomes[item.ordinality]=jsonb_build_object('error','P0401');CONTINUE;END IF;
   IF credential.project_id<>(p->>'project_id')::uuid OR NOT p->>'operation'=ANY(credential.operations) OR (cardinality(credential.models)>0 AND NOT p->>'model'=ANY(credential.models)) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0403');CONTINUE;END IF;
   IF p->>'digest' IS NOT NULL THEN
    SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
    IF FOUND THEN
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   END IF;
   IF route.id IS NULL OR NOT route.enabled OR NOT coalesce(item.model_enabled,false) OR NOT coalesce(item.provider_enabled,false) OR route.model_name<>p->>'model' OR NOT p->>'operation'=ANY(route.operations) OR NOT p->>'operation'=ANY(item.model_operations) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   pricing=(quote->'prices'->>route.provider_id::text)::uuid;
   IF quote IS NULL OR item.billing_price IS NULL OR item.cost_price IS NULL THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   wi=(wallet_index->>(credential.project_id::text||':'||(quote->>'currency')))::integer;
   IF wi IS NULL THEN
    result=jsonb_build_object('error',CASE WHEN EXISTS(SELECT 1 FROM wallets WHERE project_id=credential.project_id AND currency=quote->>'currency') THEN '55P03' ELSE 'P0402' END);
    outcomes[item.ordinality]=result;CONTINUE;
   END IF;
   atoms=(quote->>'amount_atoms')::numeric;
   IF atoms IS NULL OR atoms<0 OR atoms<>trunc(atoms) OR atoms>max_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   wallet=wallet_rows[wi];
   IF expiry_errors?wallet.id::text THEN outcomes[item.ordinality]=jsonb_build_object('error','22003');CONTINUE;END IF;
   IF wallet.freeze_reason IS NOT NULL OR wallet_freeze_reason(wallet.evidence_disputed,wallet.manual_frozen,wallet.debt_atoms,wallet.credit_reserved_atoms,wallet.credit_limit_atoms) IS NOT NULL OR EXISTS(SELECT 1 FROM wallet_disputes d WHERE d.wallet_id=wallet.id AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=d.request_id)) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0402');CONTINUE;END IF;
   first_lot=coalesce((lot_ranges->wallet.id::text->>0)::integer,1);last_lot=coalesce((lot_ranges->wallet.id::text->>1)::integer,0);
   local_lots=lot_rows[first_lot:last_lot];remaining=atoms;sequence=0;legs='[]';local_allocations='[]';asof=clock_timestamp();
   FOR i IN 1..cardinality(local_lots) LOOP
    EXIT WHEN remaining=0;lot=local_lots[i];
    IF lot.available_atoms<=0 OR lot.expires_at<=asof OR cardinality(lot.models)>0 AND NOT p->>'model'=ANY(lot.models) OR cardinality(lot.operations)>0 AND NOT p->>'operation'=ANY(lot.operations) THEN CONTINUE;END IF;
    take=least(remaining,lot.available_atoms);remaining=remaining-take;sequence=sequence+1;
    lot.available_atoms=lot.available_atoms-take;lot.reserved_atoms=lot.reserved_atoms+take;
    local_lots[i]=lot;
    local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('hold_id',new_id,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));
    legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take),money_leg('lot:'||lot.id||':reserved',take));
   END LOOP;
   credit_part=remaining;funded_part=atoms-credit_part;
   IF wallet.debt_atoms+wallet.credit_reserved_atoms+credit_part>wallet.credit_limit_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','P0402');CONTINUE;END IF;
   IF wallet.reserved_atoms+funded_part>max_atoms OR wallet.credit_reserved_atoms+credit_part>max_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','22003');CONTINUE;END IF;
   IF p->>'digest' IS NOT NULL THEN
    IF bind_execution THEN
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome,execution_budget)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','ADMITTED',quote->>'currency',quote,'PENDING',p->'execution_budget') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    ELSE
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','RUNNING',quote->>'currency',quote,'PENDING') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    END IF;
    IF NOT FOUND THEN
     SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   ELSE
    pending_request.id=new_id;pending_request.project_id=credential.project_id;pending_request.key_id=credential.id;
    pending_request.operation=p->>'operation';pending_request.model=p->>'model';pending_request.intent_digest=p->>'digest';pending_request.fingerprint=p->>'fingerprint';pending_request.billing_quote=quote;
    IF bind_execution THEN pending_request.execution_budget=p->'execution_budget';END IF;
    staged_requests=array_append(staged_requests,pending_request);
   END IF;
   wallet.available_atoms=wallet.available_atoms-funded_part;wallet.reserved_atoms=wallet.reserved_atoms+funded_part;wallet.credit_reserved_atoms=wallet.credit_reserved_atoms+credit_part;
   wallet_rows[wi]=wallet;wallet_dirty[wi]=atoms>0 OR wallet_dirty[wi];
   FOR i IN 1..cardinality(local_lots) LOOP
    li=first_lot+i-1;IF lot_rows[li] IS DISTINCT FROM local_lots[i] THEN lot_rows[li]=local_lots[i];lot_dirty[li]=true;END IF;
   END LOOP;
   pending_hold.id=new_id;pending_hold.wallet_id=wallet.id;pending_hold.amount_atoms=atoms;pending_hold.credit_atoms=credit_part;
   staged_holds=array_append(staged_holds,pending_hold);
   staged_allocations=array_append(staged_allocations,local_allocations);
   IF funded_part>0 THEN staged_journals=array_append(staged_journals,jsonb_build_object('id',ledger_uuid(),'wallet_id',wallet.id,'reference','hold:'||new_id,'kind','HOLD','currency',wallet.currency,'lines',legs));END IF;
   pending_attempt.id=attempt_id;pending_attempt.request_id=new_id;pending_attempt.provider_id=route.provider_id;pending_attempt.upstream_model=route.upstream_model;pending_attempt.pricing_id=(p->>'cost_pricing_id')::uuid;pending_attempt.billing_pricing_id=pricing;
   IF bind_execution THEN pending_attempt.binding_id=(p->>'binding_id')::uuid;END IF;
   staged_attempts=array_append(staged_attempts,pending_attempt);
   result=jsonb_build_object('created',true,'record',jsonb_build_object('id',new_id,'state',CASE WHEN bind_execution THEN 'ADMITTED' ELSE 'RUNNING' END,'fingerprint',p->>'fingerprint'),'attempt_id',attempt_id);
  outcomes[item.ordinality]=result;
 END LOOP;
 IF bind_execution THEN
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome,execution_budget)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'ADMITTED',q.billing_quote->>'currency',q.billing_quote,'PENDING',q.execution_budget
 FROM unnest(staged_requests) q;
 ELSE
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'RUNNING',q.billing_quote->>'currency',q.billing_quote,'PENDING'
 FROM unnest(staged_requests) q;
 END IF;
 INSERT INTO billing_holds(id,request_id,wallet_id,amount_atoms,credit_atoms,next_reconcile_at) SELECT h.id,h.id,h.wallet_id,h.amount_atoms,h.credit_atoms,now()+interval '30 seconds' FROM unnest(staged_holds) h;
 INSERT INTO hold_allocations(hold_id,lot_id,ordinal,amount_atoms) SELECT a.hold_id,a.lot_id,a.ordinal,a.amount_atoms
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(hold_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric);
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,credit_reserved_atoms=state.credit_reserved_atoms
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(to_jsonb(staged_journals));
 IF bind_execution THEN
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state,binding_id)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING',a.binding_id FROM unnest(staged_attempts) a;
 ELSE
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING' FROM unnest(staged_attempts) a;
 END IF;
 SELECT jsonb_object_agg((n-1)::text,outcomes[n]) INTO results FROM generate_subscripts(outcomes,1) input(n);
 RETURN results;
 EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
 END;
 IF jsonb_array_length(p_inputs)=1 THEN RETURN jsonb_build_object('0',jsonb_build_object('error',failure));END IF;
 midpoint=jsonb_array_length(p_inputs)/2;
 SELECT jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality<=midpoint),jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality>midpoint)
 INTO left_inputs,right_inputs FROM jsonb_array_elements(p_inputs) WITH ORDINALITY;
 left_results=public.allocate_customer_rows_v1(left_inputs,bind_execution);right_results=public.allocate_customer_rows_v1(right_inputs,bind_execution);
 SELECT left_results||jsonb_object_agg((key::integer+midpoint)::text,value) INTO results FROM jsonb_each(right_results);
 RETURN results;
END $function$;

REVOKE ALL ON FUNCTION public.allocate_customer_rows_v1(jsonb,boolean) FROM PUBLIC;
DO $$
DECLARE
 allocator name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='public.allocate_customer_batch_v1(jsonb)'::regprocedure));
 executor name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='core_funding.run_execution_v1(jsonb,jsonb[],integer[],jsonb[],jsonb[],boolean,numeric[])'::regprocedure));
BEGIN
 EXECUTE format('ALTER FUNCTION public.allocate_customer_rows_v1(jsonb,boolean) OWNER TO %I',allocator);
 EXECUTE format('GRANT EXECUTE ON FUNCTION public.allocate_customer_rows_v1(jsonb,boolean) TO %I',executor);
END $$;

CREATE OR REPLACE FUNCTION public.allocate_customer_batch_v1(p_inputs jsonb)
RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT public.allocate_customer_rows_v1(p_inputs,false)
$$;

CREATE OR REPLACE FUNCTION core_funding.flush_admit_v1(pending jsonb) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE inputs jsonb;results jsonb;allowed jsonb;updated integer;BEGIN
 IF pending->'admissions'='{}'::jsonb THEN RETURN;END IF;
 SELECT jsonb_agg((a.value->'customer')||jsonb_build_object('execution_budget',a.value->'execution_budget','binding_id',a.value->'binding_id') ORDER BY (a.value->>'sequence')::integer) INTO inputs FROM jsonb_each(pending->'admissions') a;
 results=public.allocate_customer_rows_v1(inputs,true);
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
 IF results->'0'->'record'->>'state'='RUNNING' THEN
  UPDATE public.gateway_requests q SET state='ADMITTED',execution_budget=a.value->'execution_budget'
  FROM jsonb_each(pending->'admissions') a WHERE q.id=(a.value->'customer'->>'id')::uuid;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>jsonb_array_length(inputs) THEN RAISE EXCEPTION 'admit_batch_request_missing' USING ERRCODE='P0B01';END IF;
  UPDATE public.attempts t SET binding_id=(a.value->>'binding_id')::uuid
  FROM jsonb_each(pending->'admissions') a WHERE t.id=a.key::uuid AND t.request_id=(a.value->'customer'->>'id')::uuid;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>jsonb_array_length(inputs) THEN RAISE EXCEPTION 'admit_batch_attempt_missing' USING ERRCODE='P0B01';END IF;
 END IF;
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
