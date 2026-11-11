SET LOCAL check_function_bodies=false;
CREATE FUNCTION public.ledger_uuid() RETURNS uuid
 LANGUAGE sql VOLATILE PARALLEL SAFE AS $$
 SELECT (pg_catalog.lpad(pg_catalog.to_hex(pg_catalog.floor(extract(epoch FROM pg_catalog.clock_timestamp())*1000)::bigint),12,'0')||'7'||pg_catalog.substr(pg_catalog.replace(pg_catalog.gen_random_uuid()::text,'-',''),14))::uuid
$$;
REVOKE ALL ON FUNCTION public.ledger_uuid() FROM PUBLIC;


--
-- PostgreSQL database dump
--


-- Dumped from database version 17.11 (Debian 17.11-1.pgdg13+2)
-- Dumped by pg_dump version 17.11 (Debian 17.11-1.pgdg13+2)


--
-- Name: adjust_billing_charge(uuid, numeric, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.adjust_billing_charge(p_charge uuid, p_target numeric, p_ref text, p_reason text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge billing_charges%ROWTYPE; req gateway_requests%ROWTYPE; existing billing_adjustments%ROWTYPE;lot wallet_lots%ROWTYPE;
 fingerprint text;delta numeric;remaining numeric;funded numeric=0;take numeric;sequence integer;legs jsonb='[]';operation uuid;asof timestamptz;
BEGIN
 IF p_target<0 OR p_target<>trunc(p_target) OR p_target>99999999999999999999999999999999999999 OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_reason NOT IN ('ADMIN_CORRECTION','PROVIDER_RECONCILIATION') THEN RAISE EXCEPTION 'invalid adjustment' USING ERRCODE='P0422';END IF;
 SELECT w.* INTO wallet FROM wallets w JOIN billing_holds h ON h.wallet_id=w.id JOIN billing_charges c ON c.hold_id=h.id WHERE c.id=p_charge FOR NO KEY UPDATE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'charge not found' USING ERRCODE='P0404';END IF;
 SELECT * INTO charge FROM billing_charges WHERE id=p_charge FOR UPDATE;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_charge,p_target::text,p_reason)::text,'UTF8')),'hex');
 SELECT * INTO existing FROM billing_adjustments WHERE charge_id=p_charge AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'adjustment identity conflict' USING ERRCODE='P0409';END IF;RETURN jsonb_build_object('id',existing.id,'replayed',true);END IF;
 operation=gen_random_uuid();delta=p_target-(charge.amount_atoms-charge.refunded_atoms);
 IF delta<0 THEN PERFORM refund_billing_charge(p_charge,-delta,'adjust:'||operation,p_reason);
 ELSIF delta>0 THEN
  SELECT * INTO req FROM gateway_requests WHERE id=charge.request_id;
  PERFORM expire_wallet(wallet.id);asof=clock_timestamp();remaining=delta;sequence=coalesce((SELECT max(ordinal) FROM charge_allocations WHERE charge_id=p_charge),0);
  FOR lot IN SELECT * FROM wallet_available_lots(wallet.id,req.model,req.operation,asof) LOOP
   EXIT WHEN remaining=0;take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;sequence=sequence+1;
   UPDATE wallet_lots SET available_atoms=available_atoms-take,spent_atoms=spent_atoms+take WHERE id=lot.id;
   INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms) VALUES(p_charge,lot.id,sequence,take) ON CONFLICT(charge_id,lot_id) DO UPDATE SET amount_atoms=charge_allocations.amount_atoms+excluded.amount_atoms;
   legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
  END LOOP;
  UPDATE billing_charges SET amount_atoms=amount_atoms+delta,funded_atoms=funded_atoms+funded,debt_atoms=debt_atoms+remaining WHERE id=p_charge;
  UPDATE wallets SET available_atoms=available_atoms-funded,spent_atoms=spent_atoms+funded,debt_atoms=debt_atoms+remaining,
   freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms+remaining,credit_reserved_atoms,credit_limit_atoms) WHERE id=wallet.id;
  legs=legs||jsonb_build_array(money_leg('system:revenue',delta));IF remaining>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',-remaining));END IF;
  INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'adjust:'||operation,'ADJUSTMENT',wallet.currency,legs);
 END IF;
 INSERT INTO billing_adjustments(id,charge_id,external_ref,fingerprint,target_atoms,delta_atoms,reason) VALUES(operation,p_charge,p_ref,fingerprint,p_target,delta,p_reason);
 RETURN jsonb_build_object('id',operation,'delta_atoms',delta::text,'replayed',false);
END $$;


--
-- Name: admit_gateway_batch(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.admit_gateway_batch(p_inputs jsonb) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
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
  LEFT JOIN models m ON m.name=r.model_name LEFT JOIN providers pr ON pr.id=r.provider_id
  LEFT JOIN billing_prices price ON price.id=(input.value->'quote'->'prices'->>r.provider_id::text)::uuid AND price.currency=input.value->'quote'->>'currency'
  LEFT JOIN cost_versions cost ON cost.id=(input.value->>'cost_pricing_id')::uuid
  ORDER BY input.ordinality LOOP
  p=item.value;quote=p->'quote';
   new_id=(p->>'id')::uuid;attempt_id=ledger_uuid();
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
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','RUNNING',quote->>'currency',quote,'PENDING') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    IF NOT FOUND THEN
     SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   ELSE
    pending_request.id=new_id;pending_request.project_id=credential.project_id;pending_request.key_id=credential.id;
    pending_request.operation=p->>'operation';pending_request.model=p->>'model';pending_request.intent_digest=p->>'digest';pending_request.fingerprint=p->>'fingerprint';pending_request.billing_quote=quote;
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
   staged_attempts=array_append(staged_attempts,pending_attempt);
   result=jsonb_build_object('created',true,'record',jsonb_build_object('id',new_id,'state','RUNNING','fingerprint',p->>'fingerprint'),'attempt_id',attempt_id);
  outcomes[item.ordinality]=result;
 END LOOP;
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'RUNNING',q.billing_quote->>'currency',q.billing_quote,'PENDING'
 FROM unnest(staged_requests) q;
 INSERT INTO billing_holds(id,request_id,wallet_id,amount_atoms,credit_atoms,next_reconcile_at) SELECT h.id,h.id,h.wallet_id,h.amount_atoms,h.credit_atoms,now()+interval '30 seconds' FROM unnest(staged_holds) h;
 INSERT INTO hold_allocations(hold_id,lot_id,ordinal,amount_atoms) SELECT a.hold_id,a.lot_id,a.ordinal,a.amount_atoms
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(hold_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric);
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,credit_reserved_atoms=state.credit_reserved_atoms
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(to_jsonb(staged_journals));
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING' FROM unnest(staged_attempts) a;
 SELECT jsonb_object_agg((n-1)::text,outcomes[n]) INTO results FROM generate_subscripts(outcomes,1) input(n);
 RETURN results;
 EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
 END;
 IF jsonb_array_length(p_inputs)=1 THEN RETURN jsonb_build_object('0',jsonb_build_object('error',failure));END IF;
 midpoint=jsonb_array_length(p_inputs)/2;
 SELECT jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality<=midpoint),jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality>midpoint)
 INTO left_inputs,right_inputs FROM jsonb_array_elements(p_inputs) WITH ORDINALITY;
 left_results=admit_gateway_batch(left_inputs);right_results=admit_gateway_batch(right_inputs);
 SELECT left_results||jsonb_object_agg((key::integer+midpoint)::text,value) INTO results FROM jsonb_each(right_results);
 RETURN results;
END $$;


--
-- Name: audit_configuration(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.audit_configuration() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE identity text;
BEGIN
 identity=coalesce(to_jsonb(NEW)->>'id',to_jsonb(NEW)->>'name','unknown');
 INSERT INTO audit_events(id,action,resource_id) VALUES(gen_random_uuid(),TG_TABLE_NAME||'.'||lower(TG_OP),encode(sha256(convert_to(identity,'UTF8')),'hex'));
 RETURN NEW;
END $$;


--
-- Name: authorize_gateway_dispatch(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.authorize_gateway_dispatch() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE allowed boolean;
BEGIN
 SELECT k.enabled AND p.enabled AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp()) AND k.role IN ('ADMIN','INVOKER')
 AND (o.id IS NULL OR o.enabled) AND (u.id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN','MEMBER')) INTO allowed
 FROM api_keys k JOIN projects p ON p.id=k.project_id LEFT JOIN organizations o ON o.id=p.organization_id LEFT JOIN principals u ON u.id=k.principal_id WHERE k.id=NEW.key_id AND k.project_id=NEW.project_id;
 IF NOT coalesce(allowed,false) THEN RAISE EXCEPTION 'dispatch permission unavailable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;


--
-- Name: collect_wallet_debt(uuid, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.collect_wallet_debt(p_wallet uuid, p_limit integer DEFAULT 32) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge record; lot wallet_lots%ROWTYPE; take numeric; remaining numeric; legs jsonb; funded numeric; count integer=0; sequence integer;
BEGIN
 SELECT * INTO wallet FROM wallets WHERE id=p_wallet FOR NO KEY UPDATE;
 IF wallet.debt_atoms=0 THEN RETURN 0; END IF;
 FOR charge IN SELECT c.*,q.model,q.operation FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id JOIN gateway_requests q ON q.id=c.request_id WHERE h.wallet_id=p_wallet AND c.debt_atoms>0 AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id)) ORDER BY c.created_at,c.id LIMIT least(p_limit,128) LOOP
  remaining=charge.debt_atoms;funded=0;legs='[]';sequence=coalesce((SELECT max(ordinal) FROM charge_allocations WHERE charge_id=charge.id),0);
  FOR lot IN SELECT * FROM wallet_lots WHERE wallet_id=p_wallet AND kind='CASH' AND available_atoms>0 ORDER BY priority,created_at,id LOOP
   EXIT WHEN remaining=0;take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;sequence=sequence+1;
   UPDATE wallet_lots SET available_atoms=available_atoms-take,spent_atoms=spent_atoms+take WHERE id=lot.id;
   INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms) VALUES(charge.id,lot.id,sequence,take) ON CONFLICT(charge_id,lot_id) DO UPDATE SET amount_atoms=charge_allocations.amount_atoms+excluded.amount_atoms;
   legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
  END LOOP;
  IF funded>0 THEN
   UPDATE billing_charges SET funded_atoms=funded_atoms+funded,debt_atoms=debt_atoms-funded WHERE id=charge.id;
   UPDATE wallets SET available_atoms=available_atoms-funded,spent_atoms=spent_atoms+funded,debt_atoms=debt_atoms-funded WHERE id=p_wallet;
   legs=legs||jsonb_build_array(money_leg('system:receivable',funded));
   INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(gen_random_uuid(),p_wallet,'collect:'||gen_random_uuid(),'DEBT_COLLECTION',wallet.currency,legs);
   count=count+1;
  END IF;
  EXIT WHEN remaining>0;
 END LOOP;
 UPDATE wallets SET freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) WHERE id=p_wallet;
 RETURN count;
END $$;


--
-- Name: commit_usage_evidence(uuid, uuid, text, text, text, jsonb, numeric, boolean, text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.commit_usage_evidence(p_attempt uuid, p_event uuid, p_source text, p_state text, p_finality text, p_usage jsonb, p_cost numeric, p_complete boolean, p_error text, p_provider_request text, p_request_state text) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE result jsonb;req uuid;pending boolean;all_known boolean;batch boolean;
BEGIN
 result=record_usage_evidence(p_attempt,p_event,p_source,p_state,p_finality,p_usage,p_cost,p_complete,p_error,p_provider_request);
 IF result->>'status' IN ('MERGE_REQUIRED','CONFLICT') THEN RETURN result;END IF;
 SELECT request_id INTO req FROM attempts WHERE id=p_attempt;
 IF p_request_state='SUCCEEDED' THEN
  SELECT coalesce(bool_or(kind='batch'),false),coalesce(bool_or(kind IN ('video','response','operation') AND state IN ('ACTIVE','PENDING_USAGE') AND coalesce(upstream_state,'queued') NOT IN ('completed','succeeded')),false)
  INTO batch,pending FROM resources WHERE attempt_id=p_attempt;
  UPDATE gateway_requests SET state='SUCCEEDED',selected_attempt_id=p_attempt,
   billing_outcome=CASE WHEN batch AND selected_attempt_id=p_attempt THEN billing_outcome WHEN batch OR pending THEN 'PENDING' ELSE 'SUCCESS' END,updated_at=now() WHERE id=req;
 ELSIF p_error IN ('client_disconnected','request_cancelled','stream_cancelled','binary_interrupted') AND p_request_state IS NOT NULL THEN
  UPDATE gateway_requests SET state='CANCELLED',selected_attempt_id=p_attempt,billing_outcome='CANCELLED',updated_at=now() WHERE id=req AND billing_outcome<>'SUCCESS';
 ELSIF p_request_state IS NOT NULL THEN
  UPDATE gateway_requests SET state=CASE WHEN state='SUCCEEDED' OR state IN ('FAILED','CANCELLED') AND p_request_state IN ('UNKNOWN','RUNNING') THEN state ELSE p_request_state END,updated_at=now() WHERE id=req;
 END IF;
 IF batch OR (p_source IN ('batch_completed','batch_cancelled','batch_failed') AND p_finality='FINAL') THEN
  UPDATE gateway_requests q SET billing_outcome=CASE fact.source WHEN 'batch_completed' THEN 'SUCCESS' WHEN 'batch_cancelled' THEN 'CANCELLED' ELSE 'UNKNOWN' END,updated_at=now()
  FROM (SELECT source FROM usage_event_index WHERE attempt_id=p_attempt AND source IN ('batch_completed','batch_cancelled','batch_failed') AND finality='FINAL' ORDER BY seq DESC LIMIT 1) fact
  WHERE q.id=req AND q.selected_attempt_id=p_attempt AND q.billing_outcome IN ('PENDING','UNKNOWN')
  AND EXISTS(SELECT 1 FROM resources r WHERE r.attempt_id=p_attempt AND r.kind='batch');
 END IF;
 IF (p_source='async_final' AND p_state='SUCCEEDED' AND p_finality='FINAL') OR p_request_state='SUCCEEDED' THEN
  UPDATE gateway_requests q SET billing_outcome='SUCCESS',updated_at=now()
  WHERE q.id=req AND q.selected_attempt_id=p_attempt AND q.billing_outcome IN ('PENDING','UNKNOWN')
  AND EXISTS(SELECT 1 FROM resources r WHERE r.attempt_id=p_attempt AND r.kind IN ('video','response','operation')
   AND ((p_source='async_final' AND p_state='SUCCEEDED' AND p_finality='FINAL') OR
    (r.upstream_state IN ('completed','succeeded') AND EXISTS(SELECT 1 FROM usage_event_index e WHERE e.attempt_id=p_attempt AND e.source='async_final' AND e.finality='FINAL'))));
 END IF;
 IF p_request_state IS DISTINCT FROM 'SUCCEEDED' AND EXISTS(SELECT 1 FROM gateway_requests WHERE id=req AND state='FAILED' AND billing_outcome NOT IN ('SUCCESS','CANCELLED')) THEN
  SELECT bool_and(a.state='NOT_EXECUTED' OR coalesce(a.state IN ('SUCCEEDED','FAILED') AND e.finality='FINAL' AND p.id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p.rates) dimension WHERE NOT e.usage?dimension),false)) INTO all_known FROM attempts a LEFT JOIN usage_events e ON e.id=a.evidence_id LEFT JOIN billing_prices p ON p.id=a.billing_pricing_id WHERE a.request_id=req;
  UPDATE gateway_requests SET billing_outcome=CASE WHEN coalesce(all_known,false) THEN 'FAILED' ELSE 'UNKNOWN' END WHERE id=req;
 END IF;RETURN result;
END $$;


--
-- Name: credit_wallet(uuid, text, text, numeric, text, integer, timestamp with time zone, text[], text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.credit_wallet(p_project uuid, p_currency text, p_ref text, p_atoms numeric, p_kind text, p_priority integer, p_expiry timestamp with time zone, p_models text[], p_operations text[]) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE wallet wallets%ROWTYPE; existing funding_operations%ROWTYPE; bucket text; fingerprint text; lot uuid; operation uuid;
BEGIN
 IF p_currency !~ '^[A-Z]{3,8}$' OR p_atoms<=0 OR p_atoms<>trunc(p_atoms) OR p_atoms>99999999999999999999999999999999999999 OR p_kind NOT IN ('CASH','GIFT') OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_models IS NULL OR p_operations IS NULL OR p_priority NOT BETWEEN 0 AND 100000 THEN RAISE EXCEPTION 'invalid funding command' USING ERRCODE='P0422'; END IF;
 IF p_kind='CASH' AND (p_expiry IS NOT NULL OR cardinality(p_models)>0 OR cardinality(p_operations)>0) THEN RAISE EXCEPTION 'cash cannot expire or be scoped' USING ERRCODE='P0422'; END IF;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_currency,p_atoms::text,p_kind,p_priority,p_expiry,p_models,p_operations)::text,'UTF8')),'hex');
 INSERT INTO wallets(id,project_id,currency) VALUES(gen_random_uuid(),p_project,p_currency) ON CONFLICT(project_id,currency) DO NOTHING;
 SELECT * INTO wallet FROM wallets WHERE project_id=p_project AND currency=p_currency FOR NO KEY UPDATE;
 SELECT * INTO existing FROM funding_operations WHERE wallet_id=wallet.id AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'funding identity conflict' USING ERRCODE='P0409'; END IF;RETURN jsonb_build_object('id',existing.id,'wallet_id',wallet.id,'lot_id',existing.lot_id,'replayed',true); END IF;
 IF p_expiry IS NOT NULL AND p_expiry<=clock_timestamp() THEN RAISE EXCEPTION 'grant is expired' USING ERRCODE='P0422'; END IF;
 PERFORM expire_wallet(wallet.id);
 bucket=encode(sha256(convert_to(jsonb_build_array(p_kind,p_priority,p_expiry,p_models,p_operations)::text,'UTF8')),'hex');
 SELECT id INTO lot FROM wallet_lots WHERE wallet_id=wallet.id AND bucket_digest=bucket;
 IF lot IS NULL THEN
  IF (SELECT count(*) FROM wallet_lots WHERE wallet_id=wallet.id AND (available_atoms>0 OR reserved_atoms>0 OR spent_atoms>0 AND (expires_at IS NULL OR expires_at>clock_timestamp())))>=256 THEN RAISE EXCEPTION 'refundable funding bucket limit' USING ERRCODE='P0422'; END IF;
  lot=gen_random_uuid();INSERT INTO wallet_lots(id,wallet_id,kind,bucket_digest,priority,expires_at,models,operations,issued_atoms,available_atoms) VALUES(lot,wallet.id,p_kind,bucket,p_priority,p_expiry,p_models,p_operations,p_atoms,p_atoms);
 ELSE UPDATE wallet_lots SET issued_atoms=issued_atoms+p_atoms,available_atoms=available_atoms+p_atoms WHERE id=lot; END IF;
 UPDATE wallets SET available_atoms=available_atoms+p_atoms,has_expiring_lots=has_expiring_lots OR p_expiry IS NOT NULL WHERE id=wallet.id;
 operation=gen_random_uuid();INSERT INTO funding_operations(id,wallet_id,external_ref,fingerprint,kind,amount_atoms,lot_id) VALUES(operation,wallet.id,p_ref,fingerprint,'CREDIT',p_atoms,lot);
 INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'credit:'||operation,'CREDIT',p_currency,jsonb_build_array(money_leg('system:funding',-p_atoms),money_leg('lot:'||lot||':available',p_atoms)));
 IF p_kind='CASH' AND wallet.debt_atoms>0 THEN PERFORM wake_wallet_debt(wallet.id);END IF;
 RETURN jsonb_build_object('id',operation,'wallet_id',wallet.id,'lot_id',lot,'replayed',false);
END $_$;


--
-- Name: exact_billing_price(jsonb, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.exact_billing_price(p_usage jsonb, p_rates jsonb) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE part record; quantity text; fraction integer; q numeric; d numeric; n numeric=0; denominator numeric=1; a numeric; b numeric; remainder numeric; result numeric;
BEGIN
 FOR part IN SELECT key,value FROM jsonb_each(p_rates) LOOP
  quantity=coalesce(p_usage->>part.key,'0');
  IF quantity !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$' OR part.value->>'numerator' !~ '^(0|[1-9][0-9]{0,37})$' OR part.value->>'denominator' !~ '^[1-9][0-9]{0,37}$' THEN RAISE EXCEPTION 'invalid price input'; END IF;
  fraction=CASE WHEN position('.' IN quantity)>0 THEN length(split_part(quantity,'.',2)) ELSE 0 END;
  q=replace(quantity,'.','')::numeric;d=(part.value->>'denominator')::numeric*power(10::numeric,fraction);
  IF d=1 THEN n=n+q*(part.value->>'numerator')::numeric*denominator;CONTINUE;END IF;
  n=n*d+q*(part.value->>'numerator')::numeric*denominator;denominator=denominator*d;
  a=n;b=denominator;WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  IF a<>0 THEN n=div(n,a);denominator=div(denominator,a);END IF;
 END LOOP;
 result=div(n,denominator)+CASE WHEN mod(n,denominator)>0 THEN 1 ELSE 0 END;
 IF result>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range'; END IF;
 RETURN result;
END $_$;


--
-- Name: expire_wallet(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.expire_wallet(p_wallet uuid) RETURNS numeric
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE lot record; amount numeric=0; cur text;
BEGIN
 SELECT currency INTO cur FROM wallets WHERE id=p_wallet FOR NO KEY UPDATE;
 FOR lot IN SELECT * FROM wallet_lots WHERE wallet_id=p_wallet AND expires_at<=clock_timestamp() AND available_atoms>0 ORDER BY id LOOP
  UPDATE wallet_lots SET available_atoms=0,expired_atoms=expired_atoms+lot.available_atoms WHERE id=lot.id;
  INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(gen_random_uuid(),p_wallet,'expire:'||lot.id||':'||gen_random_uuid(),'EXPIRE',cur,jsonb_build_array(money_leg('lot:'||lot.id||':available',-lot.available_atoms),money_leg('lot:'||lot.id||':expired',lot.available_atoms)));
  amount=amount+lot.available_atoms;
 END LOOP;
 IF amount>0 THEN UPDATE wallets SET available_atoms=available_atoms-amount,expired_atoms=expired_atoms+amount WHERE id=p_wallet; END IF;
 RETURN amount;
END $$;


--
-- Name: gateway_token_summary(uuid); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.gateway_token_summary(p_request uuid) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE attempt record;evidence record;operation text;complete boolean=true;tokens numeric=0;seen boolean=false;
BEGIN
 SELECT q.operation INTO operation FROM gateway_requests q WHERE id=p_request;
 FOR attempt IN SELECT a.id,a.state,a.metering_conflict,a.evidence_id FROM attempts a WHERE a.request_id=p_request LOOP
  seen=true;
  IF attempt.evidence_id IS NOT NULL THEN SELECT e.finality,e.usage INTO evidence FROM usage_events e WHERE e.id=attempt.evidence_id;
  ELSE SELECT NULL::text AS finality,'{}'::jsonb AS usage INTO evidence;END IF;
  IF attempt.metering_conflict OR NOT(attempt.state='NOT_EXECUTED' OR coalesce(evidence.finality='FINAL' AND evidence.usage?'input_tokens' AND (operation='embeddings' OR evidence.usage?'output_tokens'),false)) THEN complete=false;END IF;
  tokens=tokens+coalesce((evidence.usage->>'input_tokens')::numeric,0)+coalesce((evidence.usage->>'cached_input_tokens')::numeric,0)+coalesce((evidence.usage->>'cache_write_tokens')::numeric,0)+coalesce((evidence.usage->>'output_tokens')::numeric,0);
 END LOOP;RETURN jsonb_build_object('complete',seen AND complete,'tokens',tokens::text);
END $$;


--
-- Name: immutable_row(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.immutable_row() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN
 RAISE EXCEPTION 'immutable record';
END $$;


--
-- Name: money_leg(text, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.money_leg(p_account text, p_atoms numeric) RETURNS jsonb
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT jsonb_build_object('account',p_account,'atoms',p_atoms::text) $$;


--
-- Name: persist_usage_batch(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ingest_initial_usage(p_events jsonb,p_wallets uuid[]) RETURNS jsonb
 LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE item record;event jsonb;event_id uuid;cost numeric;complete boolean;staged jsonb[]='{}';requests uuid[]='{}';outcomes jsonb[]='{}';processed integer=0;
BEGIN
 FOR item IN SELECT input.ordinality FROM jsonb_array_elements(p_events) WITH ORDINALITY input
  JOIN attempts a ON a.id=(input.value->>'attemptId')::uuid JOIN billing_holds h ON h.request_id=a.request_id
  WHERE NOT h.wallet_id=ANY(p_wallets) LOOP
  outcomes[item.ordinality]='{"status":"ERROR","error":"55P03"}';processed=processed+1;
 END LOOP;
 FOR item IN SELECT input.value,input.ordinality,a.id AS attempt_id,a.request_id,a.evidence_id,a.state AS attempt_state,q.project_id,q.state AS request_state,q.billing_outcome,q.selected_attempt_id,q.operation,v.currency,v.rates,
  EXISTS(SELECT 1 FROM usage_event_index e WHERE e.attempt_id=a.id) AS prior,
  EXISTS(SELECT 1 FROM resources r WHERE r.attempt_id=a.id) AS resource
  FROM jsonb_array_elements(p_events) WITH ORDINALITY input
  JOIN attempts a ON a.id=(input.value->>'attemptId')::uuid
  JOIN gateway_requests q ON q.id=a.request_id
  JOIN billing_holds h ON h.request_id=q.id AND h.wallet_id=ANY(p_wallets)
  JOIN cost_versions v ON v.id=a.pricing_id
  ORDER BY a.id FOR NO KEY UPDATE OF a LOOP
  event=item.value;
  IF event->>'state' IS DISTINCT FROM 'SUCCEEDED' OR event->>'finality' IS DISTINCT FROM 'FINAL' OR event->>'requestState' IS DISTINCT FROM 'SUCCEEDED' OR event->>'source' IS DISTINCT FROM 'execution' OR event->>'code' IS NOT NULL
   OR item.attempt_state<>'DISPATCHING' OR item.request_state<>'RUNNING' OR item.billing_outcome<>'PENDING' OR item.evidence_id IS NOT NULL OR item.selected_attempt_id IS NOT NULL OR item.prior OR item.resource
   OR item.operation NOT IN ('chat','messages','gemini','embeddings','completions') OR item.request_id=ANY(requests) THEN RETURN NULL;END IF;
  cost=exact_billing_price(event->'usage',item.rates);
  complete=NOT EXISTS(SELECT 1 FROM jsonb_object_keys(item.rates) dimension WHERE NOT event->'usage'?dimension);
  event_id=gen_random_uuid();requests=array_append(requests,item.request_id);
  staged=array_append(staged,jsonb_build_object('id',event_id,'attempt_id',item.attempt_id,'request_id',item.request_id,'project_id',item.project_id,'usage',event->'usage','cost',cost::text,'currency',item.currency,'complete',complete,'provider_request_id',event->>'providerRequestId'));
  outcomes[item.ordinality]=jsonb_build_object('status','APPLIED','evidence_id',event_id,'request_id',item.request_id);
  processed=processed+1;
 END LOOP;
 IF processed<>jsonb_array_length(p_events) THEN RETURN NULL;END IF;
 IF cardinality(staged)=0 THEN RETURN (SELECT jsonb_object_agg((n-1)::text,outcomes[n]) FROM generate_subscripts(outcomes,1) input(n));END IF;
 PERFORM pg_advisory_xact_lock_shared(81442592);
 WITH evidence AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(to_jsonb(staged)) e(id uuid,project_id uuid,request_id uuid,attempt_id uuid,usage jsonb,cost numeric,currency text,complete boolean,provider_request_id text)
 ), inserted AS (
  INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete)
  SELECT e.id,e.project_id,e.request_id,e.attempt_id,'execution','FINAL',e.cost,e.currency,e.complete
  FROM evidence e
  RETURNING id,created_at
 ), payloads AS (
  INSERT INTO usage_payloads(id,created_at,usage)
  SELECT e.id,i.created_at,e.usage FROM evidence e JOIN inserted i ON i.id=e.id
 ), deliveries AS (
  INSERT INTO event_deliveries(consumer_id,event_id)
  SELECT c.id,i.id FROM event_consumers c CROSS JOIN inserted i
 ), completed AS (
  UPDATE attempts a SET evidence_id=e.id,state='SUCCEEDED',finished_at=now(),error_code=NULL,provider_request_id=coalesce(e.provider_request_id,a.provider_request_id)
  FROM evidence e JOIN inserted i ON i.id=e.id WHERE a.id=e.attempt_id
 )
 UPDATE gateway_requests q SET state='SUCCEEDED',selected_attempt_id=e.attempt_id,billing_outcome='SUCCESS',updated_at=now()
 FROM evidence e WHERE q.id=e.request_id;
 RETURN (SELECT jsonb_object_agg((n-1)::text,outcomes[n]) FROM generate_subscripts(outcomes,1) input(n));
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION public.ingest_initial_usage(jsonb,uuid[]) FROM PUBLIC;

CREATE FUNCTION public.persist_usage_batch(p_events jsonb) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE item record;p jsonb;usage jsonb;price cost_versions%ROWTYPE;result jsonb;cost numeric;complete boolean;failure text;outcomes jsonb[]='{}';retry integer;position integer;
 requests uuid[]='{}';identities uuid[]='{}';financial jsonb;outcome jsonb;seen uuid[]='{}';financial_request uuid;locked_wallets uuid[];initial jsonb;
BEGIN
 IF p_events IS NULL OR jsonb_typeof(p_events)<>'array' OR jsonb_array_length(p_events) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'invalid durable usage batch';END IF;
 SELECT coalesce(array_agg(id),'{}'::uuid[]) INTO locked_wallets FROM (SELECT w.id FROM wallets w JOIN (
  SELECT DISTINCT h.wallet_id FROM jsonb_array_elements(p_events) event
  JOIN LATERAL(SELECT request_id FROM attempts WHERE id=(event->>'attemptId')::uuid OFFSET 0) a ON true
  JOIN LATERAL(SELECT wallet_id FROM billing_holds WHERE request_id=a.request_id OFFSET 0) h ON true
 ) target ON target.wallet_id=w.id ORDER BY w.id FOR NO KEY UPDATE OF w SKIP LOCKED) acquired;
 initial=ingest_initial_usage(p_events,locked_wallets);
 IF initial IS NULL THEN
 FOR item IN SELECT event.value,event.ordinality,a.request_id,h.wallet_id FROM jsonb_array_elements(p_events) WITH ORDINALITY event
  LEFT JOIN LATERAL(SELECT request_id,ordinal FROM attempts WHERE id=(event.value->>'attemptId')::uuid OFFSET 0) a ON true
  LEFT JOIN LATERAL(SELECT wallet_id FROM billing_holds WHERE request_id=a.request_id OFFSET 0) h ON true
  ORDER BY h.wallet_id,a.request_id,a.ordinal,event.ordinality LOOP
  p=item.value;usage=coalesce(p->'usage','{}');
  BEGIN
   IF item.wallet_id IS NOT NULL AND NOT item.wallet_id=ANY(locked_wallets) THEN RAISE EXCEPTION 'wallet busy' USING ERRCODE='55P03';END IF;
   SELECT v.* INTO price FROM cost_versions v JOIN attempts a ON a.pricing_id=v.id WHERE a.id=(p->>'attemptId')::uuid;
   IF NOT FOUND THEN RAISE EXCEPTION 'attempt unavailable' USING ERRCODE='P0404';END IF;
   FOR retry IN 1..3 LOOP
    cost=exact_billing_price(usage,price.rates);complete=p->>'finality'='FINAL' AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(price.rates) dimension WHERE NOT usage?dimension);
    result=commit_usage_evidence((p->>'attemptId')::uuid,gen_random_uuid(),p->>'source',p->>'state',p->>'finality',usage,cost,complete,p->>'code',p->>'providerRequestId',p->>'requestState');
    EXIT WHEN result->>'status'<>'MERGE_REQUIRED';usage=result->'usage';
   END LOOP;
   IF result->>'status'='MERGE_REQUIRED' THEN RAISE EXCEPTION 'usage enrichment retry exhausted';END IF;
   requests=array_append(requests,item.request_id);identities[item.ordinality]=item.request_id;
  EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;result=jsonb_build_object('status','ERROR','error',failure);END;
  outcomes[item.ordinality]=result;
 END LOOP;
 ELSE
  FOR item IN SELECT key,value FROM jsonb_each(initial) ORDER BY key::integer LOOP
   position=item.key::integer+1;
   IF item.value?'request_id' THEN
    financial_request=(item.value->>'request_id')::uuid;
    requests=array_append(requests,financial_request);identities[position]=financial_request;
   END IF;
   outcomes[position]=item.value-'request_id';
  END LOOP;
 END IF;
 IF cardinality(requests)>0 THEN
  financial=settle_requests(requests);
  FOR position IN SELECT n FROM generate_subscripts(identities,1) input(n) WHERE identities[n] IS NOT NULL ORDER BY n LOOP
   financial_request=identities[position];outcome=financial->financial_request::text;result=outcomes[position];
   IF outcome->>'state'='ERROR' THEN result=result||jsonb_build_object('status','ERROR','error',outcome->>'error');
   ELSIF financial_request=ANY(seen) AND outcome->>'replayed'='false' THEN outcome=outcome||'{"replayed":true}'::jsonb;
   END IF;
   seen=array_append(seen,financial_request);outcomes[position]=result||jsonb_build_object('billing',outcome);
  END LOOP;
 END IF;
 RETURN (SELECT jsonb_object_agg((n-1)::text,outcomes[n]) FROM generate_subscripts(outcomes,1) input(n));
END $$;

CREATE FUNCTION public.settle_covered_requests(p_requests uuid[]) RETURNS jsonb
 LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE receipts jsonb;journals jsonb;
BEGIN
 WITH targets AS MATERIALIZED (
  SELECT h.* FROM billing_holds h WHERE h.request_id=ANY(p_requests)
 ), candidates AS MATERIALIZED (
  SELECT h.id AS hold_id,h.request_id,h.wallet_id,h.amount_atoms AS held,w.currency,
   l.id AS lot_id,p.id AS pricing_id,e.id AS evidence_id,exact_billing_price(e.usage,p.rates) AS actual
  FROM targets h JOIN wallets w ON w.id=h.wallet_id
  JOIN LATERAL(SELECT * FROM gateway_requests WHERE id=h.request_id OFFSET 0) q ON true
  JOIN LATERAL(SELECT * FROM attempts WHERE id=q.selected_attempt_id OFFSET 0) a ON true
  JOIN LATERAL(SELECT * FROM usage_events WHERE id=a.evidence_id OFFSET 0) e ON true
  JOIN billing_prices p ON p.id=a.billing_pricing_id
  JOIN LATERAL(SELECT * FROM hold_allocations WHERE hold_id=h.id AND ordinal=1 AND amount_atoms=h.amount_atoms OFFSET 0) s ON true
  JOIN wallet_lots l ON l.id=s.lot_id AND l.wallet_id=w.id AND l.expires_at IS NULL
  WHERE h.state IN ('HELD','RECONCILING') AND h.credit_atoms=0 AND w.debt_atoms=0 AND w.freeze_reason IS NULL AND NOT w.evidence_disputed
   AND q.operation IN ('chat','messages','gemini','embeddings','completions') AND q.billing_outcome='SUCCESS'
   AND a.state='SUCCEEDED' AND e.source='execution' AND e.finality='FINAL' AND p.basis='PROVIDER_USAGE' AND p.currency=w.currency
   AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p.rates) dimension WHERE NOT e.usage?dimension)
   AND NOT EXISTS(SELECT 1 FROM hold_allocations extra WHERE extra.hold_id=h.id AND extra.ordinal<>1)
   AND NOT EXISTS(SELECT 1 FROM attempts x WHERE x.request_id=q.id AND x.metering_conflict)
   AND NOT EXISTS(SELECT 1 FROM billing_resolutions x WHERE x.request_id=q.id)
   AND NOT EXISTS(SELECT 1 FROM billing_charges x WHERE x.request_id=q.id)
 ), wallet_counts AS (
  SELECT wallet_id,count(*) AS requests FROM targets GROUP BY wallet_id
 ), covered AS (
  SELECT c.wallet_id FROM candidates c JOIN wallet_counts t ON t.wallet_id=c.wallet_id
  WHERE c.actual>0 AND c.actual<=c.held GROUP BY c.wallet_id,t.requests HAVING count(*)=t.requests
 ), entries AS MATERIALIZED (
  SELECT c.*,ledger_uuid() AS charge_id,ledger_uuid() AS journal_id FROM candidates c JOIN covered w ON w.wallet_id=c.wallet_id
 ), charges AS (
  INSERT INTO billing_charges(id,request_id,hold_id,pricing_id,evidence_id,currency,amount_atoms,funded_atoms,debt_atoms)
  SELECT charge_id,request_id,hold_id,pricing_id,evidence_id,currency,actual,actual,0 FROM entries RETURNING id
 ), allocations AS (
  INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms)
  SELECT c.id,e.lot_id,1,e.actual FROM entries e JOIN charges c ON c.id=e.charge_id
 ), lot_totals AS (
  SELECT lot_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY lot_id
 ), lots AS (
  UPDATE wallet_lots l SET reserved_atoms=l.reserved_atoms-t.held,available_atoms=l.available_atoms+t.held-t.actual,spent_atoms=l.spent_atoms+t.actual
  FROM lot_totals t WHERE l.id=t.lot_id
 ), wallet_totals AS (
  SELECT wallet_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY wallet_id
 ), balances AS (
  UPDATE wallets w SET reserved_atoms=w.reserved_atoms-t.held,available_atoms=w.available_atoms+t.held-t.actual,spent_atoms=w.spent_atoms+t.actual
  FROM wallet_totals t WHERE w.id=t.wallet_id
 ), holds AS (
  UPDATE billing_holds h SET state='SETTLED',closed_at=now(),next_reconcile_at=now()+interval '1 hour',reconcile_until=NULL,reconcile_reason=NULL
  FROM entries e WHERE h.id=e.hold_id
 )
 SELECT coalesce(jsonb_object_agg(e.request_id,jsonb_build_object('state','SETTLED','charge_id',e.charge_id,'charged_atoms',e.actual::text,'funded_atoms',e.actual::text,'debt_atoms','0','replayed',false)),'{}'::jsonb),
  coalesce(jsonb_agg(jsonb_build_object('id',e.journal_id,'wallet_id',e.wallet_id,'reference','settle:'||e.request_id,'kind','SETTLE','currency',e.currency,'lines',
   jsonb_build_array(money_leg('lot:'||e.lot_id||':reserved',-e.held))||
   CASE WHEN e.held>e.actual THEN jsonb_build_array(money_leg('lot:'||e.lot_id||':available',e.held-e.actual)) ELSE '[]'::jsonb END||
   jsonb_build_array(money_leg('system:revenue',e.actual)))),'[]'::jsonb)
 INTO receipts,journals FROM entries e;
 PERFORM post_money_journals(journals);
 RETURN receipts;
EXCEPTION WHEN numeric_value_out_of_range OR check_violation OR raise_exception OR invalid_parameter_value THEN RETURN '{}'::jsonb;
END $$;
REVOKE ALL ON FUNCTION public.settle_covered_requests(uuid[]) FROM PUBLIC;

CREATE FUNCTION public.settle_requests(p_requests uuid[], p_token uuid DEFAULT NULL) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE snapshot record;source record;req gateway_requests%ROWTYPE;hold billing_holds%ROWTYPE;
 attempt attempts%ROWTYPE;evidence usage_events%ROWTYPE;price billing_prices%ROWTYPE;resolution billing_resolutions%ROWTYPE;
 existing billing_charges%ROWTYPE;wallet wallets%ROWTYPE;lot wallet_lots%ROWTYPE;
 wallet_rows wallets[];lot_rows wallet_lots[];local_lots wallet_lots[];
 wallet_index jsonb;lot_index jsonb;lot_ranges jsonb;wallet_dirty boolean[];lot_dirty boolean[];
 staged_charges jsonb[]='{}';staged_allocations jsonb[]='{}';staged_journals jsonb[]='{}';staged_holds jsonb[]='{}';wake_wallets uuid[]='{}';
 local_allocations jsonb;local_journals jsonb;legs jsonb;result jsonb;outcomes jsonb[]='{}';outcome_ids uuid[]='{}';
 wi integer;first_lot integer;last_lot integer;li integer;i integer;sequence integer;failure text;
 actual numeric;remaining numeric;take numeric;returned numeric;expired numeric;released numeric;funded numeric;extra numeric;expired_available numeric;
 max_atoms constant numeric=99999999999999999999999999999999999999;precollect_errors jsonb='{}';covered jsonb='{}';
 charge uuid;pricing_id uuid;evidence_id uuid;release_hold boolean;asof timestamptz;new_charge boolean;dirty_wallet boolean;
BEGIN
 IF p_requests IS NULL OR cardinality(p_requests) NOT BETWEEN 1 AND 256 OR array_position(p_requests,NULL) IS NOT NULL THEN RAISE EXCEPTION 'invalid settlement batch' USING ERRCODE='P0422';END IF;
 p_requests=ARRAY(SELECT DISTINCT id FROM unnest(p_requests) input(id) ORDER BY id);
 IF (SELECT count(*) FROM billing_holds WHERE request_id=ANY(p_requests))<>cardinality(p_requests) THEN RAISE EXCEPTION 'preauthorization missing' USING ERRCODE='P0404';END IF;
 PERFORM w.id FROM wallets w WHERE id IN(SELECT wallet_id FROM billing_holds WHERE request_id=ANY(p_requests)) ORDER BY id FOR NO KEY UPDATE;
 PERFORM id FROM gateway_requests WHERE id=ANY(p_requests) ORDER BY id FOR UPDATE;
 PERFORM id FROM billing_holds WHERE request_id=ANY(p_requests) ORDER BY id FOR UPDATE;
 IF p_token IS NULL THEN
  covered=settle_covered_requests(p_requests);
  IF covered<>'{}'::jsonb THEN
   p_requests=ARRAY(SELECT id FROM unnest(p_requests) input(id) WHERE NOT covered?id::text);
   IF cardinality(p_requests)=0 THEN RETURN covered;END IF;
  END IF;
 END IF;
 FOR source IN SELECT DISTINCT h.wallet_id FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id
  WHERE c.request_id=ANY(p_requests) AND c.debt_atoms>0 AND (p_token IS NULL OR h.reconcile_token=p_token AND h.reconcile_until>clock_timestamp())
  AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id))
  ORDER BY h.wallet_id LOOP
  BEGIN PERFORM collect_wallet_debt(source.wallet_id,32);
  EXCEPTION WHEN numeric_value_out_of_range THEN precollect_errors=jsonb_set(precollect_errors,ARRAY[source.wallet_id::text],to_jsonb('22003'::text));END;
 END LOOP;
 asof=clock_timestamp();
 SELECT array_agg(w ORDER BY w.id) INTO wallet_rows FROM wallets w WHERE id IN(SELECT wallet_id FROM billing_holds WHERE request_id=ANY(p_requests));
 SELECT jsonb_object_agg((wallet_rows[n]).id,n) INTO wallet_index FROM generate_subscripts(wallet_rows,1) input(n);
 SELECT array_agg(l ORDER BY l.wallet_id,
  CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
  CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id)
 INTO lot_rows FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.id IN(SELECT (v).id FROM unnest(wallet_rows) v) AND (l.available_atoms>0 OR l.reserved_atoms>0);
 lot_rows=coalesce(lot_rows,'{}'::wallet_lots[]);
 SELECT coalesce(jsonb_object_agg((lot_rows[n]).id,n),'{}') INTO lot_index FROM generate_subscripts(lot_rows,1) input(n);
 SELECT coalesce(jsonb_object_agg(wallet_id,jsonb_build_array(first_index,last_index)),'{}') INTO lot_ranges FROM(
  SELECT (lot_rows[n]).wallet_id AS wallet_id,min(n) AS first_index,max(n) AS last_index FROM generate_subscripts(lot_rows,1) input(n) GROUP BY (lot_rows[n]).wallet_id
 ) ranges;
 wallet_dirty=array_fill(false,ARRAY[cardinality(wallet_rows)]);lot_dirty=array_fill(false,ARRAY[cardinality(lot_rows)]);
 FOR snapshot IN SELECT q AS req,h AS hold,a AS attempt,e AS evidence,p AS price,r AS resolution,c AS charge,
  EXISTS(SELECT 1 FROM attempts conflict WHERE conflict.request_id=q.id AND conflict.metering_conflict) AS conflicted
  FROM gateway_requests q JOIN billing_holds h ON h.request_id=q.id LEFT JOIN attempts a ON a.id=q.selected_attempt_id
  LEFT JOIN usage_events e ON e.id=a.evidence_id
  LEFT JOIN LATERAL(SELECT * FROM billing_resolutions WHERE request_id=q.id ORDER BY seq DESC LIMIT 1) r ON true
  LEFT JOIN billing_prices p ON p.id=CASE WHEN r.id IS NOT NULL THEN CASE WHEN r.action='SETTLE' THEN r.pricing_id ELSE NULL END ELSE a.billing_pricing_id END
  LEFT JOIN billing_charges c ON c.request_id=q.id WHERE q.id=ANY(p_requests) ORDER BY h.wallet_id,q.id LOOP
  req=snapshot.req;hold=snapshot.hold;attempt=snapshot.attempt;evidence=snapshot.evidence;price=snapshot.price;resolution=snapshot.resolution;existing=snapshot.charge;
  IF p_token IS NOT NULL AND (hold.reconcile_token IS DISTINCT FROM p_token OR hold.reconcile_until IS NULL OR hold.reconcile_until<=clock_timestamp()) THEN
   outcome_ids=array_append(outcome_ids,req.id);outcomes=array_append(outcomes,'{"state":"STALE_LEASE"}'::jsonb);CONTINUE;
  END IF;
  wi=(wallet_index->>hold.wallet_id::text)::integer;wallet=wallet_rows[wi];
  first_lot=(lot_ranges->hold.wallet_id::text->>0)::integer;last_lot=(lot_ranges->hold.wallet_id::text->>1)::integer;
  IF first_lot IS NULL THEN local_lots='{}'::wallet_lots[];ELSE local_lots=lot_rows[first_lot:last_lot];END IF;
  actual=NULL;remaining=NULL;expired=0;released=0;funded=0;extra=0;expired_available=0;sequence=0;legs='[]';
  local_allocations='[]';local_journals='[]';charge=NULL;pricing_id=NULL;evidence_id=NULL;release_hold=false;new_charge=false;dirty_wallet=false;
  BEGIN
   <<decision>> BEGIN
    IF snapshot.conflicted AND resolution.id IS NULL THEN wallet.evidence_disputed=true;wallet.freeze_reason='EVIDENCE_CONFLICT';dirty_wallet=true;result='{"state":"CONFLICT"}';EXIT decision;END IF;
    IF existing.id IS NOT NULL THEN
     IF existing.debt_atoms>0 AND precollect_errors?wallet.id::text THEN result=jsonb_build_object('state','ERROR','error',precollect_errors->>wallet.id::text);EXIT decision;END IF;
     result=jsonb_build_object('state',CASE WHEN hold.state='RELEASED' THEN 'RELEASED' WHEN existing.debt_atoms>0 THEN 'DEBT' ELSE 'SETTLED' END,
      'charge_id',existing.id,'charged_atoms',existing.amount_atoms::text,'funded_atoms',existing.funded_atoms::text,'debt_atoms',existing.debt_atoms::text,'replayed',true);EXIT decision;
    END IF;
    IF hold.state NOT IN ('HELD','RECONCILING') THEN RAISE EXCEPTION 'closed hold lacks its financial receipt';END IF;
    IF resolution.id IS NOT NULL THEN
     release_hold=resolution.action='RELEASE';
     IF release_hold THEN actual=0;
     ELSE
      IF price.id IS NULL OR EXISTS(SELECT 1 FROM jsonb_object_keys(price.rates) dimension WHERE NOT resolution.usage?dimension) THEN RAISE EXCEPTION 'resolution pricing or quantities unavailable' USING ERRCODE='P0422';END IF;
      pricing_id=price.id;actual=exact_billing_price(resolution.usage,price.rates);
     END IF;
    ELSIF req.billing_outcome='FAILED' THEN
     IF EXISTS(SELECT 1 FROM attempts WHERE request_id=req.id AND state IN ('DISPATCHING','UNKNOWN','PARTIAL')) THEN result='{"state":"WAITING"}';EXIT decision;END IF;
     release_hold=true;actual=0;
    ELSE
     IF req.billing_outcome NOT IN ('SUCCESS','CANCELLED') OR req.selected_attempt_id IS NULL OR price.id IS NULL OR evidence.id IS NULL THEN result='{"state":"WAITING"}';EXIT decision;END IF;
     pricing_id=price.id;evidence_id=evidence.id;
     IF price.basis='PROVIDER_USAGE' THEN
      IF evidence.finality<>'FINAL' OR EXISTS(SELECT 1 FROM jsonb_object_keys(price.rates) dimension WHERE NOT evidence.usage?dimension) THEN result='{"state":"WAITING"}';EXIT decision;END IF;
      actual=exact_billing_price(evidence.usage,price.rates);
     ELSE
      IF req.billing_outcome='CANCELLED' THEN
       IF NOT EXISTS(SELECT 1 FROM usage_event_index e WHERE e.attempt_id=attempt.id AND e.source='batch_cancelled' AND e.finality='FINAL') THEN result='{"state":"WAITING"}';EXIT decision;END IF;
       release_hold=true;actual=0;
      ELSE
       actual=exact_billing_price(CASE WHEN price.basis='INPUT_CHARACTERS' THEN jsonb_build_object('characters',req.billing_quote->>'input_characters','requests','1') ELSE '{"requests":"1"}'::jsonb END,price.rates);
      END IF;
     END IF;
    END IF;
    IF pricing_id IS NOT NULL AND price.currency<>wallet.currency THEN RAISE EXCEPTION 'settlement currency mismatch' USING ERRCODE='P0422';END IF;
    IF actual IS NULL OR actual<0 OR actual<>trunc(actual) OR actual>max_atoms THEN RAISE EXCEPTION 'invalid settlement amount' USING ERRCODE='P0422';END IF;
    charge=ledger_uuid();remaining=actual;
    FOR source IN SELECT * FROM hold_allocations WHERE hold_id=hold.id ORDER BY ordinal LOOP
     li=(lot_index->>source.lot_id::text)::integer-first_lot+1;lot=local_lots[li];
     IF lot.id IS NULL OR lot.wallet_id<>wallet.id THEN RAISE EXCEPTION 'reservation source unavailable';END IF;
     take=least(remaining,source.amount_atoms);returned=source.amount_atoms-take;remaining=remaining-take;funded=funded+take;sequence=sequence+1;
     lot.reserved_atoms=lot.reserved_atoms-source.amount_atoms;lot.spent_atoms=lot.spent_atoms+take;
     legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':reserved',-source.amount_atoms));
     IF take>0 THEN local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('charge_id',charge,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));END IF;
     IF returned>0 THEN
      IF lot.expires_at<=asof THEN lot.expired_atoms=lot.expired_atoms+returned;expired=expired+returned;legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':expired',returned));
      ELSE lot.available_atoms=lot.available_atoms+returned;released=released+returned;legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',returned));END IF;
     END IF;
     local_lots[li]=lot;
    END LOOP;
    IF remaining>0 THEN
     FOR i IN 1..cardinality(local_lots) LOOP
      lot=local_lots[i];
      IF lot.expires_at<=asof AND lot.available_atoms>0 THEN
       take=lot.available_atoms;lot.available_atoms=0;lot.expired_atoms=lot.expired_atoms+take;expired_available=expired_available+take;
       local_journals=local_journals||jsonb_build_array(jsonb_build_object('id',gen_random_uuid(),'wallet_id',wallet.id,'reference','expire:'||lot.id||':'||gen_random_uuid(),'kind','EXPIRE','currency',wallet.currency,'lines',jsonb_build_array(money_leg('lot:'||lot.id||':available',-take),money_leg('lot:'||lot.id||':expired',take))));
       local_lots[i]=lot;
      END IF;
     END LOOP;
     FOR i IN 1..cardinality(local_lots) LOOP
      EXIT WHEN remaining=0;lot=local_lots[i];
      IF lot.available_atoms<=0 OR lot.expires_at<=asof OR cardinality(lot.models)>0 AND NOT req.model=ANY(lot.models) OR cardinality(lot.operations)>0 AND NOT req.operation=ANY(lot.operations) THEN CONTINUE;END IF;
      take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;extra=extra+take;sequence=sequence+1;
      lot.available_atoms=lot.available_atoms-take;lot.spent_atoms=lot.spent_atoms+take;local_lots[i]=lot;
      local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('charge_id',charge,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));
      legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
     END LOOP;
    END IF;
    IF actual>0 THEN legs=legs||jsonb_build_array(money_leg('system:revenue',actual));END IF;
    IF remaining>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',-remaining));END IF;
    wallet.reserved_atoms=wallet.reserved_atoms-(hold.amount_atoms-hold.credit_atoms);wallet.credit_reserved_atoms=wallet.credit_reserved_atoms-hold.credit_atoms;wallet.available_atoms=wallet.available_atoms+released-extra-expired_available;
    wallet.spent_atoms=wallet.spent_atoms+funded;wallet.expired_atoms=wallet.expired_atoms+expired+expired_available;wallet.debt_atoms=wallet.debt_atoms+remaining;
    wallet.freeze_reason=wallet_freeze_reason(wallet.evidence_disputed,wallet.manual_frozen,wallet.debt_atoms,wallet.credit_reserved_atoms,wallet.credit_limit_atoms);
    IF wallet.available_atoms NOT BETWEEN 0 AND max_atoms OR wallet.reserved_atoms NOT BETWEEN 0 AND max_atoms OR wallet.credit_reserved_atoms NOT BETWEEN 0 AND max_atoms OR wallet.spent_atoms NOT BETWEEN 0 AND max_atoms OR wallet.expired_atoms NOT BETWEEN 0 AND max_atoms OR wallet.debt_atoms NOT BETWEEN 0 AND max_atoms THEN RAISE EXCEPTION 'staged wallet amount exceeds bounds' USING ERRCODE='22003';END IF;
    FOR i IN 1..cardinality(local_lots) LOOP
     lot=local_lots[i];
     IF lot.available_atoms NOT BETWEEN 0 AND max_atoms OR lot.reserved_atoms NOT BETWEEN 0 AND max_atoms OR lot.spent_atoms NOT BETWEEN 0 AND max_atoms OR lot.expired_atoms NOT BETWEEN 0 AND max_atoms OR lot.issued_atoms<>lot.available_atoms+lot.reserved_atoms+lot.spent_atoms+lot.expired_atoms THEN RAISE EXCEPTION 'staged source amount exceeds bounds' USING ERRCODE='22003';END IF;
    END LOOP;
    IF jsonb_array_length(legs)>0 THEN local_journals=local_journals||jsonb_build_array(jsonb_build_object('id',ledger_uuid(),'wallet_id',wallet.id,'reference','settle:'||req.id,'kind',CASE WHEN release_hold THEN 'RELEASE' ELSE 'SETTLE' END,'currency',wallet.currency,'lines',legs));END IF;
    IF jsonb_array_length(legs)>2048 OR jsonb_array_length(legs)=1 THEN RAISE EXCEPTION 'settlement journal exceeds posting bounds' USING ERRCODE='P0422';END IF;
    staged_charges=array_append(staged_charges,jsonb_build_object('id',charge,'request_id',req.id,'hold_id',hold.id,'pricing_id',pricing_id,'evidence_id',evidence_id,'currency',wallet.currency,'amount_atoms',actual::text,'funded_atoms',funded::text,'debt_atoms',remaining::text));
    staged_allocations=array_append(staged_allocations,local_allocations);staged_journals=array_append(staged_journals,local_journals);
    FOR i IN 1..cardinality(local_lots) LOOP
     li=first_lot+i-1;
     IF local_lots[i] IS DISTINCT FROM lot_rows[li] THEN lot_rows[li]=local_lots[i];lot_dirty[li]=true;END IF;
    END LOOP;
    new_charge=true;dirty_wallet=true;
    IF released>0 AND wallet.debt_atoms>0 THEN wake_wallets=array_append(wake_wallets,wallet.id);END IF;
    result=jsonb_build_object('state',CASE WHEN release_hold THEN 'RELEASED' WHEN remaining>0 THEN 'DEBT' ELSE 'SETTLED' END,'charge_id',charge,'charged_atoms',actual::text,'funded_atoms',funded::text,'debt_atoms',remaining::text,'replayed',false);
   END decision;
   IF dirty_wallet AND wallet IS DISTINCT FROM wallet_rows[wi] THEN wallet_rows[wi]=wallet;wallet_dirty[wi]=true;END IF;
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
   IF failure NOT IN ('22003','22023','P0422','P0001') THEN RAISE;END IF;
   result=jsonb_build_object('state','ERROR','error',failure);new_charge=false;
  END;
  staged_holds=array_append(staged_holds,jsonb_build_object('id',hold.id,
   'state',CASE WHEN result->>'state' IN ('WAITING','CONFLICT','ERROR') AND hold.state IN ('HELD','RECONCILING') THEN 'RECONCILING' WHEN result->>'state'='RELEASED' THEN 'RELEASED' WHEN result->>'state' IN ('SETTLED','DEBT') THEN 'SETTLED' ELSE hold.state END,
   'closed_at',CASE WHEN new_charge THEN now() ELSE hold.closed_at END,
   'next_reconcile_at',now()+CASE WHEN result->>'state' IN ('WAITING','CONFLICT','ERROR') THEN least(interval '1 hour',greatest(interval '30 seconds',now()-hold.created_at)) ELSE interval '1 hour' END,
   'reconcile_reason',CASE WHEN result->>'state' IN ('WAITING','CONFLICT','DEBT','ERROR') THEN result->>'state' ELSE NULL END));
  outcome_ids=array_append(outcome_ids,req.id);outcomes=array_append(outcomes,result);
 END LOOP;
 INSERT INTO billing_charges(id,request_id,hold_id,pricing_id,evidence_id,currency,amount_atoms,funded_atoms,debt_atoms)
 SELECT c.id,c.request_id,c.hold_id,c.pricing_id,c.evidence_id,c.currency,c.amount_atoms,c.funded_atoms,c.debt_atoms FROM jsonb_to_recordset(to_jsonb(staged_charges)) c(id uuid,request_id uuid,hold_id uuid,pricing_id uuid,evidence_id uuid,currency text,amount_atoms numeric,funded_atoms numeric,debt_atoms numeric);
 INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms)
 SELECT a.charge_id,a.lot_id,min(a.ordinal),sum(a.amount_atoms)
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(charge_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric) GROUP BY a.charge_id,a.lot_id;
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,spent_atoms=state.spent_atoms,expired_atoms=state.expired_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,spent_atoms=state.spent_atoms,expired_atoms=state.expired_atoms,debt_atoms=state.debt_atoms,credit_reserved_atoms=state.credit_reserved_atoms,evidence_disputed=state.evidence_disputed,freeze_reason=state.freeze_reason
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(coalesce((SELECT jsonb_agg(j.item) FROM unnest(staged_journals) batch(items) CROSS JOIN LATERAL jsonb_array_elements(batch.items) j(item)),'[]'::jsonb));
 UPDATE billing_holds target SET state=state.state,closed_at=state.closed_at,next_reconcile_at=state.next_reconcile_at,reconcile_until=NULL,reconcile_reason=state.reconcile_reason
 FROM jsonb_to_recordset(to_jsonb(staged_holds)) state(id uuid,state text,closed_at timestamptz,next_reconcile_at timestamptz,reconcile_reason text) WHERE target.id=state.id;
 FOR source IN SELECT DISTINCT id FROM unnest(wake_wallets) w(id) LOOP PERFORM wake_wallet_debt(source.id);END LOOP;
 RETURN covered||(SELECT jsonb_object_agg(id,value) FROM unnest(outcome_ids,outcomes) result_rows(id,value));
END $$;


--
-- Name: record_usage_evidence(uuid, uuid, text, text, text, jsonb, numeric, boolean, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.record_usage_evidence(p_attempt uuid, p_event uuid, p_source text, p_state text, p_finality text, p_usage jsonb, p_cost numeric, p_complete boolean, p_error text, p_provider_request text) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE a record; prior usage_events%ROWTYPE; original usage_events%ROWTYPE; conflicting boolean; chosen boolean;
BEGIN
 IF jsonb_typeof(p_usage)<>'object' OR EXISTS(SELECT 1 FROM jsonb_each(p_usage) j WHERE jsonb_typeof(j.value)<>'string' OR j.key NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','reasoning_tokens','requests','images','audio_ms','video_ms','audio_seconds','video_seconds','characters') OR
 (j.key IN ('audio_seconds','video_seconds') AND j.value#>>'{}' !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR
 (j.key NOT IN ('audio_seconds','video_seconds') AND j.value#>>'{}' !~ '^(0|[1-9][0-9]{0,37})$')) THEN RAISE EXCEPTION 'invalid usage evidence'; END IF;
 SELECT attempts.*,q.project_id,v.currency INTO a FROM attempts JOIN gateway_requests q ON q.id=attempts.request_id JOIN cost_versions v ON v.id=attempts.pricing_id WHERE attempts.id=p_attempt FOR NO KEY UPDATE OF attempts;
 IF NOT FOUND THEN RAISE EXCEPTION 'unknown attempt'; END IF;
 IF a.evidence_id IS NOT NULL THEN SELECT * INTO prior FROM usage_events WHERE id=a.evidence_id;ELSE prior=NULL;END IF;
 SELECT * INTO original FROM usage_events WHERE attempt_id=p_attempt AND source=p_source;
 IF original.id IS NOT NULL AND original.finality=p_finality AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(p_usage)n LEFT JOIN jsonb_each_text(original.usage)o USING(key) WHERE o.key IS NULL OR n.value::numeric<>o.value::numeric) THEN
  RETURN jsonb_build_object('status','REPLAY','evidence_id',a.evidence_id);
 END IF;
 conflicting=prior.id IS NOT NULL AND ((prior.finality='FINAL' AND p_finality='FINAL') OR original.id IS NOT NULL) AND EXISTS(
 SELECT 1 FROM jsonb_each_text(p_usage)n JOIN jsonb_each_text(CASE WHEN original.id IS NOT NULL THEN original.usage ELSE prior.usage END)o USING(key) WHERE n.value::numeric<>o.value::numeric);
 IF conflicting THEN
  INSERT INTO usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint) VALUES(p_event,p_attempt,coalesce(original.id,prior.id),p_source,p_finality,p_usage,encode(sha256(convert_to(p_usage::text,'UTF8')),'hex')) ON CONFLICT DO NOTHING;
  UPDATE attempts SET metering_conflict=true WHERE id=p_attempt;
  RETURN jsonb_build_object('status','CONFLICT');
 END IF;
 IF p_source='recovery_interruption' AND a.state<>'DISPATCHING' THEN RETURN jsonb_build_object('status','REPLAY'); END IF;
 chosen=prior.id IS NULL OR (p_finality='FINAL' AND prior.finality<>'FINAL') OR p_finality=prior.finality OR (p_finality='PARTIAL' AND prior.finality IN ('UNKNOWN','ESTIMATED'));
 IF chosen AND prior.id IS NOT NULL AND EXISTS(SELECT 1 FROM jsonb_object_keys(prior.usage) k WHERE NOT p_usage?k) AND NOT EXISTS(SELECT 1 FROM jsonb_each_text(p_usage)n JOIN jsonb_each_text(prior.usage)o USING(key) WHERE n.value::numeric<>o.value::numeric) THEN
  RETURN jsonb_build_object('status','MERGE_REQUIRED','usage',prior.usage||p_usage);
 END IF;
 IF original.id IS NOT NULL THEN p_source=p_source||'_'||substr(encode(sha256(convert_to(p_usage::text||p_finality,'UTF8')),'hex'),1,24); END IF;
 PERFORM pg_advisory_xact_lock_shared(81442592);
 INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete)
 VALUES(p_event,a.project_id,a.request_id,p_attempt,p_source,p_finality,p_cost,a.currency,p_complete) ON CONFLICT(attempt_id,source) DO NOTHING;
 IF NOT FOUND THEN RETURN jsonb_build_object('status','REPLAY'); END IF;
 INSERT INTO usage_payloads(id,created_at,usage) SELECT id,created_at,p_usage FROM usage_event_index WHERE id=p_event;
 INSERT INTO event_deliveries(consumer_id,event_id) SELECT id,p_event FROM event_consumers;
 UPDATE attempts SET evidence_id=CASE WHEN chosen THEN p_event ELSE evidence_id END,
 state=CASE WHEN chosen THEN p_state ELSE state END,finished_at=now(),error_code=p_error,provider_request_id=coalesce(p_provider_request,provider_request_id) WHERE id=p_attempt;

 RETURN jsonb_build_object('status','APPLIED','evidence_id',CASE WHEN chosen THEN p_event ELSE a.evidence_id END);
END $_$;


--
-- Name: refund_billing_charge(uuid, numeric, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refund_billing_charge(p_charge uuid, p_atoms numeric, p_ref text, p_reason text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge billing_charges%ROWTYPE; existing billing_refunds%ROWTYPE; allocation record; remaining numeric; take numeric; waived numeric; refunded numeric; expired numeric=0; available numeric=0; legs jsonb='[]';operation uuid;fingerprint text;asof timestamptz;
BEGIN
 IF p_atoms<=0 OR p_atoms<>trunc(p_atoms) OR p_atoms>99999999999999999999999999999999999999 OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_reason NOT IN ('CUSTOMER_REFUND','ADMIN_CORRECTION','PROVIDER_RECONCILIATION') THEN RAISE EXCEPTION 'invalid refund' USING ERRCODE='P0422';END IF;
 SELECT w.* INTO wallet FROM wallets w JOIN billing_holds h ON h.wallet_id=w.id JOIN billing_charges c ON c.hold_id=h.id WHERE c.id=p_charge FOR NO KEY UPDATE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'charge not found' USING ERRCODE='P0404';END IF;
 SELECT * INTO charge FROM billing_charges WHERE id=p_charge FOR UPDATE;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_charge,p_atoms::text,p_reason)::text,'UTF8')),'hex');
 SELECT * INTO existing FROM billing_refunds WHERE charge_id=p_charge AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'refund identity conflict' USING ERRCODE='P0409';END IF;RETURN jsonb_build_object('id',existing.id,'replayed',true);END IF;
 IF p_atoms>charge.amount_atoms-charge.refunded_atoms THEN RAISE EXCEPTION 'refund exceeds charge' USING ERRCODE='P0422';END IF;
 asof=clock_timestamp();
 waived=least(p_atoms,charge.debt_atoms);remaining=p_atoms-waived;refunded=remaining;
 legs=jsonb_build_array(money_leg('system:revenue',-p_atoms));IF waived>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',waived));END IF;
 FOR allocation IN SELECT a.*,l.expires_at FROM charge_allocations a JOIN wallet_lots l ON l.id=a.lot_id WHERE a.charge_id=p_charge AND a.refunded_atoms<a.amount_atoms ORDER BY a.ordinal DESC LOOP
  EXIT WHEN remaining=0;take=least(remaining,allocation.amount_atoms-allocation.refunded_atoms);remaining=remaining-take;
  UPDATE charge_allocations SET refunded_atoms=refunded_atoms+take WHERE charge_id=p_charge AND lot_id=allocation.lot_id;
  IF allocation.expires_at<=asof THEN
   UPDATE wallet_lots SET spent_atoms=spent_atoms-take,expired_atoms=expired_atoms+take WHERE id=allocation.lot_id;expired=expired+take;legs=legs||jsonb_build_array(money_leg('lot:'||allocation.lot_id||':expired',take));
  ELSE UPDATE wallet_lots SET spent_atoms=spent_atoms-take,available_atoms=available_atoms+take WHERE id=allocation.lot_id;available=available+take;legs=legs||jsonb_build_array(money_leg('lot:'||allocation.lot_id||':available',take));END IF;
 END LOOP;
 IF remaining<>0 THEN RAISE EXCEPTION 'refund allocation invariant';END IF;
 UPDATE billing_charges SET debt_atoms=debt_atoms-waived,debt_refunded_atoms=debt_refunded_atoms+waived,refunded_atoms=refunded_atoms+p_atoms WHERE id=p_charge;
 UPDATE wallets SET available_atoms=available_atoms+available,expired_atoms=expired_atoms+expired,spent_atoms=spent_atoms-refunded,debt_atoms=debt_atoms-waived WHERE id=wallet.id;
 UPDATE wallets SET freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) WHERE id=wallet.id;
 operation=gen_random_uuid();INSERT INTO billing_refunds(id,charge_id,external_ref,amount_atoms,fingerprint,reason) VALUES(operation,p_charge,p_ref,p_atoms,fingerprint,p_reason);
 INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'refund:'||operation,'REFUND',wallet.currency,legs);
 IF available>0 AND wallet.debt_atoms>waived THEN PERFORM wake_wallet_debt(wallet.id);END IF;
 RETURN jsonb_build_object('id',operation,'refunded_atoms',p_atoms::text,'replayed',false);
END $$;


--
-- Name: register_wallet_dispute(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.register_wallet_dispute() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 INSERT INTO wallet_disputes(wallet_id,request_id) SELECT h.wallet_id,h.request_id FROM billing_holds h JOIN attempts a ON a.request_id=h.request_id WHERE a.id=NEW.attempt_id ON CONFLICT DO NOTHING;RETURN NEW;
END $$;


--
-- Name: resource_billing_state(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.resource_billing_state() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE req uuid;outcome text;
BEGIN
 IF NEW.kind NOT IN ('video','response','operation') OR NEW.attempt_id IS NULL THEN RETURN NEW;END IF;
 outcome=CASE WHEN NEW.upstream_state IN ('completed','succeeded') THEN 'SUCCESS' WHEN NEW.upstream_state IN ('cancelled','canceled') THEN 'CANCELLED' WHEN NEW.upstream_state IN ('failed','expired') THEN 'UNKNOWN' END;
 IF outcome IS NULL THEN RETURN NEW;END IF;
 SELECT request_id INTO req FROM attempts WHERE id=NEW.attempt_id;
 UPDATE gateway_requests SET billing_outcome=outcome WHERE id=req AND billing_outcome IN ('PENDING','UNKNOWN','CANCELLED') AND billing_outcome<>outcome AND selected_attempt_id=NEW.attempt_id;
 IF FOUND THEN UPDATE billing_holds SET next_reconcile_at=now() WHERE request_id=req AND state IN ('HELD','RECONCILING');END IF;
 RETURN NEW;
END $$;


--
-- Name: validate_billing_price(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_billing_price() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE part record;
BEGIN
 IF jsonb_typeof(NEW.rates)<>'object' OR NEW.rates='{}' THEN RAISE EXCEPTION 'invalid tariff rates';END IF;
 FOR part IN SELECT key,value FROM jsonb_each(NEW.rates) LOOP
  IF part.key NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','requests','images','audio_ms','video_ms','audio_seconds','video_seconds','characters') OR jsonb_typeof(part.value)<>'object' OR NOT(part.value?&ARRAY['numerator','denominator']) OR jsonb_typeof(part.value->'numerator')<>'string' OR jsonb_typeof(part.value->'denominator')<>'string' OR part.value->>'numerator' !~ '^(0|[1-9][0-9]{0,37})$' OR part.value->>'denominator' !~ '^[1-9][0-9]{0,37}$' OR NOT NEW.ceilings?part.key OR jsonb_typeof(NEW.ceilings->part.key)<>'string' OR NEW.ceilings->>part.key !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$' THEN RAISE EXCEPTION 'invalid tariff dimension';END IF;
  IF part.key NOT IN ('audio_seconds','video_seconds') AND NEW.ceilings->>part.key LIKE '%.%' THEN RAISE EXCEPTION 'counter ceiling is fractional';END IF;
  IF NEW.basis='REQUEST_COMPLETION' AND part.key<>'requests' OR NEW.basis='INPUT_CHARACTERS' AND part.key NOT IN ('characters','requests') THEN RAISE EXCEPTION 'billing basis dimension mismatch';END IF;
 END LOOP;
 IF NEW.rates?'audio_ms' AND NEW.rates?'audio_seconds' OR NEW.rates?'video_ms' AND NEW.rates?'video_seconds' THEN RAISE EXCEPTION 'duplicate duration unit';END IF;
 IF NEW.rates?'requests' AND (NEW.ceilings->>'requests')::numeric<1 THEN RAISE EXCEPTION 'request ceiling is zero';END IF;
 PERFORM exact_billing_price(NEW.ceilings,NEW.rates);RETURN NEW;
END $_$;


--
-- Name: validate_key_principal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_key_principal() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE principal principals%ROWTYPE;org uuid;cidr text;
BEGIN
 FOREACH cidr IN ARRAY NEW.allowed_cidrs LOOP PERFORM cidr::inet;END LOOP;
 IF NEW.principal_id IS NOT NULL THEN
  SELECT * INTO principal FROM principals WHERE id=NEW.principal_id;SELECT organization_id INTO org FROM projects WHERE id=NEW.project_id;
  IF principal.id IS NULL OR principal.organization_id IS DISTINCT FROM org THEN RAISE EXCEPTION 'principal belongs to another organization' USING ERRCODE='P0403';END IF;
  IF NEW.role='ADMIN' AND principal.role NOT IN ('OWNER','ADMIN') OR NEW.role='BILLING' AND principal.role NOT IN ('OWNER','ADMIN','BILLING') OR NEW.role='INVOKER' AND principal.role NOT IN ('OWNER','ADMIN','MEMBER') THEN RAISE EXCEPTION 'key role exceeds principal authority' USING ERRCODE='P0403';END IF;
 END IF;RETURN NEW;
END $$;


--
-- Name: validate_money_journal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_money_journal() RETURNS trigger
 LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$ BEGIN
 PERFORM validate_money_lines(NEW.wallet_id,NEW.currency,NEW.lines);RETURN NEW;
END $$;


--
-- Name: validate_project_organization(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_project_organization() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM api_keys k JOIN principals p ON p.id=k.principal_id WHERE k.project_id=NEW.id AND p.organization_id IS DISTINCT FROM NEW.organization_id) THEN RAISE EXCEPTION 'revoke principal keys before moving project' USING ERRCODE='P0409';END IF;RETURN NEW;
END $$;


--
-- Name: validate_usage(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_usage() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$ DECLARE k text; v text; BEGIN
 FOR k,v IN SELECT key,value #>> '{}' FROM jsonb_each(NEW.usage) LOOP
  IF jsonb_typeof(NEW.usage->k)<>'string' OR
   (k IN ('audio_seconds','video_seconds') AND v !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR
   (k NOT IN ('audio_seconds','video_seconds') AND (k NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','reasoning_tokens','requests','images','audio_ms','video_ms','characters') OR v !~ '^(0|[1-9][0-9]{0,37})$')) THEN
   RAISE EXCEPTION 'invalid usage dimension';
  END IF;
 END LOOP;
 RETURN NEW;
END $_$;




--
-- Name: wallet_lots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallet_lots (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    kind text NOT NULL,
    bucket_digest text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    expires_at timestamp with time zone,
    models text[] DEFAULT '{}'::text[] NOT NULL,
    operations text[] DEFAULT '{}'::text[] NOT NULL,
    issued_atoms numeric(38,0) NOT NULL,
    available_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    expired_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallet_lots_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_check CHECK ((issued_atoms = (((available_atoms + reserved_atoms) + spent_atoms) + expired_atoms))),
    CONSTRAINT wallet_lots_check1 CHECK (((kind = 'GIFT'::text) OR (expires_at IS NULL))),
    CONSTRAINT wallet_lots_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_issued_atoms_check CHECK ((issued_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_kind_check CHECK ((kind = ANY (ARRAY['CASH'::text, 'GIFT'::text]))),
    CONSTRAINT wallet_lots_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
);


CREATE FUNCTION public.wallet_freeze_reason(p_disputed boolean,p_manual boolean,p_debt numeric,p_credit_reserved numeric,p_limit numeric) RETURNS text
 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
 SELECT CASE WHEN p_disputed THEN 'EVIDENCE_CONFLICT' WHEN p_manual THEN 'MANUAL' WHEN p_debt+p_credit_reserved>p_limit THEN 'DEBT' ELSE NULL END
$$;

CREATE FUNCTION public.wake_wallet_debt(p_wallet uuid) RETURNS void
 LANGUAGE sql VOLATILE SET search_path TO 'pg_catalog','public','pg_temp' AS $$
 UPDATE billing_holds h SET next_reconcile_at=now(),reconcile_until=NULL
 FROM billing_charges c WHERE c.hold_id=h.id AND h.wallet_id=p_wallet AND c.debt_atoms>0
 AND EXISTS(SELECT 1 FROM wallet_lots l WHERE l.wallet_id=p_wallet AND l.kind='CASH' AND l.available_atoms>0)
 AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id))
$$;

--
-- Name: wallet_available_lots(uuid, text, text, timestamptz); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.wallet_available_lots(p_wallet uuid, p_model text, p_operation text, p_asof timestamptz) RETURNS SETOF public.wallet_lots
    LANGUAGE sql STABLE
    AS $$
 SELECT l.* FROM public.wallet_lots l JOIN public.wallets w ON w.id=l.wallet_id WHERE l.wallet_id=p_wallet AND l.available_atoms>0
 AND (l.expires_at IS NULL OR l.expires_at>p_asof) AND (cardinality(l.models)=0 OR p_model=ANY(l.models)) AND (cardinality(l.operations)=0 OR p_operation=ANY(l.operations))
 ORDER BY CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
 CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id
$$;


--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_keys (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name text NOT NULL,
    verifier text NOT NULL,
    prefix text NOT NULL,
    models text[] NOT NULL,
    operations text[] NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    principal_id uuid,
    role text DEFAULT 'INVOKER'::text NOT NULL,
    allowed_cidrs text[] DEFAULT '{}'::text[] NOT NULL,
    rpm integer,
    tpm bigint,
    concurrency integer,
    CONSTRAINT api_keys_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT api_keys_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'INVOKER'::text, 'BILLING'::text, 'VIEWER'::text]))),
    CONSTRAINT api_keys_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT api_keys_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);


--
-- Name: attempts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.attempts (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    ordinal integer NOT NULL,
    provider_id uuid NOT NULL,
    upstream_model text NOT NULL,
    pricing_id uuid NOT NULL,
    state text NOT NULL,
    provider_request_id text,
    error_code text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    evidence_id uuid,
    metering_conflict boolean DEFAULT false NOT NULL,
    billing_pricing_id uuid,
    CONSTRAINT attempts_error_code_check CHECK ((error_code ~ '^[a-z0-9_]{1,80}$'::text)),
    CONSTRAINT attempts_ordinal_check CHECK ((ordinal > 0)),
    CONSTRAINT attempts_state_check CHECK ((state = ANY (ARRAY['DISPATCHING'::text, 'SUCCEEDED'::text, 'NOT_EXECUTED'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'FAILED'::text])))
);


--
-- Name: audit_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_events (
    id uuid NOT NULL,
    action text NOT NULL,
    resource_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: billing_adjustments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_adjustments (
    id uuid NOT NULL,
    charge_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    target_atoms numeric(38,0) NOT NULL,
    delta_atoms numeric(38,0) NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_adjustments_reason_check CHECK ((reason = ANY (ARRAY['ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text]))),
    CONSTRAINT billing_adjustments_target_atoms_check CHECK ((target_atoms >= (0)::numeric))
);


--
-- Name: billing_charges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_charges (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    hold_id uuid NOT NULL,
    pricing_id uuid,
    evidence_id uuid,
    currency text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    funded_atoms numeric(38,0) NOT NULL,
    debt_atoms numeric(38,0) NOT NULL,
    refunded_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    debt_refunded_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT billing_charges_amount_atoms_check CHECK ((amount_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_check CHECK (((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms))),
    CONSTRAINT billing_charges_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_debt_refunded_atoms_check CHECK ((debt_refunded_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_funded_atoms_check CHECK ((funded_atoms >= (0)::numeric)),
    CONSTRAINT charge_value_conserved CHECK (((amount_atoms = ((funded_atoms + debt_atoms) + debt_refunded_atoms)) AND (refunded_atoms >= debt_refunded_atoms) AND ((refunded_atoms - debt_refunded_atoms) <= funded_atoms)))
);


--
-- Name: billing_holds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_holds (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    state text DEFAULT 'HELD'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    next_reconcile_at timestamp with time zone DEFAULT now() NOT NULL,
    reconcile_until timestamp with time zone,
    reconcile_token uuid,
    reconcile_reason text,
    CONSTRAINT billing_holds_amount_atoms_check CHECK ((amount_atoms >= (0)::numeric)),
    CONSTRAINT billing_holds_state_check CHECK ((state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text, 'SETTLED'::text, 'RELEASED'::text])))
);


--
-- Name: billing_prices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_prices (
    id uuid NOT NULL,
    currency text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb NOT NULL,
    basis text DEFAULT 'PROVIDER_USAGE'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_prices_basis_check CHECK ((basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'INPUT_CHARACTERS'::text, 'REQUEST_COMPLETION'::text]))),
    CONSTRAINT billing_prices_ceilings_check CHECK ((jsonb_typeof(ceilings) = 'object'::text)),
    CONSTRAINT billing_prices_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT billing_prices_rates_check CHECK ((jsonb_typeof(rates) = 'object'::text))
);


--
-- Name: billing_refunds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_refunds (
    id uuid NOT NULL,
    charge_id uuid NOT NULL,
    external_ref text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'CUSTOMER_REFUND'::text NOT NULL,
    CONSTRAINT billing_refunds_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT billing_refunds_reason_check CHECK ((reason = ANY (ARRAY['CUSTOMER_REFUND'::text, 'ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text])))
);


--
-- Name: billing_resolutions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_resolutions (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    action text NOT NULL,
    usage jsonb NOT NULL,
    reason text NOT NULL,
    pricing_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    seq bigint NOT NULL,
    CONSTRAINT billing_resolutions_action_check CHECK ((action = ANY (ARRAY['RELEASE'::text, 'SETTLE'::text]))),
    CONSTRAINT billing_resolutions_reason_check CHECK ((reason = ANY (ARRAY['PROVIDER_INVOICE'::text, 'ADMIN_WRITE_OFF'::text, 'EVIDENCE_RECONCILIATION'::text])))
);


--
-- Name: billing_resolutions_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.billing_resolutions ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.billing_resolutions_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: billing_tariffs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_tariffs (
    id uuid NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    provider_id uuid,
    currency text NOT NULL,
    pricing_id uuid NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    CONSTRAINT billing_tariffs_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text))
);


--
-- Name: charge_allocations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.charge_allocations (
    charge_id uuid NOT NULL,
    lot_id uuid NOT NULL,
    ordinal integer NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    refunded_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    CONSTRAINT charge_allocations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT charge_allocations_check CHECK (((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms)))
);


--
-- Name: cost_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_versions (
    id uuid NOT NULL,
    currency text NOT NULL,
    rates jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT cost_versions_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT cost_versions_rates_check CHECK ((jsonb_typeof(rates) = 'object'::text))
);


--
-- Name: event_consumers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.event_consumers (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    replay_cursor bigint DEFAULT 0 NOT NULL,
    replay_until bigint DEFAULT 0 NOT NULL
);


--
-- Name: event_deliveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.event_deliveries (
    consumer_id uuid NOT NULL,
    event_id uuid NOT NULL,
    state text DEFAULT 'PENDING'::text NOT NULL,
    lease_token uuid,
    lease_until timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    CONSTRAINT event_deliveries_state_check CHECK ((state = ANY (ARRAY['PENDING'::text, 'LEASED'::text, 'ACKED'::text])))
);


--
-- Name: funding_operations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.funding_operations (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    kind text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    lot_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT funding_operations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT funding_operations_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'REFUND'::text])))
);


--
-- Name: gateway_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gateway_requests (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    key_id uuid NOT NULL,
    operation text NOT NULL,
    model text NOT NULL,
    intent_digest text,
    fingerprint text NOT NULL,
    state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    billing_currency text,
    billing_quote jsonb,
    selected_attempt_id uuid,
    billing_outcome text DEFAULT 'PENDING'::text NOT NULL,
    CONSTRAINT gateway_requests_billing_outcome_check CHECK ((billing_outcome = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'CANCELLED'::text, 'FAILED'::text, 'UNKNOWN'::text]))),
    CONSTRAINT gateway_requests_state_check CHECK ((state = ANY (ARRAY['ADMITTED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'UNKNOWN'::text, 'CANCELLED'::text])))
);


--
-- Name: hold_allocations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hold_allocations (
    hold_id uuid NOT NULL,
    lot_id uuid NOT NULL,
    ordinal integer NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    CONSTRAINT hold_allocations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric))
);


--
-- Name: models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.models (
    name text NOT NULL,
    operations text[] NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    max_output_tokens integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    routing_strategy text DEFAULT 'WEIGHTED'::text NOT NULL,
    CONSTRAINT models_max_output_tokens_check CHECK ((max_output_tokens > 0)),
    CONSTRAINT models_routing_strategy_check CHECK ((routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text])))
);


--
-- Name: money_journal; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.money_journal (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    reference text NOT NULL,
    kind text NOT NULL,
    currency text NOT NULL,
    lines jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT money_journal_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'HOLD'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'EXPIRE'::text, 'DEBT_COLLECTION'::text, 'ADJUSTMENT'::text]))),
    CONSTRAINT money_journal_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organizations (
    id uuid NOT NULL,
    external_ref text NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer DEFAULT 100000 NOT NULL,
    tpm bigint DEFAULT 1000000000 NOT NULL,
    concurrency integer DEFAULT 10000 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT organizations_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT organizations_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT organizations_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);


--
-- Name: principals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.principals (
    id uuid NOT NULL,
    organization_id uuid NOT NULL,
    external_ref text NOT NULL,
    role text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer DEFAULT 10000 NOT NULL,
    tpm bigint DEFAULT 100000000 NOT NULL,
    concurrency integer DEFAULT 1000 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT principals_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT principals_role_check CHECK ((role = ANY (ARRAY['OWNER'::text, 'ADMIN'::text, 'MEMBER'::text, 'BILLING'::text, 'VIEWER'::text]))),
    CONSTRAINT principals_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT principals_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);


--
-- Name: projects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.projects (
    id uuid NOT NULL,
    name text NOT NULL,
    external_ref text,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer NOT NULL,
    tpm integer NOT NULL,
    concurrency integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    billing_currency text DEFAULT 'USD'::text NOT NULL,
    organization_id uuid,
    CONSTRAINT projects_billing_currency_check CHECK ((billing_currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT projects_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT projects_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT projects_tpm_check CHECK ((tpm > 0))
);


--
-- Name: providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.providers (
    id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    base_url text NOT NULL,
    secret text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    concurrency integer NOT NULL,
    rpm integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT providers_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT providers_kind_check CHECK ((kind = ANY (ARRAY['openai'::text, 'anthropic'::text, 'gemini'::text, 'deepseek'::text, 'ollama'::text, 'azure'::text, 'bedrock'::text, 'vertex'::text]))),
    CONSTRAINT providers_rpm_check CHECK ((rpm > 0))
);


--
-- Name: resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resources (
    id text NOT NULL,
    project_id uuid NOT NULL,
    provider_id uuid NOT NULL,
    remote_id text NOT NULL,
    kind text NOT NULL,
    model text,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    attempt_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    next_poll_at timestamp with time zone DEFAULT now() NOT NULL,
    poll_token uuid,
    poll_until timestamp with time zone,
    batch_endpoint text,
    batch_upstream_model text,
    batch_manifest jsonb,
    poll_count integer DEFAULT 0 NOT NULL,
    upstream_state text,
    CONSTRAINT resources_kind_check CHECK ((kind = ANY (ARRAY['file'::text, 'batch'::text, 'video'::text, 'response'::text, 'operation'::text]))),
    CONSTRAINT resources_upstream_state_check CHECK ((upstream_state = ANY (ARRAY['queued'::text, 'pending'::text, 'in_progress'::text, 'running'::text, 'completed'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'canceled'::text, 'cancelling'::text, 'validating'::text, 'finalizing'::text, 'expired'::text, 'processed'::text])))
);


--
-- Name: routes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routes (
    id uuid NOT NULL,
    model_name text NOT NULL,
    provider_id uuid NOT NULL,
    upstream_model text NOT NULL,
    operations text[] NOT NULL,
    priority integer NOT NULL,
    weight double precision NOT NULL,
    pricing_id uuid NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    CONSTRAINT routes_weight_check CHECK ((weight > (0)::double precision))
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version integer NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: usage_conflicts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_conflicts (
    id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    existing_event_id uuid NOT NULL,
    source text NOT NULL,
    finality text NOT NULL,
    usage jsonb NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: usage_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_events (
    id uuid NOT NULL,
    seq bigint NOT NULL,
    project_id uuid NOT NULL,
    request_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    source text NOT NULL,
    finality text NOT NULL,
    usage jsonb NOT NULL,
    cost_atoms numeric(38,0) NOT NULL,
    currency text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    cost_complete boolean DEFAULT false NOT NULL,
    CONSTRAINT usage_events_cost_atoms_check CHECK ((cost_atoms >= (0)::numeric)),
    CONSTRAINT usage_events_finality_check CHECK ((finality = ANY (ARRAY['FINAL'::text, 'PARTIAL'::text, 'UNKNOWN'::text, 'ESTIMATED'::text]))),
    CONSTRAINT usage_events_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);


--
-- Name: usage_events_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.usage_events ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.usage_events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: wallet_disputes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallet_disputes (
    wallet_id uuid NOT NULL,
    request_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: wallets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallets (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    currency text NOT NULL,
    policy text DEFAULT 'GIFT_FIRST'::text NOT NULL,
    available_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    spent_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    expired_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    debt_atoms numeric(38,0) DEFAULT 0 NOT NULL,
    freeze_reason text,
    has_expiring_lots boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manual_frozen boolean DEFAULT false NOT NULL,
    evidence_disputed boolean DEFAULT false NOT NULL,
    CONSTRAINT wallets_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallets_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT wallets_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT wallets_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallets_freeze_reason_check CHECK ((freeze_reason = ANY (ARRAY['MANUAL'::text, 'DEBT'::text, 'EVIDENCE_CONFLICT'::text]))),
    CONSTRAINT wallets_policy_check CHECK ((policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text]))),
    CONSTRAINT wallets_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallets_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: api_keys api_keys_verifier_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_verifier_key UNIQUE (verifier);


--
-- Name: attempts attempts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_pkey PRIMARY KEY (id);


--
-- Name: attempts attempts_request_id_ordinal_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_request_id_ordinal_key UNIQUE (request_id, ordinal);


--
-- Name: audit_events audit_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);


--
-- Name: billing_adjustments billing_adjustments_charge_id_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_charge_id_external_ref_key UNIQUE (charge_id, external_ref);


--
-- Name: billing_adjustments billing_adjustments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_pkey PRIMARY KEY (id);


--
-- Name: billing_charges billing_charges_hold_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_hold_id_key UNIQUE (hold_id);


--
-- Name: billing_charges billing_charges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_pkey PRIMARY KEY (id);


--
-- Name: billing_charges billing_charges_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_request_id_key UNIQUE (request_id);


--
-- Name: billing_holds billing_holds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_pkey PRIMARY KEY (id);


--
-- Name: billing_holds billing_holds_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_request_id_key UNIQUE (request_id);


--
-- Name: billing_prices billing_price_currency_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_prices
    ADD CONSTRAINT billing_price_currency_unique UNIQUE (id, currency);


--
-- Name: billing_prices billing_prices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_prices
    ADD CONSTRAINT billing_prices_pkey PRIMARY KEY (id);


--
-- Name: billing_refunds billing_refunds_charge_id_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_charge_id_external_ref_key UNIQUE (charge_id, external_ref);


--
-- Name: billing_refunds billing_refunds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_pkey PRIMARY KEY (id);


--
-- Name: billing_resolutions billing_resolutions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_pkey PRIMARY KEY (id);


--
-- Name: billing_resolutions billing_resolutions_request_id_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_external_ref_key UNIQUE (request_id, external_ref);


--
-- Name: billing_resolutions billing_resolutions_seq_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_seq_key UNIQUE (seq);


--
-- Name: billing_tariffs billing_tariffs_model_operation_provider_id_currency_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_model_operation_provider_id_currency_key UNIQUE NULLS NOT DISTINCT (model, operation, provider_id, currency);


--
-- Name: billing_tariffs billing_tariffs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_pkey PRIMARY KEY (id);


--
-- Name: charge_allocations charge_allocations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_pkey PRIMARY KEY (charge_id, lot_id);


--
-- Name: cost_versions cost_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_versions
    ADD CONSTRAINT cost_versions_pkey PRIMARY KEY (id);


--
-- Name: event_consumers event_consumers_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_consumers
    ADD CONSTRAINT event_consumers_name_key UNIQUE (name);


--
-- Name: event_consumers event_consumers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_consumers
    ADD CONSTRAINT event_consumers_pkey PRIMARY KEY (id);


--
-- Name: event_deliveries event_deliveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_pkey PRIMARY KEY (consumer_id, event_id);


--
-- Name: funding_operations funding_operations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_pkey PRIMARY KEY (id);


--
-- Name: funding_operations funding_operations_wallet_id_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_wallet_id_external_ref_key UNIQUE (wallet_id, external_ref);


--
-- Name: gateway_requests gateway_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_pkey PRIMARY KEY (id);


--
-- Name: gateway_requests gateway_requests_project_id_key_id_operation_intent_digest_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_project_id_key_id_operation_intent_digest_key UNIQUE (project_id, key_id, operation, intent_digest);


--
-- Name: hold_allocations hold_allocations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_pkey PRIMARY KEY (hold_id, lot_id);


--
-- Name: models models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_pkey PRIMARY KEY (name);


--
-- Name: money_journal money_journal_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_journal
    ADD CONSTRAINT money_journal_pkey PRIMARY KEY (id);


--
-- Name: money_journal money_journal_wallet_id_reference_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_journal
    ADD CONSTRAINT money_journal_wallet_id_reference_key UNIQUE (wallet_id, reference);


--
-- Name: organizations organizations_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_external_ref_key UNIQUE (external_ref);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: principals principals_organization_id_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_organization_id_external_ref_key UNIQUE (organization_id, external_ref);


--
-- Name: principals principals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_pkey PRIMARY KEY (id);


--
-- Name: projects projects_external_ref_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_external_ref_key UNIQUE (external_ref);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);


--
-- Name: providers providers_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers
    ADD CONSTRAINT providers_name_key UNIQUE (name);


--
-- Name: providers providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers
    ADD CONSTRAINT providers_pkey PRIMARY KEY (id);


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);


--
-- Name: resources resources_project_id_provider_id_kind_remote_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_project_id_provider_id_kind_remote_id_key UNIQUE (project_id, provider_id, kind, remote_id);


--
-- Name: routes routes_binding_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_binding_unique UNIQUE (model_name, provider_id, upstream_model, operations);


--
-- Name: routes routes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: usage_conflicts usage_conflicts_attempt_id_source_fingerprint_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_attempt_id_source_fingerprint_key UNIQUE (attempt_id, source, fingerprint);


--
-- Name: usage_conflicts usage_conflicts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_pkey PRIMARY KEY (id);


--
-- Name: usage_events usage_events_attempt_id_source_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_attempt_id_source_key UNIQUE (attempt_id, source);


--
-- Name: usage_events usage_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_pkey PRIMARY KEY (id);


--
-- Name: usage_events usage_events_seq_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_seq_key UNIQUE (seq);


--
-- Name: wallet_disputes wallet_disputes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_disputes
    ADD CONSTRAINT wallet_disputes_pkey PRIMARY KEY (wallet_id, request_id);


--
-- Name: wallet_lots wallet_lots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_pkey PRIMARY KEY (id);


--
-- Name: wallet_lots wallet_lots_wallet_id_bucket_digest_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_bucket_digest_key UNIQUE (wallet_id, bucket_digest);


--
-- Name: wallets wallets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_pkey PRIMARY KEY (id);


--
-- Name: wallets wallets_project_id_currency_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_project_id_currency_key UNIQUE (project_id, currency);


--
-- Name: api_keys_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX api_keys_project ON public.api_keys USING btree (project_id, created_at);


--
-- Name: attempts_stale; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX attempts_stale ON public.attempts USING btree (started_at) WHERE (state = 'DISPATCHING'::text);


--
-- Name: billing_holds_wallet; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX billing_holds_wallet ON public.billing_holds USING btree (wallet_id, created_at, id) WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));


--
-- Name: charges_unpaid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX charges_unpaid ON public.billing_charges USING btree (hold_id, created_at, id) WHERE (debt_atoms > (0)::numeric);


--
-- Name: delivery_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX delivery_due ON public.event_deliveries USING btree (consumer_id, event_id) WHERE (state <> 'ACKED'::text);


--
-- Name: holds_all_wallet; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX holds_all_wallet ON public.billing_holds USING btree (wallet_id, request_id);


--
-- Name: holds_reconcile_due; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX holds_reconcile_due ON public.billing_holds USING btree (next_reconcile_at, request_id) WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));


--
-- Name: journal_wallet; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX journal_wallet ON public.money_journal USING btree (wallet_id, created_at DESC, id);


--
-- Name: requests_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX requests_project ON public.gateway_requests USING btree (project_id, created_at DESC, id);


--
-- Name: requests_stale; Type: INDEX; Schema: public; Owner: -
--



--
-- Name: resources_poll; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX resources_poll ON public.resources USING btree (next_poll_at, id) WHERE ((state = ANY (ARRAY['ACTIVE'::text, 'PENDING_USAGE'::text])) AND (kind = ANY (ARRAY['batch'::text, 'video'::text, 'response'::text, 'operation'::text])));


--
-- Name: resources_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX resources_project ON public.resources USING btree (project_id, kind, created_at DESC, id);


--
-- Name: routes_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routes_lookup ON public.routes USING btree (model_name, priority DESC) WHERE enabled;


--
-- Name: usage_attempt; Type: INDEX; Schema: public; Owner: -
--



--
-- Name: usage_conflicts_attempt; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_conflicts_attempt ON public.usage_conflicts USING btree (attempt_id);


--
-- Name: usage_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_project ON public.usage_events USING btree (project_id, created_at DESC, id);


--
-- Name: wallet_lots_available; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX wallet_lots_wallet ON public.wallet_lots USING btree (wallet_id, kind, priority, expires_at, id);


--
-- Name: wallet_lots_expiring; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX wallet_lots_expiring ON public.wallet_lots USING btree (expires_at, id) WHERE (kind = 'GIFT'::text);


--
-- Name: api_keys audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.api_keys FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: billing_prices audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: billing_tariffs audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.billing_tariffs FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: cost_versions audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: event_consumers audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.event_consumers FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: models audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.models FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: organizations audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.organizations FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: principals audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.principals FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: projects audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.projects FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: providers audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.providers FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: routes audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.routes FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: wallets audit_config; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE OF policy, manual_frozen ON public.wallets FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();


--
-- Name: gateway_requests authorize_dispatch; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER authorize_dispatch BEFORE INSERT ON public.gateway_requests FOR EACH ROW EXECUTE FUNCTION public.authorize_gateway_dispatch();


--
-- Name: money_journal balanced_journal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER balanced_journal BEFORE INSERT ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.validate_money_journal();


--
-- Name: resources billing_resource; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER billing_resource AFTER UPDATE OF upstream_state ON public.resources FOR EACH ROW WHEN ((old.upstream_state IS DISTINCT FROM new.upstream_state)) EXECUTE FUNCTION public.resource_billing_state();


--
-- Name: billing_adjustments immutable_adjustments; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_adjustments BEFORE DELETE OR UPDATE ON public.billing_adjustments FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: audit_events immutable_audit; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_audit BEFORE DELETE OR UPDATE ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: billing_prices immutable_billing_prices; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_billing_prices BEFORE DELETE OR UPDATE ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: usage_conflicts immutable_conflicts; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_conflicts BEFORE DELETE OR UPDATE ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: wallet_disputes immutable_disputes; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_disputes BEFORE DELETE OR UPDATE ON public.wallet_disputes FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: funding_operations immutable_funding; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_funding BEFORE DELETE OR UPDATE ON public.funding_operations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: money_journal immutable_journal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_journal BEFORE DELETE OR UPDATE ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: cost_versions immutable_prices; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_prices BEFORE DELETE OR UPDATE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: billing_refunds immutable_refunds; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_refunds BEFORE DELETE OR UPDATE ON public.billing_refunds FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: billing_resolutions immutable_resolutions; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_resolutions BEFORE DELETE OR UPDATE ON public.billing_resolutions FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: usage_events immutable_usage; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER immutable_usage BEFORE DELETE OR UPDATE ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();


--
-- Name: api_keys key_principal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER key_principal BEFORE INSERT OR UPDATE ON public.api_keys FOR EACH ROW EXECUTE FUNCTION public.validate_key_principal();


--
-- Name: projects project_organization; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER project_organization BEFORE UPDATE OF organization_id ON public.projects FOR EACH ROW EXECUTE FUNCTION public.validate_project_organization();


--
-- Name: billing_resolutions resolution_usage; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER resolution_usage BEFORE INSERT ON public.billing_resolutions FOR EACH ROW EXECUTE FUNCTION public.validate_usage();


--
-- Name: usage_conflicts usage_whitelist; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER usage_whitelist BEFORE INSERT ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.validate_usage();


--
-- Name: usage_events usage_whitelist; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER usage_whitelist BEFORE INSERT ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.validate_usage();


--
-- Name: billing_prices valid_tariff; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER valid_tariff BEFORE INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.validate_billing_price();


--
-- Name: usage_conflicts wallet_dispute_fence; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER wallet_dispute_fence AFTER INSERT ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.register_wallet_dispute();


--
-- Name: api_keys api_keys_principal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_principal_id_fkey FOREIGN KEY (principal_id) REFERENCES public.principals(id);


--
-- Name: api_keys api_keys_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);


--
-- Name: attempts attempts_billing_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_billing_pricing_id_fkey FOREIGN KEY (billing_pricing_id) REFERENCES public.billing_prices(id);


--
-- Name: attempts attempts_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.usage_events(id);


--
-- Name: attempts attempts_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.cost_versions(id);


--
-- Name: attempts attempts_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: attempts attempts_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);


--
-- Name: billing_adjustments billing_adjustments_charge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);


--
-- Name: billing_charges billing_charges_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.usage_events(id);


--
-- Name: billing_charges billing_charges_hold_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_hold_id_fkey FOREIGN KEY (hold_id) REFERENCES public.billing_holds(id);


--
-- Name: billing_charges billing_charges_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);


--
-- Name: billing_charges billing_charges_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);


--
-- Name: billing_holds billing_holds_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);


--
-- Name: billing_holds billing_holds_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);


--
-- Name: billing_refunds billing_refunds_charge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);


--
-- Name: billing_resolutions billing_resolutions_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);


--
-- Name: billing_resolutions billing_resolutions_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);


--
-- Name: billing_tariffs billing_tariffs_model_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_model_fkey FOREIGN KEY (model) REFERENCES public.models(name);


--
-- Name: billing_tariffs billing_tariffs_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);


--
-- Name: billing_tariffs billing_tariffs_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: charge_allocations charge_allocations_charge_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);


--
-- Name: charge_allocations charge_allocations_lot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);


--
-- Name: event_deliveries event_deliveries_consumer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES public.event_consumers(id);


--
-- Name: event_deliveries event_deliveries_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.usage_events(id);


--
-- Name: funding_operations funding_operations_lot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);


--
-- Name: funding_operations funding_operations_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);


--
-- Name: gateway_requests gateway_requests_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_key_id_fkey FOREIGN KEY (key_id) REFERENCES public.api_keys(id);


--
-- Name: gateway_requests gateway_requests_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);


--
-- Name: gateway_requests gateway_requests_selected_attempt_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_selected_attempt_id_fkey FOREIGN KEY (selected_attempt_id) REFERENCES public.attempts(id);


--
-- Name: hold_allocations hold_allocations_hold_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_hold_id_fkey FOREIGN KEY (hold_id) REFERENCES public.billing_holds(id);


--
-- Name: hold_allocations hold_allocations_lot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);


--
-- Name: money_journal money_journal_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.money_journal
    ADD CONSTRAINT money_journal_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);


--
-- Name: principals principals_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: projects projects_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: resources resources_attempt_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);


--
-- Name: resources resources_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);


--
-- Name: resources resources_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: routes routes_model_name_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_model_name_fkey FOREIGN KEY (model_name) REFERENCES public.models(name);


--
-- Name: routes routes_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.cost_versions(id);


--
-- Name: routes routes_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: billing_tariffs tariff_currency_matches; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT tariff_currency_matches FOREIGN KEY (pricing_id, currency) REFERENCES public.billing_prices(id, currency);


--
-- Name: usage_conflicts usage_conflicts_attempt_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);


--
-- Name: usage_conflicts usage_conflicts_existing_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_existing_event_id_fkey FOREIGN KEY (existing_event_id) REFERENCES public.usage_events(id);


--
-- Name: usage_events usage_events_attempt_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);


--
-- Name: usage_events usage_events_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);


--
-- Name: usage_events usage_events_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_events
    ADD CONSTRAINT usage_events_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);


--
-- Name: wallet_lots wallet_lots_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);


--
-- Name: wallets wallets_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);


--
-- PostgreSQL database dump complete
--



REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

ALTER TABLE public.wallets SET (fillfactor=70);
ALTER TABLE public.wallet_lots SET (fillfactor=70);

ALTER TABLE public.gateway_requests SET (fillfactor=70);
