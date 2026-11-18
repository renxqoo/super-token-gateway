CREATE TYPE core_funding.first_final_cost_v1 AS (
 hold public.upstream_holds,
 evidence_id uuid,
 snapshot_revision numeric,
 snapshot_digest text,
 final_dimensions text[],
 cost_atoms numeric,
 journal_id uuid
);

CREATE FUNCTION core_funding.flush_first_final_costs_v1(items core_funding.first_final_cost_v1[],pending jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 item core_funding.first_final_cost_v1;
 cells jsonb=jsonb_build_object('cells',pending->'cells');
 journals jsonb;
 entries jsonb;
 changed integer;
BEGIN
 FOREACH item IN ARRAY items LOOP
  IF item.journal_id IS NOT NULL THEN
   cells=core_funding.project_v1(item.hold,item.cost_atoms,0,cells);
  END IF;
 END LOOP;

 INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id)
 SELECT (s.hold).attempt_id,1,(s.hold).currency,s.cost_atoms,true,false,
  public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(s.final_dimensions),'cost_version_id',(s.hold).cost_version_id,'estimated_quantities_digest',(s.hold).estimated_quantities_digest,'cost_atoms',s.cost_atoms::text)),
  s.evidence_id,(s.hold).cost_version_id
 FROM unnest(items) s;
 INSERT INTO public.upstream_cost_heads(attempt_id,revision)
 SELECT (s.hold).attempt_id,1 FROM unnest(items) s;
 UPDATE public.upstream_holds h SET state='SETTLED',current_reserved_atoms=0,final_cost_atoms=s.cost_atoms
 FROM unnest(items) s WHERE h.attempt_id=(s.hold).attempt_id AND h.state='HELD'
 AND h.current_reserved_atoms=(s.hold).current_reserved_atoms;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>cardinality(items) THEN RAISE EXCEPTION 'evidence_hold_base_changed' USING ERRCODE='P0500';END IF;

 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.journal_id,'account_id',(s.hold).account_id,'channel_id',(s.hold).channel_id,
  'attempt_id',(s.hold).attempt_id,'currency',(s.hold).currency,'kind',CASE WHEN s.cost_atoms<>0 THEN 'COST' ELSE 'RISK' END,
  'reference','cost:'||(s.hold).attempt_id||':'||s.snapshot_revision)),'[]')
 INTO journals FROM unnest(items) s WHERE s.journal_id IS NOT NULL;
 SELECT coalesce(jsonb_agg(to_jsonb(leg)),'[]') INTO entries FROM (
  SELECT s.journal_id,row_number() OVER (PARTITION BY s.journal_id ORDER BY v.n)-1 AS ordinal,(s.hold).currency,v.account,v.atoms
  FROM unnest(items) s CROSS JOIN LATERAL (VALUES
   (0,'upstream:available',(s.hold).current_reserved_atoms-s.cost_atoms),
   (1,'upstream:reserved',-(s.hold).current_reserved_atoms),
   (2,'upstream:expense',s.cost_atoms)
  ) v(n,account,atoms) WHERE s.journal_id IS NOT NULL AND v.atoms<>0
 ) leg;
 RETURN pending||cells||jsonb_build_object('journals',(pending->'journals')||journals,'entries',(pending->'entries')||entries);
END $$;

REVOKE ALL ON TYPE core_funding.first_final_cost_v1 FROM PUBLIC;
REVOKE ALL ON FUNCTION core_funding.flush_first_final_costs_v1(core_funding.first_final_cost_v1[],jsonb) FROM PUBLIC;
DO $$
DECLARE owner name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='core_funding.persist_v1(jsonb,boolean)'::regprocedure));
BEGIN
 EXECUTE format('GRANT USAGE ON TYPE core_funding.first_final_cost_v1 TO %I',owner);
 EXECUTE format('GRANT EXECUTE ON FUNCTION core_funding.flush_first_final_costs_v1(core_funding.first_final_cost_v1[],jsonb) TO %I',owner);
END $$;

CREATE FUNCTION core_funding.bill_first_final_v1(p_requests uuid[],snapshots core_funding.evidence_snapshot_stage_v1[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE receipts jsonb;journals jsonb;
BEGIN
 WITH facts AS MATERIALIZED (
  SELECT (f).id,(f).attempt_id,(f).request_id,(f).source,(f).finality,x.usage
  FROM unnest(snapshots) x(f,usage) WHERE (f).record_kind='CANONICAL'
 ), targets AS MATERIALIZED (
  SELECT h.* FROM billing_holds h WHERE h.request_id=ANY(p_requests)
 ), candidates AS MATERIALIZED (
  SELECT h.id AS hold_id,h.request_id,h.wallet_id,h.amount_atoms AS held,w.currency,
   l.id AS lot_id,p.id AS pricing_id,e.id AS evidence_id,exact_billing_price(CASE p.basis WHEN 'REQUEST_COMPLETION' THEN '{"requests":"1"}'::jsonb ELSE e.usage END,p.rates) AS actual
  FROM targets h JOIN wallets w ON w.id=h.wallet_id
  JOIN LATERAL(SELECT id,selected_attempt_id,operation,billing_outcome FROM gateway_requests WHERE id=h.request_id OFFSET 0) q ON true
  JOIN LATERAL(SELECT id,evidence_id,billing_pricing_id,state FROM attempts WHERE id=q.selected_attempt_id OFFSET 0) a ON true
  JOIN facts e ON (e.id,e.attempt_id,e.request_id)=(a.evidence_id,a.id,q.id)
  JOIN billing_prices p ON p.id=a.billing_pricing_id
  JOIN LATERAL(SELECT * FROM hold_allocations WHERE hold_id=h.id AND ordinal=1 AND amount_atoms=h.amount_atoms OFFSET 0) s ON true
  JOIN wallet_lots l ON l.id=s.lot_id AND l.wallet_id=w.id AND l.expires_at IS NULL
  WHERE h.state IN ('HELD','RECONCILING') AND h.credit_atoms=0 AND w.debt_atoms=0 AND w.freeze_reason IS NULL AND NOT w.evidence_disputed
   AND q.operation IN ('chat','messages','gemini','embeddings','completions') AND q.billing_outcome='SUCCESS'
   AND a.state='SUCCEEDED' AND e.source='execution' AND e.finality='FINAL' AND p.basis IN ('PROVIDER_USAGE','REQUEST_COMPLETION') AND NOT w.has_expiring_lots AND p.currency=w.currency
   AND (p.basis='REQUEST_COMPLETION' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p.rates) dimension WHERE NOT e.usage?dimension))
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
EXCEPTION WHEN numeric_value_out_of_range OR check_violation OR raise_exception OR invalid_parameter_value OR insufficient_privilege THEN RETURN '{}'::jsonb;
END $function$;

REVOKE ALL ON FUNCTION core_funding.bill_first_final_v1(uuid[],core_funding.evidence_snapshot_stage_v1[]) FROM PUBLIC;
DO $$
DECLARE owner name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='core_funding.persist_v1(jsonb,boolean)'::regprocedure));
BEGIN
 EXECUTE format('GRANT EXECUTE ON FUNCTION core_funding.bill_first_final_v1(uuid[],core_funding.evidence_snapshot_stage_v1[]) TO %I',owner);
END $$;

CREATE OR REPLACE FUNCTION core_funding.evidence_group_v1(g jsonb,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 delivered boolean[];event_claims boolean[];revision_ids uuid[];revision_digests text[];blocked_groups integer[]='{}';
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;auth text[];invalid_subject boolean;basis text;seen_final boolean;first_copy integer;snapshot core_funding.evidence_snapshot_stage_v1;
 stages core_funding.evidence_attempt_stage_v1[];
 requests core_funding.evidence_request_stage_v1[];
 stage core_funding.evidence_attempt_stage_v1;
 request_row core_funding.evidence_request_stage_v1;
 staged_receipt public.core_evidence_receipts%ROWTYPE;
 staged_receipts public.core_evidence_receipts[]='{}';
 staged_snapshots core_funding.evidence_snapshot_stage_v1[]='{}';
 exposures jsonb[]='{}';
 first_costs core_funding.first_final_cost_v1[]='{}';
 first_cost core_funding.first_final_cost_v1;
 first_final boolean=false; close_together boolean=false; closed_count integer;
 stage_index integer;request_index integer;
BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') e(value) ORDER BY value->>'attempt_id',value->>'origin_identity',value->>'source',lpad(value->>'source_revision',38,'0'),value->>'event_id' LOOP
  IF jsonb_typeof(ev) IS DISTINCT FROM 'object' OR jsonb_typeof(ev->'signature') IS DISTINCT FROM 'string' OR ev->>'signature' !~ '^[a-f0-9]{128}$'
   OR jsonb_typeof(ev->'signature_payload_digest') IS DISTINCT FROM 'string' OR ev->>'signature_payload_digest' !~ '^[a-f0-9]{64}$'
   OR jsonb_typeof(ev->'producer_key_sha256') IS DISTINCT FROM 'string' OR ev->>'producer_key_sha256' !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
  PERFORM public.core_evidence_validate_v1(ev-ARRAY['signature','signature_payload_digest','producer_key_sha256']);
  events=events||(ev||jsonb_build_object('body_digest',public.core_digest_v1(ev-ARRAY['event_id','signature','signature_payload_digest','producer_key_sha256'])));
 END LOOP;
 n=cardinality(events);
 FOR k IN SELECT DISTINCT key FROM unnest(events) e,LATERAL (VALUES('core-event:'||(e->>'origin_identity')||':'||(e->>'event_id')),('core-revision:'||(e->>'attempt_id')||':'||(e->>'origin_identity')||':'||(e->>'source')||':'||(e->>'source_revision'))) l(key) ORDER BY key LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(k,81442607)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM unnest(events) e WHERE NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=(e->>'origin_session_id')::uuid)) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
 PERFORM 1 FROM public.internal_service_instances WHERE (service,instance_id) IN(SELECT s.service,s.instance_id FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY service COLLATE "C",instance_id COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_service_certificates WHERE certificate_sha256 IN(SELECT s.certificate_sha256 FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY certificate_sha256 COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_startup_sessions WHERE session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e) ORDER BY session_id FOR SHARE NOWAIT;
 attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
 request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
 PERFORM core_funding.share_v1(ARRAY(SELECT DISTINCT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY account_id));
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM 1 FROM public.wallets WHERE id IN(SELECT wallet_id FROM public.billing_holds WHERE request_id=ANY(request_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.gateway_requests WHERE id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.billing_holds WHERE request_id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.attempts WHERE id=ANY(attempt_ids) ORDER BY request_id,ordinal,id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.execution_permits WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR UPDATE NOWAIT;
 WITH event_keys AS MATERIALIZED (
  SELECT data,ordinal,(data->>'attempt_id')::uuid AS attempt_id,(data->>'origin_session_id')::uuid AS session_id
  FROM unnest(events) WITH ORDINALITY e(data,ordinal)
 ), producers AS MATERIALIZED (
  SELECT s.* FROM (SELECT DISTINCT session_id FROM event_keys) k
  JOIN LATERAL (
   SELECT s.session_id,s.service,s.instance_id,s.incarnation_id,s.public_key_sha256,s.certificate_sha256,
    s.security_revoked_at IS NOT NULL OR i.security_revoked_at IS NOT NULL OR c.security_revoked_at IS NOT NULL AS revoked
   FROM public.internal_startup_sessions s
   LEFT JOIN public.internal_service_instances i ON (i.service,i.instance_id)=(s.service,s.instance_id)
   LEFT JOIN public.internal_service_certificates c ON c.certificate_sha256=s.certificate_sha256
   WHERE s.session_id=k.session_id OFFSET 0
  ) s ON true
 ), permits AS MATERIALIZED (
  SELECT ep.* FROM unnest(attempt_ids) a(id) JOIN LATERAL (
   SELECT p.attempt_id,p.state,p.granted_at,p.permit_epoch,p.owner_service,p.gateway_instance_id,
    p.owner_session_id,p.incarnation_id,p.owner_key_sha256,p.certificate_sha256,
    EXISTS (SELECT 1 FROM public.upstream_holds uh WHERE uh.attempt_id=p.attempt_id AND uh.estimated_quantities IS NOT NULL) AS estimated
   FROM public.execution_permits p WHERE p.attempt_id=a.id OFFSET 0
  ) ep ON true
 )
 SELECT array_agg(CASE WHEN s.revoked THEN 'SECURITY_REVOKED' WHEN p.state IN ('RESERVED','ABORTED') THEN 'UNGRANTED' ELSE 'NORMAL' END ORDER BY e.ordinal),
  array_agg(EXISTS (
   SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.body_digest=e.data->>'body_digest'
  ) ORDER BY e.ordinal),
  array_agg(EXISTS (
   SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.event_claim
  ) ORDER BY e.ordinal),
  array_agg(revision.id ORDER BY e.ordinal),array_agg(revision.body_digest ORDER BY e.ordinal),
  bool_or(
   p.attempt_id IS NULL OR NOT (p.state IN ('SEND_GRANTED','UNKNOWN','TERMINAL') OR (p.state IN ('RESERVED','ABORTED') AND p.granted_at IS NULL))
   OR s.service||':'||s.instance_id IS DISTINCT FROM e.data->>'origin_identity'
   OR s.incarnation_id IS DISTINCT FROM (e.data->>'origin_incarnation')::uuid
   OR s.public_key_sha256 IS DISTINCT FROM e.data->>'producer_key_sha256'
   OR p.permit_epoch IS DISTINCT FROM (e.data->>'permit_epoch')::numeric
   OR ((p.owner_service||':'||p.gateway_instance_id IS DISTINCT FROM e.data->>'origin_identity'
    OR p.owner_session_id IS DISTINCT FROM s.session_id OR p.incarnation_id IS DISTINCT FROM s.incarnation_id
    OR (p.owner_key_sha256 IS NOT NULL AND p.owner_key_sha256<>s.public_key_sha256)
    OR (p.certificate_sha256 IS NOT NULL AND p.certificate_sha256<>s.certificate_sha256))
    AND NOT (e.data->>'source' IN ('async_partial','async_final','batch_completed','batch_cancelled','batch_failed') AND EXISTS (
     SELECT 1 FROM public.resources r WHERE r.attempt_id=p.attempt_id AND r.lease_session_id=s.session_id AND r.lease_epoch=(e.data->>'source_revision')::numeric
    )))
   OR NOT p.estimated
   OR EXISTS (
    SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.attempt_id<>p.attempt_id
   )
  ) INTO auth,delivered,event_claims,revision_ids,revision_digests,invalid_subject
 FROM event_keys e
 LEFT JOIN permits p ON p.attempt_id=e.attempt_id
 LEFT JOIN producers s ON s.session_id=e.session_id
 LEFT JOIN LATERAL (
  SELECT r.id,r.body_digest FROM public.core_evidence_receipts r
  WHERE r.attempt_id=e.attempt_id AND r.origin_identity=e.data->>'origin_identity' AND r.source=e.data->>'source' AND r.source_revision=(e.data->>'source_revision')::numeric AND r.revision_claim
  OFFSET 0
 ) revision ON true;
 IF invalid_subject THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;

 IF pending?'partition' AND EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE auth[j]<>'NORMAL' OR delivered[j] OR event_claims[j] OR revision_ids[j] IS NOT NULL) THEN
  RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
 END IF;
 mode=array_fill('NEW'::text,ARRAY[n]);contested=array_fill(false,ARRAY[n]);
 FOR i IN 1..n LOOP
  ev=events[i];
  IF ev->>'finality'='NOT_EXECUTED' THEN
   SELECT v.rates INTO rates FROM public.upstream_holds hh JOIN public.cost_versions v ON v.id=hh.cost_version_id WHERE hh.attempt_id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(p.rates,'{}') INTO prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(jsonb_object_agg(key,'0'),'{}') INTO usage FROM (SELECT key FROM jsonb_each(rates) UNION SELECT key FROM jsonb_each(prices)) dims;
   events[i]=ev||jsonb_build_object('normalized_usage',usage,'normalized_finals',(SELECT coalesce(jsonb_agg(key ORDER BY key),'[]') FROM jsonb_each(usage)));
  ELSE
   events[i]=ev||jsonb_build_object('normalized_usage',ev->'usage','normalized_finals',ev->'final_dimensions');
  END IF;
 END LOOP;
 FOR i IN 1..n LOOP
  ev=events[i];
  IF delivered[i] OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'=ev->>'body_digest') THEN mode[i]='DUPLICATE';CONTINUE;END IF;
  IF auth[i]<>'NORMAL' THEN mode[i]='QUARANTINED';CONTINUE;END IF;
  IF event_claims[i] OR (revision_ids[i] IS NOT NULL AND revision_digests[i]<>ev->>'body_digest')
   OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND ((events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'<>ev->>'body_digest') OR (events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND events[j]->>'body_digest'<>ev->>'body_digest')))
  THEN mode[i]='CONFLICT';contested[i]=true;CONTINUE;END IF;
  IF revision_ids[i] IS NOT NULL OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND mode[j] IN ('NEW','REPLAY')) THEN mode[i]='REPLAY';CONTINUE;END IF;
  a=(ev->>'attempt_id')::uuid;
  IF NOT claims?a::text THEN
   SELECT i2.final_dimensions,p.usage INTO head FROM public.usage_event_index i2 JOIN public.usage_payloads p ON p.id=i2.id AND p.created_at=i2.created_at WHERE i2.attempt_id=a AND i2.record_kind='CANONICAL' ORDER BY i2.snapshot_revision DESC LIMIT 1;
   claims=claims||jsonb_build_object(a::text,coalesce((SELECT jsonb_object_agg(f,jsonb_build_object('value',head.usage->>f,'owner',0)) FROM unnest(head.final_dimensions) f),'{}'));
  END IF;
  FOR d,q IN SELECT f,ev->'normalized_usage'->>f FROM jsonb_array_elements_text(ev->'normalized_finals') f LOOP
   claim=claims->a::text->d;
   IF claim IS NULL THEN claims=jsonb_set(claims,ARRAY[a::text,d],jsonb_build_object('value',q,'owner',i));
   ELSIF (claim->>'value')::numeric<>q::numeric OR coalesce((claim->>'contested')::boolean,false) THEN
    contested[i]=true;owner=(claim->>'owner')::integer;
    IF owner>0 THEN contested[owner]=true;claims=jsonb_set(claims,ARRAY[a::text,d,'contested'],'true');END IF;
   END IF;
  END LOOP;
 END LOOP;
 FOR i IN 1..n LOOP
  CONTINUE WHEN mode[i]<>'REPLAY';
  SELECT min(j2) INTO first_copy FROM generate_series(1,i-1) j2 WHERE events[j2]->>'origin_identity'=events[i]->>'origin_identity' AND events[j2]->>'attempt_id'=events[i]->>'attempt_id' AND events[j2]->>'source'=events[i]->>'source' AND events[j2]->>'source_revision'=events[i]->>'source_revision' AND mode[j2] IN ('NEW','REPLAY');
  IF first_copy IS NOT NULL AND contested[first_copy] THEN contested[i]=true;mode[i]='NEW';END IF;
 END LOOP;
 IF pending?'partition' THEN
  IF EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL')
   OR EXISTS(
    SELECT 1 FROM unnest(attempt_ids) ids(attempt_id)
    LEFT JOIN LATERAL(SELECT request_id FROM public.attempts WHERE id=ids.attempt_id OFFSET 0) t ON true
    LEFT JOIN LATERAL(SELECT wallet_id FROM public.billing_holds WHERE request_id=t.request_id OFFSET 0) bh ON true
    LEFT JOIN LATERAL(SELECT has_expiring_lots FROM public.wallets WHERE id=bh.wallet_id OFFSET 0) w ON true
    GROUP BY bh.wallet_id HAVING bh.wallet_id IS NULL OR bool_or(w.has_expiring_lots) OR count(DISTINCT pending->'partition'->>ids.attempt_id::text)>1
   ) THEN RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';END IF;
 END IF;
 IF pending IS NOT NULL AND NOT EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL') THEN
  IF pending?'partition' THEN
   WITH required AS MATERIALIZED (
    SELECT DISTINCT (pending->'partition'->>hh.attempt_id::text)::integer group_number,hh.account_id,hh.currency,hh.funding_cell
    FROM unnest(events) e JOIN LATERAL (
     SELECT attempt_id,account_id,currency,funding_cell FROM public.upstream_holds WHERE attempt_id=(e->>'attempt_id')::uuid OFFSET 0
    ) hh ON true
    WHERE e->>'finality'<>'ESTIMATED'
   ), acquired AS MATERIALIZED (
    SELECT c.account_id,c.currency,c.cell FROM core_funding.balance_cells c
    WHERE (c.account_id,c.currency,c.cell) IN(SELECT r.account_id,r.currency,r.funding_cell FROM required r)
    ORDER BY c.account_id,c.currency COLLATE "C",c.cell FOR NO KEY UPDATE SKIP LOCKED
   )
   SELECT coalesce(array_agg(DISTINCT r.group_number),'{}') INTO blocked_groups FROM required r
   LEFT JOIN acquired ac ON (ac.account_id,ac.currency,ac.cell)=(r.account_id,r.currency,r.funding_cell)
   WHERE ac.account_id IS NULL;
   IF cardinality(blocked_groups)>0 THEN
    SELECT coalesce(array_agg(e.value ORDER BY e.ordinal),'{}') INTO events FROM unnest(events) WITH ORDINALITY e(value,ordinal)
    WHERE NOT ((pending->'partition'->>((e.value->>'attempt_id')::uuid)::text)::integer=ANY(blocked_groups));
    n=cardinality(events);
    IF n=0 THEN
     RETURN jsonb_build_object('result',jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing),'pending',pending,'blocked_groups',to_jsonb(blocked_groups));
    END IF;
    attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
    request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
   END IF;
  ELSE
   PERFORM 1 FROM core_funding.balance_cells c
    WHERE (c.account_id,c.currency,c.cell) IN(SELECT hh.account_id,hh.currency,hh.funding_cell FROM public.upstream_holds hh WHERE hh.attempt_id IN(SELECT (e->>'attempt_id')::uuid FROM unnest(events) e WHERE e->>'finality'<>'ESTIMATED'))
    ORDER BY c.account_id,c.currency COLLATE "C",c.cell FOR NO KEY UPDATE NOWAIT;
  END IF;
  stages=core_funding.evidence_attempt_stages_v1(attempt_ids);
  IF cardinality(stages)<>cardinality(attempt_ids) THEN RAISE EXCEPTION 'evidence_stage_missing' USING ERRCODE='P0500';END IF;
  first_final=NOT EXISTS(SELECT 1 FROM unnest(events) e WHERE e->>'source'<>'execution' OR e->>'finality'<>'FINAL')
   AND NOT EXISTS(SELECT 1 FROM unnest(stages) st JOIN public.upstream_accounts ac ON ac.id=(st.hold).account_id
     WHERE st.revision<>0 OR (st.hold).state<>'HELD' OR ac.state<>'ACTIVE')
   AND NOT EXISTS(SELECT 1 FROM public.upstream_costs c WHERE c.attempt_id=ANY(attempt_ids))
   AND NOT EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=ANY(attempt_ids));
  requests=ARRAY(
   SELECT ROW(q.id,q.selected_attempt_id,q.state,q.billing_outcome,NULL,false)::core_funding.evidence_request_stage_v1
   FROM public.gateway_requests q WHERE q.id=ANY(request_ids) ORDER BY q.id
  );
  IF cardinality(requests)<>cardinality(request_ids) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
 END IF;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=(contested[i] OR mode[i]='DUPLICATE');
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    IF prior.signature_payload_digest<>ev->>'signature_payload_digest' OR prior.producer_key_sha256<>ev->>'producer_key_sha256' THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   IF stages IS NULL THEN
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT p.basis,coalesce(p.rates,'{}') INTO basis,prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=a;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   ELSE
    stage_index=array_position(attempt_ids,a);
    stage=stages[stage_index];
    h=stage.hold;rates=stage.rates;basis=stage.basis;prices=stage.prices;
    cur.id=stage.head_id;cur.final_dimensions=stage.finals;merged=stage.usage;
   END IF;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   IF stages IS NULL THEN event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);ELSE event_claimed=false;END IF;
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    IF rev_prior.id IS NULL OR rev_prior.body_digest<>ev->>'body_digest' THEN RAISE EXCEPTION 'evidence_replay_unanchored' USING ERRCODE='P0503';END IF;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,rev_prior.canonical_evidence_id,'{}','VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   IF stages IS NULL THEN raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     raw=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
   IF mode[i]='QUARANTINED' THEN
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'QUARANTINED',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,'{}','QUARANTINED',auth[i],ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','QUARANTINED','duplicate',false));CONTINUE;
   END IF;
   IF ev->>'finality'='PARTIAL' THEN
    risk=ARRAY(SELECT d2 FROM jsonb_object_keys(rates) d2 WHERE usage?d2 AND NOT d2=ANY(finals) AND NOT d2=ANY(merged_finals) AND (usage->>d2)::numeric>(h.estimated_quantities->>d2)::numeric ORDER BY d2);
   END IF;
   IF contested[i] THEN
    outcome='CONFLICT';merged=merged||usage;merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
    existing=coalesce(cur.id,(SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND (r.event_id=(ev->>'event_id')::uuid OR (r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric)) ORDER BY r.created_at LIMIT 1),
     (SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='CONFLICT' ORDER BY r.created_at LIMIT 1),
     (SELECT i3.id FROM public.usage_event_index i3 WHERE i3.attempt_id=a AND i3.record_kind='RAW' AND i3.id<>(raw->>'id')::uuid ORDER BY i3.snapshot_revision DESC LIMIT 1),(raw->>'id')::uuid);
    BEGIN x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
    EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
     WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
    IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
    disputed=(snap->>'id')::uuid;conflict=gen_random_uuid();
    INSERT INTO public.usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint,incoming_evidence_id,disputed_evidence_id,incoming_receipt_id)
    VALUES(conflict,a,existing,ev->>'source',finality,usage,public.core_digest_v1(jsonb_build_object('attempt_id',a,'existing',existing,'incoming',raw->>'id','usage',usage,'final_dimensions',to_jsonb(finals))),(raw->>'id')::uuid,disputed,receipt);
    UPDATE public.attempts SET metering_conflict=true WHERE id=a;
   ELSIF ev->>'finality'='ESTIMATED' THEN outcome='ESTIMATED';risk='{}';
   ELSE
    IF stages IS NULL THEN stale=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.outcome='ACCEPTED' AND r.source_revision>(ev->>'source_revision')::numeric);ELSE stale=coalesce((stage.sources->>jsonb_build_array(ev->>'origin_identity',ev->>'source')::text)::numeric,0)>(ev->>'source_revision')::numeric;END IF;
    IF stale THEN outcome='STALE';
    ELSE
     merged=merged||(usage-merged_finals);merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
     BEGIN
      IF stages IS NOT NULL AND exposures[stage_index] IS NOT NULL AND stage.usage=merged AND stage.finals=merged_finals THEN
       x=exposures[stage_index];
      ELSE
       x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
      END IF;
     EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
      WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      seen_final=finality='FINAL' OR CASE WHEN stages IS NULL THEN EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='ACCEPTED' AND r.finality IN ('FINAL','NOT_EXECUTED')) ELSE stage.seen_final END;
      IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
      canonical=(snap->>'id')::uuid;
      IF stages IS NULL THEN
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
      ELSE
       exposures[stage_index]=x;
       IF first_final THEN
        IF (x->>'complete')::boolean AND NOT (x->>'overrun')::boolean AND (x->>'reserve_atoms')::numeric=0
         AND (x->>'cost_atoms')::numeric<=h.current_reserved_atoms THEN
         first_cost=ROW(h,canonical,stage.revision,(snapshot.fact).snapshot_digest,merged_finals,(x->>'cost_atoms')::numeric,
          CASE WHEN h.current_reserved_atoms<>0 OR (x->>'cost_atoms')::numeric<>0 THEN gen_random_uuid() END)::core_funding.first_final_cost_v1;
         first_costs[stage_index]=first_cost;
        ELSE first_final=false;END IF;
       END IF;
       stage.head_id=canonical;stage.usage=merged;stage.finals=merged_finals;stage.dirty=true;
       IF stage.state NOT IN('SUCCEEDED','FAILED','NOT_EXECUTED') AND ev->>'attempt_state' IS NOT NULL THEN stage.state=ev->>'attempt_state';END IF;
       IF ev->>'attempt_state' IN('SUCCEEDED','FAILED','NOT_EXECUTED') THEN stage.finished_at=coalesce(stage.finished_at,clock_timestamp());END IF;
       stage.error_code=coalesce(ev->>'error_code',stage.error_code);
       stage.provider_request_id=coalesce(ev->>'provider_request_id',stage.provider_request_id);
       stage.seen_final=seen_final;
       stage.sources=jsonb_set(stage.sources,ARRAY[jsonb_build_array(ev->>'origin_identity',ev->>'source')::text],to_jsonb((ev->>'source_revision')::numeric));
       IF ev->>'request_state' IN('SUCCEEDED','CANCELLED','FAILED') THEN
        request_index=array_position(request_ids,stage.request_id);
        request_row=requests[request_index];
        IF request_row.selected_attempt_id IS NULL AND request_row.billing_outcome IN('PENDING','UNKNOWN') THEN
         request_row.selected_attempt_id=CASE WHEN ev->>'request_state' IN('SUCCEEDED','CANCELLED') THEN a END;
         request_row.state=ev->>'request_state';
         request_row.billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END;
         request_row.updated_at=clock_timestamp();request_row.dirty=true;
         requests[request_index]=request_row;
        END IF;
       END IF;
      END IF;
     END IF;
    END IF;
   END IF;
   IF stages IS NULL THEN
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault,'VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
   ELSE
    staged_receipt.id=receipt;staged_receipt.origin_identity=ev->>'origin_identity';
    staged_receipt.origin_incarnation=(ev->>'origin_incarnation')::uuid;staged_receipt.origin_session_id=(ev->>'origin_session_id')::uuid;
    staged_receipt.event_id=(ev->>'event_id')::uuid;staged_receipt.attempt_id=a;
    staged_receipt.source=ev->>'source';staged_receipt.source_revision=(ev->>'source_revision')::numeric;
    staged_receipt.body_digest=ev->>'body_digest';staged_receipt.event_claim=NOT event_claimed;staged_receipt.revision_claim=outcome<>'CONFLICT';
    staged_receipt.outcome=outcome;staged_receipt.finality=ev->>'finality';
    staged_receipt.attempt_state=ev->>'attempt_state';staged_receipt.request_state=ev->>'request_state';
    staged_receipt.error_code=ev->>'error_code';staged_receipt.provider_request_id=ev->>'provider_request_id';
    staged_receipt.raw_evidence_id=(raw->>'id')::uuid;staged_receipt.canonical_evidence_id=canonical;
    staged_receipt.disputed_evidence_id=disputed;staged_receipt.conflict_id=conflict;staged_receipt.risk_dimensions=risk;staged_receipt.numeric_fault=fault;
    staged_receipt.authentication_disposition='VERIFIED';staged_receipt.authentication_reason='NORMAL';
    staged_receipt.producer_key_sha256=ev->>'producer_key_sha256';staged_receipt.signature_version=1;
    staged_receipt.signature=decode(ev->>'signature','hex');staged_receipt.signature_payload_digest=ev->>'signature_payload_digest';
    staged_receipts=array_append(staged_receipts,staged_receipt);
   END IF;
   IF outcome<>'ACCEPTED' THEN first_final=false;END IF;
   IF stages IS NULL THEN
    touched=jsonb_set(touched,ARRAY[a::text],jsonb_build_object('canonical',CASE WHEN outcome='ACCEPTED' THEN to_jsonb(canonical) ELSE coalesce(touched->a::text->'canonical','null'::jsonb) END,'revision',CASE WHEN outcome='ACCEPTED' THEN snap->>'revision' ELSE coalesce(touched->a::text->>'revision',raw->>'revision') END,'money',coalesce((touched->a::text->>'money')::boolean,false) OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0));
   ELSE
    IF outcome='ACCEPTED' THEN
     stage.touched_canonical=canonical;stage.touched_revision=snap->>'revision';
    ELSE
     stage.touched_revision=coalesce(stage.touched_revision,raw->>'revision');
    END IF;
    stage.money=stage.money OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0;
    stages[stage_index]=stage;
   END IF;
   receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome',outcome,'duplicate',false));
  END LOOP;
 END LOOP;
 IF first_final THEN
  close_together=cardinality(requests)>0 AND NOT EXISTS(
   SELECT 1 FROM unnest(requests) r WHERE NOT r.dirty OR r.state<>'SUCCEEDED' OR r.billing_outcome<>'SUCCESS'
    OR NOT EXISTS(SELECT 1 FROM unnest(stages) t WHERE t.id=r.selected_attempt_id AND t.request_id=r.id AND t.state='SUCCEEDED')
    OR NOT EXISTS(SELECT 1 FROM unnest(staged_receipts) f WHERE f.attempt_id=r.selected_attempt_id AND f.outcome='ACCEPTED' AND f.attempt_state='SUCCEEDED' AND f.request_state='SUCCEEDED')
  );
 END IF;
 IF stages IS NOT NULL THEN
  PERFORM core_funding.flush_evidence_stages_v1(staged_snapshots,staged_receipts,stages,CASE WHEN close_together THEN '{}'::core_funding.evidence_request_stage_v1[] ELSE requests END);
 END IF;
 IF close_together THEN
  WITH targets AS MATERIALIZED (
   SELECT n.*,q.execution_closed_at FROM unnest(requests) n JOIN public.gateway_requests q ON q.id=n.id
  ), updated AS (
   UPDATE public.gateway_requests q SET selected_attempt_id=n.selected_attempt_id,state=n.state,billing_outcome=n.billing_outcome,
    updated_at=n.updated_at,execution_closed_at=coalesce(n.execution_closed_at,clock_timestamp())
   FROM targets n WHERE q.id=n.id RETURNING q.id,n.execution_closed_at IS NULL AS closed
  ), released AS (
   UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id IN(SELECT id FROM updated WHERE closed) AND state IN('ADOPTION_PENDING','ACTIVE')
  ) SELECT count(*) INTO closed_count FROM updated;
  IF closed_count<>cardinality(requests) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
 ELSE
 WITH closed AS (
  UPDATE public.gateway_requests q SET execution_closed_at=clock_timestamp()
  WHERE q.id=ANY(request_ids) AND q.execution_closed_at IS NULL AND q.state='SUCCEEDED' AND q.billing_outcome='SUCCESS'
   AND EXISTS(SELECT 1 FROM public.attempts t WHERE t.id=q.selected_attempt_id AND t.request_id=q.id AND t.state='SUCCEEDED')
   AND EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=q.selected_attempt_id AND r.outcome='ACCEPTED' AND r.attempt_state='SUCCEEDED' AND r.request_state='SUCCEEDED')
  RETURNING q.id
 ) UPDATE public.execution_quota_leases l SET state='RELEASE_PENDING' WHERE l.request_id IN(SELECT id FROM closed) AND l.state IN('ADOPTION_PENDING','ACTIVE');
 END IF;
 IF first_final AND cardinality(first_costs)=cardinality(stages)
  AND NOT EXISTS(SELECT 1 FROM unnest(first_costs) c WHERE c.evidence_id IS NULL) THEN
  pending=core_funding.flush_first_final_costs_v1(first_costs,pending);
 ELSE
 FOR row IN
  SELECT attempt,snapshot_id,revision FROM (
   SELECT key::uuid attempt,(value->>'canonical')::uuid snapshot_id,value->>'revision' revision
   FROM jsonb_each(touched) WHERE (value->>'money')::boolean
   UNION ALL
   SELECT s.id,s.touched_canonical,s.touched_revision FROM unnest(stages) s WHERE s.money
  ) work ORDER BY attempt::text
 LOOP
  settled=core_funding.supplier_settle_v1(row.attempt,row.snapshot_id,CASE WHEN row.snapshot_id IS NULL THEN 'risk:' ELSE 'cost:' END||row.attempt||':'||row.revision,pending);
  IF pending IS NOT NULL THEN pending=settled->'pending';END IF;
 END LOOP;
 pending=core_funding.flush_costs_v1(pending);
 END IF;
 request_ids=ARRAY(SELECT q.id FROM public.gateway_requests q WHERE q.id=ANY(request_ids) AND q.billing_outcome IN ('SUCCESS','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM public.billing_charges c WHERE c.request_id=q.id) ORDER BY q.id);
 IF first_final AND cardinality(request_ids)>0 THEN
  billing=core_funding.bill_first_final_v1(request_ids,staged_snapshots);
  request_ids=ARRAY(SELECT id FROM unnest(request_ids) id WHERE NOT billing?id::text ORDER BY id);
 END IF;
 IF cardinality(request_ids)>0 THEN
  billing=billing||public.settle_requests(request_ids);
  IF EXISTS(SELECT 1 FROM jsonb_each(billing) b WHERE b.value->>'state'='ERROR') THEN RAISE EXCEPTION 'customer_settlement_failed' USING ERRCODE='P0500';END IF;
 END IF;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt IMMEDIATE;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt DEFERRED;
 RETURN jsonb_build_object('result',jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing),'pending',pending,'blocked_groups',to_jsonb(blocked_groups));
END $$;
