CREATE OR REPLACE FUNCTION core_funding.coalesce_groups_v1(groups jsonb, pending jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 events jsonb;
 attempt_owners jsonb;
 event_owners jsonb;
 request_owners jsonb;
 applied jsonb;
 result jsonb;
 results jsonb='[]';
BEGIN
 IF EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) g
  WHERE jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb
   OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array'
   OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64
 ) THEN RETURN NULL; END IF;

 SELECT jsonb_agg(e.value ORDER BY g.n,e.n),
  jsonb_object_agg(((e.value->>'attempt_id')::uuid)::text,g.n),
  jsonb_object_agg(((e.value->>'event_id')::uuid)::text,g.n)
 INTO events,attempt_owners,event_owners
 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
 CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') WITH ORDINALITY e(value,n);
 IF jsonb_array_length(events)>64 THEN RETURN NULL; END IF;

 IF EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
  CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e
  GROUP BY (e->>'attempt_id')::uuid HAVING count(DISTINCT g.n)>1
 ) OR EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
  CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e
  GROUP BY (e->>'event_id')::uuid HAVING count(DISTINCT g.n)>1
 ) THEN RETURN NULL; END IF;

 IF EXISTS (
  SELECT 1 FROM jsonb_each_text(attempt_owners) o(attempt_id,group_number)
  CROSS JOIN LATERAL (
   SELECT h.wallet_id,w.has_expiring_lots FROM public.attempts t
   JOIN public.billing_holds h ON h.request_id=t.request_id
   JOIN public.wallets w ON w.id=h.wallet_id
   WHERE t.id=o.attempt_id::uuid OFFSET 0
  ) w
  GROUP BY w.wallet_id HAVING count(DISTINCT o.group_number)>1 OR bool_or(w.has_expiring_lots)
 ) THEN RETURN NULL; END IF;

 applied=core_funding.evidence_group_v1(
  jsonb_build_object('events',events),pending||jsonb_build_object('partition',attempt_owners)
 );
 result=applied->'result';
 SELECT jsonb_object_agg(t.request_id::text,o.group_number) INTO request_owners
 FROM jsonb_each_text(attempt_owners) o(attempt_id,group_number)
 CROSS JOIN LATERAL (SELECT request_id FROM public.attempts WHERE id=o.attempt_id::uuid OFFSET 0) t;

 FOR i IN 1..jsonb_array_length(groups) LOOP
  IF applied->'blocked_groups' @> jsonb_build_array(i) THEN
   results=results||jsonb_build_array(jsonb_build_object('status','ERROR','error','55P03'));
   CONTINUE;
  END IF;
  results=results||jsonb_build_array(jsonb_build_object(
   'status','COMMITTED',
   'receipts',coalesce((
    SELECT jsonb_agg(r.value ORDER BY r.n)
    FROM jsonb_array_elements(result->'receipts') WITH ORDINALITY r(value,n)
    WHERE (event_owners->>(((r.value->>'event_id')::uuid)::text))::integer=i
   ),'[]'),
   'billing',coalesce((
    SELECT jsonb_object_agg(b.key,b.value) FROM jsonb_each(result->'billing') b
    WHERE (request_owners->>b.key)::integer=i
   ),'{}')
  ));
 END LOOP;
 RETURN jsonb_build_object('results',results,'pending',(applied->'pending')-'partition');
END $$;
