SET LOCAL check_function_bodies=false;
CREATE FUNCTION public.validate_money_lines(p_wallet uuid,p_currency text,p_lines jsonb) RETURNS void
 LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $validation$
DECLARE item jsonb; total numeric=0; count integer; i integer; account text; atoms text; lot_id uuid; prior_lot uuid; lot_ids uuid[]='{}';
BEGIN
 count=jsonb_array_length(p_lines);
 IF count NOT BETWEEN 2 AND 2048 THEN RAISE EXCEPTION 'invalid money entry'; END IF;
 FOR i IN 0..count-1 LOOP
  item=p_lines->i;account=item->>'account';atoms=item->>'atoms';
  IF jsonb_typeof(item)<>'object' OR item-ARRAY['account','atoms']<>'{}'::jsonb OR NOT(item?&ARRAY['account','atoms']) OR jsonb_typeof(item->'account')<>'string' OR account !~ '^(lot:[a-f0-9-]{36}:(available|reserved|spent|expired)|system:(funding|revenue|receivable))$' OR jsonb_typeof(item->'atoms')<>'string' OR atoms !~ '^-?(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'invalid money leg'; END IF;
  total=total+atoms::numeric;
  IF left(account,4)='lot:' THEN
   lot_id=substr(account,5,36)::uuid;
   IF lot_id IS DISTINCT FROM prior_lot THEN lot_ids=array_append(lot_ids,lot_id);prior_lot=lot_id;END IF;
  END IF;
 END LOOP;
 IF total<>0 THEN RAISE EXCEPTION 'unbalanced money entry'; END IF;
 IF EXISTS(SELECT 1 FROM unnest(lot_ids) AS sources(id) WHERE NOT EXISTS(SELECT 1 FROM wallet_lots l WHERE l.id=sources.id AND l.wallet_id=p_wallet)) THEN RAISE EXCEPTION 'foreign money source';END IF;
 IF NOT EXISTS(SELECT 1 FROM wallets WHERE id=p_wallet AND currency=p_currency) THEN RAISE EXCEPTION 'wallet currency mismatch'; END IF;
 RETURN;
END $validation$;

ALTER TABLE public.providers DROP CONSTRAINT providers_kind_check;
ALTER TABLE public.providers ADD CONSTRAINT providers_kind_check CHECK(kind=ANY(ARRAY['openai','anthropic','gemini','deepseek','ollama','azure','bedrock','vertex']));
DROP TRIGGER usage_whitelist ON public.usage_events;
DROP TRIGGER balanced_journal ON public.money_journal;
ALTER TABLE public.usage_events RENAME TO usage_event_index;
ALTER TABLE public.money_journal RENAME TO money_entries;
ALTER TABLE public.usage_event_index ADD CONSTRAINT usage_index_time UNIQUE(id,created_at);
ALTER TABLE public.money_entries ADD CONSTRAINT journal_index_time UNIQUE(id,created_at);
CREATE INDEX usage_archive_range ON public.usage_event_index(created_at,id);
CREATE INDEX journal_archive_range ON public.money_entries(created_at,id);
CREATE TABLE public.usage_payloads(
 id uuid NOT NULL,created_at timestamptz NOT NULL,usage jsonb NOT NULL CHECK(jsonb_typeof(usage)='object'),
 PRIMARY KEY(id,created_at),FOREIGN KEY(id,created_at) REFERENCES public.usage_event_index(id,created_at)
) PARTITION BY RANGE(created_at);
CREATE TABLE public.journal_payloads(
 id uuid NOT NULL,created_at timestamptz NOT NULL,lines jsonb NOT NULL CHECK(jsonb_typeof(lines)='array'),
 PRIMARY KEY(id,created_at),FOREIGN KEY(id,created_at) REFERENCES public.money_entries(id,created_at)
) PARTITION BY RANGE(created_at);
CREATE FUNCTION public.prepare_evidence_month(p_month date) RETURNS void LANGUAGE plpgsql
 SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE parent text;name text;first_day timestamptz;next_day timestamptz;
BEGIN
 IF p_month IS NULL OR p_month<>date_trunc('month',p_month)::date THEN RAISE EXCEPTION 'month boundary required';END IF;
 first_day=p_month::timestamp AT TIME ZONE 'UTC';next_day=(p_month+interval '1 month')::timestamp AT TIME ZONE 'UTC';
 FOREACH parent IN ARRAY ARRAY['usage_payloads','journal_payloads'] LOOP
  name=parent||'_'||to_char(p_month,'YYYYMM');
  IF to_regclass('public.'||name) IS NULL THEN
   EXECUTE format('CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',name,parent,first_day,next_day);
  END IF;
 END LOOP;
END $$;
DO $$ DECLARE month date; BEGIN
 FOR month IN SELECT DISTINCT date_trunc('month',created_at AT TIME ZONE 'UTC')::date FROM public.usage_event_index
  UNION SELECT DISTINCT date_trunc('month',created_at AT TIME ZONE 'UTC')::date FROM public.money_entries
  UNION SELECT (date_trunc('month',now() AT TIME ZONE 'UTC')+i*interval '1 month')::date FROM generate_series(0,3)i
 LOOP PERFORM public.prepare_evidence_month(month);END LOOP;
END $$;
INSERT INTO public.usage_payloads SELECT id,created_at,usage FROM public.usage_event_index;
INSERT INTO public.journal_payloads SELECT id,created_at,lines FROM public.money_entries;
DO $$ BEGIN
 IF EXISTS(SELECT id,created_at,usage FROM public.usage_event_index EXCEPT SELECT id,created_at,usage FROM public.usage_payloads) OR
    EXISTS(SELECT id,created_at,usage FROM public.usage_payloads EXCEPT SELECT id,created_at,usage FROM public.usage_event_index) OR
    EXISTS(SELECT id,created_at,lines FROM public.money_entries EXCEPT SELECT id,created_at,lines FROM public.journal_payloads) OR
    EXISTS(SELECT id,created_at,lines FROM public.journal_payloads EXCEPT SELECT id,created_at,lines FROM public.money_entries)
 THEN RAISE EXCEPTION 'evidence migration mismatch';END IF;
END $$;
ALTER TABLE public.usage_event_index DROP COLUMN usage;
ALTER TABLE public.money_entries DROP COLUMN lines;
ALTER TABLE public.usage_event_index ADD CONSTRAINT usage_index_payload FOREIGN KEY(id,created_at) REFERENCES public.usage_payloads(id,created_at) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE public.money_entries ADD CONSTRAINT journal_index_payload FOREIGN KEY(id,created_at) REFERENCES public.journal_payloads(id,created_at) DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE public.usage_payloads_default PARTITION OF public.usage_payloads DEFAULT;
CREATE TABLE public.journal_payloads_default PARTITION OF public.journal_payloads DEFAULT;
CREATE VIEW public.usage_events AS SELECT i.id,i.seq,i.project_id,i.request_id,i.attempt_id,i.source,i.finality,p.usage,i.cost_atoms,i.currency,i.created_at,i.cost_complete FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at;
CREATE VIEW public.money_journal AS SELECT i.id,i.wallet_id,i.reference,i.kind,i.currency,p.lines,i.created_at FROM public.money_entries i JOIN public.journal_payloads p ON p.id=i.id AND p.created_at=i.created_at;
ALTER VIEW public.usage_events ALTER COLUMN created_at SET DEFAULT now();
ALTER VIEW public.usage_events ALTER COLUMN cost_complete SET DEFAULT false;
ALTER VIEW public.money_journal ALTER COLUMN created_at SET DEFAULT now();
CREATE FUNCTION public.insert_usage_view() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$ BEGIN
 IF NEW.seq IS NOT NULL THEN RAISE EXCEPTION 'usage sequence is generated always';END IF;
 INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,created_at,cost_complete)
 VALUES(NEW.id,NEW.project_id,NEW.request_id,NEW.attempt_id,NEW.source,NEW.finality,NEW.cost_atoms,NEW.currency,NEW.created_at,NEW.cost_complete) RETURNING seq INTO NEW.seq;
 INSERT INTO usage_payloads VALUES(NEW.id,NEW.created_at,NEW.usage);RETURN NEW;
END $$;
CREATE FUNCTION public.insert_journal_view() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$ BEGIN
 INSERT INTO money_entries(id,wallet_id,reference,kind,currency,created_at) VALUES(NEW.id,NEW.wallet_id,NEW.reference,NEW.kind,NEW.currency,NEW.created_at);
 INSERT INTO journal_payloads VALUES(NEW.id,NEW.created_at,NEW.lines);RETURN NEW;
END $$;
CREATE FUNCTION public.validate_journal_payload() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE entry money_entries%ROWTYPE;BEGIN
 SELECT * INTO entry FROM money_entries WHERE id=NEW.id AND created_at=NEW.created_at;
 IF NOT FOUND THEN RAISE EXCEPTION 'journal identity missing';END IF;
 PERFORM validate_money_lines(entry.wallet_id,entry.currency,NEW.lines);RETURN NEW;
END $$;
CREATE TRIGGER insert_evidence INSTEAD OF INSERT ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.insert_usage_view();
CREATE TRIGGER insert_journal INSTEAD OF INSERT ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.insert_journal_view();
CREATE TRIGGER usage_whitelist BEFORE INSERT ON public.usage_payloads FOR EACH ROW EXECUTE FUNCTION public.validate_usage();
CREATE TRIGGER balanced_journal BEFORE INSERT ON public.journal_payloads FOR EACH ROW EXECUTE FUNCTION public.validate_journal_payload();
CREATE TRIGGER immutable_usage_payload BEFORE UPDATE OR DELETE ON public.usage_payloads FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_journal_payload BEFORE UPDATE OR DELETE ON public.journal_payloads FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_usage_view INSTEAD OF UPDATE OR DELETE ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_journal_view INSTEAD OF UPDATE OR DELETE ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
REVOKE ALL ON FUNCTION public.validate_money_lines(uuid,text,jsonb),public.prepare_evidence_month(date),public.insert_usage_view(),public.insert_journal_view(),public.validate_journal_payload() FROM PUBLIC;
CLUSTER public.usage_event_index USING usage_events_pkey;
CLUSTER public.money_entries USING money_journal_pkey;

CREATE FUNCTION public.post_money_journals(p_items jsonb) RETURNS void LANGUAGE plpgsql
 SET search_path TO 'pg_catalog','public','pg_temp' AS $$ BEGIN
 IF jsonb_typeof(p_items)<>'array' OR jsonb_array_length(p_items)>65792 THEN RAISE EXCEPTION 'invalid posting batch';END IF;
 IF jsonb_array_length(p_items)=0 THEN RETURN;END IF;
 WITH journals AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(p_items) j(id uuid,wallet_id uuid,reference text,kind text,currency text,lines jsonb)
 ), entries AS (
  INSERT INTO money_entries(id,wallet_id,reference,kind,currency)
  SELECT j.id,j.wallet_id,j.reference,j.kind,j.currency FROM journals j
  RETURNING id,created_at
 )
 INSERT INTO journal_payloads(id,created_at,lines)
 SELECT j.id,e.created_at,j.lines FROM journals j JOIN entries e ON e.id=j.id;
END $$;
REVOKE ALL ON FUNCTION public.post_money_journals(jsonb) FROM PUBLIC;
