CREATE TABLE public.admin_audit_events (
  request_id UUID NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('STARTED', 'COMPLETED')),
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  target TEXT NOT NULL,
  status SMALLINT CHECK (status BETWEEN 100 AND 599),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (request_id, phase),
  CHECK ((phase = 'STARTED') = (status IS NULL))
);

CREATE TABLE public.admin_audit_pending (
  request_id UUID PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX admin_audit_pending_created_idx ON public.admin_audit_pending (created_at);

CREATE TRIGGER immutable_admin_audit_events
BEFORE UPDATE OR DELETE ON public.admin_audit_events
FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE INDEX billing_holds_supplier_unknown_idx ON public.billing_holds (expires_at)
WHERE supplier_holds @> '[{"state":"UNKNOWN"}]'::jsonb;

CREATE FUNCTION public.operations_health(p_consumer TEXT) RETURNS TABLE (
  expired_hold DOUBLE PRECISION, customer_unknown DOUBLE PRECISION, supplier_unknown DOUBLE PRECISION,
  expired_wallet_lease DOUBLE PRECISION, expired_supplier_lease DOUBLE PRECISION, outbox_head DOUBLE PRECISION,
  admin_audit_pending DOUBLE PRECISION
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO pg_catalog, pg_temp
AS $$
  SELECT
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.state = 'HELD' AND h.expires_at <= now() ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.state = 'UNKNOWN' ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM h.expires_at)::float8 FROM public.billing_holds h
      WHERE h.supplier_holds @> '[{"state":"UNKNOWN"}]'::jsonb ORDER BY h.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM l.expires_at)::float8 FROM public.wallet_leases l
      WHERE l.state = 'ACTIVE' AND l.expires_at <= now() ORDER BY l.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM l.expires_at)::float8 FROM public.supplier_leases l
      WHERE l.state = 'ACTIVE' AND l.expires_at <= now() ORDER BY l.expires_at LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM o.created_at)::float8 FROM public.outbox_events o
      WHERE p_consumer IS NOT NULL
        AND o.seq > COALESCE((SELECT c.acked_seq FROM public.outbox_consumers c WHERE c.name = p_consumer), 0)
      ORDER BY o.seq LIMIT 1), 0),
    COALESCE((SELECT extract(epoch FROM a.created_at)::float8 FROM public.admin_audit_pending a
      ORDER BY a.created_at LIMIT 1), 0)
$$;

REVOKE ALL ON FUNCTION public.operations_health(TEXT) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.prepare_ledger_month(p_month DATE) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, pg_temp
AS $$
DECLARE
  wanted_month DATE := date_trunc('month', clock_timestamp() AT TIME ZONE 'UTC')::DATE;
  part_name TEXT;
  first_day TIMESTAMPTZ;
  next_day TIMESTAMPTZ;
BEGIN
  IF p_month IS NULL OR (p_month <> wanted_month AND p_month <> (wanted_month + INTERVAL '1 month')::DATE) THEN
    RAISE EXCEPTION 'only the current and next UTC month may be prepared' USING ERRCODE = 'P0422';
  END IF;
  part_name := 'ledger_entries_' || to_char(p_month, 'YYYYMM');
  first_day := p_month::TIMESTAMP AT TIME ZONE 'UTC';
  next_day := (p_month + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
  IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('public.' || part_name)
    AND inhparent = 'public.ledger_entries'::regclass) THEN
    RETURN;
  END IF;
  LOCK TABLE public.ledger_entries IN ACCESS EXCLUSIVE MODE;
  IF EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('public.' || part_name)
    AND inhparent = 'public.ledger_entries'::regclass) THEN
    RETURN;
  END IF;
  EXECUTE format('CREATE TABLE public.%I (LIKE public.ledger_entries INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', part_name);
  ALTER TABLE public.ledger_entries_default DISABLE TRIGGER immutable_ledger_entries;
  EXECUTE format(
    'WITH moved AS (DELETE FROM public.ledger_entries_default WHERE created_at >= $1 AND created_at < $2 RETURNING *) INSERT INTO public.%I SELECT * FROM moved',
    part_name
  ) USING first_day, next_day;
  ALTER TABLE public.ledger_entries_default ENABLE TRIGGER immutable_ledger_entries;
  EXECUTE format('ALTER TABLE public.ledger_entries ATTACH PARTITION public.%I FOR VALUES FROM (%L) TO (%L)', part_name, first_day, next_day);
END;
$$;

REVOKE ALL ON FUNCTION public.prepare_ledger_month(DATE) FROM PUBLIC;
