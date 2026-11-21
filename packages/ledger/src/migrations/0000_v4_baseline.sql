CREATE TABLE public.schema_migrations (
  version INTEGER PRIMARY KEY,
  checksum TEXT NOT NULL CHECK (checksum ~ '^[a-f0-9]{64}$'),
  manifest JSONB NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.config_epoch (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.config_epoch (id, version) VALUES (1, 1);

CREATE TABLE public.projects (
  id UUID PRIMARY KEY,
  external_ref TEXT UNIQUE,
  name TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3,8}$'),
  rpm INTEGER NOT NULL CHECK (rpm > 0),
  tpm BIGINT NOT NULL CHECK (tpm > 0 AND tpm <= 9007199254740991),
  concurrency INTEGER NOT NULL CHECK (concurrency > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.api_keys (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id),
  name TEXT NOT NULL,
  verifier TEXT NOT NULL UNIQUE,
  prefix TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'INVOKER' CHECK (role IN ('ADMIN', 'BILLING', 'INVOKER', 'VIEWER')),
  allowed_cidrs TEXT[] NOT NULL DEFAULT '{}',
  models TEXT[] NOT NULL DEFAULT '{}',
  operations TEXT[] NOT NULL DEFAULT '{}',
  enabled BOOLEAN NOT NULL DEFAULT true,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX api_keys_project_idx ON public.api_keys (project_id);

CREATE TABLE public.upstream_accounts (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'DISABLED', 'RECONCILE_REQUIRED')),
  revision NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.channels (
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES public.upstream_accounts(id),
  name TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('openai', 'anthropic', 'gemini', 'azure', 'bedrock', 'vertex', 'deepseek', 'ollama', 'openai_compatible')),
  base_url TEXT NOT NULL,
  secret_ciphertext TEXT NOT NULL,
  rpm INTEGER NOT NULL CHECK (rpm > 0),
  concurrency INTEGER NOT NULL CHECK (concurrency > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  routing_policy JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(routing_policy) = 'object'),
  config_revision BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX channels_account_idx ON public.channels (account_id);

CREATE TABLE public.models (
  name TEXT PRIMARY KEY,
  operations TEXT[] NOT NULL,
  routing_strategy TEXT NOT NULL DEFAULT 'WEIGHTED' CHECK (routing_strategy IN ('WEIGHTED', 'ROUND_ROBIN', 'LEAST_LATENCY')),
  max_output_tokens INTEGER NOT NULL CHECK (max_output_tokens > 0),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.price_versions (
  id UUID PRIMARY KEY,
  side TEXT NOT NULL CHECK (side IN ('CUSTOMER', 'SUPPLIER')),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  basis TEXT NOT NULL DEFAULT 'PROVIDER_USAGE' CHECK (basis IN ('PROVIDER_USAGE', 'REQUEST_COMPLETION', 'INPUT_CHARACTERS')),
  rates JSONB NOT NULL CHECK (jsonb_typeof(rates) = 'object'),
  ceilings JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(ceilings) = 'object'),
  source_digest TEXT NOT NULL CHECK (source_digest ~ '^[a-f0-9]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.plugins (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('WASM', 'WEBHOOK')),
  digest TEXT NOT NULL CHECK (digest ~ '^[a-f0-9]{64}$'),
  wasm_bytes BYTEA CHECK (wasm_bytes IS NULL OR octet_length(wasm_bytes) BETWEEN 1 AND 16777216),
  webhook_config JSONB CHECK (webhook_config IS NULL OR jsonb_typeof(webhook_config) = 'object'),
  signature TEXT,
  signing_key_id TEXT,
  manifest JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(manifest) = 'object'),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (name, version)
);

CREATE TABLE public.routes (
  id UUID PRIMARY KEY,
  model_name TEXT NOT NULL REFERENCES public.models(name),
  channel_id UUID NOT NULL REFERENCES public.channels(id),
  upstream_model TEXT NOT NULL,
  operations TEXT[] NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  weight DOUBLE PRECISION NOT NULL DEFAULT 1 CHECK (weight > 0),
  billing_price_id UUID NOT NULL REFERENCES public.price_versions(id),
  cost_version_id UUID NOT NULL REFERENCES public.price_versions(id),
  request_policy JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(request_policy) = 'object'),
  plugin_pipeline JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(plugin_pipeline) = 'array'),
  retry_policy JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(retry_policy) = 'object'),
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX routes_model_lookup_idx ON public.routes (model_name, priority DESC) WHERE enabled;

CREATE TABLE public.wallets (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  policy TEXT NOT NULL DEFAULT 'GIFT_FIRST' CHECK (policy IN ('GIFT_FIRST', 'CASH_FIRST', 'EARLIEST_EXPIRY')),
  available_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (available_atoms >= 0),
  reserved_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (reserved_atoms >= 0),
  spent_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (spent_atoms >= 0),
  expired_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (expired_atoms >= 0),
  debt_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (debt_atoms >= 0),
  credit_limit_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (credit_limit_atoms >= 0),
  credit_reserved_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (credit_reserved_atoms >= 0),
  credit_policy_version NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (credit_policy_version >= 0),
  manual_frozen BOOLEAN NOT NULL DEFAULT false,
  evidence_disputed BOOLEAN NOT NULL DEFAULT false,
  freeze_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (project_id, currency)
);

CREATE TABLE public.wallet_lots (
  id UUID PRIMARY KEY,
  wallet_id UUID NOT NULL REFERENCES public.wallets(id),
  external_ref TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('CASH', 'GRANT')),
  priority INTEGER NOT NULL DEFAULT 100,
  models TEXT[] NOT NULL DEFAULT '{}',
  operations TEXT[] NOT NULL DEFAULT '{}',
  expires_at TIMESTAMPTZ,
  issued_atoms NUMERIC(38,0) NOT NULL CHECK (issued_atoms > 0),
  available_atoms NUMERIC(38,0) NOT NULL CHECK (available_atoms >= 0),
  reserved_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (reserved_atoms >= 0),
  spent_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (spent_atoms >= 0),
  expired_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (expired_atoms >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (wallet_id, external_ref),
  CHECK (issued_atoms = available_atoms + reserved_atoms + spent_atoms + expired_atoms)
);

CREATE INDEX wallet_lots_avail_idx ON public.wallet_lots (wallet_id, priority, expires_at NULLS LAST, created_at) WHERE available_atoms > 0;
CREATE INDEX wallet_lots_expiry_idx ON public.wallet_lots (expires_at) WHERE available_atoms > 0 AND expires_at IS NOT NULL;

CREATE TABLE public.supplier_root_balances (
  account_id UUID NOT NULL REFERENCES public.upstream_accounts(id),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  balance_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  spent_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (spent_atoms >= 0),
  revision NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (revision >= 0),
  PRIMARY KEY (account_id, currency)
);

CREATE TABLE public.supplier_cells (
  account_id UUID NOT NULL,
  currency TEXT NOT NULL,
  cell SMALLINT NOT NULL CHECK (cell BETWEEN 0 AND 15),
  balance_atoms NUMERIC(38,0) NOT NULL DEFAULT 0,
  reserved_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (reserved_atoms >= 0),
  spent_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (spent_atoms >= 0),
  revision NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (revision >= 0),
  PRIMARY KEY (account_id, currency, cell),
  FOREIGN KEY (account_id, currency) REFERENCES public.supplier_root_balances(account_id, currency)
);

CREATE TABLE public.wallet_leases (
  id UUID PRIMARY KEY,
  node_id TEXT NOT NULL,
  wallet_id UUID NOT NULL REFERENCES public.wallets(id),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  lot_allocations JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(lot_allocations) = 'array'),
  remaining_atoms NUMERIC(38,0) NOT NULL CHECK (remaining_atoms >= 0),
  earliest_lot_expires_at TIMESTAMPTZ,
  state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'CLOSED')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX wallet_leases_active_idx ON public.wallet_leases (expires_at) WHERE state = 'ACTIVE';
CREATE INDEX wallet_leases_wallet_idx ON public.wallet_leases (wallet_id) WHERE state = 'ACTIVE';

CREATE TABLE public.supplier_leases (
  id UUID PRIMARY KEY,
  node_id TEXT NOT NULL,
  account_id UUID NOT NULL REFERENCES public.upstream_accounts(id),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  cell SMALLINT NOT NULL CHECK (cell BETWEEN 0 AND 15),
  remaining_atoms NUMERIC(38,0) NOT NULL CHECK (remaining_atoms >= 0),
  state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE', 'CLOSED')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX supplier_leases_active_idx ON public.supplier_leases (expires_at) WHERE state = 'ACTIVE';

CREATE TABLE public.billing_holds (
  request_id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES public.projects(id),
  wallet_id UUID NOT NULL REFERENCES public.wallets(id),
  operation TEXT NOT NULL,
  model TEXT NOT NULL,
  idempotency_key TEXT,
  fingerprint TEXT NOT NULL,
  held_atoms NUMERIC(38,0) NOT NULL CHECK (held_atoms >= 0),
  credit_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (credit_atoms >= 0),
  lot_allocations JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(lot_allocations) = 'array'),
  supplier_holds JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(supplier_holds) = 'array'),
  wallet_lease_id UUID REFERENCES public.wallet_leases(id),
  supplier_lease_id UUID REFERENCES public.supplier_leases(id),
  state TEXT NOT NULL DEFAULT 'HELD' CHECK (state IN ('HELD', 'SETTLED', 'RELEASED', 'UNKNOWN', 'CONFLICT')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX billing_holds_idempotency_idx ON public.billing_holds (project_id, operation, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX billing_holds_open_idx ON public.billing_holds (expires_at) WHERE state IN ('HELD', 'UNKNOWN');
CREATE INDEX billing_holds_wallet_lease_idx ON public.billing_holds (wallet_lease_id) WHERE state IN ('HELD', 'UNKNOWN') AND wallet_lease_id IS NOT NULL;
CREATE INDEX billing_holds_supplier_lease_idx ON public.billing_holds (supplier_lease_id) WHERE state IN ('HELD', 'UNKNOWN') AND supplier_lease_id IS NOT NULL;

CREATE TABLE public.billing_charges (
  id UUID PRIMARY KEY,
  request_id UUID NOT NULL UNIQUE REFERENCES public.billing_holds(request_id),
  project_id UUID NOT NULL REFERENCES public.projects(id),
  wallet_id UUID NOT NULL REFERENCES public.wallets(id),
  selected_attempt_id UUID,
  channel_id UUID REFERENCES public.channels(id),
  model TEXT NOT NULL,
  operation TEXT NOT NULL,
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  billing_price_id UUID REFERENCES public.price_versions(id),
  cost_version_id UUID REFERENCES public.price_versions(id),
  usage JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(usage) = 'object'),
  usage_digest TEXT NOT NULL CHECK (usage_digest ~ '^[a-f0-9]{64}$'),
  customer_atoms NUMERIC(38,0) NOT NULL CHECK (customer_atoms >= 0),
  funded_atoms NUMERIC(38,0) NOT NULL CHECK (funded_atoms >= 0),
  debt_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (debt_atoms >= 0),
  refunded_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (refunded_atoms >= 0 AND refunded_atoms <= customer_atoms),
  debt_refunded_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (debt_refunded_atoms >= 0),
  supplier_cost_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (supplier_cost_atoms >= 0),
  supplier_currency TEXT CHECK (supplier_currency IS NULL OR supplier_currency ~ '^[A-Z]{3,8}$'),
  lot_allocations JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(lot_allocations) = 'array'),
  plugin_audit JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(plugin_audit) = 'array'),
  charge_policy TEXT NOT NULL DEFAULT 'CHARGE_CUSTOMER' CHECK (charge_policy IN ('CHARGE_CUSTOMER', 'PLATFORM_ABSORB')),
  outcome TEXT NOT NULL CHECK (outcome IN ('SETTLED', 'RELEASED', 'DEBT', 'MODERATION_BLOCKED', 'UNKNOWN_RESOLVED')),
  customer_journal_id UUID,
  supplier_journal_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX billing_charges_project_created_idx ON public.billing_charges (project_id, created_at DESC);
CREATE INDEX billing_charges_wallet_debt_idx ON public.billing_charges (wallet_id, created_at) WHERE debt_atoms > 0;

CREATE TABLE public.ledger_entries (
  journal_id UUID NOT NULL,
  ordinal SMALLINT NOT NULL CHECK (ordinal >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  side TEXT NOT NULL CHECK (side IN ('CUSTOMER', 'SUPPLIER')),
  entity_id UUID NOT NULL,
  request_id UUID,
  reference TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('CREDIT', 'SETTLE', 'RELEASE', 'REFUND', 'ADJUST', 'EXPIRE', 'DEBT_COLLECT', 'MODERATION_ABSORB', 'SUPPLIER_RECHARGE', 'SUPPLIER_COST', 'SUPPLIER_ADJUST')),
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  account TEXT NOT NULL CHECK (account ~ '^(lot:[a-f0-9-]{36}:(available|spent|expired)|system:(funding|revenue|receivable|moderation_loss|adjustment)|upstream:(available|expense|funding|adjustment))$'),
  atoms NUMERIC(38,0) NOT NULL,
  PRIMARY KEY (journal_id, ordinal, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE public.ledger_entries_default PARTITION OF public.ledger_entries DEFAULT;
CREATE INDEX ledger_entries_entity_idx ON public.ledger_entries (side, entity_id, created_at DESC);
CREATE INDEX ledger_entries_request_idx ON public.ledger_entries (request_id) WHERE request_id IS NOT NULL;

CREATE TABLE public.funding_operations (
  id UUID PRIMARY KEY,
  side TEXT NOT NULL CHECK (side IN ('CUSTOMER', 'SUPPLIER')),
  entity_id UUID NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('CREDIT', 'REFUND', 'ADJUST', 'SUPPLIER_RECHARGE', 'SUPPLIER_ADJUST')),
  external_ref TEXT NOT NULL CHECK (length(external_ref) BETWEEN 1 AND 200),
  fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^[a-f0-9]{64}$'),
  amount_atoms NUMERIC(38,0) NOT NULL,
  currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3,8}$'),
  reason TEXT NOT NULL DEFAULT '',
  payload JSONB NOT NULL DEFAULT '{}',
  receipt JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (side, entity_id, kind, external_ref)
);

CREATE TABLE public.billing_resolutions (
  id UUID PRIMARY KEY,
  request_id UUID NOT NULL UNIQUE REFERENCES public.billing_holds(request_id),
  action TEXT NOT NULL CHECK (action IN ('SETTLE', 'RELEASE')),
  billing_price_id UUID REFERENCES public.price_versions(id),
  usage JSONB NOT NULL DEFAULT '{}',
  reason TEXT NOT NULL,
  actor TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.outbox_events (
  seq BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  kind TEXT NOT NULL,
  request_id UUID,
  project_id UUID,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_events_project_idx ON public.outbox_events (project_id, seq);

CREATE TABLE public.outbox_consumers (
  name TEXT PRIMARY KEY,
  acked_seq BIGINT NOT NULL DEFAULT 0 CHECK (acked_seq >= 0),
  lease_token UUID,
  leased_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION public.immutable_record() RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
BEGIN
  RAISE EXCEPTION 'immutable record' USING ERRCODE = 'P0403';
END $$;

CREATE FUNCTION public.check_ledger_journal_balance() RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
DECLARE
  line_count INTEGER;
  total NUMERIC(38,0);
BEGIN
  SELECT count(*), coalesce(sum(atoms), 0) INTO line_count, total
  FROM public.ledger_entries
  WHERE journal_id = NEW.journal_id;
  IF line_count < 2 OR total <> 0 THEN
    RAISE EXCEPTION 'unbalanced ledger journal: %', NEW.journal_id USING ERRCODE = 'P0422';
  END IF;
  RETURN NULL;
END $$;

CREATE FUNCTION public.prepare_ledger_month(p_month DATE) RETURNS void
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
DECLARE
  part_name TEXT;
  first_day TIMESTAMPTZ;
  next_day TIMESTAMPTZ;
BEGIN
  IF p_month IS NULL OR p_month <> date_trunc('month', p_month)::DATE THEN
    RAISE EXCEPTION 'month boundary required' USING ERRCODE = 'P0422';
  END IF;
  part_name := 'ledger_entries_' || to_char(p_month, 'YYYYMM');
  first_day := p_month::TIMESTAMP AT TIME ZONE 'UTC';
  next_day := (p_month + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
  IF to_regclass('public.' || part_name) IS NULL THEN
    EXECUTE format(
      'CREATE TABLE public.%I PARTITION OF public.ledger_entries FOR VALUES FROM (%L) TO (%L)',
      part_name,
      first_day,
      next_day
    );
  END IF;
END $$;

CREATE TRIGGER immutable_price_versions
BEFORE UPDATE OR DELETE ON public.price_versions
FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE TRIGGER immutable_ledger_entries
BEFORE UPDATE OR DELETE ON public.ledger_entries
FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

CREATE CONSTRAINT TRIGGER balanced_ledger_journal
AFTER INSERT ON public.ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.check_ledger_journal_balance();
