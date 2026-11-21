CREATE TABLE IF NOT EXISTS public.wallet_leases (
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

CREATE INDEX IF NOT EXISTS wallet_leases_active_idx ON public.wallet_leases (expires_at) WHERE state = 'ACTIVE';
CREATE INDEX IF NOT EXISTS wallet_leases_wallet_idx ON public.wallet_leases (wallet_id) WHERE state = 'ACTIVE';

CREATE TABLE IF NOT EXISTS public.supplier_leases (
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

CREATE INDEX IF NOT EXISTS supplier_leases_active_idx ON public.supplier_leases (expires_at) WHERE state = 'ACTIVE';

ALTER TABLE public.billing_holds
  ADD COLUMN IF NOT EXISTS wallet_lease_id UUID REFERENCES public.wallet_leases(id),
  ADD COLUMN IF NOT EXISTS supplier_lease_id UUID REFERENCES public.supplier_leases(id);

CREATE INDEX IF NOT EXISTS billing_holds_wallet_lease_idx
  ON public.billing_holds (wallet_lease_id) WHERE state IN ('HELD', 'UNKNOWN') AND wallet_lease_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS billing_holds_supplier_lease_idx
  ON public.billing_holds (supplier_lease_id) WHERE state IN ('HELD', 'UNKNOWN') AND supplier_lease_id IS NOT NULL;

ALTER TABLE public.billing_charges
  ADD COLUMN IF NOT EXISTS customer_journal_id UUID,
  ADD COLUMN IF NOT EXISTS supplier_journal_id UUID;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'plugins' AND column_name = 'provider_config'
  ) THEN
    ALTER TABLE public.plugins
      DROP CONSTRAINT plugins_kind_check,
      ADD COLUMN capability TEXT NOT NULL DEFAULT 'CONTENT' CHECK (capability IN ('CONTENT', 'PROVIDER')),
      ADD COLUMN provider_config JSONB CHECK (provider_config IS NULL OR jsonb_typeof(provider_config) = 'object'),
      ADD CONSTRAINT plugins_name_check CHECK (name ~ '^[a-z0-9][a-z0-9_-]{1,63}$') NOT VALID,
      ADD CONSTRAINT plugins_kind_check CHECK (kind IN ('WASM', 'WEBHOOK', 'DECLARATIVE')),
      ADD CONSTRAINT plugins_capability_kind_check CHECK (
        (capability = 'CONTENT' AND kind IN ('WASM', 'WEBHOOK'))
        OR (capability = 'PROVIDER' AND kind IN ('DECLARATIVE', 'WASM'))
      ),
      ADD CONSTRAINT plugins_kind_payload_check CHECK (
        (kind = 'WASM' AND wasm_bytes IS NOT NULL)
        OR (kind = 'WEBHOOK' AND webhook_config IS NOT NULL)
        OR (kind = 'DECLARATIVE' AND provider_config IS NOT NULL)
      ) NOT VALID;

    ALTER TABLE public.channels
      DROP CONSTRAINT channels_kind_check,
      ADD CONSTRAINT channels_kind_check CHECK (kind ~ '^[a-z0-9][a-z0-9_-]{1,63}$'),
      ADD COLUMN provider_plugin_id UUID REFERENCES public.plugins(id),
      ADD COLUMN adapter_config JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(adapter_config) = 'object');

    ALTER TABLE public.routes
      ADD COLUMN adapter_overrides JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(adapter_overrides) = 'object');
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS plugins_provider_name_idx
  ON public.plugins (name, created_at DESC) WHERE capability = 'PROVIDER' AND enabled;
CREATE INDEX IF NOT EXISTS channels_provider_plugin_idx
  ON public.channels (provider_plugin_id) WHERE provider_plugin_id IS NOT NULL;

ALTER TABLE public.outbox_consumers
  ADD COLUMN leased_through_seq BIGINT CHECK (leased_through_seq >= 0);

CREATE FUNCTION public.order_outbox_publish() RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(81442591, 1);
  RETURN NULL;
END;
$$;

CREATE TRIGGER order_outbox_publish
  BEFORE INSERT ON public.outbox_events
  FOR EACH STATEMENT EXECUTE FUNCTION public.order_outbox_publish();
