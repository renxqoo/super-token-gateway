ALTER TABLE public.price_versions
  ADD COLUMN pricing_spec jsonb,
  ADD CONSTRAINT price_versions_pricing_spec_check CHECK (
    pricing_spec IS NULL OR jsonb_typeof(pricing_spec) = 'object'
  );

ALTER TABLE public.billing_accounts
  ADD COLUMN customer_group_id text,
  ADD CONSTRAINT billing_accounts_customer_group_id_check CHECK (
    customer_group_id IS NULL
    OR (char_length(customer_group_id) >= 1 AND char_length(customer_group_id) <= 120)
  );

CREATE TABLE public.pricing_modifiers (
  id uuid PRIMARY KEY,
  layer text NOT NULL CHECK (layer IN ('PRODUCT', 'COMMERCIAL')),
  kind text NOT NULL CHECK (kind IN ('MULTIPLIER', 'EXACT_RATE_CARD')),
  stacking text NOT NULL CHECK (stacking IN ('MULTIPLY', 'BEST_FOR_CUSTOMER', 'EXCLUSIVE')),
  priority integer NOT NULL DEFAULT 0,
  enabled boolean NOT NULL DEFAULT true,
  effective_from timestamptz,
  effective_to timestamptz,
  time_windows jsonb CHECK (time_windows IS NULL OR jsonb_typeof(time_windows) = 'array'),
  match jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(match) = 'object'),
  multiplier_numerator numeric(38,0),
  multiplier_denominator numeric(38,0),
  rate_card_price_version_id uuid REFERENCES public.price_versions (id),
  source_revision integer NOT NULL DEFAULT 0 CHECK (source_revision >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT pricing_modifiers_effective_window_check CHECK (
    effective_from IS NULL OR effective_to IS NULL OR effective_from < effective_to
  ),
  CONSTRAINT pricing_modifiers_shape_check CHECK (
    (
      kind = 'MULTIPLIER'
      AND rate_card_price_version_id IS NULL
      AND multiplier_numerator > 0
      AND multiplier_denominator > 0
    )
    OR (
      kind = 'EXACT_RATE_CARD'
      AND stacking <> 'BEST_FOR_CUSTOMER'
      AND rate_card_price_version_id IS NOT NULL
      AND multiplier_numerator IS NULL
      AND multiplier_denominator IS NULL
    )
  )
);

CREATE INDEX pricing_modifiers_enabled_idx
  ON public.pricing_modifiers (layer, priority ASC, id)
  WHERE enabled;

CREATE TABLE public.fx_rate_versions (
  id uuid PRIMARY KEY,
  base_currency text NOT NULL CHECK (
    char_length(base_currency) >= 3 AND char_length(base_currency) <= 8
    AND ltrim(base_currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  ),
  quote_currency text NOT NULL CHECK (
    char_length(quote_currency) >= 3 AND char_length(quote_currency) <= 8
    AND ltrim(quote_currency, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') = ''
  ),
  rate_numerator numeric(38,0) NOT NULL CHECK (rate_numerator > 0),
  rate_denominator numeric(38,0) NOT NULL CHECK (rate_denominator > 0),
  effective_from timestamptz NOT NULL,
  effective_to timestamptz,
  source_digest text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fx_rate_versions_currency_pair_check CHECK (base_currency <> quote_currency),
  CONSTRAINT fx_rate_versions_window_check CHECK (
    effective_to IS NULL OR effective_from < effective_to
  ),
  CONSTRAINT fx_rate_versions_pair_effective_uniq UNIQUE (base_currency, quote_currency, effective_from)
);

CREATE INDEX fx_rate_versions_pair_effective_idx
  ON public.fx_rate_versions (base_currency, quote_currency, effective_from DESC, id DESC);

CREATE TRIGGER immutable_fx_rate_versions
  BEFORE DELETE OR UPDATE ON public.fx_rate_versions
  FOR EACH ROW EXECUTE FUNCTION public.immutable_record();

ALTER TABLE public.billing_charges
  ADD COLUMN pricing_snapshot jsonb,
  ADD CONSTRAINT billing_charges_pricing_snapshot_check CHECK (
    pricing_snapshot IS NULL OR jsonb_typeof(pricing_snapshot) = 'object'
  );
