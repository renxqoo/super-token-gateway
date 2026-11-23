CREATE TABLE IF NOT EXISTS public.platform_credit_defaults (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  default_credit_limit_atoms NUMERIC(38, 0) NOT NULL DEFAULT 0 CHECK (default_credit_limit_atoms >= 0),
  default_wallet_policy TEXT NOT NULL DEFAULT 'GIFT_FIRST' CHECK (default_wallet_policy IN ('GIFT_FIRST', 'CASH_FIRST', 'EARLIEST_EXPIRY')),
  updated_by UUID REFERENCES public.users(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.platform_credit_defaults (id, default_credit_limit_atoms, default_wallet_policy)
VALUES (1, 0, 'GIFT_FIRST')
ON CONFLICT (id) DO NOTHING;
