ALTER TABLE public.billing_holds ADD COLUMN reconciliation_started_at TIMESTAMPTZ;

CREATE TABLE public.billing_reconciliation_receipts (
  attempt_id UUID PRIMARY KEY,
  request_id UUID NOT NULL REFERENCES public.billing_holds(request_id),
  account_id UUID NOT NULL REFERENCES public.upstream_accounts(id),
  channel_id UUID NOT NULL REFERENCES public.channels(id),
  currency TEXT NOT NULL,
  provider_request_id TEXT NOT NULL,
  evidence_ref TEXT NOT NULL,
  match_reason TEXT,
  actor TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (verdict IN ('NOT_EXECUTED', 'FINAL')),
  supplier_cost_atoms NUMERIC(38,0),
  customer_usage JSONB,
  customer_actor TEXT,
  state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'APPLIED', 'REVIEW', 'DISMISSED')),
  result TEXT,
  review_history JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(review_history) = 'array'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reviewed_at TIMESTAMPTZ,
  UNIQUE (account_id, provider_request_id),
  UNIQUE (account_id, evidence_ref),
  CHECK ((verdict = 'NOT_EXECUTED' AND supplier_cost_atoms IS NULL AND customer_usage IS NULL)
      OR (verdict = 'FINAL' AND supplier_cost_atoms IS NOT NULL AND supplier_cost_atoms >= 0)),
  CHECK ((customer_usage IS NULL) = (customer_actor IS NULL))
);

CREATE INDEX billing_reconciliation_receipts_due_idx
  ON public.billing_reconciliation_receipts(created_at, attempt_id) WHERE state = 'PENDING';
CREATE INDEX billing_reconciliation_receipts_request_idx
  ON public.billing_reconciliation_receipts(request_id, created_at, attempt_id);
CREATE INDEX billing_reconciliation_unknown_idx
  ON public.billing_holds(created_at, request_id) WHERE state = 'UNKNOWN';
CREATE INDEX billing_reconciliation_supplier_pending_idx ON public.billing_holds(created_at, request_id)
  WHERE state IN ('SETTLED', 'RELEASED') AND supplier_holds @> '[{"state":"UNKNOWN"}]';

CREATE TABLE public.billing_reconciliation_budgets (
  currency TEXT PRIMARY KEY CHECK (currency ~ '^[A-Z]{3,8}$'),
  limit_atoms NUMERIC(38,0) NOT NULL CHECK (limit_atoms >= 0),
  used_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (used_atoms >= 0 AND used_atoms <= limit_atoms)
);

CREATE TABLE public.billing_reconciliation_policies (
  project_id UUID NOT NULL REFERENCES public.projects(id),
  channel_id UUID NOT NULL REFERENCES public.channels(id),
  supplier_currency TEXT NOT NULL REFERENCES public.billing_reconciliation_budgets(currency),
  enabled BOOLEAN NOT NULL,
  include_historical BOOLEAN NOT NULL,
  eligible_since TIMESTAMPTZ NOT NULL DEFAULT now(),
  age_seconds INTEGER NOT NULL CHECK (age_seconds > 0),
  max_customer_atoms NUMERIC(38,0) NOT NULL CHECK (max_customer_atoms > 0),
  limit_atoms NUMERIC(38,0) NOT NULL CHECK (limit_atoms > 0),
  used_atoms NUMERIC(38,0) NOT NULL DEFAULT 0 CHECK (used_atoms >= 0 AND used_atoms <= limit_atoms),
  PRIMARY KEY (project_id, channel_id, supplier_currency)
);

CREATE TABLE public.billing_reconciliation_waivers (
  request_id UUID PRIMARY KEY REFERENCES public.billing_holds(request_id),
  attempt_id UUID NOT NULL,
  project_id UUID NOT NULL,
  channel_id UUID NOT NULL,
  supplier_currency TEXT NOT NULL,
  supplier_risk_atoms NUMERIC(38,0) NOT NULL CHECK (supplier_risk_atoms >= 0),
  customer_released_atoms NUMERIC(38,0) NOT NULL CHECK (customer_released_atoms >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (project_id, channel_id, supplier_currency)
    REFERENCES public.billing_reconciliation_policies(project_id, channel_id, supplier_currency)
);

CREATE TABLE public.billing_reconciliation_polls (
  attempt_id UUID PRIMARY KEY,
  request_id UUID NOT NULL REFERENCES public.billing_holds(request_id),
  poll_count INTEGER NOT NULL DEFAULT 1 CHECK (poll_count > 0),
  next_at TIMESTAMPTZ NOT NULL
);
