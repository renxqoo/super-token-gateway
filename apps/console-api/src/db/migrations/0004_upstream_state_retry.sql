ALTER TABLE public.upstream_accounts
  ADD COLUMN pending_state TEXT CHECK (pending_state IN ('ACTIVE', 'DISABLED', 'RECONCILE_REQUIRED')),
  ADD COLUMN expected_state TEXT CHECK (expected_state IN ('ACTIVE', 'DISABLED', 'RECONCILE_REQUIRED'));
