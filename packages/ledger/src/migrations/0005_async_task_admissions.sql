CREATE TABLE public.async_task_admissions (
  request_id uuid PRIMARY KEY,
  billing_account_id uuid NOT NULL REFERENCES public.billing_accounts (id),
  expires_at timestamptz NOT NULL
);
CREATE INDEX async_task_admissions_account_expiry_idx
  ON public.async_task_admissions (billing_account_id, expires_at);
CREATE INDEX async_task_admissions_expiry_idx
  ON public.async_task_admissions (expires_at, request_id);
