ALTER TABLE public.billing_reconciliation_policies
  ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE public.billing_reconciliation_waivers
  DROP CONSTRAINT billing_reconciliation_waiver_policy_fkey;
ALTER TABLE public.billing_reconciliation_policies
  DROP CONSTRAINT billing_reconciliation_policies_pkey;
ALTER TABLE public.billing_reconciliation_policies
  ADD CONSTRAINT billing_reconciliation_policies_pkey PRIMARY KEY (id);
ALTER TABLE public.billing_reconciliation_policies
  ALTER COLUMN billing_account_id DROP NOT NULL;
CREATE UNIQUE INDEX billing_reconciliation_policies_account_scope_idx
  ON public.billing_reconciliation_policies (billing_account_id, channel_id, supplier_currency)
  WHERE billing_account_id IS NOT NULL;
CREATE UNIQUE INDEX billing_reconciliation_policies_channel_default_idx
  ON public.billing_reconciliation_policies (channel_id, supplier_currency)
  WHERE billing_account_id IS NULL;
ALTER TABLE public.billing_reconciliation_waivers
  ADD COLUMN policy_id uuid;
UPDATE public.billing_reconciliation_waivers w
SET policy_id = p.id
FROM public.billing_reconciliation_policies p
WHERE p.billing_account_id = w.billing_account_id
  AND p.channel_id = w.channel_id
  AND p.supplier_currency = w.supplier_currency;
ALTER TABLE public.billing_reconciliation_waivers
  ALTER COLUMN policy_id SET NOT NULL,
  ADD CONSTRAINT billing_reconciliation_waivers_policy_id_fkey
    FOREIGN KEY (policy_id) REFERENCES public.billing_reconciliation_policies(id);
