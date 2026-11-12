DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_management_api') THEN CREATE ROLE core_management_api NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_management_api;
GRANT EXECUTE ON FUNCTION public.verify_platform_credential_v1(text),public.core_platform_authorize_v1(jsonb,text),public.credit_wallet(uuid,text,text,numeric,text,integer,timestamptz,text[],text[]) TO core_management_api;
GRANT SELECT ON supplier_brands,upstream_accounts,channels,models,routes,cost_versions,billing_prices,billing_tariffs,projects,api_keys,financial_vouchers,upstream_funding_operations,upstream_account_balances TO core_management_api;
GRANT INSERT ON supplier_brands,upstream_accounts,channels,models,routes,cost_versions,billing_prices,billing_tariffs,projects,api_keys,financial_vouchers,upstream_funding_operations,upstream_account_balances,upstream_journals,upstream_entries,upstream_audit_events,audit_events TO core_management_api;
GRANT UPDATE ON supplier_brands,channels,routes,billing_tariffs,upstream_account_balances TO core_management_api;
GRANT EXECUTE ON FUNCTION public.exact_billing_price(jsonb,jsonb) TO core_management_api;
GRANT UPDATE(id) ON financial_vouchers TO core_management_api;
GRANT UPDATE(id) ON upstream_accounts TO core_management_api;
GRANT SELECT ON upstream_journals,upstream_entries TO core_management_api;
GRANT UPDATE(id) ON supplier_brands TO core_management_api;
