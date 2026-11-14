DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_public_api') THEN CREATE ROLE core_public_api NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_public_api;
GRANT EXECUTE ON FUNCTION public.core_account_auth_v1(text,text) TO core_public_api;
GRANT SELECT ON wallets TO core_public_api;
GRANT SELECT(wallet_id,request_id) ON wallet_disputes TO core_public_api;
GRANT SELECT(request_id) ON billing_resolutions TO core_public_api;
GRANT SELECT(id,project_id,model,operation,billing_currency,billing_outcome) ON gateway_requests TO core_public_api;
GRANT SELECT(request_id,state,amount_atoms,credit_atoms) ON billing_holds TO core_public_api;
GRANT SELECT(id,request_id,amount_atoms,funded_atoms,debt_atoms,refunded_atoms,pricing_id,evidence_id) ON billing_charges TO core_public_api;
