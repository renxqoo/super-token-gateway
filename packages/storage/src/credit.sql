ALTER TABLE public.wallets
 ADD COLUMN credit_limit_atoms numeric(38,0) NOT NULL DEFAULT 0 CHECK(credit_limit_atoms>=0),
 ADD COLUMN credit_reserved_atoms numeric(38,0) NOT NULL DEFAULT 0 CHECK(credit_reserved_atoms>=0),
 ADD COLUMN credit_policy_version numeric(38,0) NOT NULL DEFAULT 0 CHECK(credit_policy_version>=0);
ALTER TABLE public.billing_holds ADD COLUMN credit_atoms numeric(38,0) NOT NULL DEFAULT 0 CHECK(credit_atoms>=0 AND credit_atoms<=amount_atoms);

CREATE TABLE public.wallet_credit_policies(
 id uuid PRIMARY KEY,
 wallet_id uuid NOT NULL REFERENCES public.wallets(id),
 external_ref text NOT NULL CHECK(length(external_ref) BETWEEN 1 AND 200),
 fingerprint text NOT NULL,
 credit_limit_atoms numeric(38,0) NOT NULL CHECK(credit_limit_atoms>=0),
 version numeric(38,0) NOT NULL CHECK(version>0),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(wallet_id,external_ref),
 UNIQUE(wallet_id,version)
);
CREATE TRIGGER immutable_credit_policies BEFORE UPDATE OR DELETE ON public.wallet_credit_policies FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE FUNCTION public.configure_wallet_credit(p_project uuid,p_currency text,p_ref text,p_version numeric,p_limit numeric) RETURNS jsonb
 LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE wallet wallets%ROWTYPE;previous wallet_credit_policies%ROWTYPE;digest text;command uuid;next_version numeric;
BEGIN
 IF p_ref IS NULL OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_version IS NULL OR p_limit IS NULL OR p_version::text !~ '^(0|[1-9][0-9]{0,37})$' OR p_limit::text !~ '^(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'invalid credit policy' USING ERRCODE='P0422';END IF;
 SELECT * INTO wallet FROM wallets WHERE project_id=p_project AND currency=p_currency FOR NO KEY UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'wallet not found' USING ERRCODE='P0404';END IF;
 digest=encode(sha256(convert_to(jsonb_build_array(p_version::text,p_limit::text)::text,'UTF8')),'hex');
 SELECT * INTO previous FROM wallet_credit_policies WHERE wallet_id=wallet.id AND external_ref=p_ref;
 IF FOUND THEN
  IF previous.fingerprint<>digest THEN RAISE EXCEPTION 'credit policy identity conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('id',previous.id,'credit_policy_version',previous.version::text,'credit_limit_atoms',previous.credit_limit_atoms::text,'replayed',true);
 END IF;
 IF p_version<>wallet.credit_policy_version OR p_limit<wallet.debt_atoms+wallet.credit_reserved_atoms THEN RAISE EXCEPTION 'credit policy exposure or version conflict' USING ERRCODE='P0409';END IF;
 IF p_version>=99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'credit policy version exhausted' USING ERRCODE='P0409';END IF;
 command=gen_random_uuid();next_version=p_version+1;
 UPDATE wallets SET credit_limit_atoms=p_limit,credit_policy_version=next_version,
  freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,p_limit) WHERE id=wallet.id;
 INSERT INTO wallet_credit_policies(id,wallet_id,external_ref,fingerprint,credit_limit_atoms,version) VALUES(command,wallet.id,p_ref,digest,p_limit,next_version);
 RETURN jsonb_build_object('id',command,'credit_policy_version',next_version::text,'credit_limit_atoms',p_limit::text,'replayed',false);
END $$;
REVOKE ALL ON FUNCTION public.configure_wallet_credit(uuid,text,text,numeric,numeric) FROM PUBLIC;
