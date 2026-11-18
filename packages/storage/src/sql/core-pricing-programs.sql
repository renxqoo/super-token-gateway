REVOKE ALL ON SCHEMA core_funding FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core_funding FROM PUBLIC;

CREATE FUNCTION core_funding.compile_price_v1(rates jsonb) RETURNS TABLE(dimensions text[],weights numeric[],denominator numeric) LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE part record;numerators numeric[]='{}';denominators numeric[]='{}';numerator numeric;divisor numeric;a numeric;b numeric;remainder numeric;i integer;BEGIN
 IF jsonb_typeof(rates) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'invalid price input';END IF;
 IF rates?'cache_write_tokens' AND (rates?'cache_write_5m_tokens' OR rates?'cache_write_1h_tokens') THEN RAISE EXCEPTION 'duplicate cache write rate';END IF;
 dimensions='{}';weights='{}';denominator=1;
 FOR part IN SELECT key,value FROM jsonb_each(rates) ORDER BY key COLLATE "C" LOOP
  IF (part.value->>'numerator' ~ '^(0|[1-9][0-9]{0,37})$') IS NOT TRUE OR (part.value->>'denominator' ~ '^[1-9][0-9]{0,37}$') IS NOT TRUE THEN RAISE EXCEPTION 'invalid price input';END IF;
  dimensions=array_append(dimensions,part.key);
  numerator=(part.value->>'numerator')::numeric;
  divisor=(part.value->>'denominator')::numeric;
  a=numerator;b=divisor;
  WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  numerator=div(numerator,a);divisor=div(divisor,a);
  numerators=array_append(numerators,numerator);
  denominators=array_append(denominators,divisor);
  a=denominator;b=divisor;
  WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  denominator=div(denominator,a)*divisor;
 END LOOP;
 FOR i IN 1..cardinality(dimensions) LOOP weights=array_append(weights,numerators[i]*div(denominator,denominators[i]));END LOOP;
 RETURN NEXT;
END $$;

CREATE FUNCTION core_funding.quantity_v1(quantity text) RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE fraction integer;BEGIN
 IF (quantity ~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') IS NOT TRUE THEN RAISE EXCEPTION 'invalid price input';END IF;
 fraction=CASE WHEN position('.' IN quantity)>0 THEN length(split_part(quantity,'.',2)) ELSE 0 END;
 RETURN replace(quantity,'.','')::numeric*power(10::numeric,18-fraction);
END $$;

CREATE FUNCTION core_funding.price_v1(usage jsonb,dimensions text[],weights numeric[],denominator numeric) RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE i integer;total numeric=0;divisor numeric;result numeric;BEGIN
 IF dimensions IS NULL OR weights IS NULL OR cardinality(dimensions)<>cardinality(weights) OR denominator IS NULL OR denominator<=0 OR denominator<>trunc(denominator) THEN RAISE EXCEPTION 'invalid compiled price';END IF;
 FOR i IN 1..cardinality(dimensions) LOOP
  total=total+core_funding.quantity_v1(coalesce(usage->>dimensions[i],'0'))*weights[i];
 END LOOP;
 divisor=denominator*1000000000000000000;
 result=div(total,divisor)+CASE WHEN mod(total,divisor)>0 THEN 1 ELSE 0 END;
 IF result>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range';END IF;
 RETURN result;
END $$;

CREATE FUNCTION core_funding.exposure_v1(price uuid,estimated jsonb,usage jsonb,finals text[],risk jsonb) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE program core_funding.cost_prices%ROWTYPE;i integer;dimension text;estimated_text text;quantity_text text;e numeric;q numeric;term numeric;known_n numeric=0;total_n numeric=0;base_n numeric=0;ceiling_n numeric=0;divisor numeric;cost numeric;total numeric;base_total numeric;ceiling numeric;complete boolean=true;overrun boolean=false;risky text[]='{}';BEGIN
 SELECT * INTO program FROM core_funding.cost_prices WHERE price_id=price;
 IF program.price_id IS NULL THEN RAISE EXCEPTION 'supplier_price_missing' USING ERRCODE='P0422';END IF;
 IF jsonb_typeof(estimated) IS DISTINCT FROM 'object' OR jsonb_typeof(usage) IS DISTINCT FROM 'object' OR finals IS NULL OR jsonb_typeof(risk) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'supplier_exposure_invalid' USING ERRCODE='P0422';END IF;
 FOR i IN 1..cardinality(program.dimensions) LOOP
  dimension=program.dimensions[i];estimated_text=estimated->>dimension;
  IF estimated_text IS NULL THEN RAISE EXCEPTION 'supplier_estimate_missing' USING ERRCODE='P0422';END IF;
  e=core_funding.quantity_v1(estimated_text);term=e*program.weights[i];ceiling_n=ceiling_n+term;
  IF dimension=ANY(finals) THEN
   quantity_text=usage->>dimension;
   IF quantity_text IS NULL THEN RAISE EXCEPTION 'supplier_final_quantity_missing' USING ERRCODE='P0422';END IF;
   q=core_funding.quantity_v1(quantity_text);
   IF q>e THEN overrun=true;END IF;
   term=q*program.weights[i];known_n=known_n+term;total_n=total_n+term;base_n=base_n+term;
  ELSE
   complete=false;base_n=base_n+term;
   IF coalesce((risk->>dimension)::numeric,0)>estimated_text::numeric THEN
    risky=array_append(risky,dimension);term=core_funding.quantity_v1(risk->>dimension)*program.weights[i];
   END IF;
   total_n=total_n+term;
  END IF;
 END LOOP;
 divisor=program.denominator*1000000000000000000;
 cost=div(known_n,divisor)+CASE WHEN mod(known_n,divisor)>0 THEN 1 ELSE 0 END;
 total=div(total_n,divisor)+CASE WHEN mod(total_n,divisor)>0 THEN 1 ELSE 0 END;
 base_total=div(base_n,divisor)+CASE WHEN mod(base_n,divisor)>0 THEN 1 ELSE 0 END;
 ceiling=div(ceiling_n,divisor)+CASE WHEN mod(ceiling_n,divisor)>0 THEN 1 ELSE 0 END;
 IF greatest(cost,total,base_total,ceiling)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range';END IF;
 IF cardinality(risky)>1 THEN risky=ARRAY(SELECT d FROM unnest(risky) d ORDER BY d);END IF;
 RETURN jsonb_build_object('cost_atoms',cost::text,'reserve_atoms',(total-cost)::text,'base_reserve_atoms',(base_total-cost)::text,'complete',complete,'overrun',overrun OR base_total>ceiling,'risk_dimensions',to_jsonb(risky));
END $$;

CREATE FUNCTION core_funding.capture_price_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE rates jsonb;program record;source text;failure text;reason text;BEGIN
 IF TG_TABLE_NAME='cost_prices' THEN source='cost_versions';SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=NEW.price_id;
 ELSIF TG_TABLE_NAME='billing_prices' THEN source='billing_prices';SELECT v.rates INTO rates FROM public.billing_prices v WHERE v.id=NEW.price_id;
 ELSE RAISE EXCEPTION 'price_program_source_invalid';END IF;
 IF rates IS NULL THEN RAISE EXCEPTION 'price_program_source_invalid';END IF;
 BEGIN
  SELECT * INTO STRICT program FROM core_funding.compile_price_v1(rates);
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,reason=MESSAGE_TEXT;
  RAISE EXCEPTION 'price_program_compile_failed:%:%:%',source,NEW.price_id,left(reason,256) USING ERRCODE=failure;
 END;
 NEW.compiler_version=1;
 NEW.source_digest=encode(sha256(convert_to(rates::text,'UTF8')),'hex');
 NEW.dimensions=program.dimensions;NEW.weights=program.weights;NEW.denominator=program.denominator;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.compile_inserted_price_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_TABLE_NAME='cost_versions' THEN INSERT INTO core_funding.cost_prices(price_id) VALUES(NEW.id);
 ELSIF TG_TABLE_NAME='billing_prices' THEN INSERT INTO core_funding.billing_prices(price_id) VALUES(NEW.id);
 ELSE RAISE EXCEPTION 'price_program_source_invalid';END IF;
 RETURN NEW;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core_funding FROM PUBLIC;
CREATE TRIGGER capture_cost_price BEFORE INSERT ON core_funding.cost_prices FOR EACH ROW EXECUTE FUNCTION core_funding.capture_price_v1();
CREATE TRIGGER capture_billing_price BEFORE INSERT ON core_funding.billing_prices FOR EACH ROW EXECUTE FUNCTION core_funding.capture_price_v1();
CREATE TRIGGER immutable_cost_price BEFORE UPDATE OR DELETE ON core_funding.cost_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_billing_price BEFORE UPDATE OR DELETE ON core_funding.billing_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER compile_cost_price AFTER INSERT ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION core_funding.compile_inserted_price_v1();
CREATE TRIGGER compile_billing_price AFTER INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION core_funding.compile_inserted_price_v1();
INSERT INTO core_funding.cost_prices(price_id) SELECT id FROM public.cost_versions ORDER BY id;
INSERT INTO core_funding.billing_prices(price_id) SELECT id FROM public.billing_prices ORDER BY id;
