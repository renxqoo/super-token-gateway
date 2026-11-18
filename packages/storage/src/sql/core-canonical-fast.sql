CREATE OR REPLACE FUNCTION public.core_utf16_sort_v1(value text) RETURNS bytea LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE result text='';point integer;char_index integer;BEGIN
 IF value COLLATE "C" !~ $ascii$[^\x01-\x7f]$ascii$ THEN RETURN decode(regexp_replace(encode(convert_to(value,'UTF8'),'hex'),'(..)',E'00\\1','g'),'hex');END IF;
 FOR char_index IN 1..char_length(value) LOOP
  point=ascii(substr(value,char_index,1));
  IF point>65535 THEN point=point-65536;result=result||lpad(to_hex(55296+point/1024),4,'0')||lpad(to_hex(56320+point%1024),4,'0');
  ELSE result=result||lpad(to_hex(point),4,'0');END IF;
 END LOOP;RETURN decode(result,'hex');
END $$;
CREATE OR REPLACE FUNCTION public.core_canonical_v1(v jsonb) RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r text;BEGIN
 IF jsonb_typeof(v)='object' THEN SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||CASE WHEN jsonb_typeof(value) IN ('object','array') THEN public.core_canonical_v1(value) ELSE value::text END,',' ORDER BY public.core_utf16_sort_v1(key)),'')||'}' INTO r FROM jsonb_each(v);
 ELSIF jsonb_typeof(v)='array' THEN SELECT '['||coalesce(string_agg(CASE WHEN jsonb_typeof(value) IN ('object','array') THEN public.core_canonical_v1(value) ELSE value::text END,',' ORDER BY n),'')||']' INTO r FROM jsonb_array_elements(v) WITH ORDINALITY a(value,n);
 ELSE r=v::text;END IF;RETURN r;
END $$;
