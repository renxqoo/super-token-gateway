CREATE OR REPLACE FUNCTION public.core_canonical_v1(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE encoded text;ascii_keys boolean;
BEGIN
 CASE jsonb_typeof(v)
 WHEN 'object' THEN
  SELECT NOT EXISTS(SELECT 1 FROM jsonb_object_keys(v) k WHERE k COLLATE "C" ~ $ascii$[^\x01-\x7f]$ascii$) INTO ascii_keys;
  SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||CASE WHEN jsonb_typeof(value) IN ('object','array')
   THEN public.core_canonical_v1(value) ELSE value::text END,','
   ORDER BY CASE WHEN ascii_keys THEN key END COLLATE "C",CASE WHEN NOT ascii_keys THEN public.core_utf16_sort_v1(key) END),'')||'}'
  INTO encoded FROM jsonb_each(v);
 WHEN 'array' THEN
  SELECT '['||coalesce(string_agg(CASE WHEN jsonb_typeof(value) IN ('object','array') THEN public.core_canonical_v1(value)
   ELSE value::text END,',' ORDER BY n),'')||']' INTO encoded FROM jsonb_array_elements(v) WITH ORDINALITY a(value,n);
 ELSE encoded=v::text;
 END CASE;
 RETURN encoded;
END $$;
