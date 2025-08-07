CREATE OR REPLACE PROCEDURE tenant_default.pro_get_master_data(
	IN fa_master_type text,
	IN fa_search_value text,
	INOUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_json JSONB;
BEGIN
IF UPPER(fa_master_type) in ('PORT_MASTER','PORT') THEN
    SELECT jsonb_agg(
        jsonb_build_object(
            'portName', name,
            'portCountry', country_code,
            'portCode', code,
            'type', 'PORT',
            'transportMode', transport_mode,
            'listValue', name || '-(' || code || ')'
        )
    )
    INTO lv_json
    FROM efs_port_master
    WHERE name like '%'||upper(fa_search_value)||'%';
END IF;
IF lv_json is null then
	fa_status := '[]';
else
	fa_status := lv_json::TEXT;
end if;
EXCEPTION
    WHEN OTHERS THEN
        fa_status := '{"Status":"Failure","Message":"' || REPLACE(SQLERRM, '"', '\"') || '"}';
END;
$BODY$;