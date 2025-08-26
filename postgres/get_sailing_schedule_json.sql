-- PROCEDURE: tenant_default.get_sailing_schedule_json(text, text, text, text)

-- DROP PROCEDURE IF EXISTS tenant_default.get_sailing_schedule_json(text, text, text, text);

CREATE OR REPLACE PROCEDURE tenant_default.get_sailing_schedule_json(
	IN fa_origin text,
	IN fa_destination text,
	IN fa_service text,
	INOUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_data_json jsonb;
BEGIN
    SELECT jsonb_agg(shipment_json)
    INTO lv_data_json
    FROM (
        SELECT jsonb_build_object(
            'originCode', epm.code,
			'originName',epm.name,
            'destinationCode', epm1.code,
            'isCoload', CASE 
                WHEN nssh.co_loader_id IS NOT NULL THEN 'Y'
                ELSE 'N'
            END,
            'pol', epm.code,
            'polName', nssh.origin_name,
            'pod', epm1.code,
            'podName', nssh.destination_name,
            'originCountryCode', epm.country_code,
            'destinationCountryCode', epm1.country_code,
            'scheduleId', nssh.schedule_id,
			'vessel',nssh.vessel_name,
			'voyage',nssh.route_no,
			'departureDate',nssh.etd,
			'arrivalDate',nssh.eta,
			'cutoffDate',nssh.load_port_cutoff_date,
			--'cutOffToArrival',eta-load_port_cutoff_date,
			--'departureToArrival',eta-etd
			'cutOffToArrival',EXTRACT(DAY FROM eta - load_port_cutoff_date)::INT ||' Days',
			'departureToArrival',EXTRACT(DAY FROM eta - etd)::INT ||' Days'
        ) AS shipment_json
        FROM nxt_sailing_schedule_header nssh
        JOIN efs_port_master epm ON nssh.origin_id = epm.id
        JOIN efs_port_master epm1 ON nssh.destination_id = epm1.id
        WHERE ((epm.code = fa_origin and epm1.code = fa_destination)
		 		or((fa_origin is null or fa_origin='')
				    and (fa_destination is null or fa_destination = '')))
		 and nssh.service like '%'||fa_service||'%'
    ) AS shipment_data;
 	 IF lv_data_json IS NULL THEN
        fa_status := '[]';
    ELSE
        fa_status := lv_data_json::text;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        fa_status := '{"Status":"Failure","Message":"' || REPLACE(SQLERRM, '"', '\"') || '"}';
END;
$BODY$;
ALTER PROCEDURE tenant_default.get_sailing_schedule_json(text, text, text, text)
    OWNER TO dev_user;
