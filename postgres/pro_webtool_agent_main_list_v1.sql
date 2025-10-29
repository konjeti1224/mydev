-- PROCEDURE: tenant_default.pro_webtool_agent_main_list_v1(text, bigint, integer, text)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_webtool_agent_main_list_v1(text, bigint, integer, text);

CREATE OR REPLACE PROCEDURE tenant_default.pro_webtool_agent_main_list_v1(
	IN fa_role text,
	IN fa_login_id bigint,
	IN fa_date_filter_days integer,
	INOUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_json jsonb;
BEGIN
    SELECT jsonb_build_object(
    'statusWiseCount', jsonb_build_object(
        'self', COUNT(*) FILTER (WHERE t.roleType = 'SELF'),
        'nominated', COUNT(*) FILTER (WHERE t.roleType = 'NOMINATED'),
        'all', COUNT(*)
    ),
    'shipment', COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'id', t.id,
                'shipment', t.shipment_uid,
                'bookedDate', t.shipment_date,
                'hblNumber', t.hbl_no,
                'origin', t.origin_name,
                'originCountryCode', t.origin_country_code,
                'destination', t.destination_name,
                'destinationCountryCode', t.destination_country_code,
                'mode', t.transport_mode,
                'orderNo', t.order_no,
                'tosName', t.tos_name,
                'bookingDate', t.order_confimration,
                'readyDate', t.order_ready,
                'pickupDate', t.order_pickup,
                'departure', t.order_departure,
                'arrival', t.order_arrival,
                'estimatedDeparture', to_char(t.etd, 'DD-MON-YY'),
                'actualDeparture', to_char(t.atd, 'DD-MON-YY'),
                'estimatedArrival', to_char(t.eta, 'DD-MON-YY'),
                'actualArrival', to_char(t.ata, 'DD-MON-YY'),
                'status', t.status,
                'roleType', t.roleType
            )
        ),
        '[]'::jsonb
    )
) into lv_json
FROM (
    SELECT 
        wslv.id,
        wslv.shipment_uid,
        wslv.shipment_date,
        wslv.hbl_no,
        wslv.origin_name,
        wslv.origin_country_code,
        wslv.destination_name,
        wslv.destination_country_code,
        wslv.transport_mode,
        wslv.order_no,
        wslv.tos_name,
        wslv.order_confimration,
        wslv.order_ready,
        wslv.order_pickup,
        wslv.order_departure,
        wslv.order_arrival,
        wslv.etd,
        wslv.atd,
        wslv.eta,
        wslv.ata,
        wslv.status,
        wslv.routed,
        wslv.routed_by_id,
        party.origin_agent_id,
        party.destination_agent_id,
        wud.nxt_customer_id,
        wum.id AS user_id,
        CASE 
            -- ADMIN logic
            WHEN fa_role = 'ADMIN'
                 AND wslv.routed = 'Self'
                 AND (party.origin_agent_id = wud.nxt_customer_id OR party.destination_agent_id = wud.nxt_customer_id)
            THEN 'SELF'
            WHEN fa_role = 'ADMIN'
                 AND wslv.routed = 'Agent'
                 AND wslv.routed_by_id = wud.nxt_customer_id
            THEN 'NOMINATED'
            
            -- AGENT logic
            WHEN fa_role = 'AGENT'
                 AND wslv.routed = 'Self'
                 AND (party.origin_agent_id = wud.nxt_customer_id OR party.destination_agent_id = wud.nxt_customer_id)
            THEN 'NOMINATED'
            WHEN fa_role = 'AGENT'
                 AND wslv.routed = 'Agent'
                 AND wslv.routed_by_id = wud.nxt_customer_id
            THEN 'SELF'
            ELSE NULL
        END AS roleType
    FROM webtool_shipment_list_view wslv
    LEFT JOIN shipment_party_detail party 
        ON wslv.id = party.shipment_header_id
    JOIN web_user_detail wud 
        ON (party.origin_agent_id = wud.nxt_customer_id 
            OR party.destination_agent_id = wud.nxt_customer_id 
            OR wslv.routed_by_id = wud.nxt_customer_id)
    JOIN web_user_master wum 
        ON wum.id = wud.registration_no
    WHERE wum.id = fa_login_id
      AND wslv.shipment_date >= CURRENT_DATE - (fa_date_filter_days || ' days')::interval
) t;

    -- Return result
    IF lv_json IS NULL THEN
        fa_status := '{"Status":"Error","Message":"Shipment List not available"}';
    ELSE
        fa_status := lv_json::text;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        fa_status := '{"Status":"Failure","Message":"' || REPLACE(SQLERRM, '"', '\"') || '"}';
END;
$BODY$;
ALTER PROCEDURE tenant_default.pro_webtool_agent_main_list_v1(text, bigint, integer, text)
    OWNER TO dev_user;
