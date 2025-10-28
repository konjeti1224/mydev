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
            'booked', COUNT(*) FILTER (WHERE wslv.status IN ('Booked', 'Received')),
            'arrived', COUNT(*) FILTER (WHERE wslv.status = 'Arrived'),
            'inTransit', COUNT(*) FILTER (WHERE wslv.status = 'In Transit'),
            'cancelled', COUNT(*) FILTER (WHERE wslv.status = 'Cancelled'),
            'delivered', COUNT(*) FILTER (WHERE wslv.status = 'Delivered'),
            'all', COUNT(*)
        ),
        'shipment', COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'id', wslv.id,
                    'shipment', wslv.shipment_uid,
                    'bookedDate', wslv.shipment_date,
                    'hblNumber', wslv.hbl_no,
                    'origin', wslv.origin_name,
                    'originCountryCode', wslv.origin_country_code,
                    'destination', wslv.destination_name,
                    'destinationCountryCode', wslv.destination_country_code,
                    'mode', wslv.transport_mode,
                    'orderNo', wslv.order_no,
                    'tosName', wslv.tos_name,
                    'bookingDate', wslv.order_confimration,
                    'readyDate', wslv.order_ready,
                    'pickupDate', wslv.order_pickup,
                    'departure', wslv.order_departure,
                    'arrival', wslv.order_arrival,
                    'estimatedDeparture', to_char(wslv.etd, 'DD-MON-YY'),
                    'actualDeparture', to_char(wslv.atd, 'DD-MON-YY'),
                    'estimatedArrival', to_char(wslv.eta, 'DD-MON-YY'),
                    'actualArrival', to_char(wslv.ata, 'DD-MON-YY'),
                    'status', wslv.status,
                    'roleType',
                        CASE 
                            -- ADMIN logic
                            WHEN fa_role = 'ADMIN'
                                 AND wslv.routed = 'SELF'
                                 AND (party.origin_agent_id = wud.nxt_customer_id or party.destination_agent_id = wud.nxt_customer_id)
                            THEN 'SELF'
                            WHEN fa_role = 'ADMIN'
                                 AND wslv.routed = 'AGENT'
                                 AND wslv.routed_by_id = wud.nxt_customer_id
                            THEN 'NOMINATED'
                            
                            -- AGENT logic
                            WHEN fa_role = 'AGENT'
                                 AND wslv.routed = 'SELF'
                                  AND (party.origin_agent_id = wud.nxt_customer_id or party.destination_agent_id = wud.nxt_customer_id)
                            THEN 'NOMINATED'
                            WHEN fa_role = 'AGENT'
                                 AND wslv.routed = 'AGENT'
                                 AND wslv.routed_by_id = wud.nxt_customer_id
                            THEN 'SELF'

                            ELSE NULL
                        END
                )
            ),
            '[]'::jsonb
        )
    )
    INTO lv_json
    FROM webtool_shipment_list_view wslv
	 LEFT JOIN shipment_party_detail party ON wslv.id = party.shipment_header_id
    JOIN web_user_detail wud ON (party.origin_agent_id = wud.nxt_customer_id or party.destination_agent_id = wud.nxt_customer_id or  wslv.routed_by_id = wud.nxt_customer_id)
    JOIN web_user_master wum ON wum.id = wud.registration_no   
    WHERE wum.id = fa_login_id
      AND wslv.shipment_date >= CURRENT_DATE - (fa_date_filter_days || ' days')::interval;

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
