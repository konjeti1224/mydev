-- PROCEDURE: tenant_default.pro_webtool_dsr_report(bigint, integer, text)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_webtool_dsr_report(bigint, integer, text);

CREATE OR REPLACE PROCEDURE tenant_default.pro_webtool_dsr_report(
	IN fa_login_id bigint,
	IN fa_date_filter_days integer,
	INOUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_data_json jsonb;
    lv_final_json jsonb;
    lv_user_columns jsonb;
    lv_user_trigger jsonb;
BEGIN
    -- Step 1: Get column preferences for the user
    SELECT column_preferences
    INTO lv_user_columns
    FROM web_user_column
    WHERE web_user_master_id = fa_login_id;

    -- Step 2: Generate the data JSON array
    SELECT jsonb_agg(shipment_json) INTO lv_data_json
    FROM (
        SELECT jsonb_build_object(
            'id', ROW_NUMBER() OVER (ORDER BY wslv.shipment_uid),
            'shipmentid', wslv.id,
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
            'estimatedDeparture', to_char(wslv.etd,'DD-MON-YY'),
            'actualDeparture', to_char(wslv.atd,'DD-MON-YY'),
            'estimatedArrival', to_char(wslv.eta,'DD-MON-YY'),
            'actualArrival', to_char(wslv.ata,'DD-MON-YY'),
            'status', wslv.status,
            'shipper', wslv.shipper_name,
            'consignee', wslv.consignee_name,
            'notify', wslv.notify_name,
            'billing', wslv.billing_customer_name,
            'commodityDescription', wslv.commodity_description,
            'totalWeight', wslv.gross_weight_kgs,
            'totalVolume', wslv.volume_in_cbm,
            'packageType', wslv.pack_name,
            'noOfUnits', wslv.no_of_pieces,
            'cargoPickup', wslv.pickup_required,
            'doorDelivery', wslv.delivery_required,
            'cargoReadyDate', wslv.order_ready,
            'cargoType', wslv.cargo_type,
            'hsCode', '',
            'noOfContainers', (
                SELECT string_agg(count_and_code, ',')
                FROM (
                    SELECT COUNT(*) || '*' || ecm.code AS count_and_code
                    FROM shipment_container_detail scd
                    JOIN master_container_detail mcd ON scd.master_container_details_id = mcd.id
                    JOIN efs_container_master ecm ON mcd.container_id = ecm.id
                    WHERE scd.shipment_id = wslv.id
                    GROUP BY ecm.code
                ) AS container_counts
            ),
            'containerNumber', (
                SELECT string_agg(mcd.container_number, ',')
                FROM shipment_container_detail scd
                JOIN master_container_detail mcd ON scd.master_container_details_id = mcd.id
                WHERE scd.shipment_id = wslv.id
            )				
        ) AS shipment_json
        FROM webtool_shipment_list_view wslv
        JOIN web_user_detail wud ON wslv.customer_id = wud.nxt_customer_id
        JOIN web_user_master wum ON wum.id = wud.registration_no
        WHERE wum.id = fa_login_id
          AND wslv.shipment_date >= CURRENT_DATE - (fa_date_filter_days || ' days')::interval
    ) AS shipment_data;

    -- Step 3: Get column preferences for the user
    SELECT jsonb_agg(trigger_data) INTO lv_user_trigger
    FROM (
        SELECT jsonb_build_object('mailId',wudt.mail_id,
		   'dayStatus',wudt.day_status,
		   'days',wudt.days,
		   'statusList',wudt.status_list,
		   'lastTriggeredDate',wudt.last_trigger_date
        ) AS trigger_data
		FROM web_user_dsr_trigger wudt
        WHERE web_user_master_id = fa_login_id
    );
    

    -- Step 4: Combine column preferences and data
    IF lv_data_json IS NULL THEN
        fa_status := '{"Status":"Error","Message":"Shipment List not available"}';
    ELSE
        lv_final_json := jsonb_build_object(
            'userColumns', lv_user_columns,
            'data', lv_data_json,
            'userTrigger', lv_user_trigger
        );

        fa_status := lv_final_json::text;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        fa_status := '{"Status":"Failure","Message":"' || REPLACE(SQLERRM, '"', '\"') || '"}';
END;
$BODY$;
ALTER PROCEDURE tenant_default.pro_webtool_dsr_report(bigint, integer, text)
    OWNER TO dev_user;
