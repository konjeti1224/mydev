CREATE OR REPLACE PROCEDURE tenant_default.pro_webtool_main_summary_v1(
	IN fa_shipment_id bigint,
	INOUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE --set search_path to tenant_default
lv_json jsonb;
-- call pro_webtool_main_summary_v1(19899,'')
BEGIN
    SELECT jsonb_build_object(
        'shipmentSummary', COALESCE(
        jsonb_build_object(
		     'groupCompanyId', wslv.group_company_id,
              'companyId',wslv.company_id,
              'branchId',wslv.branch_id,
            'id', wslv.id,
            'shipment', wslv.shipment_uid,
            'shipper', wslv.shipper_name,
            'consignee', wslv.consignee_name,
			'notify',wslv.notify_name,
            'billing', wslv.billing_customer_name,
			'commodityDescription',wslv.commodity_description,
			'totalWeight',wslv.gross_weight_kgs,
			'totalVolume',wslv.volume_in_cbm,
			'packageType',wslv.pack_name,
			'noOfUnits',wslv.no_of_pieces,			
			'cargoPickup',wslv.pickup_required,
    		'doorDelivery',wslv.delivery_required,
			'cargoReadyDate',wslv.order_ready,
            'cargoType',wslv.cargo_type,
			'hsCode','',
			'noOfContainers', (SELECT string_agg(count_and_code, ',')
						        FROM (
						            SELECT COUNT(*) || '*' || ecm.code AS count_and_code
						            FROM shipment_container_detail scd
						            JOIN master_container_detail mcd 
						                ON scd.master_container_details_id = mcd.id
						            JOIN efs_container_master ecm 
						                ON mcd.container_id = ecm.id
						            WHERE scd.shipment_id = fa_shipment_id
						            GROUP BY ecm.code
						           ) as a),
			'containerNumber',(SELECT string_agg(mcd.container_number, ',')
						        FROM shipment_container_detail scd
						        JOIN master_container_detail mcd 
						        ON scd.master_container_details_id = mcd.id
						        WHERE scd.shipment_id = fa_shipment_id
						      ),
			'status', wslv.status
        ),'{}'::jsonb),
'milestones', COALESCE(
            (
                SELECT jsonb_agg(event_obj ORDER BY sort_order)
                FROM (
                    SELECT jsonb_build_object(
                        'id', ed.event_id,
                        'eventCode', ed.event_code,
                        'milestone', ed.event_name,
                        'eventDate', ed.event_date,
                        'plannedDate', ed.planned_date,
                        'actualDate', ed.actual_date,
                        'status', ed.status
                    ) AS event_obj,
                    CASE ed.event_code
                        WHEN '101' THEN 1  -- Order Confirmation
                        WHEN 'CRD' THEN 2  -- Cargo Received
                        WHEN '201' THEN 3  -- Departure
                        WHEN '203' THEN 4  -- Arrival
						 WHEN '304' THEN 5  -- Do
                        WHEN '306' THEN 6  -- Proof of Delivery
                        ELSE 999
                    END AS sort_order
                    FROM webtool_milestone_event_detail_view ed
                    WHERE ed.source_id = fa_shipment_id
                      AND ed.source_type = 'SHIPMENT'
                      AND ed.event_code IN ('101', 'CRD', '201', '203','304','306')
                ) ordered_events
            ),
            '[]'::jsonb),
'co2e',(SELECT json_agg(response::jsonb -> 'co2e') AS co2e_data
        FROM searoutes_response
		WHERE shipment_id = fa_shipment_id),
'co2eSummary',(SELECT response::jsonb  AS co2e_summary
		FROM searoutes_response
		WHERE shipment_id = fa_shipment_id),
'airTrackingEvents',(select mawb_info
					from shipment_air_trackings_events  
					where shipment_id = fa_shipment_id),
'trackingEvents', COALESCE(
    (
        SELECT jsonb_agg(tracking_data)
        FROM (
            SELECT 
                cse.response_header_id AS "responseHeaderId",
                cst.tracking_url       AS "trackingUrl",
                cst.container_numbers  AS "containerNumber",
                jsonb_agg(
                    jsonb_build_object(
                        'eventCode',      cse.event_code,
                        'eventName',      cse.event_name,
                        'estimatedTime',  cse.estimated_time,
                        'actualTime',     cse.actual_time,
                        'location',       cse.location,
                        'locationCode',   cse.location_code,
                        'transportName',  cse.transport_name,
                        'tripNumber',     cse.trip_number,
                        'transportMode',  cse.transport_mode
                    )
                    ORDER BY cse.estimated_time
                ) AS eventDetails
            FROM cargoes_shipment_events cse
            JOIN cargoes_shipment_tracking_response_header cstrh
              ON cse.response_header_id = cstrh.id
            JOIN cargoes_shipment_tracking cst
              ON cstrh.container_number = cst.container_numbers
             AND cstrh.master_id = cst.master_shipment_id
            WHERE cstrh.master_id = wslv.master_header_id
            GROUP BY cse.response_header_id, cst.tracking_url, cst.container_numbers
        ) AS tracking_data
    ),
    '[]'::jsonb),
'documents', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'id', ad.id,
                        'documentName',ad.document_name,
                        'referenceNumber',ad.reference,
                        'lastUpdated',ad.updated_date,
                        'documentType',(select name from efs_document_type_master
										 where id =ad.document_type_id),
                        'link', ''
                    )
                )
                FROM attachment_detail ad
                WHERE ad.source_id = fa_shipment_id
                  AND ad.source_type = 'SHIPMENT'
            ),
            '[]'::jsonb
        ),
        'originMilestones', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'id', ed.event_id,
                        'source_id', ed.source_id,
                        'eventCode', ed.event_code,
                        'milestone', ed.event_name,
                        'description', ed.description,
                        'eventDate', ed.event_date,
                        'status', ed.status
                    )
                )
                FROM webtool_milestone_event_detail_view ed
                WHERE ed.source_id = fa_shipment_id
                  AND ed.source_type = 'SHIPMENT'
                  AND ed.milestone_type = 'ORIGIN'
            ),
            '[]'::jsonb
        ),
        'freightMilestones', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'id', ed.event_id,
                        'eventCode', ed.event_code,
                        'milestone', ed.event_name,
                        'description', ed.description,
                        'eventDate', ed.event_date,
                        'status', ed.status
                    )
                )
                FROM webtool_milestone_event_detail_view ed
                WHERE ed.source_id = fa_shipment_id
                  AND ed.source_type = 'SHIPMENT'
                  AND ed.milestone_type = 'FREIGHT'
            ),
            '[]'::jsonb
        ),
        'destinationMilestones', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'id', ed.event_id,
                        'eventCode', ed.event_code,
                        'milestone', ed.event_name,
                        'description', ed.description,
                        'eventDate', ed.event_date,
                        'status', ed.status
                    )
                )
                FROM webtool_milestone_event_detail_view ed
                WHERE ed.source_id = fa_shipment_id
                  AND ed.source_type = 'SHIPMENT'
                  AND ed.milestone_type = 'DESTINATION'
            ),
            '[]'::jsonb
        )
    ) INTO lv_json
    FROM webtool_shipment_list_view wslv
    WHERE wslv.id = fa_shipment_id;

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
