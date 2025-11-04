CREATE OR REPLACE VIEW tenant_default.webtool_shipment_list_view
 AS
 SELECT ssd.group_company_id,
    ssd.company_id,
    ssd.branch_id,
    sh.id,
    sh.shipment_uid,
    sh.shipment_date,
    sh.customer_id,
    sh.customer_name,
    sh.routed,
    sh.routed_by_id,
    sh.origin_id,
    ( SELECT efs_port_master.country_code
           FROM efs_port_master
          WHERE efs_port_master.id = sh.origin_id) AS origin_country_code,
    sh.origin_name,
    sh.destination_id,
    ( SELECT efs_port_master.country_code
           FROM efs_port_master
          WHERE efs_port_master.id = sh.destination_id) AS destination_country_code,
    sh.destination_name,
    sh.service_type,
    sh.tos_id,
    sh.tos_name,
    esm.transport_mode,
    esm.name AS service_name,
    esm.import_export,
    party.shipper_name,
    party.consignee_name,
    event.order_confimration,
    event.order_ready,
    event.order_pickup,
    event.order_departure,
    event.order_arrival,
    COALESCE(mh.etd, sad.etd) AS etd,
    mh.atd,
    COALESCE(mh.eta, sad.eta) AS eta,
    mh.ata,
    sad.po_number AS order_no,
        CASE
            WHEN ssd.service_status::text = 'Cancelled'::text THEN 'Cancelled'::text
            WHEN event.order_delivered IS NOT NULL THEN 'Delivered'::text
            WHEN event.order_arrival IS NOT NULL THEN 'Arrived'::text
            WHEN event.order_departure IS NOT NULL THEN 'In Transit'::text
            WHEN event.order_cargo_received IS NOT NULL THEN 'Received'::text
            ELSE 'Booked'::text
        END AS status,
    COALESCE(( SELECT doc_union.document_no
           FROM ( SELECT transport_document_ocean.document_no
                   FROM transport_document_ocean
                  WHERE transport_document_ocean.source_id = sh.id AND transport_document_ocean.document_type::text = 'HBL'::text
                UNION ALL
                 SELECT transport_document_air.document_no
                   FROM transport_document_air
                  WHERE transport_document_air.source_id = sh.id AND transport_document_air.document_type::text = 'HAWB'::text) doc_union
         LIMIT 1), sad.transport_document_no) AS hbl_no,
    ssd.billing_customer_id,
    ( SELECT customer_master.name
           FROM customer_master
          WHERE customer_master.id = ssd.billing_customer_id) AS billing_customer_name,
    scd.commodity_description,
    ( SELECT customer_master.name
           FROM customer_master
          WHERE customer_master.id = party.notify_customer_1_id) AS notify_name,
    scd.pack_id,
    epm.name AS pack_name,
    scd.gross_weight_kgs,
    scd.volume_in_cbm,
    scd.no_of_pieces,
    spdd.pickup_required,
    spdd.delivery_required,
    mh.service_code AS cargo_type,
    mh.id AS master_header_id,
    scd.external_pack_id,
    epm1.name AS external_pack_name,
    party.origin_agent_id,
    party.destination_agent_id,
	cm.customer_uid as origin_agent_code,
	cm.name as origin_agent_name,
	cm1.customer_uid as destination_agent_code,
	cm1.name as destination_agent_name
   FROM shipment_header sh
     JOIN shipment_service_detail ssd ON sh.id = ssd.shipment_header_id
     JOIN shipment_cargo_detail scd ON sh.id = scd.shipment_header_id
     JOIN shipment_party_detail party ON sh.id = party.shipment_header_id
     JOIN efs_service_master esm ON ssd.service_id = esm.id
     JOIN efs_branch_master ebm ON ssd.branch_id = ebm.id
     LEFT JOIN efs_pack_master epm ON epm.id = scd.pack_id
     LEFT JOIN efs_pack_master epm1 ON epm1.id = scd.external_pack_id
	 LEFT JOIN customer_master cm on cm.id = party.origin_agent_id
	 LEFT JOIN customer_master cm1 on cm1.id = party.destination_agent_id
     LEFT JOIN shipment_pickup_delivery_detail spdd ON sh.id = spdd.shipment_header_id
     LEFT JOIN shipment_addl_detail sad ON sh.id = sad.shipment_header_id
     LEFT JOIN master_service_link_detail msd ON sh.id = msd.shipment_id
     LEFT JOIN master_header mh ON mh.id = msd.master_id
     LEFT JOIN ( SELECT nxt_event_detail.source_id,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '101'::text THEN nxt_event_detail.event_date
                    ELSE NULL::timestamp without time zone
                END) AS order_confimration,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '102'::text THEN nxt_event_detail.event_date
                    ELSE NULL::timestamp without time zone
                END) AS order_ready,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = 'CRD'::text THEN nxt_event_detail.event_date
                    ELSE NULL::timestamp without time zone
                END) AS order_cargo_received,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '103'::text THEN nxt_event_detail.event_date
                    ELSE NULL::timestamp without time zone
                END) AS order_pickup,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '201'::text THEN nxt_event_detail.actual_date
                    ELSE NULL::timestamp without time zone
                END) AS order_departure,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '203'::text THEN nxt_event_detail.actual_date
                    ELSE NULL::timestamp without time zone
                END) AS order_arrival,
            max(
                CASE
                    WHEN nxt_event_detail.event_code::text = '306'::text THEN nxt_event_detail.actual_date
                    ELSE NULL::timestamp without time zone
                END) AS order_delivered
           FROM nxt_event_detail
          WHERE nxt_event_detail.source_type::text = 'SHIPMENT'::text
          GROUP BY nxt_event_detail.source_id) event ON sh.id = event.source_id;


