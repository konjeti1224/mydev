
-- PROCEDURE: tenant_default.pro_sales_enquiry_json(bigint, integer)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_sales_enquiry_json(bigint, integer);

CREATE OR REPLACE PROCEDURE tenant_default.pro_sales_enquiry_json(
	IN p_registration_no bigint,
	IN p_upto_days integer,
	OUT p_result json)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    PERFORM set_config('search_path', 'tenant_default', true);

    SELECT json_agg(row_to_json(t) ORDER BY t."enquiryDate")
    INTO p_result
    FROM (
        SELECT
            EH.enquiry_no as "enquiryNo",
            EH.enquiry_date as "enquiryDate",
            EPM.code AS "origin",
            ESD.origin_name as "originName",
            EPM.country_code AS "originCountry",
            EPM1.code AS "destination",
            ESD.destination_name as "destinationName",
            EPM1.country_code AS "destinationCountry",
            EH.quote_by_date AS "validUpto",
            ETM.name AS "tos",
            SUM(ESDD.gross_weight_kgs) AS "grossWeightKgs",
            SUM(ESDD.volume_in_weight)  AS "volumeInWeight"          
        FROM
            web_user_detail WUD
            LEFT JOIN enquiry_header EH 
                   ON EH.customer_id = WUD.nxt_customer_id
            LEFT JOIN enquiry_service_detail ESD 
                   ON ESD.enquiry_detail_id = EH.id
            LEFT JOIN efs_tos_master ETM 
                   ON ETM.id = ESD.tos_id
            LEFT JOIN efs_port_master epm 
                   ON epm.id = ESD.origin_id
            LEFT JOIN efs_port_master epm1 
                   ON epm1.id = ESD.destination_id
            LEFT JOIN enquriy_service_dimention_detail ESDD 
                   ON ESDD.enquiry_detail_id = EH.id
        WHERE
            WUD.registration_no = p_registration_no
            AND EH.enquiry_date >= current_date - p_upto_days
        GROUP BY  
            EH.enquiry_no,
            EH.enquiry_date,
            EPM.code,
            ESD.origin_name,
            EPM.country_code,
            EPM1.code,
            ESD.destination_name,
            EPM1.country_code,
            EH.quote_by_date,
            ETM.name
    ) t;
END;
$BODY$;

