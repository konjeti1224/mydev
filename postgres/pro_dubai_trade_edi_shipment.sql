CREATE OR REPLACE PROCEDURE tenant_default.pro_dubai_trade_edi_shipment(
	IN p_master_id text,
	OUT p_result text,
	IN p_shipment_id bigint DEFAULT NULL::bigint)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
--voyage
    v_line_code TEXT;
    v_voyage_agent_code TEXT;
    v_vessel_name TEXT;
    v_agent_voyage_number TEXT;
    v_port_code TEXT;
    v_eta DATE;
    v_rotation_number TEXT;
    v_message_type TEXT := 'MFI';
    v_no_of_instalment INT;
    v_manifest_seq_num TEXT;
    lv_voy_line_text  TEXT;
--bill of lading
    v_bill_of_lading_no              TEXT;
    v_box_partnering_line_code       TEXT;
    v_box_partnering_agent_code      TEXT;
    v_port_code_of_origin            TEXT;
    v_port_code_of_loading           TEXT;
    v_port_code_of_discharge         TEXT;
    v_port_code_of_destination       TEXT;
    v_date_of_loading                DATE;
    v_manifest_registration_number   TEXT;
    v_trade_code                     TEXT;
    v_trans_shipment_mode            TEXT;
    v_bill_of_lading_owner_name      TEXT;
    v_bill_of_lading_owner_address   TEXT;
    v_cargo_code                     TEXT;
    v_consolidated_cargo_indicator   TEXT;
    v_storage_request_code           TEXT;
    v_container_service_type         TEXT;
    v_country_of_origin              TEXT;
    v_original_consignee_name        TEXT;
    v_original_consignee_address     TEXT;
    v_original_vessel_name           TEXT;
    v_original_voyage_number         TEXT;
    v_original_bol_number            TEXT;
    v_original_shipper_name          TEXT;
    v_original_shipper_address       TEXT;
    v_shipper_name                   TEXT;
    v_shipper_address                TEXT;
    v_shipper_country_code           TEXT;
    v_consignee_code                 TEXT;
    v_consignee_name                 TEXT;
    v_consignee_address              TEXT;
    v_notify1_code                   TEXT;
    v_notify1_name                   TEXT;
    v_notify1_address                TEXT;
    v_notify2_code                   TEXT;
    v_notify2_name                   TEXT;
    v_notify2_address                TEXT;
    v_notify3_code                   TEXT;
    v_notify3_name                   TEXT;
    v_notify3_address                TEXT;
    v_marks_numbers                  TEXT;
    v_commodity_code                 TEXT;
    v_commodity_description          TEXT;
    v_packages                       NUMERIC(9,0);
    v_package_type                   TEXT;
    v_package_type_code              TEXT;
    v_container_number               TEXT;
    v_check_digit                    TEXT;
    v_no_of_containers               NUMERIC(3,0);
    v_no_of_teus                     NUMERIC(3,0);
    v_total_tare_weight_in_mt        NUMERIC(4,1);
    v_cargo_weight_in_kg             NUMERIC(9,3);
    v_gross_weight_in_kg             NUMERIC(9,3);
    v_cargo_volume_in_cubic_metre    NUMERIC(9,3);
    v_total_quantity                 NUMERIC(9,0);
    v_freight_tonne                  NUMERIC(9,3);
    v_no_of_pallets                  INT;
    v_slac_indicator                 TEXT;
    v_contract_carriage_condition    TEXT;
    v_remarks                        TEXT;
    lv_bol_line_text  TEXT; 
    v_seal_number     TEXT;
    lv_con_line_text TEXT;
    V_USED_NEW TEXT;
    v_dangerous_goods_indicator        TEXT;
    v_imo_class_number                 TEXT;
    v_un_number_of_dangerous_goods     TEXT;
    v_flash_point                      NUMERIC(5,1);
    v_unit_of_temperature              TEXT;
    v_storage_requested_for_dg         TEXT;
    v_refrigeration_required           TEXT;
    v_minimum_temperature_refrigeration NUMERIC(5,1);
    v_maximum_temperature_refrigeration NUMERIC(5,1);
    v_unit_of_temperature_ref          TEXT;
    lv_ctr_line_text TEXT;
    rec RECORD;  
    v_no_of_container_related_bol NUMERIC(4,0) := 0;
    v_no_of_other_bol NUMERIC(4,0) := 0;
    v_end_remarks TEXT := 'Version No: 1, Product Sr. No: EDI/1.0';
    lv_end_line_text TEXT;
    v_service TEXT;
    v_master_id BIGINT;
    v_current_shipment_id BIGINT;
    v_temp_count INT;
BEGIN
select clvd.config_value
INTO v_line_code
from configuration_header ch
left JOIN configuration_link_value_details clvd ON clvd.config_header_id = ch.id
where ch.config_key = 'LINE_CODE';

select clvd.config_value
INTO v_voyage_agent_code
from configuration_header ch
left JOIN configuration_link_value_details clvd ON clvd.config_header_id = ch.id
where ch.config_key = 'AGENT_CODE';

 SELECT 
    mcd.vessel_name,
    mcd.route_no,
    epm.code,
    mh.eta,
    mscd.rotation_no,
    substring(mh.service FROM '\(([^)]+)\)'),
     CASE service_code
        WHEN 'FCL CONTAINER'     THEN 'F'
        WHEN 'LCL CONTAINER'     THEN 'L'
        WHEN 'EMPTY CONTAINER'   THEN 'M'
        WHEN 'BULK SOLID'        THEN 'B'
        WHEN 'BULK LIQUID'       THEN 'Q'
        WHEN 'RO-RO UNIT'        THEN 'R'
        WHEN 'PASSENGER'         THEN 'P'
        WHEN 'GENERAL CARGO ( BREAK BULK )' THEN 'G'
        ELSE NULL
    END AS cargo_code
    INTO v_vessel_name,
         v_agent_voyage_number,
         v_port_code,
         v_eta,
         v_rotation_number,
         v_service,
         v_cargo_code
FROM master_header mh
JOIN master_carrier_detail mcd ON mcd.master_id = mh.id
JOIN master_service_customs_detail mscd ON mscd.master_id = mh.id
JOIN efs_port_master epm ON epm.id = mh.destination_id
WHERE mh.master_uid = p_master_id;
    -- Build output string in the required format
    lv_voy_line_text := 'VOY,"' ||
	                   COALESCE(v_line_code,'') || '","' ||
					   COALESCE(v_voyage_agent_code,'') || '","' ||
				       COALESCE( v_vessel_name,'') || '","' || 
					   v_agent_voyage_number || '","' || 
					   v_port_code    || '","' ||
				       TO_CHAR(v_eta, 'DD-Mon-YYYY') || '","' ||  
					   COALESCE(v_rotation_number,'') || '","' || 
					   v_message_type || '","' || 
					   COALESCE(v_no_of_instalment::TEXT,'') || '","' || 
				    LPAD(COALESCE(v_manifest_seq_num,'0'), 5, '0') || '"';

    SELECT id INTO v_master_id FROM master_header WHERE master_uid = p_master_id;

    p_result := lv_voy_line_text;

    FOR v_current_shipment_id IN
        SELECT distinct sh.id
        FROM shipment_header sh
        JOIN shipment_service_detail ssd ON sh.id = ssd.shipment_header_id
        JOIN master_service_link_detail msld ON ssd.id = msld.shipment_service_id
        WHERE msld.master_id = v_master_id
        AND (p_shipment_id IS NULL OR sh.id = p_shipment_id)
    LOOP

SELECT
    COALESCE(TDA.DOCUMENT_NO,TDO.DOCUMENT_NO,SAD.TRANSPORT_DOCUMENT_NO) AS HBL_NO,
    epm.CODE AS ORIGIN_PORT_CODE,
    epm1.CODE AS DEST_PORT_CODE,
    CASE 
    WHEN SSD.other_trade = 'ReExport' THEN 'Import'
    ELSE SSD.other_trade
    END,
    SH.SERVICE_TYPE,
    --SSD.PRODUCT_CODE AS SERVICE_NAME,
    epm.country_code,
    SP.SHIPPER_NAME,
    PRC_GET_CUSTOMER_ADDRESS (SP.SHIPPER_ADDRESS_ID) AS SHIPPER_ADDRESS,
    --SP.CONSIGNEE_ID,
    SP.CONSIGNEE_NAME,
    PRC_GET_CUSTOMER_ADDRESS (SP.CONSIGNEE_ADDRESS_ID) AS CONSIGNEE_ADDRESS,
    --SP.NOTIFY_CUSTOMER_1_ID,
    cm.name,
    PRC_GET_CUSTOMER_ADDRESS (SP.NOTIFY_CUSTOMER_1_ADDRESS_ID) AS NOTIFY1_ADDRESS,
    cm1.name,
    PRC_GET_CUSTOMER_ADDRESS (SP.NOTIFY_CUSTOMER_2_ADDRESS_ID) AS NOTIFY2_ADDRESS,
    regexp_replace(sc.MARKS_AND_NUMBERS, '\s+', ' ', 'g'),
    sc.imo_class_id,
    regexp_replace(sc.COMMODITY_DESCRIPTION, '\s+', ' ', 'g'),
     sc.no_of_pieces,
     ep.code,
     ep.name,
    sc.chargeable_unit,
    sc.gross_weight_kgs,
    CASE 
    WHEN sc.hazardous = 'Yes' THEN 'Y'
    ELSE 'N'
    END
    
INTO v_bill_of_lading_no,
     v_port_code_of_origin,
     v_port_code_of_discharge,
     v_trade_code,
     v_container_service_type,
     v_country_of_origin,
     v_shipper_name,
     v_shipper_address,
    -- v_consignee_code,
     v_consignee_name,
     v_consignee_address,
    -- v_notify1_code,
    v_notify1_name,
     v_notify1_address,
     v_notify2_name,
     v_notify2_address,
     v_marks_numbers,
     v_commodity_code,
     v_commodity_description,
     v_no_of_pallets,
     v_package_type_code,
     v_package_type,
     v_cargo_volume_in_cubic_metre,
     v_gross_weight_in_kg,
     v_dangerous_goods_indicator
FROM
    SHIPMENT_HEADER SH
    LEFT JOIN SHIPMENT_SERVICE_DETAIL SSD ON SSD.SHIPMENT_HEADER_ID = SH.ID
    LEFT JOIN shipment_addl_detail sad ON sad.shipment_header_id = sh.id
    LEFT JOIN MASTER_SERVICE_LINK_DETAIL MSLD ON MSLD.SHIPMENT_SERVICE_ID = SSD.ID
    LEFT JOIN MASTER_SERVICE_DETAIL MSD ON MSLD.MASTER_SERVICE_ID = MSD.ID
    LEFT JOIN MASTER_HEADER MH ON MSD.MASTER_ID = MH.ID
    LEFT JOIN MASTER_CARRIER_DETAIL MCD ON MCD.MASTER_ID = MH.ID
    LEFT JOIN SHIPMENT_PARTY_DETAIL SP ON SP.SHIPMENT_HEADER_ID = SH.ID
    LEFT JOIN SHIPMENT_CARGO_DETAIL SC ON SC.SHIPMENT_HEADER_ID = SH.ID
    LEFT JOIN TRANSPORT_DOCUMENT_AIR TDA ON TDA.SOURCE_ID = SH.ID
    LEFT JOIN TRANSPORT_DOCUMENT_OCEAN TDO ON TDO.SOURCE_ID = SH.ID
    LEFT JOIN efs_port_master epm ON epm.id = SH.ORIGIN_ID
    LEFT JOIN efs_port_master epm1 ON epm1.id = SH.destination_id
    LEFT JOIN efs_pack_master ep ON ep.id = SC.PACK_ID
    LEFT JOIN CUSTOMER_MASTER cm on cm.ID = SP.notify_customer_1_id
    LEFT JOIN CUSTOMER_MASTER cm1 on cm1.ID = SP.notify_customer_2_id
WHERE    SH.ID = v_current_shipment_id;

if substr(v_trade_code,1,1) = 'T' then
select CASE transport_mode
        WHEN 'Ocean' THEN 'S'
        WHEN 'Air'   THEN 'A'
        WHEN 'Road'  THEN 'R'
        ELSE NULL
    END AS mode_code
    into v_trans_shipment_mode
from efs_service_master 
where code =v_container_service_type;
v_consignee_code = 'T9999';
else
v_consignee_code ='D9999';
end if;
if v_container_service_type = 'LCL' then                                                                                                                                                                                                                                          
    v_consolidated_cargo_indicator = 'Y';    
    SELECT container_sl_no
    into v_container_number
    FROM shipment_container_detail
    WHERE shipment_id = v_current_shipment_id
    LIMIT 1;
v_check_digit = RIGHT(v_container_number, 1);
v_freight_tonne = v_gross_weight_in_kg/1000;
else                                                                                                                                                                                                                                          
    v_consolidated_cargo_indicator = 'N';
    SELECT count(1)
    into v_no_of_containers
    FROM shipment_container_detail
    WHERE shipment_id = v_current_shipment_id;

SELECT SUM(ecm.teu::numeric) AS teu,
CAST(
           SUM(
               COALESCE(NULLIF(TRIM(ecm.empty_tare_weight), '')::numeric, 0) / 1000
           ) AS NUMERIC(4,1)
       ) AS total_empty_tare_weight_mt
into v_no_of_teus,v_total_tare_weight_in_mt
FROM master_header mh
JOIN master_container_detail mcd
  ON mh.id = mcd.master_id
JOIN efs_container_master ecm
  ON mcd.container_id = ecm.id
JOIN shipment_container_detail scd
  ON mcd.id = scd.master_container_details_id
WHERE mh.id = v_master_id
  AND scd.shipment_id = v_current_shipment_id;     

 
end if;        

SELECT ecm.code
INTO v_shipper_country_code
FROM shipment_party_detail spd
LEFT JOIN party_address_detail pad 
       ON pad.id = spd.shipper_address_id
 LEFT JOIN efs_country_master ecm 
       ON ecm.id = pad.country_id
WHERE spd.shipment_header_id = v_current_shipment_id;

 lv_bol_line_text := 'BOL,"' ||v_bill_of_lading_no|| '","' ||
                             COALESCE(v_line_code,'')|| '","' ||
                             COALESCE(v_voyage_agent_code,'')|| '","' ||
                             v_port_code_of_origin|| '","' ||
                             v_port_code_of_origin|| '","' ||
                             v_port_code_of_discharge|| '","' ||
                             v_port_code_of_discharge|| '","' ||
                             COALESCE(TO_CHAR(v_eta, 'DD-Mon-YYYY')::text,'')|| '","' ||
                             COALESCE(v_manifest_registration_number,'')|| '","' ||
                             COALESCE(substr(v_trade_code,1,1),'')|| '","' ||
                             COALESCE(v_trans_shipment_mode,'')|| '","' ||
                             COALESCE(v_bill_of_lading_owner_name,'')|| '","' ||
                             COALESCE(v_bill_of_lading_owner_address,'')|| '","' ||
                             COALESCE(v_cargo_code,'')|| '","' ||
                             COALESCE(v_consolidated_cargo_indicator,'')|| '","' ||
                             COALESCE(v_storage_request_code,'')|| '","' ||
                             COALESCE(v_service,'')|| '","' ||
                             COALESCE(v_country_of_origin,'')|| '","' ||
                             COALESCE(v_original_consignee_name,'')|| '","' ||
                             COALESCE(v_original_consignee_address,'')|| '","' ||
                             COALESCE(v_original_vessel_name,'')|| '","' ||
                             COALESCE(v_original_voyage_number,'')|| '","' ||
                             COALESCE(v_original_bol_number,'')|| '","' ||             
                             COALESCE(v_original_shipper_name,'')|| '","' ||
                             COALESCE(v_original_shipper_address,'')|| '","' ||
                             COALESCE(v_shipper_name,'')|| '","' ||
                             COALESCE(v_shipper_address,'')|| '","' ||
                             COALESCE(v_shipper_country_code,'')|| '","' ||
                             COALESCE(v_consignee_code,'')|| '","' ||
                             COALESCE(v_consignee_name,'')|| '","' ||
                             COALESCE(v_consignee_address,'')|| '","' ||
                             COALESCE(v_notify1_code,'')|| '","' ||
                             COALESCE(v_notify1_name,'')|| '","' ||
                             COALESCE(v_notify1_address,'')|| '","' ||
                             COALESCE(v_notify2_code,'')|| '","' ||
                             COALESCE(v_notify2_name,'')|| '","' ||
                             COALESCE(v_notify2_address,'')|| '","' ||
                             COALESCE(v_notify3_code,'')|| '","' ||
                             COALESCE(v_notify3_name,'')|| '","' ||
                             COALESCE(v_notify3_address,'')|| '","' ||                             
                             COALESCE(v_marks_numbers,'')|| '","' ||
                             COALESCE(v_commodity_code,'770000')|| '","' ||
                             COALESCE(v_commodity_description,'')|| '","' ||
                             COALESCE(v_no_of_pallets::text,'')|| '","' ||
                             COALESCE(v_package_type,'')|| '","' ||
                             COALESCE(v_package_type_code,'')|| '","' ||
                             COALESCE(v_container_number,'')|| '","' ||
                             COALESCE(v_check_digit,'')|| '","' ||
                             COALESCE(v_no_of_containers::text,'')|| '","' ||
                             COALESCE(v_no_of_teus::text,'')|| '","' ||
                             COALESCE(v_total_tare_weight_in_mt::text,'')|| '","' ||
                             COALESCE(v_gross_weight_in_kg::text,'')|| '","' ||
                             COALESCE(v_gross_weight_in_kg::text,'')|| '","' ||
                             COALESCE(v_cargo_volume_in_cubic_metre::text,'')|| '","' ||
                             COALESCE(v_no_of_pallets::text,'')|| '","' ||
                             COALESCE(v_freight_tonne::text,'')|| '","' ||
                             COALESCE(v_no_of_pallets::text,'')|| '","' ||
                             COALESCE(v_slac_indicator::text,'')|| '","' ||
                             COALESCE(v_contract_carriage_condition::text,'')|| '","' ||
                             COALESCE(v_remarks,'')||'"';

 /*lv_con_line_text := 'CON,"'|| COALESCE(v_seal_number,'')|| '","' ||
                             COALESCE(v_marks_numbers,'')|| '","' ||
                            COALESCE(v_commodity_description,'')|| '","' ||
                            COALESCE(V_USED_NEW,'')|| '","' ||
                            COALESCE(v_commodity_code,'770000')|| '","' ||
                            COALESCE(v_no_of_pallets::text,'')|| '","' ||
                            COALESCE(v_package_type,'')|| '","' ||
                            COALESCE(v_package_type_code,'')|| '","' ||
                            COALESCE(v_no_of_pallets::text,'')|| '","' ||
                            COALESCE(v_gross_weight_in_kg::text,'')|| '","' ||
                            COALESCE(v_no_of_pallets::text,'')|| '","' ||
                            COALESCE(v_dangerous_goods_indicator,'')|| '","' ||
                            COALESCE(v_imo_class_number,'')|| '","' ||
                            COALESCE(v_un_number_of_dangerous_goods,'')|| '","' ||
                            COALESCE(v_flash_point,'')|| '","' ||
                            COALESCE(v_unit_of_temperature,'')|| '","' ||
                            COALESCE(v_storage_requested_for_dg,'')|| '","' ||
                            COALESCE(v_refrigeration_required,'')|| '","' ||
                            COALESCE(v_minimum_temperature_refrigeration::text,'')|| '","' ||
                            COALESCE(v_maximum_temperature_refrigeration::text,'')|| '","' ||
                            COALESCE(v_unit_of_temperature_ref,'')||'"';*/
                             
                            
                            

 
--p_result := p_result ||lv_bol_line_text;
p_result := p_result || chr(10) || lv_bol_line_text;
IF v_dangerous_goods_indicator = 'Y' THEN
SELECT 
    LEFT(imo_class, 3),
    LEFT(un_number, 5),
    CAST(flash_point AS NUMERIC(5,1)) AS flash_point,
    LEFT(temperature_code, 1),
    CASE storage_code
        WHEN 'Direct Delivery'   THEN 'D'
        WHEN 'Storage in Sheds'  THEN 'S'
        WHEN 'Storage in Yards'  THEN 'Y'
        ELSE ''
    END AS storage_code_short,
    LEFT(refrigeration, 1),
    CAST(minimum_temperature AS NUMERIC(5,1)) AS minimum_temperature,
    CAST(maximum_temperature AS NUMERIC(5,1)) AS maximum_temperature,
    LEFT(temperature_code, 1)
INTO v_imo_class_number,
     v_un_number_of_dangerous_goods,
     v_flash_point,
     v_unit_of_temperature,
     v_storage_requested_for_dg,
     v_refrigeration_required,
     v_minimum_temperature_refrigeration,
     v_maximum_temperature_refrigeration,
     v_unit_of_temperature_ref
FROM shipment_cargo_detail
WHERE shipment_header_id = v_current_shipment_id;
END IF;

    SELECT count(1) INTO v_temp_count FROM shipment_container_detail WHERE shipment_id = v_current_shipment_id;
    IF v_temp_count > 0 THEN
        v_no_of_container_related_bol := v_no_of_container_related_bol + 1;
    ELSE
        v_no_of_other_bol := v_no_of_other_bol + 1;
    END IF;

 FOR rec IN
  SELECT 
    ROW_NUMBER() OVER (ORDER BY RIGHT(scd.container_sl_no, 1)) AS sr_no,
    --scd.container_sl_no,
    SUBSTRING(container_sl_no FROM 1 FOR LENGTH(container_sl_no) - 1) AS ctr_no, 
    RIGHT(scd.container_sl_no, 1) AS chkdigit,
    regexp_replace(sc.MARKS_AND_NUMBERS, '\s+', ' ', 'g') as marks_and_numbers,
    regexp_replace(sc.COMMODITY_DESCRIPTION, '\s+', ' ', 'g') as commodity,
    scd.external_no_of_piece,
    scd.gross_weight_kgs,
    scd.volume_in_cbm,
    mcd.actual_seal_number,
    CAST(
        SUM(
            COALESCE(NULLIF(TRIM(ecm.empty_tare_weight), '')::numeric, 0)
        ) AS NUMERIC(10,1)
    ) AS total_empty_tare_weight_mt
FROM master_header mh
JOIN master_container_detail mcd
  ON mh.id = mcd.master_id
JOIN efs_container_master ecm
  ON mcd.container_id = ecm.id
LEFT JOIN shipment_container_detail scd
  ON mcd.id = scd.master_container_details_id
   LEFT JOIN SHIPMENT_CARGO_DETAIL SC ON SC.SHIPMENT_HEADER_ID = SCD.SHIPMENT_ID
WHERE mh.id = v_master_id
  AND scd.shipment_id = v_current_shipment_id
GROUP BY 
    --scd.container_sl_no,
    SUBSTRING(container_sl_no FROM 1 FOR LENGTH(container_sl_no) - 1), 
    RIGHT(scd.container_sl_no, 1),
    regexp_replace(sc.MARKS_AND_NUMBERS, '\s+', ' ', 'g'),
    regexp_replace(sc.COMMODITY_DESCRIPTION, '\s+', ' ', 'g'),
    scd.external_no_of_piece,
    scd.gross_weight_kgs,
    scd.volume_in_cbm,
    mcd.actual_seal_number
ORDER BY RIGHT(scd.container_sl_no, 1)
    LOOP

        lv_ctr_line_text := 'CON,"'||rec.sr_no || '","' ||
                             COALESCE(rec.marks_and_numbers,'') || '","' ||    
                             COALESCE(rec.commodity,'') || '","' ||
                            COALESCE(V_USED_NEW,'')|| '","' ||
                            COALESCE(v_commodity_code,'770000')|| '","' ||
                            COALESCE(rec.external_no_of_piece::text,'')|| '","' ||
                            COALESCE(v_package_type,'')|| '","' ||
                            COALESCE(v_package_type_code,'')|| '","' ||
                            COALESCE(rec.external_no_of_piece::text,'')|| '","' ||
                            COALESCE(rec.gross_weight_kgs::text,'')|| '","' ||
                            COALESCE(rec.volume_in_cbm::text,'')|| '","' ||
                            COALESCE(v_dangerous_goods_indicator,'')|| '","' ||
                            COALESCE(v_imo_class_number,'')|| '","' ||
                            COALESCE(v_un_number_of_dangerous_goods,'')|| '","' ||
                            COALESCE(v_flash_point::TEXT,'')|| '","' ||
                            COALESCE(v_unit_of_temperature,'')|| '","' ||
                            COALESCE(v_storage_requested_for_dg,'')|| '","' ||
                            COALESCE(v_refrigeration_required,'')|| '","' ||
                            COALESCE(v_minimum_temperature_refrigeration::text,'')|| '","' ||
                            COALESCE(v_maximum_temperature_refrigeration::text,'')|| '","' ||
                            COALESCE(v_unit_of_temperature_ref,'')||'"'||
                            chr(10)||
                            'CTR,"' ||
                            COALESCE(rec.ctr_no,'') || '","' ||
                            COALESCE(rec.chkdigit,'') || '","' ||
                            COALESCE(rec.total_empty_tare_weight_mt::text,'') || '","' ||
                            COALESCE(rec.actual_seal_number,'') || '"';

        p_result := p_result || chr(10) || lv_ctr_line_text;
    END LOOP;

    END LOOP;
      lv_end_line_text := 'END,"' ||
                            COALESCE(v_no_of_container_related_bol::text,'') || '","' ||
                            COALESCE(v_no_of_other_bol::text,'') || '","' ||
                            COALESCE( v_end_remarks ,'') || '"';
p_result := p_result|| chr(10) ||lv_end_line_text;
END;
$BODY$;
