CREATE OR REPLACE PROCEDURE pro_ccn_edi_fhl(
    IN  p_reference_id  BIGINT, -- id from shipment_header (ex: shipment_id)
    OUT v_result_text   TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_origin text := '';
    v_destination text := '';

    -- Shipment cursor
    cur_mbl CURSOR  IS
        SELECT substr(master_id,1,3)||'-'||substr(master_id,4) AS mbl,
               no_of_pieces,
               gross_weight_kgs,
               hbl_or_hawb_no,
               origin_id,
               destination_id,
			   shipper_name,
			   consignee_name
        FROM shipment_list_view
        WHERE id = p_reference_id;
    rec_mbl record;

    -- Port cursor
   cur_port  CURSOR (ca_id bigint) IS
        SELECT code
        FROM efs_port_master
        WHERE id = ca_id;
    rec_port record;

    -- Commodity cursor
   cur_commodity CURSOR  IS
        SELECT commodity_description
        FROM shipment_cargo_detail
        WHERE shipment_header_id = p_reference_id;
    rec_commodity record;
cur_add_id cursor is
select shipper_address_id,
       consignee_address_id 
from shipment_party_detail
	where shipment_header_id = p_reference_id;
rec_add_id record;

cur_party_address cursor(ca_id bigint) is
select 
  COALESCE(ad1.building_no, '') ||' '||
  COALESCE(ad1.street_name, '') ||' '||
  COALESCE(ad1.city_name, '')||' '||
  COALESCE(ad1.po_box, '')   address,
  COALESCE(ad1.state_name, '') state_name,
  COALESCE(ecm.code, '') country_code
from party_address_detail ad1
JOIN efs_country_master ecm on ecm.id = ad1.country_id
where ad1.id = ca_id;
rec_party_address record;

BEGIN
    -- Initialize with header lines
    v_result_text := 'QK CSGAGT85GHAAIFHLSIN' || chr(10) ||
                     '.CSGAGT86SGTWG01/SIN01 090516' || chr(10) ||
                     'FHL/4' || chr(10);

    -- Fetch shipment (MBL info)
    OPEN cur_mbl;
    FETCH cur_mbl INTO rec_mbl;
    CLOSE cur_mbl;

    IF rec_mbl IS NULL THEN
        v_result_text := v_result_text || 'NO DATA FOUND FOR SHIPMENT ' || p_reference_id;
        RETURN;
    END IF;

    -- Fetch origin port
    OPEN cur_port(rec_mbl.origin_id);
    FETCH cur_port INTO rec_port;
    CLOSE cur_port;
    v_origin := COALESCE(rec_port.code, '');

    -- Reset and fetch destination port
    rec_port := NULL;
    OPEN cur_port(rec_mbl.destination_id);
    FETCH cur_port INTO rec_port;
    CLOSE cur_port;
    v_destination := COALESCE(rec_port.code, '');

    -- Fetch first commodity
    OPEN cur_commodity;
    FETCH cur_commodity INTO rec_commodity;
    CLOSE cur_commodity;

    -- Append MBI line
    v_result_text := v_result_text ||
        'MBI/' || rec_mbl.mbl ||
        '/T' || rec_mbl.no_of_pieces ||
        'K' || rec_mbl.gross_weight_kgs || chr(10);

    -- Append HBS line
    v_result_text := v_result_text ||
        'HBS/' || rec_mbl.hbl_or_hawb_no || '/' ||
        v_origin || v_destination || '/' ||
        rec_mbl.no_of_pieces ||
        '/K' || rec_mbl.gross_weight_kgs || '//' ||
        COALESCE(rec_commodity.commodity_description, '') || chr(10);
 v_result_text := v_result_text ||'SHP/'||rec_mbl.shipper_name|| chr(10);
 open cur_add_id;
 fetch cur_add_id into rec_add_id;
 close cur_add_id;
open cur_party_address(rec_add_id.shipper_address_id);
fetch cur_party_address into rec_party_address;
close cur_party_address;
  v_result_text := v_result_text ||'/'||rec_party_address.address||chr(10);
  v_result_text := v_result_text ||'/'||rec_party_address.state_name||chr(10);
   v_result_text := v_result_text ||'/'||rec_party_address.country_code||chr(10);
rec_party_address := null;
open cur_party_address(rec_add_id.consignee_address_id);
fetch cur_party_address into rec_party_address;
close cur_party_address;
 v_result_text := v_result_text ||'CNE/'||rec_mbl.consignee_name|| chr(10);
 v_result_text := v_result_text ||'/'||rec_party_address.address||chr(10);
 v_result_text := v_result_text ||'/'||rec_party_address.state_name||chr(10);
  v_result_text := v_result_text ||'/'||rec_party_address.country_code||chr(10);
END;
$$;
/
DO $$
DECLARE
    v_out TEXT;
BEGIN
    CALL pro_ccn_edi_fhl(21167, v_out);
    RAISE NOTICE 'EDI Output:%', v_out;
END;
$$;