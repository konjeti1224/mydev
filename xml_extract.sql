-- =====================================================
-- ETI Manifest XML Data Extraction for PostgreSQL
-- =====================================================

-- 1. Extract Header Information
INSERT INTO manifest_headers (
    message_id, action_code, sender_code, receiver_code, message_type,
    company_code, branch_code, send_datetime, version, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    (xpath('//Header/ActionCode/text()', xml_data::xml))[1]::text as action_code,
    (xpath('//Header/SenderCode/text()', xml_data::xml))[1]::text as sender_code,
    (xpath('//Header/ReceiverCode/text()', xml_data::xml))[1]::text as receiver_code,
    (xpath('//Header/MessageType/text()', xml_data::xml))[1]::text as message_type,
    (xpath('//Header/CompanyCode/text()', xml_data::xml))[1]::text as company_code,
    (xpath('//Header/BranchCode/text()', xml_data::xml))[1]::text as branch_code,
    (xpath('//Header/SendDateTime/text()', xml_data::xml))[1]::text::timestamp as send_datetime,
    (xpath('//Header/Version/text()', xml_data::xml))[1]::text as version,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 2. Extract Master Shipment Information
INSERT INTO master_shipments (
    message_id, mbl_number, master_file_number, op_type, master_bl_type_code,
    mbl_origin_agent_code, mbl_dest_agent_code, carrier_code, carrier_name,
    master_service_code, mbl_load_type, type_of_move, mbl_pay_type,
    mbl_total_package, mbl_marking, mbl_description, mbl_weight_kg, 
    mbl_volume_cbm, seaway_bl_indicator, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    (xpath('//Master/MBLNumber/text()', xml_data::xml))[1]::text as mbl_number,
    (xpath('//Master/MasterFileNumber/text()', xml_data::xml))[1]::text as master_file_number,
    (xpath('//Master/OPType/text()', xml_data::xml))[1]::text as op_type,
    (xpath('//Master/MasterBlTypeCode/text()', xml_data::xml))[1]::text as master_bl_type_code,
    (xpath('//Master/MBLOriginAgentCode/text()', xml_data::xml))[1]::text as mbl_origin_agent_code,
    (xpath('//Master/MBLDestAgentCode/text()', xml_data::xml))[1]::text as mbl_dest_agent_code,
    (xpath('//Master/CarrierCode/text()', xml_data::xml))[1]::text as carrier_code,
    (xpath('//Master/CarrierName/text()', xml_data::xml))[1]::text as carrier_name,
    (xpath('//Master/MasterServiceCode/text()', xml_data::xml))[1]::text as master_service_code,
    (xpath('//Master/MBLLoadType/text()', xml_data::xml))[1]::text as mbl_load_type,
    (xpath('//Master/TypeOfMove/text()', xml_data::xml))[1]::text as type_of_move,
    (xpath('//Master/MBLPayType/text()', xml_data::xml))[1]::text as mbl_pay_type,
    (xpath('//Master/MBLTotalPackage/text()', xml_data::xml))[1]::text::integer as mbl_total_package,
    (xpath('//Master/MBLMarking/text()', xml_data::xml))[1]::text as mbl_marking,
    (xpath('//Master/MBLDescription/text()', xml_data::xml))[1]::text as mbl_description,
    (xpath('//Master/MBLWeightKg/text()', xml_data::xml))[1]::text::numeric as mbl_weight_kg,
    (xpath('//Master/MBLVolumnCBM/text()', xml_data::xml))[1]::text::numeric as mbl_volume_cbm,
    (xpath('//Master/SeawayBLIndicator/text()', xml_data::xml))[1]::text::boolean as seaway_bl_indicator,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 3. Extract Routing Information
INSERT INTO routing_info (
    message_id, depart_vessel_name, depart_voyage_no, arrival_vessel_name,
    etd, loading_port_code, loading_port_name, tranship_port_code, 
    tranship_port_name, discharge_port_code, discharge_port_name,
    carrier_place_receipt, carrier_place_delivery, atd, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    (xpath('//Master/Routing/DepartVesselName/text()', xml_data::xml))[1]::text as depart_vessel_name,
    (xpath('//Master/Routing/DepartVoyageNo/text()', xml_data::xml))[1]::text as depart_voyage_no,
    (xpath('//Master/Routing/ArrivalVesselName/text()', xml_data::xml))[1]::text as arrival_vessel_name,
    (xpath('//Master/Routing/ETD/text()', xml_data::xml))[1]::text::timestamp as etd,
    (xpath('//Master/Routing/LoadingPortCode/text()', xml_data::xml))[1]::text as loading_port_code,
    (xpath('//Master/Routing/LoadingPortName/text()', xml_data::xml))[1]::text as loading_port_name,
    (xpath('//Master/Routing/TranshipPortCode/text()', xml_data::xml))[1]::text as tranship_port_code,
    (xpath('//Master/Routing/TranshipPortName/text()', xml_data::xml))[1]::text as tranship_port_name,
    (xpath('//Master/Routing/DischargePortCode/text()', xml_data::xml))[1]::text as discharge_port_code,
    (xpath('//Master/Routing/DischargePortName/text()', xml_data::xml))[1]::text as discharge_port_name,
    (xpath('//Master/Routing/CarrierPlaceReceipt/text()', xml_data::xml))[1]::text as carrier_place_receipt,
    (xpath('//Master/Routing/CarrierPlaceDelivery/text()', xml_data::xml))[1]::text as carrier_place_delivery,
    (xpath('//Master/Routing/ATD/text()', xml_data::xml))[1]::text::timestamp as atd,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 4. Extract Container Information
INSERT INTO containers (
    message_id, container_number, manifest_no, container_type, seal_number,
    piece_count, weight_kg, cbm, unit_measure_code, reefer_indicator,
    dg_indicator, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    unnest(xpath('//Master/Containers/Container/ContianerNumber/text()', xml_data::xml))::text as container_number,
    unnest(xpath('//Master/Containers/Container/ManifestNo/text()', xml_data::xml))::text as manifest_no,
    unnest(xpath('//Master/Containers/Container/ContainerType/text()', xml_data::xml))::text as container_type,
    unnest(xpath('//Master/Containers/Container/SealNumber/text()', xml_data::xml))::text as seal_number,
    unnest(xpath('//Master/Containers/Container/PieceCount/text()', xml_data::xml))::text::integer as piece_count,
    unnest(xpath('//Master/Containers/Container/WeightKg/text()', xml_data::xml))::text::numeric as weight_kg,
    unnest(xpath('//Master/Containers/Container/CBM/text()', xml_data::xml))::text::numeric as cbm,
    unnest(xpath('//Master/Containers/Container/UnitMeasureCode/text()', xml_data::xml))::text as unit_measure_code,
    unnest(xpath('//Master/Containers/Container/ReeferIndicator/text()', xml_data::xml))::text::boolean as reefer_indicator,
    unnest(xpath('//Master/Containers/Container/DGIndicator/text()', xml_data::xml))::text::boolean as dg_indicator,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 5. Extract House Bills (Individual Shipments)
INSERT INTO house_bills (
    message_id, hbl_number, hbl_doc_number, reference_number, hbl_bl_type_code,
    shipper_code, shipper_info, consignee_code, consignee_info, notify_party_code,
    notify_info, hbl_booking_number, hbl_service_code, incoterms, hbl_pay_type,
    place_receipt_port_code, place_receipt, place_delivery_port_code, place_delivery,
    country_origin_code, ultimate_country_code, hbl_marking, hbl_description,
    hbl_total_package, unit_measure_code, hbl_weight_kg, hbl_cbm, hbl_packing,
    telex_release_indicator, hbl_origin_agent_code, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    unnest(xpath('//Master/Houses/House/HBLNumber/text()', xml_data::xml))::text as hbl_number,
    unnest(xpath('//Master/Houses/House/HBLDocNumber/text()', xml_data::xml))::text as hbl_doc_number,
    unnest(xpath('//Master/Houses/House/ReferenceNumber/text()', xml_data::xml))::text as reference_number,
    unnest(xpath('//Master/Houses/House/HblBlTypeCode/text()', xml_data::xml))::text as hbl_bl_type_code,
    unnest(xpath('//Master/Houses/House/ShipperCode/text()', xml_data::xml))::text as shipper_code,
    unnest(xpath('//Master/Houses/House/ShipperInfo/text()', xml_data::xml))::text as shipper_info,
    unnest(xpath('//Master/Houses/House/ConsigneeCode/text()', xml_data::xml))::text as consignee_code,
    unnest(xpath('//Master/Houses/House/ConsigneeInfo/text()', xml_data::xml))::text as consignee_info,
    unnest(xpath('//Master/Houses/House/NotifyPartyCode/text()', xml_data::xml))::text as notify_party_code,
    unnest(xpath('//Master/Houses/House/NotifyInfo/text()', xml_data::xml))::text as notify_info,
    unnest(xpath('//Master/Houses/House/HBLBookingNumber/text()', xml_data::xml))::text as hbl_booking_number,
    unnest(xpath('//Master/Houses/House/HBLServiceCode/text()', xml_data::xml))::text as hbl_service_code,
    unnest(xpath('//Master/Houses/House/INCOTERMS/text()', xml_data::xml))::text as incoterms,
    unnest(xpath('//Master/Houses/House/HBLPayType/text()', xml_data::xml))::text as hbl_pay_type,
    unnest(xpath('//Master/Houses/House/PlaceReceiptPortCode/text()', xml_data::xml))::text as place_receipt_port_code,
    unnest(xpath('//Master/Houses/House/PlaceReceipt/text()', xml_data::xml))::text as place_receipt,
    unnest(xpath('//Master/Houses/House/PlaceDeliveryPortCode/text()', xml_data::xml))::text as place_delivery_port_code,
    unnest(xpath('//Master/Houses/House/PlaceDelivery/text()', xml_data::xml))::text as place_delivery,
    unnest(xpath('//Master/Houses/House/CountryOriginCode/text()', xml_data::xml))::text as country_origin_code,
    unnest(xpath('//Master/Houses/House/UltimateCountryCode/text()', xml_data::xml))::text as ultimate_country_code,
    unnest(xpath('//Master/Houses/House/HBLMarking/text()', xml_data::xml))::text as hbl_marking,
    unnest(xpath('//Master/Houses/House/HBLDescription/text()', xml_data::xml))::text as hbl_description,
    unnest(xpath('//Master/Houses/House/HBLTotalPackage/text()', xml_data::xml))::text::integer as hbl_total_package,
    unnest(xpath('//Master/Houses/House/UnitMeasureCode/text()', xml_data::xml))::text as unit_measure_code,
    unnest(xpath('//Master/Houses/House/HBLWeightKg/text()', xml_data::xml))::text::numeric as hbl_weight_kg,
    unnest(xpath('//Master/Houses/House/HBLCBM/text()', xml_data::xml))::text::numeric as hbl_cbm,
    unnest(xpath('//Master/Houses/House/HBLPacking/text()', xml_data::xml))::text as hbl_packing,
    unnest(xpath('//Master/Houses/House/TelexReleaseIndicator/text()', xml_data::xml))::text::boolean as telex_release_indicator,
    unnest(xpath('//Master/Houses/House/HBLOriginAgentCode/text()', xml_data::xml))::text as hbl_origin_agent_code,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 6. Extract House Container Relationships
INSERT INTO house_containers (
    message_id, hbl_number, master_container_number, package, unit_measure_code,
    gross_weight, cbm, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    house_data.hbl_number,
    house_data.master_container_number,
    house_data.package::integer,
    house_data.unit_measure_code,
    house_data.gross_weight::numeric,
    house_data.cbm::numeric,
    NOW() as processed_at
FROM your_xml_storage_table,
LATERAL (
    SELECT 
        unnest(xpath('//Master/Houses/House/HBLNumber/text()', xml_data::xml))::text as hbl_number,
        unnest(xpath('//Master/Houses/House/HouseContainers/HouseContainer/MasterContianerNumber/text()', xml_data::xml))::text as master_container_number,
        unnest(xpath('//Master/Houses/House/HouseContainers/HouseContainer/Package/text()', xml_data::xml))::text as package,
        unnest(xpath('//Master/Houses/House/HouseContainers/HouseContainer/UnitMeasureCode/text()', xml_data::xml))::text as unit_measure_code,
        unnest(xpath('//Master/Houses/House/HouseContainers/HouseContainer/GrossWeight/text()', xml_data::xml))::text as gross_weight,
        unnest(xpath('//Master/Houses/House/HouseContainers/HouseContainer/CBM/text()', xml_data::xml))::text as cbm
) house_data
WHERE processed = false;

-- 7. Extract Shipping Parties
INSERT INTO shipping_parties (
    message_id, party_name, address_line1, address_line2, city, region,
    postal_code, country_code, country_name, contact_name, phone_area,
    phone, fax_area, fax, mail, processed_at
)
SELECT 
    (xpath('//Header/MessageID/text()', xml_data::xml))[1]::text as message_id,
    unnest(xpath('//ShippingParties/ShippingParty/PartyName/text()', xml_data::xml))::text as party_name,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/AddressLine1/text()', xml_data::xml))::text as address_line1,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/AddressLine2/text()', xml_data::xml))::text as address_line2,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/City/text()', xml_data::xml))::text as city,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/Region/text()', xml_data::xml))::text as region,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/PostalCode/text()', xml_data::xml))::text as postal_code,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/CountryCode/text()', xml_data::xml))::text as country_code,
    unnest(xpath('//ShippingParties/ShippingParty/PartyAddress/CountryName/text()', xml_data::xml))::text as country_name,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/ContactName/text()', xml_data::xml))::text as contact_name,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/PhoneArea/text()', xml_data::xml))::text as phone_area,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/Phone/text()', xml_data::xml))::text as phone,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/FaxArea/text()', xml_data::xml))::text as fax_area,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/Fax/text()', xml_data::xml))::text as fax,
    unnest(xpath('//ShippingParties/ShippingParty/ContactPerson/Mail/text()', xml_data::xml))::text as mail,
    NOW() as processed_at
FROM your_xml_storage_table 
WHERE processed = false;

-- 8. Mark records as processed
UPDATE your_xml_storage_table 
SET processed = true, processed_at = NOW()
WHERE processed = false;

-- 9. Complete Processing Function
CREATE OR REPLACE FUNCTION process_eti_manifest_daily()
RETURNS void AS $$
DECLARE
    record_count integer;
BEGIN
    -- Get count of unprocessed records
    SELECT COUNT(*) INTO record_count 
    FROM your_xml_storage_table 
    WHERE processed = false;
    
    -- Process all extraction queries
    -- (Include all the INSERT statements above)
    
    -- Log processing results
    INSERT INTO processing_log (process_date, records_processed, process_type) 
    VALUES (CURRENT_DATE, record_count, 'ETI_MANIFEST');
    
    RAISE NOTICE 'Processed % ETI Manifest records', record_count;
END;
$$ LANGUAGE plpgsql;

-- Schedule with pg_cron (if available)
-- SELECT cron.schedule('process-eti-manifest', '0 2 * * *', 'SELECT process_eti_manifest_daily();');