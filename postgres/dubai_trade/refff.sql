call tenant_default.pro_dubai_trade_edi_shipment('AEDUBMAS0008850',null,null);--FCL
call tenant_default.pro_dubai_trade_edi_shipment('AEDUBMAS0008852',null,null);--LCL
/
select * from tenant_default.wms_stock_aging_report;
select * from dubai_edi_instalment;
/
select * from efs_code_master;
/
CREATE SEQUENCE IF NOT EXISTS tenant_default.dubai_edi_agent_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;
	/
	 SELECT id FROM master_header WHERE master_uid = 'AEDUBMAS0008850';
	 /
	  SELECT distinct sh.id
        FROM shipment_header sh
        JOIN shipment_service_detail ssd ON sh.id = ssd.shipment_header_id
        JOIN master_service_link_detail msld ON ssd.id = msld.shipment_service_id
        WHERE msld.master_id = 8659
		/
		select service_trade,other_trade,SHIPMENT_HEADER_ID from SHIPMENT_SERVICE_DETAIL where SHIPMENT_HEADER_ID in (21472,
21494,
21495);
/
SELECT ROUND(6063.14);