create or replace procedure generate_ic2_edi(fa_subjob_uid subjob_tree.subjob_uid%type,
                                              fa_preview   varchar2) is

cursor cur_edi_ic2(fa_company_code  edi_ic2_mapping_master.company_code%type,
                   fa_branch_code   edi_ic2_mapping_master.branch_code%type,
				   fa_location_code edi_ic2_mapping_master.location_code%type) IS
    select sender_code,   
           receiver_code, 
           document_type, 
           LRN,           
           member_country,
           declarant_name,   
		   declarant_id,   
		   declarant_city,  
		   declarant_country, 
		   declarant_street,
           declarant_postcode,
           declarant_number,
           declarant_email,
           sftp_ip,       
           sftp_user,     
           sftp_password,
           sftp_folder           
    from edi_ic2_mapping_master
    where company_code =  fa_company_code
    --and branch_code = fa_branch_code
    --and location_code = fa_location_code
    and sftp_type = 'IN';
  rec_edi_ic2    cur_edi_ic2%rowtype;

cursor cur_job_dtls is
    select sj.company_code,
           sj.branch_code,
           sj.location_code,
		   sj.pol,
		   sj.pod,
		   j.master_no,
		   sj.house_no,
		   j.carrier_booking_no,
		   sj.consignee_code,
		   sj.consignee_manifest_name,
           sj.shipper_code,
		   sj.shipper_manifest_name,
		   sj.mark_no
   from job j,subjob_tree st,subjob sj
   where st.subjob_uid = sj.subjob_uid
     and st.job_uid = j.job_uid
     and st.subjob_uid = fa_subjob_uid;
   rec_job_dtls cur_job_dtls%rowtype;

   cursor cur_gross_wt is
   select sum(nvl(gross_weight,0)) gross_weight
     from subjob_container
	where subjob_uid = fa_subjob_uid;
	rec_gross_wt cur_gross_wt%rowtype;

	cursor cur_address(fa_code customer_address_master.customer_code%type) is
	select city,country_code,address,zip_code,mobile_no 
	from customer_address_master
	where customer_code = fa_code
     and address_type = 'PRIMARY';
	 rec_address cur_address%rowtype;

	cursor cur_goods_item is
	select rownum ,sj.subjob_uid,jc.container_code,jc.container_no,jc.actual_seal,sj.EXTERNAL_PACK_CODE,sj.commodity_description,
    sum(sjc.external_no_of_pack) external_no_of_pack,
        sum(sjc.gross_weight) gross_weight,
       sum(sjc.volume) volume
from subjob sj,
     job_container jc,
     subjob_container sjc
where sj.subjob_uid = sjc.subjob_uid
and jc.job_uid      = sjc.job_uid
and jc.container_uid  = sjc.container_uid
and jc.segment_code   = sjc.segment_code
and jc.job_no         = sjc.job_no
and sj.subjob_uid      = fa_subjob_uid
group by rownum,sj.subjob_uid,jc.container_code,jc.container_no,jc.actual_seal,sj.EXTERNAL_PACK_CODE,sj.commodity_description;
rec_goods_item cur_goods_item%rowtype;

 cursor cur_max_id is
select nvl(max(file_uid),0) +1 file_uid
from web_sftp_file;
rec_max_id cur_max_id%rowtype;    





    lv_xml CLOB;
    lv_document_issue_date  varchar2(100);
    lv_mode_of_transport    varchar2(100);
    lv_container_indicator  varchar2(100);
	lv_harmonized_code      varchar2(100);-----
	lv_un_number            varchar2(100);-----
	lv_payment_method       varchar2(100);----
	lv_document_type_house  varchar2(100);
	lv_file_name            varchar2(1000);
    lv_sftp_user             varchar2(100);
    lv_sftp_password         varchar2(100);
    lv_sftp_folder           varchar2(100);
    lv_sftp_ip               varchar2(1000);


begin
  open cur_job_dtls;
  fetch cur_job_dtls into rec_job_dtls;
  close cur_job_dtls;

  open cur_edi_ic2(rec_job_dtls.company_code,
                   rec_job_dtls.branch_code,
                   rec_job_dtls.location_code);
 fetch cur_edi_ic2 into rec_edi_ic2;
 close cur_edi_ic2;
 if rec_edi_ic2.sender_code is null then
    RAISE_APPLICATION_ERROR(-20001, 'Mapping details not found in edi_ic2_mapping_master.');
 end if;


 SELECT TO_CHAR(CAST(SYSTIMESTAMP AS TIMESTAMP),'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"')
   into lv_document_issue_date
 FROM dual;

 open cur_gross_wt;
 fetch cur_gross_wt into rec_gross_wt;
 close cur_gross_wt;

	 lv_mode_of_transport   := '1'; --Sea
	 lv_container_indicator := '1';
	 lv_payment_method      := 'A';
	 lv_document_type_house := 'N714';
	 lv_file_name         := 'IC2_'||fa_subjob_uid||'.txt';
 	 lv_sftp_user         := rec_edi_ic2.sftp_user;
	 lv_sftp_password     := rec_edi_ic2.sftp_password;
	 lv_sftp_folder       := rec_edi_ic2.sftp_folder;
	 lv_sftp_ip           := rec_edi_ic2.sftp_ip;


	 lv_xml  := '';
	 lv_xml  := '<?xml version="1.0" encoding="UTF-8"?>' || 
				  '<S:Envelope xmlns:S="http://www.w3.org/2003/05/soap-envelope"' || 
				  ' xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/03/addressing"' || 
				  ' xmlns:ebi="http://www.myvan.descartes.com/ebi/2004/r1"' ||
				  ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' ||
				  ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">' ||
				  '  <S:Header>' || CHR(10);
     lv_xml  :=  lv_xml||'    <wsa:From><wsa:Address>urn:scac:'||rec_edi_ic2.sender_code||'</wsa:Address></wsa:From>' || CHR(10);
     lv_xml := lv_xml || '<wsa:To>urn:zz:' || rec_edi_ic2.receiver_code || '</wsa:To>' || CHR(10);
	 lv_xml := lv_xml || '<wsa:Action>urn:myvan:ICS2_EDI</wsa:Action>' || CHR(10);
	 lv_xml := lv_xml || '<ebi:TestIndicator>T</ebi:TestIndicator>' || CHR(10);
	 lv_xml := lv_xml || '</S:Header>' || CHR(10);
	 lv_xml := lv_xml || '<S:Body>' || CHR(10);
	 lv_xml := lv_xml || '<SMFENS2>' || CHR(10);
	 lv_xml := lv_xml || '<SMFAPI>' || CHR(10);
	 lv_xml := lv_xml || '<senderID>' || rec_edi_ic2.sender_code || '</senderID>' || CHR(10);
	 lv_xml := lv_xml || '<recipientID>' || rec_edi_ic2.receiver_code || '</recipientID>' || CHR(10);
	 lv_xml := lv_xml || '<docType>' || rec_edi_ic2.document_type || '</docType>' || CHR(10);
	 lv_xml := lv_xml || '</SMFAPI>' || CHR(10);
	 lv_xml := lv_xml || '<SMFENS2FilingBody>' || CHR(10);
	 lv_xml := lv_xml || '<LRN>' || rec_edi_ic2.LRN || '-' || TO_CHAR(SYSDATE, 'yyyymmdd') || '-' || rec_edi_ic2.document_type || '-001' || '</LRN>' || CHR(10);
	 lv_xml := lv_xml || '<documentIssueDate>' || lv_document_issue_date || '</documentIssueDate>' || CHR(10);
	 lv_xml := lv_xml || '<specificCircumstanceIndicator>' || rec_edi_ic2.document_type || '</specificCircumstanceIndicator>' || CHR(10);
	 lv_xml := lv_xml || '<AddressedMemberState>' || CHR(10);
	 lv_xml := lv_xml || '<country>' || rec_edi_ic2.member_country || '</country>' || CHR(10);
	 lv_xml := lv_xml || '</AddressedMemberState>' || CHR(10);
	 lv_xml := lv_xml || '<SplitConsignment></SplitConsignment>' || CHR(10);
	 lv_xml := lv_xml || '<ActiveBorderTransportMeans>' || CHR(10);
	 lv_xml := lv_xml || '<modeOfTransport>' || lv_mode_of_transport || '</modeOfTransport>' || CHR(10);
	 lv_xml := lv_xml || '</ActiveBorderTransportMeans>' || CHR(10);
	 lv_xml := lv_xml || '<ConsignmentMasterLevel>' || CHR(10);
     lv_xml := lv_xml ||' <ConsignmentHouseLevel>' || CHR(10);
     lv_xml  :=  lv_xml||'<containerIndicator>' || lv_container_indicator || '</containerIndicator>' || CHR(10);
	 lv_xml  :=  lv_xml||'<totalGrossMass>' || TO_CHAR(rec_gross_wt.gross_weight, 'FM999999.000') || '</totalGrossMass>' || CHR(10);
	 lv_xml  :=  lv_xml||'<PlaceOfAcceptance>' || CHR(10);
	 lv_xml  :=  lv_xml||'<unlocode>' || rec_job_dtls.pol || '</unlocode>' || CHR(10);
	 lv_xml  :=  lv_xml||'</PlaceOfAcceptance>' || CHR(10);
	 lv_xml  :=  lv_xml||'<TransportDocumentMasterLevel>' || CHR(10);
     lv_xml  :=  lv_xml||'<documentNumber>' || NVL(rec_job_dtls.master_no, '') || '</documentNumber>' || CHR(10);
     lv_xml  :=  lv_xml||'<type>' || 'N704' || '</type>' || CHR(10) ;
     lv_xml  :=  lv_xml||'</TransportDocumentMasterLevel>' || CHR(10);
	 lv_xml  :=  lv_xml||'<Carrier>' || CHR(10);
     lv_xml  :=  lv_xml||'<identificationNumber>' || rec_job_dtls.carrier_booking_no || '</identificationNumber>' || CHR(10);
     lv_xml  :=  lv_xml||'</Carrier>' || CHR(10);
	 lv_xml  :=  lv_xml||'<Consignee>' || CHR(10);
     lv_xml  :=  lv_xml||'<name>' || NVL(rec_job_dtls.consignee_manifest_name, '') || '</name>' || CHR(10);
     lv_xml  :=  lv_xml||'<typeOfPerson>' || '2' || '</typeOfPerson>' || CHR(10);

	 rec_address := null;
	 open cur_address(rec_job_dtls.consignee_code);
	 fetch cur_address into rec_address;
	 close cur_address;

     lv_xml  :=  lv_xml||'<Address>' || CHR(10);
     lv_xml  :=  lv_xml||'<city>' || rec_address.city || '</city>' || CHR(10);
     lv_xml  :=  lv_xml||'<country>' || rec_address.country_code || '</country>' || CHR(10);
     lv_xml  :=  lv_xml||'<street>' || rec_address.address	 || '</street>' || CHR(10);
     lv_xml  :=  lv_xml||'<postCode>' || rec_address.zip_code || '</postCode>' || CHR(10);
     lv_xml  :=  lv_xml||'<number>' || rec_address.mobile_no || '</number>' || CHR(10);
     lv_xml  :=  lv_xml||'</Address>' || CHR(10);
     lv_xml  :=  lv_xml||'</Consignee>' || CHR(10);
	for rec_goods_item in cur_goods_item loop
        lv_xml := lv_xml ||'<GoodsItem>' || CHR(10);
        lv_xml := lv_xml||'<goodsItemNumber>' || rec_goods_item.rownum || '</goodsItemNumber>' || CHR(10);
        lv_xml := lv_xml||'<Commodity>' || CHR(10);
        lv_xml := lv_xml||'<descriptionOfGoods>' || rec_goods_item.commodity_description || '</descriptionOfGoods>' || CHR(10);
        lv_xml := lv_xml||'<CommodityCode>' || CHR(10);
        lv_xml := lv_xml||'<harmonizedSystemSubHeadingCode>' || NVL(lv_harmonized_code, '') || '</harmonizedSystemSubHeadingCode>' || CHR(10);
        lv_xml := lv_xml||'</CommodityCode>' || CHR(10);              
        lv_xml := lv_xml||'  <DangerousGoods>' || CHR(10);   
        lv_xml := lv_xml||'    <unNumber>' || lv_un_number || '</unNumber>' || CHR(10);   
        lv_xml := lv_xml||'  </DangerousGoods>' || CHR(10);
        lv_xml := lv_xml||'</Commodity>' || CHR(10);
		lv_xml := lv_xml || '<grossMass>' || TO_CHAR(rec_goods_item.gross_weight, 'FM999999.000') || '</grossMass>' || CHR(10);
		lv_xml := lv_xml || '</Weight>' || CHR(10);
		lv_xml := lv_xml || '<Packaging>' || CHR(10);
		lv_xml := lv_xml || '<shippingMarks>' || rec_job_dtls.mark_no || '</shippingMarks>' || CHR(10);
		lv_xml := lv_xml || '<numberOfPackages>' || rec_goods_item.external_no_of_pack || '</numberOfPackages>' || CHR(10);
		lv_xml := lv_xml || '<typeOfPackages>' || NVL(rec_goods_item.EXTERNAL_PACK_CODE, '') || '</typeOfPackages>' || CHR(10);
		lv_xml := lv_xml || '</Packaging>' || CHR(10);
		lv_xml := lv_xml || '<TransportEquipment>' || CHR(10);
		lv_xml := lv_xml || '<containerSizeAndType>' || pk_edi_code_mapping_master.get_edi_code('EDI_ICS2','CONTAINER_MASTER',rec_goods_item.container_code) || '</containerSizeAndType>' || CHR(10);
		lv_xml := lv_xml || '<containerPackedStatus>' || 'B' || '</containerPackedStatus>' || CHR(10);
		lv_xml := lv_xml || '<containerSupplierType>' || '2' || '</containerSupplierType>' || CHR(10);
		lv_xml := lv_xml || '<containerIdentificationNumber>' || NVL(rec_goods_item.container_no, '') || '</containerIdentificationNumber>' || CHR(10);
		lv_xml := lv_xml || '<numberOfSeals>' || '1' || '</numberOfSeals>' || CHR(10);
		lv_xml := lv_xml || '<Seal>' || CHR(10);
		lv_xml := lv_xml || '<identifier>' || NVL(rec_goods_item.actual_seal, '') || '</identifier>' || CHR(10);
		lv_xml := lv_xml || '</Seal>' || CHR(10);
		lv_xml := lv_xml || '</TransportEquipment>' || CHR(10);
        lv_xml := lv_xml || '</GoodsItem>' || CHR(10);
    END LOOP;
 lv_xml := lv_xml ||'<Consignor>' || CHR(10);
 lv_xml := lv_xml ||'<name>' || NVL(rec_job_dtls.shipper_manifest_name, '') || '</name>' || CHR(10);
 lv_xml := lv_xml ||'<typeOfPerson>' || '2' || '</typeOfPerson>' || CHR(10);
 lv_xml := lv_xml ||'<Address>' || CHR(10);
	rec_address := null;
	 open cur_address(rec_job_dtls.shipper_code);
	 fetch cur_address into rec_address;
	 close cur_address;
     lv_xml := lv_xml ||'<city>' || rec_address.city || '</city>' || CHR(10);
     lv_xml := lv_xml ||'<country>' || rec_address.country_code || '</country>' || CHR(10);
     lv_xml := lv_xml ||'<street>' || rec_address.address || '</street>' || CHR(10);
     lv_xml := lv_xml ||'<postCode>' || rec_address.zip_code || '</postCode>' || CHR(10);
     lv_xml := lv_xml ||'<streetAdditionalLine>' || '' || '</streetAdditionalLine>' || CHR(10);
     lv_xml := lv_xml ||'<number>' || rec_address.mobile_no || '</number>' || CHR(10);
     lv_xml := lv_xml ||'</Address>' || CHR(10);
     lv_xml := lv_xml ||'</Consignor>' || CHR(10);
     lv_xml := lv_xml ||'<TransportCharges>' || CHR(10);
     lv_xml := lv_xml ||'<methodOfPayment>' || lv_payment_method || '</methodOfPayment>' || CHR(10);
     lv_xml := lv_xml ||'</TransportCharges>' || CHR(10);
     lv_xml := lv_xml ||'<PlaceOfDelivery>' || CHR(10);
     lv_xml := lv_xml ||'<unlocode>' || rec_job_dtls.pod || '</unlocode>' || CHR(10);
     lv_xml := lv_xml ||'</PlaceOfDelivery>' || CHR(10);
     lv_xml := lv_xml ||'<GoodsShipment>' || CHR(10);

	 rec_address := null;
	 open cur_address(rec_job_dtls.consignee_code);
	 fetch cur_address into rec_address;
	 close cur_address;

     lv_xml := lv_xml ||'<Buyer>' || CHR(10);
     lv_xml := lv_xml ||'<name>' || NVL(rec_job_dtls.consignee_manifest_name, '') || '</name>' || CHR(10);
     lv_xml := lv_xml ||'<identificationNumber>' || rec_job_dtls.consignee_code || '</identificationNumber>' || CHR(10);
     lv_xml := lv_xml ||'<typeOfPerson>' || '2' || '</typeOfPerson>' || CHR(10);
     lv_xml := lv_xml ||'<Address>' || CHR(10);
     lv_xml := lv_xml ||'<city>' || rec_address.city || '</city>' || CHR(10);
     lv_xml := lv_xml ||'<country>' || rec_address.country_code || '</country>' || CHR(10);
     lv_xml := lv_xml ||'<street>' || rec_address.address || '</street>' || CHR(10);
     lv_xml := lv_xml ||'<postCode>' || rec_address.zip_code || '</postCode>' || CHR(10);
     lv_xml := lv_xml ||'<number>' || rec_address.mobile_no || '</number>' || CHR(10);
     lv_xml := lv_xml ||'</Address>' || CHR(10);
     lv_xml := lv_xml ||'</Buyer>' || CHR(10);

	 rec_address := null;
	 open cur_address(rec_job_dtls.shipper_code);
	 fetch cur_address into rec_address;
	 close cur_address;

     lv_xml := lv_xml ||'<Seller>' || CHR(10);
     lv_xml := lv_xml ||'<name>' || NVL(rec_job_dtls.shipper_manifest_name, '') || '</name>' || CHR(10);
     lv_xml := lv_xml ||'<typeOfPerson>' || '2' || '</typeOfPerson>' || CHR(10);
     lv_xml := lv_xml ||'<Address>' || CHR(10);
     lv_xml := lv_xml ||'<city>' || rec_address.city || '</city>' || CHR(10);
     lv_xml := lv_xml ||'<country>' || rec_address.country_code || '</country>' || CHR(10);
     lv_xml := lv_xml ||'<street>' || rec_address.address || '</street>' || CHR(10);
     lv_xml := lv_xml ||'<postCode>' || rec_address.zip_code || '</postCode>' || CHR(10);
     lv_xml := lv_xml ||'</Address>' || CHR(10);
     lv_xml := lv_xml ||'<number>' || rec_address.mobile_no || '</number>' || CHR(10);
     lv_xml := lv_xml ||'</Seller>' || CHR(10);
     lv_xml := lv_xml ||'</GoodsShipment>' || CHR(10);
     lv_xml := lv_xml ||'<CountriesOfRoutingOfConsignment>' || CHR(10);
     lv_xml := lv_xml ||'<sequenceNumber>1</sequenceNumber>' || CHR(10);
     lv_xml := lv_xml ||'</CountriesOfRoutingOfConsignment>' || CHR(10);
     lv_xml := lv_xml ||'<country>'||pk_port_master.get_port_country_code(rec_job_dtls.pol)||'</country>' || CHR(10);
     lv_xml := lv_xml ||'<CountriesOfRoutingOfConsignment>' || CHR(10);
     lv_xml := lv_xml ||'<sequenceNumber>2</sequenceNumber>' || CHR(10);
     lv_xml := lv_xml ||'<country>'||pk_port_master.get_port_country_code(rec_job_dtls.pod)||'</country>' || CHR(10);
     lv_xml := lv_xml ||'</CountriesOfRoutingOfConsignment>' || CHR(10);
     lv_xml := lv_xml ||'<TransportDocumentHouseLevel>' || CHR(10);
     lv_xml := lv_xml ||'<documentNumber>' || NVL(rec_job_dtls.house_no, '') || '</documentNumber>' || CHR(10);
     lv_xml := lv_xml ||'<type>' || lv_document_type_house || '</type>' || CHR(10);
     lv_xml := lv_xml ||'</TransportDocumentHouseLevel>' || CHR(10);
     lv_xml := lv_xml ||'</ConsignmentHouseLevel>' || CHR(10);
     lv_xml := lv_xml ||'</ConsignmentMasterLevel>' || CHR(10);
     lv_xml := lv_xml ||'<Declarant>' || CHR(10);
     lv_xml := lv_xml ||'<name>' || rec_edi_ic2.declarant_name || '</name>' || CHR(10);
     lv_xml := lv_xml ||'<identificationNumber>' || rec_edi_ic2.declarant_id || '</identificationNumber>' || CHR(10);
     lv_xml := lv_xml ||'<Address>' || CHR(10);
     lv_xml := lv_xml ||'<city>' || rec_edi_ic2.declarant_city || '</city>' || CHR(10);
     lv_xml := lv_xml ||'<country>' || rec_edi_ic2.declarant_country || '</country>' || CHR(10);
     lv_xml := lv_xml ||'<street>' || rec_edi_ic2.declarant_street || '</street>' || CHR(10);
     lv_xml := lv_xml ||'<postCode>' || rec_edi_ic2.declarant_postcode  || '</postCode>' || CHR(10);
     lv_xml := lv_xml ||'<number>' || rec_edi_ic2.declarant_number  || '</number>' || CHR(10);
     lv_xml := lv_xml ||'</Address>' || CHR(10);
     lv_xml := lv_xml ||'<Communication>' || CHR(10);
     lv_xml := lv_xml ||'<identifier>' || rec_edi_ic2.declarant_email || '</identifier>' || CHR(10);
     lv_xml := lv_xml ||'<type>EM</type>' || CHR(10);
     lv_xml := lv_xml ||'</Communication>' || CHR(10);
     lv_xml := lv_xml ||'</Declarant>' || CHR(10);
     lv_xml := lv_xml ||'</SMFENS2FilingBody>' || CHR(10);
     lv_xml := lv_xml ||'</SMFENS2>' || CHR(10);
     lv_xml := lv_xml ||'</S:Body>' || CHR(10);
     lv_xml := lv_xml ||'</S:Envelope>';

 if nvl(fa_preview,'Y') = 'Y' then
	delete from  gtt_xml ;
	insert into  gtt_xml values(lv_xml);
 ELSE
	open cur_max_id;
    fetch cur_max_id into rec_max_id;
    close cur_max_id;

	insert into cms_sftp_file(create_user,
                                        create_date,
                                        run_user,
                                        run_date,
                                        company_code,
                                        branch_code,
                                        location_code,
                                        file_uid,
                                        file_name,
                                        ftp_address,
                                        ftp_user_name,
                                        ftp_password,
                                        ftp_destination,
                                        file_content,
                                        status,
                                        attempt)
                                values (user,
                                        get_sysdate(rec_job_dtls.location_code),
                                        user,
                                        get_sysdate(rec_job_dtls.location_code),
                                        rec_job_dtls.company_code,
                                        rec_job_dtls.branch_code,
                                        rec_job_dtls.location_code,
                                        rec_max_id.file_uid,
                                        lv_file_name,
                                        lv_sftp_ip,
                                        lv_sftp_user,
                                        lv_sftp_password,
                                        lv_sftp_folder,
                                        clob2blob(lv_xml),
                                        'NEW',
                                        0);
 end if;
commit;	

exception
   when others then
   raise_application_error(-20001,sqlerrm||'Error Line No -'||dbms_utility.format_error_backtrace||sqlerrm);
end; 