create or replace procedure generate_stellantis_edi_530(fa_did fas_document_header.did%type) IS

cursor cur_edi_530(fa_company_code  edi_530_mapping_master.company_code%type,
                   fa_branch_code   edi_530_mapping_master.branch_code%type,
				   fa_location_code edi_530_mapping_master.location_code%type) IS
select sender_code,
       receiver_code,
	   route_code_origin,
	   route_code_destination,
	   sftp_ip,
	   sftp_user,
	   sftp_password,
	   sftp_folder
from edi_530_mapping_master
where company_code = fa_company_code
  and branch_code = fa_branch_code
  and location_code = fa_location_code
  and sftp_type = 'IN';
rec_edi_530 cur_edi_530%rowtype;

cursor cur_doc IS
select company_code,
       branch_code,
	   location_code,
       daybook_code||'-'||document_no document_no,
	   job_uid,
       subjob_uid
  from fas_document_header
  where did = fa_did;
rec_doc cur_doc%rowtype;

cursor cur_job(fa_job_uid subjob_tree.job_uid%type) is
select master_no,
       por,
       pol,
       pod,
       fdc,
       no_of_pack
       from job
    where job_uid = fa_job_uid;
rec_job cur_job%rowtype;

cursor cur_subjob(fa_subjob_uid  subjob_tree.subjob_uid%type) is
select pickup_date
 from subjob
 where subjob_uid = fa_subjob_uid;
 rec_subjob cur_subjob%rowtype;
 
 cursor cur_max_id is
select nvl(max(file_uid),0) +1 file_uid
from web_sftp_file;
rec_max_id cur_max_id%rowtype;   


lv_text                 clob;
lv_sender_code   varchar2(100);
lv_receiver_code varchar2(100);
lv_gs_04         varchar2(1000);
lv_gs_05         varchar2(1000);
lv_gs_06         number;
lv_st_02         number := 1;
lv_bv3_02        varchar2(1000);
lv_bv3_05        number; 
lv_bv3_06        varchar2(1000);
lv_bv3_07        varchar2(1000);
lv_bv3_09        varchar2(1000);
lv_bv3_10       varchar2(1000);
lv_bv3_11        varchar2(1000);
lv_bv3_12        varchar2(1000);
lv_vi_01         varchar2(1000);--vin
lv_vi_02         varchar2(1000);
lv_vi_03         varchar2(1000);
lv_vi_04         varchar2(1000);
lv_vi_05         varchar2(1000);
lv_vi_06         varchar2(1000);
lv_vi_07         varchar2(1000);
lv_vi_08         varchar2(1000);
lv_vi_09         varchar2(1000);
lv_vi_10         varchar2(1000);
lv_sftp_user             varchar2(100);
lv_sftp_password         varchar2(100);
lv_sftp_folder           varchar2(100);
lv_sftp_ip               varchar2(1000);
lv_file_name             varchar2(1000);



BEGIN
	delete from gtt_xml;
	
	open cur_doc;
	fetch cur_doc into rec_doc;
	close cur_doc;
	
	open cur_edi_530(rec_doc.company_code,rec_doc.branch_code,rec_doc.location_code);
	fetch cur_edi_530 into rec_edi_530;
    close cur_edi_530;
	
	IF rec_edi_530.sender_code is null THEN
        RAISE_APPLICATION_ERROR(-20001, 'Mapping details not found in EDI_530 master.');
    END IF;



	
	open cur_job(rec_doc.job_uid);
	fetch cur_job into rec_job;
	close cur_job;
	
	open cur_subjob(rec_doc.subjob_uid);
	fetch cur_subjob into rec_subjob;
	close cur_subjob;
	
	open cur_max_id;
    fetch cur_max_id into rec_max_id;
    close cur_max_id;

	lv_text              := null;
	lv_sftp_user         := rec_edi_530.sftp_user;
	lv_sftp_password     := rec_edi_530.sftp_password;
	lv_sftp_folder       := rec_edi_530.sftp_folder;
	lv_sftp_ip           := rec_edi_530.sftp_ip;
	lv_file_name         := 'EDI_530_'||fa_did||'.txt';
	
	
	lv_sender_code      := rec_edi_530.sender_code;
	lv_receiver_code    := rec_edi_530.receiver_code;
	lv_gs_04   := to_char(get_sysdate(rec_doc.location_code),'YYMMDD');
    lv_gs_05   := to_char(get_sysdate(rec_doc.location_code),'HH24MI');
	lv_gs_06   := seq_edi_530.nextval;
    lv_bv3_02  := PK_EDI_CODE_MAPPING_MASTER.get_edi_code('EDI_530','PORT_MASTER',rec_job.pol);--'998205010';
	lv_bv3_05  := rec_job.no_of_pack;
	lv_bv3_06  := to_char(rec_subjob.pickup_date,'YYMMDD');
	lv_bv3_07  := to_char(rec_subjob.pickup_date,'HH24MI');
	lv_bv3_09  := rec_job.master_no;
	lv_bv3_12  := PK_EDI_CODE_MAPPING_MASTER.get_edi_code('EDI_530','PORT_MASTER',rec_job.pod);--'998205010';
	lv_vi_02   := rec_edi_530.route_code_origin;
	lv_vi_03   := rec_edi_530.route_code_destination;
	

	lv_text := 'ICS*+ANSI1.1  VT48708          VTVISTA          2503270703002826444'||chr(10);
	lv_text := lv_text||'GS*VI*'||lv_sender_code||'*'||lv_receiver_code||'*'||lv_gs_04||'*'||lv_gs_05||'*'||lv_gs_06||'*T*1'||chr(10);
   
    lv_text := lv_text||'ST*530*'||lpad(lv_st_02,9,'0')||chr(10);
    lv_text := lv_text||'BV3*'||lv_sender_code||'*'||lv_bv3_02||'*BK*'||rec_doc.document_no||'*'||lv_bv3_05||'*'||lv_bv3_06||'*'||lv_bv3_07||'**'||lv_bv3_09||'*'||lv_bv3_10||'*'||lv_bv3_11||'*'||lv_bv3_12||chr(10);
	lv_text := lv_text||'VI*'||lv_vi_01||'*'||lv_vi_02||'*'||lv_vi_03||'*'||lv_vi_04||'*'||lv_vi_05||'*'||lv_vi_06||'*'||lv_vi_07||'*'||lv_vi_08||'*'||lv_vi_09||'*'||lv_vi_10||chr(10);
	lv_text := lv_text||'SE*'||'4'||'*'||lpad(lv_st_02,9,'0')||chr(10);
	
	lv_text := lv_text||'GE*'||lv_st_02||'*'||lv_gs_06;
	
	lv_text := lv_text||'ICE*000001*002826444';
	
	
	
	insert into web_sftp_file(create_user,
                                        create_date,
                                        run_user,
                                        run_date,
                                        --company_code,
                                        --branch_code,
                                        --location_code,
                                        --file_uid,
                                        --file_name,
                                      --  ftp_address,
                                       -- ftp_user_name,
                                      --  ftp_password,
                                       -- ftp_destination,
                                       -- file_content,
                                        status,
                                        attempt)
                                values (user,
                                        get_sysdate(fa_location_code),
                                        user,
                                        get_sysdate(fa_location_code),
                                     -- rec_doc.company_code,
                                        --rec_doc.branch_code,
                                        --rec_doc.location_code,
                                       -- rec_max_id.file_uid,
                                        --lv_file_name,
                                        --lv_sftp_ip,
                                        --lv_sftp_user,
                                       -- lv_sftp_password,
                                        --lv_sftp_folder,
                                       -- clob2blob(lv_text),
                                        'NEW',
                                        0);
    
	dbms_output.put_line(lv_text);

    commit;
exception
   when others then
   raise_application_error(-20001,sqlerrm||'Error Line No -'||dbms_utility.format_error_backtrace||sqlerrm);
end; 