CREATE TABLE edi_ic2_mapping_master (
    company_code     VARCHAR2(10),
    branch_code      VARCHAR2(10),
    location_code    VARCHAR2(10),
    sender_code      VARCHAR2(12),
    receiver_code    VARCHAR2(12),
    document_type    VARCHAR2(50),
    LRN              VARCHAR2(100),
    member_country   VARCHAR2(10),
	declarant_name        VARCHAR2(100),
	declarant_id          VARCHAR2(100),
	declarant_city        VARCHAR2(100),
	declarant_country     VARCHAR2(100),
	declarant_street      VARCHAR2(100),
	declarant_postcode    VARCHAR2(100),
	declarant_number      VARCHAR2(100),
	declarant_email       VARCHAR2(100),
    sftp_ip          VARCHAR2(500),
    sftp_user        VARCHAR2(100),
    sftp_password    VARCHAR2(100),
    sftp_folder      VARCHAR2(500),
    sftp_type        VARCHAR2(100)
);
/
insert into edi_ic2_mapping_master values('ACGL','IN','40051','TMGB','EU_ICS2','F15','WDLL','DE',null,null,null,null,null,null,null,null,NULL,NULL,NULL,NULL,'IN');
commit;