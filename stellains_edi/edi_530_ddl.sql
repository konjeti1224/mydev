---sequence for running serial no
CREATE SEQUENCE seq_edi_530
    START WITH 1
    INCREMENT BY 1;
	
--Table for hardcode values edi_530
create tale edi_530_mapping_master
(
company_code varchar2(10),
branch_code varchar2(10),
location_code varchar2(10),
sender_code varchar2(12),
receiver_code varchar2(12),
route_code_origin varchar2(13),
route_code_destination varchar2(13),
sftp_ip varchar2(500),
sftp_user varchar2(100),
sftp_password varchar2(100),
sftp_folder varchar2(500),
sftp_type varchar2(100));

insert into edi_530_mapping_master values('ACGL','IN','40051','SCAC','VISTA','SPEC','AUTH','','','','','IN');
commit;
