CREATE TABLE integration_data (
    id bigint,
    json_data     JSONB,
    xml_data      XML,
    edifact_data  edifact
);
--------------------
CREATE TABLE edi_inbound_data(
    id 			 SERIAL PRIMARY KEY,
    source 		 VARCHAR(500) NOT NULL,
    received_at  TIMESTAMP DEFAULT NOW(),
    status 	     TEXT DEFAULT 'pending',
    json_data    JSONB,     -- For structured JSON EDI
    xml_data     XML,        -- For XML format EDI
    other_data   TEXT,     -- For EDIFACT, CSV, or plain-text formats
    processed_at TIMESTAMP
);
--------------
set search_path to mydev
master_header
master_container
master_house_link_detail
house_header
house_containter
CREATE TABLE master_header(
master_id varchar(50),
mbl_number varchar(100),
mbl_origin_agent varchar(50),
mbl_carrier varchar(50));
create table master_container
(master_id varchar(50),
ContianerNumber  varchar(50),
ManifestNo  varchar(50),
ContainerType  varchar(50),
SealNumber   varchar(50));
create table master_house_link_detail(
master_id varchar(50),
house_id varchar(50));
create table house_header(
house_id varchar(50),
HBLNumber varchar(50),
por varchar(50));
create table house_containter(
house_id varchar(50),
ContianerNumber varchar(50),
GrossWeight varchar(50));





