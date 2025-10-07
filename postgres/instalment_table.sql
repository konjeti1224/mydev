CREATE SEQUENCE IF NOT EXISTS tenant_default.dubai_edi_instalment_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;
	
CREATE SEQUENCE IF NOT EXISTS tenant_default.dubai_edi_agent_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;
	
CREATE TABLE IF NOT EXISTS tenant_default.dubai_edi_instalment
(
    id bigint NOT NULL DEFAULT nextval('dubai_edi_instalment_id_seq'::regclass),
	master_uid character varying COLLATE pg_catalog."default" NOT NULL,
	rotation_no text COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_user character varying(30) COLLATE pg_catalog."default" NOT NULL,
    updated_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_user character varying(30) COLLATE pg_catalog."default" NOT NULL,
    instalment_no bigint,
    version bigint NOT NULL,
    CONSTRAINT dubai_edi_instalment_pkey PRIMARY KEY (id)
  
)
ALTER SEQUENCE tenant_default.dubai_edi_instalment_id_seq
    OWNED BY tenant_default.dubai_edi_instalment.id;

ALTER SEQUENCE tenant_default.dubai_edi_instalment_id_seq
    OWNER TO dev_user;

GRANT ALL ON SEQUENCE tenant_default.dubai_edi_instalment_id_seq TO dev_user;




