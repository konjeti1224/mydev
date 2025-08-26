-- PROCEDURE: tenant_default.pro_web_user_column(jsonb, text)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_web_user_column(jsonb, text);

CREATE OR REPLACE PROCEDURE tenant_default.pro_web_user_column(
	IN json_input jsonb,
	IN fa_method text,
	OUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_id bigint;
    lv_column_preferences text;
    lv_create_user varchar;
    lv_create_date timestamp;
    lv_update_user varchar;
    lv_update_date timestamp;
    lv_registration_no varchar;
    lv_web_user_master_id bigint;
    lv_version int;
    lv_nxt_customer_id bigint;
	lv_cnt int;
BEGIN
    -- Extract values from JSON input
    --lv_id                 := (json_input ->> 'webUserMasterId')::bigint;
    lv_column_preferences := (json_input -> 'columnPreferences')::text; -- Array converted to text
    lv_create_user        := json_input ->> 'createUser';
    lv_create_date        := NULLIF(json_input ->> 'createDate','')::timestamp;
    lv_update_user        := json_input ->> 'updateUser';
    lv_update_date        := NULLIF(json_input ->> 'updateDate','')::timestamp;
    lv_registration_no    := json_input ->> 'registrationNo';
    lv_web_user_master_id := (json_input ->> 'webUserMasterId')::bigint;
    lv_version            := (json_input ->> 'version')::int;
    lv_nxt_customer_id    := NULLIF(json_input ->> 'nxtCustomerId','')::bigint;

IF fa_method = 'U' THEN
    select count(1)
	  into lv_cnt
	  from web_user_column
	 where web_user_master_id = lv_web_user_master_id;
	 
    IF lv_cnt = 0 THEN
        INSERT INTO tenant_default.web_user_column (
            nxt_customer_id, 
            column_preferences,
            create_user,
            create_date,
            update_user,
            update_date,
            registration_no,
            web_user_master_id,
            version
        )
        VALUES (
            lv_nxt_customer_id,
            lv_column_preferences,
            lv_create_user,
            lv_create_date,
            lv_update_user,
            lv_update_date,
            lv_registration_no,
            lv_web_user_master_id,
            lv_version
        );

        fa_status := 'SUCCESS - INSERTED';

    ELSE
        UPDATE tenant_default.web_user_column
        SET column_preferences = lv_column_preferences,
		    nxt_customer_id = lv_nxt_customer_id,
            update_user        = lv_update_user,
            update_date        = lv_update_date,
            registration_no    = lv_registration_no,
            version            = lv_version
        WHERE web_user_master_id = lv_web_user_master_id;

    
            fa_status := 'SUCCESS - UPDATED';
        END IF;

ELSIF fa_method = 'S' THEN
        SELECT column_preferences
        INTO fa_status
        FROM tenant_default.web_user_column
        WHERE web_user_master_id = lv_web_user_master_id;
ELSE 
fa_status := 'ERROR - INVALID METHOD'; 
 END IF;

EXCEPTION
    WHEN OTHERS THEN
        fa_status := 'FAILED - ' || SQLERRM;
END;
$BODY$;
ALTER PROCEDURE tenant_default.pro_web_user_column(jsonb, text)
    OWNER TO dev_user;
