-- PROCEDURE: tenant_default.pro_web_user_dsr_trigger(jsonb, text)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_web_user_dsr_trigger(jsonb, text);

CREATE OR REPLACE PROCEDURE tenant_default.pro_web_user_dsr_trigger(
	IN json_input jsonb,
	IN fa_method text,
	OUT fa_status text)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    lv_create_user         varchar;
    lv_create_date         timestamp;
    lv_update_user         varchar;
    lv_update_date         timestamp;
    lv_web_user_master_id  bigint;
    lv_mail_id             text[];
    lv_day_status          boolean;
    lv_days                text[];
    lv_status_list         text[];
    lv_last_triggered_date timestamp;
    lv_cnt                 int;
BEGIN
    -- Extract values from JSON input
    lv_create_user         := json_input ->> 'createUser';
    lv_create_date         := NULLIF(json_input ->> 'createDate','')::timestamp;
    lv_update_user         := json_input ->> 'updateUser';
    lv_update_date         := NULLIF(json_input ->> 'updateDate','')::timestamp;
    lv_web_user_master_id  := (json_input ->> 'webUserMasterId')::bigint;

    -- Correct way to handle JSON arrays → text[]
    lv_mail_id             := ARRAY(SELECT jsonb_array_elements_text(json_input -> 'mailId'));
    lv_day_status          := (json_input ->> 'dayStatus')::boolean;
    lv_days                := ARRAY(SELECT jsonb_array_elements_text(json_input -> 'days'));
    lv_status_list         := ARRAY(SELECT jsonb_array_elements_text(json_input -> 'statusList'));
    lv_last_triggered_date := NULLIF(json_input ->> 'lastTriggeredDate','')::timestamp;

    -- UPSERT logic
    IF fa_method = 'U' THEN
        SELECT count(1)
        INTO lv_cnt
        FROM tenant_default.web_user_dsr_trigger
        WHERE web_user_master_id = lv_web_user_master_id;

        IF lv_cnt = 0 THEN
            INSERT INTO tenant_default.web_user_dsr_trigger (
                created_user,
                created_date,
                updated_user,
                updated_date,
                web_user_master_id,
                mail_id,
                day_status,
                days,
                status_list,
                last_trigger_date
            )
            VALUES (
                lv_create_user,
                lv_create_date,
                lv_update_user,
                lv_update_date,
                lv_web_user_master_id,
                lv_mail_id,
                lv_day_status,
                lv_days,
                lv_status_list,
                lv_last_triggered_date
            );

            fa_status := 'SUCCESS - INSERTED';

        ELSE
            UPDATE tenant_default.web_user_dsr_trigger
            SET mail_id             = lv_mail_id,
                day_status          = lv_day_status,
                days                = lv_days,
                status_list         = lv_status_list,
                last_trigger_date = lv_last_triggered_date,
                updated_user         = lv_update_user,
                updated_date         = lv_update_date
            WHERE web_user_master_id = lv_web_user_master_id;

            fa_status := 'SUCCESS - UPDATED';
        END IF;

    -- DELETE logic
    ELSIF fa_method = 'D' THEN
        DELETE FROM tenant_default.web_user_dsr_trigger
        WHERE web_user_master_id = lv_web_user_master_id;

        fa_status := 'SUCCESS - DELETED';
 
    ELSE 
        fa_status := 'ERROR - INVALID METHOD'; 
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        fa_status := 'FAILED - ' || SQLERRM;
END;
$BODY$;
ALTER PROCEDURE tenant_default.pro_web_user_dsr_trigger(jsonb, text)
    OWNER TO dev_user;
