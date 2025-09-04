CREATE OR REPLACE PROCEDURE tenant_default.validate_user_login(
    IN p_username text,
    IN p_password text,
    OUT p_result json
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_id bigint;
    v_lov_status text;
    v_expiry_date timestamp;
    v_email text;
    v_customers json;
    v_modules json;
	v_company_logo text;
BEGIN
    -- Step 1: check username and password
    SELECT id, lov_status, password_expiry_date, email, encode(company_logo, 'base64')
    INTO v_id, v_lov_status, v_expiry_date, v_email,v_company_logo
    FROM tenant_default.web_user_master
    WHERE user_id = p_username
      AND password = p_password;

    IF v_id IS NULL THEN
        p_result := json_build_object(
                        'status', 'FAIL',
                        'message', 'Invalid username or password'
                    );
        RETURN;
    END IF;

    -- Step 2: check account status
    IF v_lov_status <> 'Active' THEN
        p_result := json_build_object(
                        'status', 'FAIL',
                        'message', 'Account is inactive'
                    );
        RETURN;
    END IF;

    -- Step 3: check expiry date
    IF v_expiry_date IS NOT NULL AND v_expiry_date < CURRENT_TIMESTAMP THEN
        p_result := json_build_object(
                        'status', 'FAIL',
                        'message', 'Password has expired'
                    );
        RETURN;
    END IF;

    -- Step 4a: get customers
    SELECT COALESCE(
               json_agg(
                   json_build_object(
                       'customerCode', cm.customer_uid,
                       'customerName', cm.name,
                       'companyCode', wud.company_code
                   )
               ) FILTER (WHERE cm.id IS NOT NULL),
               '[]'::json
           )
    INTO v_customers
    FROM tenant_default.web_user_detail wud
    LEFT JOIN tenant_default.customer_master cm
           ON cm.id = wud.nxt_customer_id
    WHERE wud.registration_no = v_id;

    -- Step 4b: get modules
    SELECT COALESCE(
               json_agg(
                   json_build_object(
                       'label', wumm.label,
                       'path', wumm.path
                   )
               ) FILTER (WHERE wumm.id IS NOT NULL),
               '[]'::json
           )
    INTO v_modules
    FROM tenant_default.web_user_module_map wummap
    JOIN tenant_default.web_user_module_master wumm
         ON wumm.id = wummap.module_id
    WHERE wummap.master_id = v_id;

    -- Step 4c: final JSON result
    p_result := json_build_object(
                    'status', 'SUCCESS',
                    'data', json_build_object(
                                'userName', p_username,
                                'email', v_email,
								 'companyLogo', v_company_logo,
                                'customers', v_customers,
                                'modules', v_modules
                            )
                );
END;
$$;
