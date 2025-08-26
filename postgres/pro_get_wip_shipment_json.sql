-- PROCEDURE: tenant_default.pro_get_wip_shipment_json(bigint, bigint, bigint, date, date, text, bigint, text, bigint)

-- DROP PROCEDURE IF EXISTS tenant_default.pro_get_wip_shipment_json(bigint, bigint, bigint, date, date, text, bigint, text, bigint);

CREATE OR REPLACE PROCEDURE tenant_default.pro_get_wip_shipment_json(
	IN p_group_company_id bigint,
	IN p_company_id bigint,
	IN p_branch_id bigint,
	IN p_from_date date,
	IN p_to_date date,
	IN p_shipment_status text,
	IN p_service_id bigint,
	IN p_service_type text,
	IN p_shipment_id bigint,
	OUT p_json_result jsonb)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_result jsonb;
	--https://dev.newage-nxt.com/fx-finance/api/v1/finance/coaWip/api/v1/finance/coaWip?fromDate=2025-08-01&toDate=2025-08-14&status=Active&serviceId=&serviceType=&shipmentId=&shipmentType=All_Shipment
BEGIN
    WITH filtered_shipments AS (
        SELECT 
            sh.id AS "shipmentId",
            ssd.service_type AS "service",
            sh.shipment_uid AS "shipmentUid",
            sh.shipment_date AS "jobDate",
            SUM(srd.sell_amount * srd.currency_rate_of_exchange) AS "provisionRevenue",
            SUM(srd.cost_amount * srd.cost_roe) AS "provisionCost",
		    CASE 
        WHEN SUM(COALESCE(srd.sell_amount, 0) * COALESCE(srd.currency_rate_of_exchange, 0)) = 0 THEN 0  -- Prevent division by zero
        ELSE 
            ((SUM(COALESCE(srd.sell_amount, 0) * COALESCE(srd.currency_rate_of_exchange, 0)) - 
              SUM(COALESCE(srd.cost_amount, 0) * COALESCE(srd.cost_roe, 0))) / 
              NULLIF(SUM(COALESCE(srd.sell_amount, 0) * COALESCE(srd.currency_rate_of_exchange, 0)), 0)) * 100
    END AS "netGpPercentage",	
            SUM(COALESCE(voucher_summary.actual_revenue_local_amount, 0)) AS "pnlIncome",
            SUM(COALESCE(voucher_summary.actual_cost_local_amount, 0)) AS "pnlCost",
            SUM(COALESCE(voucher_summary.wip_revenue_local_amount, 0)) AS "wipRevenue",
            SUM(COALESCE(voucher_summary.wip_cost_local_amount, 0)) AS "wipCost"

			/*
"jobDate": "2025-07-30T11:46:59", 
"pnlCost": 0, 
"service": "LCL", 
"wipCost": 0, 
"pnlIncome": 0, 
"costToPost": 0, 
"shipmentId": 20784, 
"wipRevenue": 0, 
"provisionCost": null, 
"revenueToPost": 0,
"shipmentNumber": "SHP0020812", 
"netGpPercentage": 0, 
"provisionRevenue": null
*/
        FROM shipment_header sh
        LEFT JOIN shipment_service_detail ssd 
            ON sh.id = ssd.shipment_header_id 
        LEFT JOIN shipment_rates_detail srd 
            ON sh.id = srd.shipment_header_id
        LEFT JOIN (
            SELECT 
                vd.shipment_rate_id,
                SUM(CASE 
                        WHEN vd.dr_cr = 'CR' 
                             AND ctd.rcn_type IN ('C_REVENUE', 'S_REVENUE')
                             AND ctd.account_id NOT IN (SELECT WIP_REVENUE_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN vd.local_amount
                        WHEN vd.dr_cr = 'DR' 
                             AND ctd.rcn_type IN ('C_REVENUE', 'S_REVENUE')
                             AND ctd.account_id NOT IN (SELECT WIP_REVENUE_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN -vd.local_amount
                        ELSE 0
                    END) AS actual_revenue_local_amount,
                SUM(CASE 
                        WHEN vd.dr_cr = 'DR' 
                             AND ctd.rcn_type = 'C_COST' 
                             AND ctd.account_id NOT IN (SELECT WIP_COST_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN vd.local_amount
                        WHEN vd.dr_cr = 'CR' 
                             AND ctd.rcn_type = 'C_COST' 
                             AND ctd.account_id NOT IN (SELECT WIP_COST_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN -vd.local_amount
                        ELSE 0
                    END) AS actual_cost_local_amount,
                SUM(CASE 
                        WHEN vd.dr_cr = 'CR' 
                             AND ctd.rcn_type IN ('C_REVENUE', 'S_REVENUE')
                             AND ctd.account_id IN (SELECT WIP_REVENUE_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN vd.local_amount
                        WHEN vd.dr_cr = 'DR' 
                             AND ctd.rcn_type IN ('C_REVENUE', 'S_REVENUE')
                             AND ctd.account_id IN (SELECT WIP_REVENUE_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN -vd.local_amount
                        ELSE 0
                    END) AS wip_revenue_local_amount,
                SUM(CASE 
                        WHEN vd.dr_cr = 'DR' 
                             AND ctd.rcn_type = 'C_COST' 
                             AND ctd.account_id IN (SELECT WIP_COST_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN vd.local_amount
                        WHEN vd.dr_cr = 'CR' 
                             AND ctd.rcn_type = 'C_COST' 
                             AND ctd.account_id IN (SELECT WIP_COST_account_ID FROM wip_master_header WHERE WIP_REVENUE_account_ID IS NOT NULL)
                        THEN -vd.local_amount
                        ELSE 0
                    END) AS wip_cost_local_amount
            FROM voucher_detail vd
            JOIN coa_transaction_detail ctd 
                ON vd.id = ctd.record_id
            GROUP BY vd.shipment_rate_id
        ) AS voucher_summary
        ON srd.id = voucher_summary.shipment_rate_id
        WHERE sh.group_company_id = p_group_company_id
        AND sh.company_id = p_company_id
        AND sh.branch_id = p_branch_id
        AND ((p_from_date IS NOT NULL AND sh.shipment_date::DATE BETWEEN p_from_date AND p_to_date)
              OR (p_from_date IS NULL AND sh.shipment_date::DATE <= p_to_date)
              )
          --AND (p_shipment_status IS NULL OR sh.status = p_shipment_status)
          AND (p_service_id IS NULL OR ssd.service_id = p_service_id)
          AND (p_shipment_id IS NULL OR sh.id = p_shipment_id)
        GROUP BY sh.id, sh.shipment_uid, sh.shipment_date, ssd.service_type
    ),
    final_shipments AS (
        SELECT * FROM filtered_shipments
        WHERE 
            (p_service_type IS NULL OR p_service_type = 'All_Shipment')
            OR (p_service_type = 'Fully_Revenue_Booked' AND "provisionRevenue" = ("wipRevenue" + "pnlIncome"))
            OR (p_service_type = 'Partial_Revenue_Booked' AND "provisionRevenue" <> ("wipRevenue" + "pnlIncome"))
            OR (p_service_type = 'Fully_Cost_Booked' AND "provisionCost" = ("wipCost" + "pnlCost"))
            OR (p_service_type = 'Partial_cost_Booked' AND "provisionCost" <> ("wipCost" + "pnlCost"))
    )
    SELECT jsonb_agg(to_jsonb(f)) INTO v_result
    FROM final_shipments f;

    IF v_result IS NULL THEN
        p_json_result := jsonb_build_object('status', 'error', 'message', 'No shipment data found');
    ELSE
        p_json_result := v_result;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        p_json_result := jsonb_build_object('status', 'failure', 'message', SQLERRM);
END;
$BODY$;
ALTER PROCEDURE tenant_default.pro_get_wip_shipment_json(bigint, bigint, bigint, date, date, text, bigint, text, bigint)
    OWNER TO dev_user;
