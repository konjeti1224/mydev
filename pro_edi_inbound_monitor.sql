CREATE OR REPLACE PROCEDURE pro_edi_inbound_monitor(
    IN fa_type text,
	IN fa_from_date date,
	IN fa_to_date date,
	IN fa_search_no text,
    OUT fa_json_output TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_file_log_json JSONB;
    v_final_json JSONB;
	v_from_date DATE;
    v_to_date DATE;
BEGIN
    v_to_date := COALESCE(fa_to_date, CURRENT_DATE);
    v_from_date := COALESCE(fa_from_date, v_to_date - INTERVAL '30 days');
    -- Build the fileProcessingLog JSON array using a subquery
    SELECT jsonb_agg(jsonb_build_object(
        'serialNo', serial_no,
        'date', log_date,
        'time', log_time,
        'fileName', file_name,
        'jobId', job_id,
        'mblNo', mbl_no,
        'houseCount', house_count
    ))
    INTO v_file_log_json
    FROM (
        SELECT 
            row_number() OVER (ORDER BY eid.received_at) AS serial_no,
            to_char(eid.received_at, 'YYYY-MM-DD') AS log_date,
            to_char(eid.received_at, 'HH24:MI:SS') AS log_time,
            eid.file_name,
            eomh.master_uid AS job_id,
            eomh.obl_number AS mbl_no,
            COUNT(esd.id) AS house_count
        FROM edi_inbound_data eid
        JOIN edi_ocean_master_header eomh 
            ON eid.id = eomh.source_reference_id
        LEFT JOIN edi_shipment_detail esd 
            ON esd.edi_ocean_master_header_id = eomh.id
        WHERE eid.source = fa_type
		--and to_char(eid.received_at, 'DD-Mon-YYYY') between to_char(fa_from_date, 'DD-Mon-YYYY') and to_char(fa_to_date, 'DD-Mon-YYYY')
		AND eid.received_at::date BETWEEN v_from_date AND v_to_date
		and eid.file_name like '%'||upper(fa_search_no)||'%'
        GROUP BY eid.received_at, eid.file_name, eomh.master_uid, eomh.obl_number
    ) sub;

    -- Compose the final JSON object
    v_final_json := jsonb_build_object(
        'httpStatus', 200,
        'success', true,
        'timestamp', to_char(current_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"+05:30"'),
        'result', jsonb_build_object(
            'totalFilesRequired',  (
                SELECT count(*) FROM edi_inbound_data
                WHERE received_at::date BETWEEN v_from_date AND v_to_date
				--to_char(received_at, 'DD-Mon-YYYY') between to_char(fa_from_date, 'DD-Mon-YYYY') and to_char(fa_to_date, 'DD-Mon-YYYY')
				and file_name like '%'||upper(fa_search_no)||'%'
            ),
            'successfullyProcessed', (
                SELECT count(*) FROM edi_inbound_data
                WHERE  received_at::date BETWEEN v_from_date AND v_to_date
				--to_char(received_at, 'DD-Mon-YYYY') between to_char(fa_from_date, 'DD-Mon-YYYY') and to_char(fa_to_date, 'DD-Mon-YYYY')
                  and file_name like '%'||upper(fa_search_no)||'%'
				  AND status = 'Completed'
            ),
            'failedErrors', (
                SELECT count(*) FROM edi_inbound_data
                WHERE  received_at::date BETWEEN v_from_date AND v_to_date
				--to_char(received_at, 'DD-Mon-YYYY') between to_char(fa_from_date, 'DD-Mon-YYYY') and to_char(fa_to_date, 'DD-Mon-YYYY')
                  and file_name like '%'||upper(fa_search_no)||'%'
				  AND status = 'Error'
            ),
            'lastSyncTime', now(),
            'fileProcessingLog', COALESCE(v_file_log_json, '[]'::jsonb)
        ),
        'error', NULL
    );

    -- Assign output
    fa_json_output := v_final_json::TEXT;
END;
$$;