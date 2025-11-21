CREATE OR REPLACE PROCEDURE tenant_default.pro_webtool_poms_summary(
    IN p_type text,
    IN p_interval integer,
    OUT result json
)
LANGUAGE plpgsql
AS $$
BEGIN

    ---------------------------------------------------------------------
    -- 1. PURCHASE ORDER
    ---------------------------------------------------------------------
    IF p_type = 'PURCHASE_ORDER' THEN

        WITH summary AS (
            SELECT
                d.order_number AS name,
                to_char(d.order_date, 'DD-MON-YY') AS orderDate,
                d.supplier_name AS supplierName,
                d.origin,
                d.destination,
                COUNT(*) FILTER (WHERE d.status = 'In-Transit') AS inTransitCount,
                COUNT(*) FILTER (WHERE d.status = 'Closed') AS closedCount,
                COUNT(*) FILTER (WHERE d.status = 'Pending') AS pendingCount,
                MAX(s.house_no) AS bookingNo,
                COUNT(*) AS total,
                json_build_array(
                    json_build_object(
                        'product', '',
                        'productCode', '0',
                        'productName', '0',
                        'supplierName', '0',
                        'ordered', '0',
                        'pending', '0',
                        'booked', '0',
                        'inTransit', '0',
                        'delivered', ''
                    )
                ) AS product
            FROM WEB_POMS_DETAIL d
            LEFT JOIN WEB_POMS_SHIPMENT_DETAIL s 
                ON s.order_id = d.order_id
            WHERE d.order_date >= CURRENT_DATE - p_interval
            GROUP BY d.order_number, d.order_date, d.supplier_name, d.origin, d.destination
        )
        SELECT json_agg(
            json_build_object(
                'name', name,
                'orderDate', orderDate,
                'supplierName', supplierName,
                'origin', origin,
                'destination', destination,
                'inTransitCount', inTransitCount,
                'closedCount', closedCount,
                'pendingCount', pendingCount,
                'bookingNo', bookingNo,
                'total', total,
                'product', product
            )
        )
        INTO result
        FROM summary;

        RETURN;
    END IF;



    ---------------------------------------------------------------------
    -- 2. SUPPLIERS
    ---------------------------------------------------------------------
    IF p_type = 'SUPPLIERS' THEN

        WITH summary AS (
            SELECT
                d.supplier_name AS name,
                d.supplier_code AS supplierCode,

                COUNT(*) FILTER (WHERE d.status = 'In-Transit') AS inTransitCount,
                COUNT(*) FILTER (WHERE d.status = 'Closed') AS closedCount,
                COUNT(*) FILTER (WHERE d.status = 'Pending') AS pendingCount,

                COUNT(*) AS total,

                json_build_array(
                    json_build_object(
                        'product', d.supplier_name,
                        'productCode', d.supplier_code,
                        'productName', COUNT(*)::TEXT,
                        'supplierName', d.supplier_name,
                        'ordered', '0',
                        'pending', COUNT(*) FILTER (WHERE d.status = 'Pending')::TEXT,
                        'booked', '0',
                        'inTransit', COUNT(*) FILTER (WHERE d.status = 'In-Transit')::TEXT,
                        'delivered', COUNT(*) FILTER (WHERE d.status = 'Closed')::TEXT
                    )
                ) AS product
            FROM WEB_POMS_DETAIL d
            WHERE d.order_date >= CURRENT_DATE - p_interval
            GROUP BY d.supplier_name, d.supplier_code
        )
        SELECT json_agg(
            json_build_object(
                'name', name,
                'supplierCode', supplierCode,
                'inTransitCount', inTransitCount,
                'closedCount', closedCount,
                'pendingCount', pendingCount,
                'total', total,
                'product', product
            )
        )
        INTO result
        FROM summary;

        RETURN;
    END IF;



    ---------------------------------------------------------------------
    -- 3. COUNTRY
    ---------------------------------------------------------------------
    IF p_type = 'COUNTRY' THEN

        WITH summary AS (
            SELECT
                d.destination AS name,

                COUNT(*) FILTER (WHERE d.status = 'In-Transit') AS inTransitCount,
                COUNT(*) FILTER (WHERE d.status = 'Closed') AS closedCount,
                COUNT(*) FILTER (WHERE d.status = 'Pending') AS pendingCount,
                COUNT(*) AS total,

                json_build_array(
                    json_build_object(
                        'product', '',
                        'productCode', '0',
                        'productName', '0',
                        'supplierName', '0',
                        'ordered', '0',
                        'pending', '0',
                        'booked', '0',
                        'inTransit', '0',
                        'delivered', ''
                    )
                ) AS product

            FROM WEB_POMS_DETAIL d
            WHERE d.order_date >= CURRENT_DATE - p_interval
            GROUP BY d.destination
        )
        SELECT json_agg(
            json_build_object(
                'name', name,
                'inTransitCount', inTransitCount,
                'closedCount', closedCount,
                'pendingCount', pendingCount,
                'total', total,
                'product', product
            )
        )
        INTO result
        FROM summary;

        RETURN;
    END IF;



    ---------------------------------------------------------------------
    -- 4. PRODUCT
    ---------------------------------------------------------------------
    IF p_type = 'PRODUCT' THEN

        WITH summary AS (
            SELECT
                d.item_description AS name,

                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE d.status = 'Pending') AS pendingCount,
                COUNT(*) FILTER (WHERE d.status = 'In-Transit') AS inTransitCount,
                COUNT(*) FILTER (WHERE d.status = 'Closed') AS closedCount,

                d.origin,
                d.destination,
                'View Details' AS action

            FROM WEB_POMS_DETAIL d
            WHERE d.order_date >= CURRENT_DATE - p_interval
            GROUP BY d.item_description, d.origin, d.destination
        )
        SELECT json_agg(
            json_build_object(
                'name', name,
                'total', total,
                'pendingCount', pendingCount,
                'inTransitCount', inTransitCount,
                'closedCount', closedCount,
                'origin', origin,
                'destination', destination,
                'action', action
            )
        )
        INTO result
        FROM summary;

        RETURN;
    END IF;



    ---------------------------------------------------------------------
    -- INVALID TYPE
    ---------------------------------------------------------------------
    result := json_build_object(
        'error', 'Invalid p_type. Allowed: PURCHASE_ORDER, SUPPLIERS, COUNTRY, PRODUCT'
    );

END;
$$;
