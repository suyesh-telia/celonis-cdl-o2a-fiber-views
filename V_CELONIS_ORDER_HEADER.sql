CREATE VIEW prod_swe_access.V_CELONIS_ORDER_HEADER AS (
    SELECT 
        orders.created_date,
        orders.row_id,
        accounts.ts_customer_id as ts_cid, 
        orders.order_date,
        orders.order_number,
        orders.requested_ship_date,
        orders.revision,
        orders.status as order_status,
        orders.ts_channel_name,
        orders.ts_order_sub_type,
        orders.last_updated_date,
        orders.ing_day,
        orders.ing_month,
        orders.ing_year,
        orders.cdl_ingest_time
    FROM
        prod_swe_base.t_siebel_order orders
    INNER JOIN 
        prod_swe_access.t_siebel_order_line_item_latest_state order_lines
    ON
        orders.row_id = order_lines.order_header_id
    INNER JOIN 
        prod_swe_access.t_celonis_products celonis_products 
    ON
        order_lines.product_id = celonis_products.product_id
    LEFT JOIN
        prod_swe_access.t_siebel_account_latest_state accounts
    ON 
        orders.account_id = accounts.row_id
    LEFT JOIN
        prod_swe_access.t_customer_permissions permissions
    ON 
        accounts.`location` = permissions.customer_identification_number
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        accounts.ts_customer_id = blacklist.tscid
    WHERE 
        orders.created_date >= '2021-06-01'
        AND
        order_lines.created_date >= '2021-06-01'
        AND
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
);