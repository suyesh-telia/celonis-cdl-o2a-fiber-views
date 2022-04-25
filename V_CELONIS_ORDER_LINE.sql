CREATE VIEW prod_swe_access.V_CELONIS_ORDER_LINE AS (
    SELECT 
        order_lines.action_code,
        order_lines.created_date as orderline_created_date,
        order_lines.due_date as ordeline_due_date,
        order_lines.row_id,
        order_lines.order_header_id,
        order_lines.product_id,
        order_lines.line_number,
        order_lines.status as orderline_status,
        order_lines.last_updated_date as orderline_last_updated_date,
        order_lines.completed_date as orderline_completed_date,
        order_lines.expected_delivery_date,
        IF(order_lines.ts_mdu_activation_type IS NULL, 'False', 'True') as ts_multi_dwelling_unit_activation_type,
        IF(order_lines.ts_mdu_delivery_contract_num IS NULL, 'False', 'True') as ts_multi_dwelling_unit_delivery_contract_num,
        order_lines.milestone as order_milestone,
        order_lines.ts_hardware_milestone,
        order_lines.ing_day,
        order_lines.ing_month,
        order_lines.ing_year,
        order_lines.cdl_ingest_time
    FROM
        prod_swe_base.t_siebel_order_line_item order_lines
    INNER JOIN 
        prod_swe_access.t_celonis_products celonis_products
    ON
        order_lines.product_id = celonis_products.product_id
    LEFT JOIN
        prod_swe_access.t_siebel_account_latest_state accounts
    ON 
        order_lines.owner_accnt_id = accounts.row_id
    LEFT JOIN
        prod_swe_access.t_customer_permissions permissions
    ON 
        accounts.`location` = permissions.customer_identification_number
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        accounts.ts_customer_id = blacklist.tscid
    WHERE 
        order_lines.completed_date >= '2021-06-01'
        AND
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
)