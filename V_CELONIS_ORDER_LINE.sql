

CREATE VIEW prod_swe_access.V_CELONIS_ORDER_LINE AS (
    SELECT 
        order_lines.action_code,
        order_lines.created_date, -- orderline_created_date in specification
        order_lines.due_date, -- orderline_due_date in specification
        order_lines.row_id,
        order_lines.order_header_id,
        order_lines.product_id,
        order_lines.line_number,
        order_lines.status, -- orderline_status in specification
        order_lines.last_updated_date, --orderline_last_updated_date in specification
        order_lines.completed_date, -- orderline_completed_date in specification
        order_lines.expected_delivery_date,
        order_lines.ts_mdu_activation_type, -- ts_multi_dwelling_unit_activation_type in specification
        order_lines.ts_mdu_delivery_contract_num, -- ts_multi_dwelling_unit_delivery_contract_num in specification
        order_lines.milestone, -- order_milestone in specification
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
        accounts.`location` = permissions.customer_identification_number -- location column is strange in database
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        accounts.ts_customer_id = blacklist.tscid
    WHERE 
        order_lines.created_date >= '2021-06-01'
        AND 
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
)