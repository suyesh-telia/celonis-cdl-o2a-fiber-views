CREATE OR REPLACE VIEW prod_swe_access.V_CELONIS_ORDER_LINE AS (
SELECT
        order_lines.action_code,
        order_lines.created_date AS orderline_created_date,
        order_lines.due_date AS ordeline_due_date,
        order_lines.row_id,
        order_lines.order_header_id,
        order_lines.product_id,
        order_lines.line_number,
        order_lines.status AS orderline_status,
        MIN(order_lines.last_updated_date) AS orderline_last_updated_date,
        order_lines.completed_date AS orderline_completed_date,
        order_lines.expected_delivery_date,
        IF(order_lines.ts_mdu_activation_type IS NULL, 'False', 'True') AS ts_multi_dwelling_unit_activation_type,
        IF(order_lines.ts_mdu_delivery_contract_num IS NULL, 'False', 'True') AS ts_multi_dwelling_unit_delivery_contract_num,
        order_lines.milestone AS order_milestone,
        order_lines.ts_hardware_milestone,
        addr.ts_fiber_status as ts_fiber_status_ord,
asset1.ts_fiber_status as ts_fiber_status,
        MIN(order_lines.ing_year*10000+order_lines.ing_month*100+order_lines.ing_day) ingestion_date,
        MIN(order_lines.cdl_ingest_time) AS cdl_ingest_time
    FROM
        prod_swe_base.t_siebel_order_line_item order_lines
    INNER JOIN
        prod_swe_access.V_CELONIS_PRODUCTS celonis_products
    ON
        order_lines.product_id = celonis_products.row_id
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
    LEFT JOIN
        prod_swe_access.t_siebel_address_latest_state addr
    ON
        addr.row_id = order_lines.ship_to_address_id
    LEFT JOIN
    (
    select integration_id, ts_move_address_id, addr1.ts_fiber_status from prod_swe_access.t_siebel_asset_latest_state asset
    inner join prod_swe_access.t_siebel_address_latest_state addr1
    on asset.ts_move_address_id = addr1.row_id
    inner join prod_swe_access.v_celonis_products cprod1
    on cprod1.row_id = asset.product_id
    group by integration_id, ts_move_address_id, addr1.ts_fiber_status
    ) as asset1
    on asset1.integration_id = order_lines.asset_integration_id
    WHERE
        (order_lines.completed_date >= '2021-06-01' OR order_lines.created_date >=  '2021-06-01' )
        AND
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
    GROUP BY
        order_lines.action_code,
        order_lines.created_date,
        order_lines.due_date,
        order_lines.row_id,
        order_lines.order_header_id,
        order_lines.product_id,
        order_lines.line_number,
        order_lines.status,
        order_lines.completed_date,
        order_lines.expected_delivery_date,
        order_lines.ts_mdu_activation_type,
        order_lines.ts_mdu_delivery_contract_num,
        order_lines.milestone,
        order_lines.ts_hardware_milestone,
        addr.ts_fiber_status,
        asset1.ts_fiber_status
);