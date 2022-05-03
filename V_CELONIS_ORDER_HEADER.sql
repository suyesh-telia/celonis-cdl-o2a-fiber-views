CREATE VIEW prod_swe_access.V_CELONIS_ORDER_HEADER AS (
    SELECT
        orders.created_date,
        orders.row_id,
        accounts.ts_customer_id AS ts_cid,
        orders.order_date,
        orders.order_number,
        orders.requested_ship_date,
        orders.revision,
        orders.status AS order_status,
        orders.ts_channel_name,
        orders.ts_order_sub_type,
        MIN(orders.last_updated_date),
        CASE WHEN colle_orders.order_row_id_qo IS NOT NUll THEN 'Y' ELSE 'N' END AS collective_order_flag,
        colle_orders.ts_mdu_network_name,
        MIN(orders.ing_year*10000+orders.ing_month*100+orders.ing_day) ingestion_date,
        MIN(orders.cdl_ingest_time) AS cdl_ingest_time
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
    LEFT JOIN
    (
        SELECT DISTINCT
            ord.row_id order_row_id_qo,
            qo.ts_mdu_network_name
        FROM
            prod_swe_access.t_siebel_order_latest_state ord
        INNER JOIN
            prod_swe_access.t_siebel_order_line_item_latest_state oli
        ON
            ord.row_id = oli.order_header_id
        INNER JOIN
            prod_swe_access.t_siebel_product_latest_state prd
        ON
            prd.row_id = oli.product_id
        INNER JOIN
            prod_swe_access.t_celonis_products celonis_products
        ON
            celonis_products.product_id = prd.row_id
        INNER JOIN
            prod_swe_access.t_siebel_quote_latest_state qo
        ON
            qo.ts_mdu_delivery_contract_num = oli.ts_mdu_delivery_contract_num
            AND
            oli.ts_mdu_delivery_contract_rev_num = qo.revision
            AND
            qo.quote_type = 'MDU Quote'
        INNER JOIN
            prod_swe_access.t_siebel_quote_item_latest_state qoi
        ON
            qoi.quote_id = qo.row_id
            AND
            qoi.action_code <> 'Delete'
            AND
            oli.product_id = qoi.product_id
            AND
            prd.ts_product_line = 'Telia SP'
            AND
            prd.billing_type = 'Subscription'
        WHERE
            oli.completed_date >= '2021-06-01' OR oli.created_date >= '2021-06-01'
    )
        AS colle_orders
        ON
           colle_orders.order_row_id_qo = orders.row_id
    WHERE
        (order_lines.completed_date >= '2021-06-01' OR order_lines.created_date >= '2021-06-01')
        AND
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
    GROUP BY
        orders.created_date,
        orders.row_id,
        accounts.ts_customer_id,
        orders.order_date,
        orders.order_number,
        orders.requested_ship_date,
        orders.revision,
        orders.status,
        orders.ts_channel_name,
        orders.ts_order_sub_type,
        colle_orders.order_row_id_qo,
        colle_orders.ts_mdu_network_name
);