CREATE VIEW prod_swe_access.v_celonis_order_header AS (
    SELECT orders.created_date,
     orders.row_id,
     accounts.ts_customer_id ts_cid,
     orders.order_date,
     orders.order_number,
     orders.requested_ship_date,
     orders.revision,
     orders.`status` order_status,
     orders.ts_channel_name,
     orders.ts_order_sub_type,
     MIN(orders.last_updated_date) last_update_date,
     if(colle_orders.order_row_id_qo IS NOT NULL,
     'Y',
     'N') collective_order_flag,
     colle_orders.ts_mdu_network_name,
     orders.ts_preorder_flag,
     orders.ts_bulk_order,
     MIN(orders.ing_year * 10000 + orders.ing_month * 100 + orders.ing_day) ingestion_date,
     MIN(orders.cdl_ingest_time) cdl_ingest_time
     from prod_swe_base.t_siebel_order orders
     INNER JOIN prod_swe_access.t_siebel_order_line_item_latest_state order_lines
     ON orders.row_id = order_lines.order_header_id
     INNER JOIN prod_swe_access.v_celonis_products celonis_products
     ON order_lines.product_id = celonis_products.row_id
     LEFT OUTER JOIN prod_swe_access.t_siebel_account_latest_state accounts
     ON orders.account_id = accounts.row_id
     LEFT OUTER JOIN prod_swe_access.t_customer_permissions permissions
     ON accounts.`location` = permissions.customer_identification_number
     LEFT OUTER JOIN prod_swe_base.t_blacklisted_cirrus_customers blacklist
     ON accounts.ts_customer_id = blacklist.tscid
     LEFT OUTER JOIN (SELECT DISTINCT ord.row_id order_row_id_qo,
     ord.order_number order_number_qo,
     q.ts_mdu_network_name
     from prod_swe_access.t_siebel_order_latest_state ord
     INNER JOIN prod_swe_access.t_siebel_order_line_item_latest_state ol
     ON ol.order_header_id = ord.row_id
     INNER JOIN prod_swe_access.t_siebel_product_latest_state prod
     ON ol.product_id = prod.row_id
     INNER JOIN prod_swe_access.t_siebel_quote_latest_state q
     ON ol.ts_mdu_delivery_contract_num = q.ts_mdu_delivery_contract_num
     AND ol.ts_mdu_delivery_contract_rev_num = q.revision
     AND q.quote_type = 'MDU Quote'
     INNER JOIN prod_swe_access.t_siebel_quote_item_latest_state qi_coll
     ON qi_coll.quote_id = q.row_id
     AND qi_coll.action_code != 'Delete'
     AND ol.product_id = qi_coll.product_id
     AND qi_coll.adjusted_list_price = 0
     INNER JOIN prod_swe_access.t_siebel_product_latest_state prd_coll
     ON qi_coll.product_id = prd_coll.row_id
     AND prd_coll.ts_product_line = 'Telia SP'
     AND prd_coll.billing_type = 'Subscription'
     WHERE ol.completed_date >= '2021-06-01' OR ol.created_date >= '2021-06-01') colle_orders
     ON colle_orders.order_row_id_qo = orders.row_id
     WHERE (order_lines.completed_date >= '2021-06-01' OR order_lines.created_date >= '2021-06-01')
     AND permissions.cust_helix_pur1033 IS NULL
     AND blacklist.export_to_cloud IS NULL GROUP BY orders.created_date,
     orders.row_id,
     accounts.ts_customer_id,
     orders.order_date,
     orders.order_number,
     orders.requested_ship_date,
     orders.revision,
     orders.`status`,
     orders.ts_channel_name,
     orders.ts_order_sub_type,
     colle_orders.order_row_id_qo,
     colle_orders.ts_mdu_network_name,
     orders.ts_preorder_flag,
     orders.ts_bulk_order
 );
