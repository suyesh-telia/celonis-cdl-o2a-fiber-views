create or replace view prod_swe_access.v_celonis_order_header as
(select orders.created_date,
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
 network_name.ts_mdu_network_name,
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
LEFT JOIN
(select ord1.order_number, quote.ts_mdu_network_name  ,
row_number() over(partition by ord1.order_number order by quote.last_updated_date desc) as filter_row
from prod_swe_access.t_siebel_order_latest_state ord1 inner join
prod_swe_access.t_siebel_order_line_item_latest_state oli1
on oli1.order_header_id = ord1.row_id
inner join prod_swe_access.t_siebel_quote_latest_state quote
on oli1.ts_mdu_delivery_contract_num = quote.ts_mdu_delivery_contract_num
 and oli1.ts_mdu_delivery_contract_rev_num = quote.revision
 and quote.quote_type = 'MDU Quote'
inner join prod_swe_access.v_celonis_products prd
on prd.row_id = oli1.product_id
where prd.product_category = 'fixedbroadband'
and prd.product_sub_category = 'lan'
and oli1.action_code = 'Add'
and ord1.ts_order_sub_type in ('Change Access','New','Move')
--and ord1.ts_bulk_order = 'N'
and ord1.created_date >= '2021-06-01') network_name
on orders.order_number = network_name.order_number
and network_name.filter_row=1
 LEFT OUTER JOIN (select distinct ord.row_id order_row_id_qo,
 ord.order_number order_number_qo,
 q.ts_mdu_network_name
 from prod_swe_access.t_siebel_order_latest_state ord
 inner join prod_swe_access.t_siebel_order_line_item_latest_state ol
 on ol.order_header_id = ord.row_id
 inner join prod_swe_access.t_siebel_product_latest_state prod
 on ol.product_id = prod.row_id
 inner join prod_swe_access.t_siebel_quote_latest_state q
 on ol.ts_mdu_delivery_contract_num = q.ts_mdu_delivery_contract_num
 and ol.ts_mdu_delivery_contract_rev_num = q.revision
 and q.quote_type = 'MDU Quote'
 inner join prod_swe_access.t_siebel_quote_item_latest_state qi_coll
 on qi_coll.quote_id = q.row_id
 and qi_coll.action_code != 'Delete'
 and ol.product_id = qi_coll.product_id
 and qi_coll.adjusted_list_price = 0
 inner join prod_swe_access.t_siebel_product_latest_state prd_coll
 on qi_coll.product_id = prd_coll.row_id
 and prd_coll.ts_product_line = 'Telia SP'
 and prd_coll.billing_type = 'Subscription'
 where ol.completed_date >= '2021-06-01' or ol.created_date >= '2021-06-01') colle_orders
 ON colle_orders.order_row_id_qo = orders.row_id
 WHERE (order_lines.completed_date >= '2021-06-01' OR order_lines.created_date >= '2021-06-01')
 AND permissions.cust_helix_pur1033 IS NULL
 AND blacklist.export_to_cloud IS NULL
 GROUP BY orders.created_date,
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
 network_name.ts_mdu_network_name,
 orders.ts_preorder_flag,
 orders.ts_bulk_order);