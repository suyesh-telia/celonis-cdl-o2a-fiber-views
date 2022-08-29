CREATE OR REPLACE VIEW prod_swe_access.V_CELONIS_PRODUCTS AS (
    SELECT
        DISTINCT
        siebel_products.row_id,
        siebel_products.name as product_name,
        siebel_products.product_category,
        siebel_products.product_sub_category,
        siebel_products.last_updated_date,
        siebel_products.cdl_ingest_time,
        price.original_list_price
    FROM
        prod_swe_access.t_siebel_product_latest_state siebel_products
    INNER JOIN
        prod_swe_access.t_siebel_product_price_list_item_latest_state price
    ON
        siebel_products.row_id = price.product_id
    where
        (siebel_products.product_category = 'fixedbroadband' AND siebel_products.product_sub_category = 'lan') OR
        (siebel_products.product_category = 'fixedbroadband' AND siebel_products.product_sub_category = 'promotion') OR
        (siebel_products.product_category = 'goods' AND siebel_products.product_sub_category = 'rgw')

);