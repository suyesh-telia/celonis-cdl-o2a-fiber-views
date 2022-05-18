CREATE VIEW prod_swe_access.V_CELONIS_PRODUCTS AS (
    SELECT
        DISTINCT
        siebel_products.row_id,
        siebel_products.name as product_name,
        siebel_products.product_category,
        siebel_products.product_sub_category,
        siebel_products.last_updated_date,
        siebel_products.cdl_ingest_time
    FROM
        prod_swe_access.t_siebel_product_latest_state siebel_products
    where
        siebel_products.product_category = 'fixedbroadband'
);