CREATE VIEW prod_swe_access.V_CELONIS_PRODUCTS AS (
    SELECT 
        siebel_products.row_id,
        siebel_products.name, --in specification it says product_name but there is no such column there
        siebel_products.product_category,
        siebel_products.product_sub_category,
        siebel_products.last_updated_date,
        siebel_products.ing_day,
        siebel_products.ing_month,
        siebel_products.ing_year,
        siebel_products.cdl_ingest_time
    FROM 
        prod_swe_base.t_siebel_product siebel_products
    INNER JOIN
        prod_swe_access.t_celonis_products celonis_products
    ON  
        siebel_products.row_id = celonis_products.product_id
    WHERE 
        siebel_products.created_date >= '2021-06-01'
)