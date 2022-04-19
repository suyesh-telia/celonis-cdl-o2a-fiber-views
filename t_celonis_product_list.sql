CREATE EXTERNAL TABLE IF NOT EXISTS prod_swe_access.t_celonis_products (
    product_id STRING,
    product_name STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/data/prod2/swe/access/celonis/products';
