CREATE VIEW prod_swe_access.V_CELONIS_OPTOUT_CUSTOMERS as (
    SELECT 
        accounts.ts_customer_id
    FROM
        prod_swe_access.t_customer_permissions permissions
    INNER JOIN 
        prod_swe_access.t_siebel_account_latest_state accounts
    ON 
        accounts.`location` = permissions.customer_identification_number
    WHERE 
        permissions.cust_helix_pur1033 IS NOT NULL
);