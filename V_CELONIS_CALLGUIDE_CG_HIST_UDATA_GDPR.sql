CREATE VIEW prod_swe_access.V_CELONIS_CALLGUIDE_CG_HIST_UDATA_GDPR AS (
    SELECT
        DISTINCT
        callguide_udata_gdpr.orgarea_name,
        callguide_udata_gdpr.udata_key,
        callguide_udata_gdpr.udata_value,
        callguide_udata_gdpr.contact_id,
        callguide_udata_gdpr.business_year,
        callguide_udata_gdpr.business_month,
        callguide_udata_gdpr.business_day,
        callguide_udata_gdpr.last_changed_time,
        callguide_udata_gdpr.dt_last_changed_time,
        callguide_udata_gdpr.ing_year,
        callguide_udata_gdpr.ing_month,
        callguide_udata_gdpr.ing_day,
        callguide_udata_gdpr.cdl_ingest_time
    FROM
        prod_swe_access.v_celonis_order_header v_order_headers
    INNER JOIN
        prod_swe_access.t_siebel_account_latest_state latest_accounts
    ON
        v_order_headers.ts_cid = latest_accounts.ts_customer_id
    INNER JOIN
        prod_swe_access.t_callguide_cg_hist_udata_gdpr callguide_udata_gdpr
    ON
        latest_accounts.`location` = callguide_udata_gdpr.customer_identification_number
    LEFT JOIN
        prod_swe_access.t_customer_permissions permissions
    ON
        latest_accounts.`location` = permissions.customer_identification_number
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        latest_accounts.ts_customer_id = blacklist.tscid
    WHERE
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
);