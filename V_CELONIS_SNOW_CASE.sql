CREATE VIEW prod_swe_access.V_CELONIS_SNOW_CASE AS (
    SELECT
        DISTINCT
        snow_case.status,
        snow_case.type,
        snow_case.contact_type,
        snow_case.case_number,
        snow_case.contact_tscid,
        snow_case.product,
        snow_case.product_category,
        snow_case.product_supercategory,
        snow_case.location_city,
        snow_case.case_category,
        snow_case.case_subcategory,
        snow_case.opened_by_group,
        snow_case.assignment_group,
        snow_case.assigned_to,
        snow_case.created_by,
        snow_case.updated_by,
        snow_case.resolved_by,
        snow_case.closed_by,
        snow_case.user_id,
        snow_case.created_at,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.created_at, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.created_at, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) created_at_cet,
        snow_case.due_date,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.due_date, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.due_date, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) due_date_cet,
        snow_case.updated_at,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.updated_at, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.updated_at, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) updated_at_cet,
        snow_case.resolved,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.resolved, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.resolved, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) resolved_cet,
        snow_case.follow_up,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.follow_up, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.follow_up, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) follow_up_cet,
        snow_case.closed_at,
        CAST(
            nvl(
                if(
                    CAST(from_utc_timestamp(snow_case.closed_at, 'CET') AS STRING) IS DISTINCT FROM 'NULL',
                    CAST(from_utc_timestamp(snow_case.closed_at, 'CET') AS STRING),
                    NULL
                ),
                ''
            )
            AS STRING
        ) closed_at_cet,
        snow_case.closure_code,
        snow_case.sub_closure_code,
        snow_case.impact,
        snow_case.priority,
        snow_case.urgency,
        snow_case.comments_count,
        snow_case.updated_count,
        snow_case.reassignment_count,
        snow_case.escalation_count,
        snow_case.children_count,
        snow_case.parent,
        snow_case.ing_year,
        snow_case.ing_month,
        snow_case.ing_day,
        snow_case.cdl_ingest_time
    FROM
        prod_swe_access.v_celonis_order_header v_order_headers
    INNER JOIN
        prod_swe_base.t_snow_case snow_case
    ON
        v_order_headers.ts_cid = snow_case.contact_tscid
    LEFT JOIN
        prod_swe_access.t_customer_permissions permissions
    ON
        snow_case.personal_identity_number = permissions.customer_identification_number
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        snow_case.contact_tscid = blacklist.tscid
    WHERE
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
);