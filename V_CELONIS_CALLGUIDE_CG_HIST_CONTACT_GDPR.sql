CREATE VIEW prod_swe_access.V_CELONIS_CALLGUIDE_CG_HIST_CONTACT_GDPR AS (
    SELECT
        DISTINCT
        si_acc.ts_customer_id,
        callguide_contact_gdpr.auto_service_time,
        callguide_contact_gdpr.call_connected_time,
        callguide_contact_gdpr.calls_merged_time,
        callguide_contact_gdpr.campaign_name,
        callguide_contact_gdpr.categorized_time,
        callguide_contact_gdpr.close_time,
        callguide_contact_gdpr.contact_delivered_time,
        callguide_contact_gdpr.contact_established_time,
        callguide_contact_gdpr.contact_source_type,
        callguide_contact_gdpr.contact_transfered_time,
        callguide_contact_gdpr.create_time,
        callguide_contact_gdpr.delivering_time,
        callguide_contact_gdpr.destination_location,
        callguide_contact_gdpr.destination_party,
        callguide_contact_gdpr.dialer_dialing_time,
        callguide_contact_gdpr.drop_time,
        callguide_contact_gdpr.dt_auto_service_time,
        callguide_contact_gdpr.dt_call_connected_time,
        callguide_contact_gdpr.dt_calls_merged_time,
        callguide_contact_gdpr.dt_categorized_time,
        callguide_contact_gdpr.dt_close_time,
        callguide_contact_gdpr.dt_contact_delivered_time,
        callguide_contact_gdpr.dt_contact_established_time,
        callguide_contact_gdpr.dt_contact_transfered_time,
        callguide_contact_gdpr.dt_create_time,
        callguide_contact_gdpr.dt_delivering_time,
        callguide_contact_gdpr.dt_from_escpoint_time,
        callguide_contact_gdpr.dt_last_changed_time,
        callguide_contact_gdpr.dt_queue_situation_time,
        callguide_contact_gdpr.dt_ringback_detected_time,
        callguide_contact_gdpr.dt_ringing_time,
        callguide_contact_gdpr.dt_route_req_time,
        callguide_contact_gdpr.dt_to_escpoint_time,
        callguide_contact_gdpr.entrance_escpoint,
        callguide_contact_gdpr.first_time_established,
        callguide_contact_gdpr.from_escpoint_time,
        callguide_contact_gdpr.interrupted,
        callguide_contact_gdpr.last_changed_time,
        callguide_contact_gdpr.orgarea_name,
        callguide_contact_gdpr.original_contact_source_type,
        callguide_contact_gdpr.queue_situation_time,
        callguide_contact_gdpr.release_type,
        callguide_contact_gdpr.requeue_count,
        callguide_contact_gdpr.ringback_detected_time,
        callguide_contact_gdpr.ringing_time,
        callguide_contact_gdpr.route_req_time,
        callguide_contact_gdpr.source_location,
        callguide_contact_gdpr.source_party,
        callguide_contact_gdpr.target_domain,
        callguide_contact_gdpr.target_type,
        callguide_contact_gdpr.tm_auto_service_time,
        callguide_contact_gdpr.tm_call_connected_time,
        callguide_contact_gdpr.tm_calls_merged_time,
        callguide_contact_gdpr.tm_categorized_time,
        callguide_contact_gdpr.tm_close_time,
        callguide_contact_gdpr.tm_contact_delivered_time,
        callguide_contact_gdpr.tm_contact_established_time,
        callguide_contact_gdpr.tm_contact_transfered_time,
        callguide_contact_gdpr.tm_create_time,
        callguide_contact_gdpr.tm_delivering_time,
        callguide_contact_gdpr.tm_dialer_dialing_time,
        callguide_contact_gdpr.tm_drop_time,
        callguide_contact_gdpr.tm_from_escpoint_time,
        callguide_contact_gdpr.tm_last_changed_time,
        callguide_contact_gdpr.tm_queue_situation_time,
        callguide_contact_gdpr.tm_ringback_detected_time,
        callguide_contact_gdpr.tm_ringing_time,
        callguide_contact_gdpr.tm_route_req_time,
        callguide_contact_gdpr.tm_to_escpoint_time,
        callguide_contact_gdpr.to_escpoint_time,
        callguide_contact_gdpr.ing_year,
        callguide_contact_gdpr.ing_month,
        callguide_contact_gdpr.ing_day,
        callguide_contact_gdpr.subscription_service_type_name,
        callguide_contact_gdpr.customer_identification_number_exists,
        callguide_contact_gdpr.business_year,
        callguide_contact_gdpr.business_month,
        callguide_contact_gdpr.business_day,
        callguide_contact_gdpr.cdl_ingest_time
    FROM
        prod_swe_access.v_celonis_order_header v_order_headers
    INNER JOIN
        prod_swe_access.t_siebel_account_latest_state latest_accounts
    ON
        v_order_headers.ts_cid = latest_accounts.ts_customer_id
    INNER JOIN
        prod_swe_access.t_callguide_cg_hist_contact_gdpr callguide_contact_gdpr
    ON
        latest_accounts.`location` = callguide_contact_gdpr.customer_identification_number
    LEFT JOIN
        prod_swe_access.t_customer_permissions permissions
    ON
        latest_accounts.`location` = permissions.customer_identification_number
    LEFT JOIN
        prod_swe_base.t_blacklisted_cirrus_customers blacklist
    ON
        latest_accounts.ts_customer_id = blacklist.tscid
    LEFT JOIN
    (
        SELECT DISTINCT
            `location`,
            ts_customer_id
        FROM prod_swe_access.t_siebel_account_latest_state
        WHERE account_type_code = 'Customer'
    )
    AS
        si_acc
    ON
        si_acc.`location` = callguide_contact_gdpr.customer_identification_number
    WHERE
        permissions.cust_helix_pur1033 IS NULL
        AND
        blacklist.export_to_cloud IS NULL
);