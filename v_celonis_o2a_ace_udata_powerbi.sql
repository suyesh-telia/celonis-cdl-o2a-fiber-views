CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_ace_udata_powerbi AS
WITH connected_contact_ids AS (
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        1 AS connected_by
    FROM
        prod_swe_access.t_siebel_contact_latest_state AS SB_CONTACT
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SUBSTR(SB_CONTACT.cellular_phone, 3)
        INNER JOIN prod_swe_access.t_siebel_account_latest_state AS SB_ACC ON SB_ACC.primary_contact_id = SB_CONTACT.row_id
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key IN ('Ani', 'ani', 'ivAni', 'Cid', 'cid', 'ivCid')
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        11 AS connected_by
    FROM
        prod_swe_access.t_siebel_contact_latest_state AS SB_CONTACT
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON SUBSTR(CG.udata_value, 1) = SUBSTR(SB_CONTACT.cellular_phone, 2)
        INNER JOIN prod_swe_access.t_siebel_account_latest_state AS SB_ACC ON SB_ACC.primary_contact_id = SB_CONTACT.row_id
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key IN ('Ani', 'ani', 'ivAni', 'Cid', 'cid', 'ivCid')
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        2 AS connected_by
    FROM
        prod_swe_access.t_siebel_contact_latest_state AS SB_CONTACT
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SB_CONTACT.cellular_phone
        INNER JOIN prod_swe_access.t_siebel_account_latest_state AS SB_ACC ON SB_ACC.primary_contact_id = SB_CONTACT.row_id
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key IN ('Ani', 'ani', 'ivAni', 'Cid', 'cid', 'ivCid')
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        3 AS connected_by
    FROM
        prod_swe_access.t_siebel_account_latest_state AS SB_ACC
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SUBSTR(SB_ACC.`location`, 3)
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key IN (
            'Ani',
            'ani',
            'ivAni',
            'Cid',
            'cid',
            'ivCid',
            'pnr_bankid',
            'pnr_bankId',
            'pnr',
            'PNR',
            'Pnr'
        )
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        4 AS connected_by
    FROM
        prod_swe_access.t_siebel_account_latest_state AS SB_ACC
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SB_ACC.`location`
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key IN (
            'Ani',
            'ani',
            'ivAni',
            'Cid',
            'cid',
            'ivCid',
            'pnr_bankid',
            'pnr_bankId',
            'pnr',
            'PNR',
            'Pnr'
        )
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        5 AS connected_by
    FROM
        prod_swe_access.t_siebel_account_latest_state AS SB_ACC
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SB_ACC.ts_customer_id
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key = 'TSCid'
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        6 AS connected_by
    FROM
        prod_swe_access.t_siebel_order_latest_state AS SB_ORDER
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SB_ORDER.order_number
        INNER JOIN prod_swe_access.t_siebel_account_latest_state AS SB_ACC ON SB_ACC.row_id = SB_ORDER.account_id
        LEFT OUTER JOIN prod_swe_access.t_customer_permissions AS permissions ON SB_ACC.`location` = permissions.customer_identification_number
    WHERE
        CG.dt_last_changed_time > '2021-05-31'
        AND (
            (
                CG.business_year = 2021
                AND CG.business_month > 1
            )
            OR CG.business_year = 2022
        )
        AND CG.orgarea_name = 'pri'
        AND CG.udata_key = 'ordernr'
        AND permissions.cust_helix_pur1033 IS NULL
    UNION
    ALL
    SELECT
        DISTINCT SB_ACC.ts_customer_id,
        CG.contact_id,
        CG.udata_key,
        7 AS connected_by
    FROM
        (WITH phone_tscid AS (
            SELECT
                DISTINCT B.phone_number,
                SB_ACC.ts_customer_id
            FROM
                (
                    SELECT
                        UDATA.contact_id,
                        MAX(
                            CASE
                                WHEN UDATA.udata_key IN ('Ani', 'ani', 'ivAni') THEN UDATA.udata_value
                            END
                        ) AS phone_number,
                        MAX(
                            CASE
                                WHEN UDATA.udata_key IN (
                                    'pnr_bankid',
                                    'pnr_bankId',
                                    'pnr',
                                    'PNR',
                                    'Pnr',
                                    'Cid',
                                    'cid',
                                    'ivCid'
                                ) THEN UDATA.udata_value
                            END
                        ) AS ssn
                    FROM
                        prod_swe_access.t_callguide_cg_hist_udata_gdpr AS UDATA
                    WHERE
                        UDATA.udata_value IS NOT NULL
                        AND (
                            (
                                UDATA.business_year = 2021
                                AND UDATA.business_month > 1
                            )
                            OR UDATA.business_year = 2022
                        )
                        AND UDATA.dt_last_changed_time > '2020-05-31'
                        AND UDATA.orgarea_name = 'pri'
                    GROUP BY
                        UDATA.contact_id
                ) AS B
                INNER JOIN prod_swe_access.t_siebel_account_latest_state AS SB_ACC ON SB_ACC.`location` = B.ssn
            WHERE
                B.ssn IS NOT NULL
                AND B.phone_number IS NOT NULL
        ),
        phone_tscid_count AS (
            SELECT
                phone_number,
                COUNT(DISTINCT ts_customer_id) AS tscid_count
            FROM
                phone_tscid
            GROUP BY
                phone_number
        )
        SELECT
            DISTINCT phone_tscid.phone_number,
            phone_tscid.ts_customer_id
        FROM
            phone_tscid
            INNER JOIN phone_tscid_count ON phone_tscid_count.phone_number = phone_tscid.phone_number
        WHERE
            phone_tscid_count.tscid_count = 1) AS SB_ACC
        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG ON CG.udata_value = SB_ACC.phone_number
        WHERE
            CG.dt_last_changed_time > '2021-05-31'
            AND (
                (
                    CG.business_year = 2021
                    AND CG.business_month > 1
                )
                OR CG.business_year = 2022
            )
            AND CG.orgarea_name = 'pri'
            AND CG.udata_key IN ('Ani', 'ani', 'ivAni')
)
SELECT
    CC.ts_customer_id AS ts_cid,
    CC.connected_by,
    CC.udata_key,
    CG.contact_id,
    MAX(
        CASE
            WHEN CG.udata_key = 'firstContactId' THEN CG.udata_value
        END
    ) AS firstContactId,
    MAX(
        CASE
            WHEN CG.udata_key = 'origContactId' THEN CG.udata_value
        END
    ) AS origContactId,
    MAX(
        CASE
            WHEN CG.udata_key = 'firstContactSourceType' THEN CG.udata_value
        END
    ) AS firstContactSourceType,
    MAX(
        CASE
            WHEN CG.udata_key = 'origContactSourceType' THEN CG.udata_value
        END
    ) AS origContactSourceType,
    MAX(
        CASE
            WHEN CG.udata_key = 'Menuchoice' THEN CG.udata_value
        END
    ) AS Menuchoice,
    MAX(
        CASE
            WHEN CG.udata_key = 'asrAppCat' THEN CG.udata_value
        END
    ) AS asrAppCat,
    MAX(
        CASE
            WHEN CG.udata_key = 'Inittarget' THEN CG.udata_value
        END
    ) AS Inittarget,
    MAX(
        CASE
            WHEN CG.udata_key = 'Target' THEN CG.udata_value
        END
    ) AS Target,
    MAX(
        CASE
            WHEN CG.udata_key = 'Entrance' THEN CG.udata_value
        END
    ) AS Entrance,
    MAX(
        CASE
            WHEN CG.udata_key = 'routeReqTime' THEN CG.udata_value
        END
    ) AS routeReqTime,
    MAX(
        CASE
            WHEN CG.udata_key = 'ivInteractionTimeStart' THEN CG.udata_value
        END
    ) AS ivInteractionTimeStart,
    CG_HIST.subscription_service_type_name,
    CG_HIST.release_type,
    CG_HIST.entrance_escpoint,
    CG_HIST.create_time,
    CG_HIST.delivering_time,
    CG_HIST.contact_delivered_time,
    CG_HIST.contact_established_time,
    CG_HIST.close_time
FROM
    prod_swe_access.t_callguide_cg_hist_udata_gdpr AS CG
    INNER JOIN connected_contact_ids AS CC ON CC.contact_id = CG.contact_id
    LEFT JOIN prod_swe_access.t_callguide_cg_hist_contact_gdpr AS CG_HIST ON CG_HIST.contact_id = CG.contact_id
    LEFT OUTER JOIN prod_swe_base.t_blacklisted_cirrus_customers AS blacklist ON CC.ts_customer_id = blacklist.tscid
WHERE
    blacklist.export_to_cloud IS NULL
    AND (
        (
            CG.business_year = 2021
            AND CG.business_month > 1
        )
        OR CG.business_year = 2022
    )
GROUP BY
    CC.ts_customer_id,
    CG.contact_id,
    CC.connected_by,
    CC.udata_key,
    CG_HIST.subscription_service_type_name,
    CG_HIST.release_type,
    CG_HIST.entrance_escpoint,
    CG_HIST.create_time,
    CG_HIST.delivering_time,
    CG_HIST.contact_delivered_time,
    CG_HIST.contact_established_time,
    CG_HIST.close_time;