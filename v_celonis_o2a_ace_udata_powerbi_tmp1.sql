CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_ace_udata_powerbi_tmp1 AS
WITH phone_tscid AS (
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
                t_callguide_cg_hist_udata_gdpr AS UDATA
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
        INNER JOIN t_siebel_account_latest_state AS SB_ACC ON SB_ACC.`location` = B.ssn
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
    phone_tscid_count.tscid_count = 1 -- Make sure that we only extract phonenumbers that has only one customer id connected to it
;