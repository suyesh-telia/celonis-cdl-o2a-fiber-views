/*

 2022-10-24 Inserted 5297261 row(s)
 2022-10-31 Inserted 5371376 row(s)

 */
DROP TABLE IF EXISTS prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata_total_time;

CREATE TABLE prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata_total_time AS
SELECT
    DISTINCT B.first_contact_id,
    CAST(B.first_create_time AS TIMESTAMP) AS first_create_time,
    CAST(B.last_close_time AS TIMESTAMP) AS last_close_time
FROM
    (
        WITH all_contact_ids AS (
            SELECT
                O2A.contact_id AS first_contact_id,
                UDATA.contact_id
            FROM
                t_callguide_cg_hist_udata_gdpr AS UDATA
                INNER JOIN prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata AS O2A ON O2A.contact_id = CAST(UDATA.udata_value AS INTEGER)
            WHERE
                UDATA.udata_key = 'firstContactId'
        )
        SELECT
            CAST(all_contact_ids.first_contact_id AS INTEGER) AS first_contact_id,
            FIRST_VALUE(CONTACT.create_time) OVER (
                PARTITION BY all_contact_ids.first_contact_id
                ORDER BY
                    CONTACT.create_time RANGE BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS first_create_time,
            LAST_VALUE(CONTACT.close_time) OVER (
                PARTITION BY all_contact_ids.first_contact_id
                ORDER BY
                    CONTACT.close_time RANGE BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS last_close_time
        FROM
            t_callguide_cg_hist_contact_gdpr AS CONTACT
            INNER JOIN all_contact_ids ON all_contact_ids.contact_id = CONTACT.contact_id
        GROUP BY
            all_contact_ids.first_contact_id,
            CONTACT.create_time,
            CONTACT.close_time
    ) AS B;