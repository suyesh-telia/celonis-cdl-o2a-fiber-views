CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_ace_udata_total_time AS

        WITH all_contact_ids AS (
            SELECT
                O2A.contact_id AS first_contact_id,
                UDATA.contact_id
            FROM
                prod_swe_access.t_callguide_cg_hist_udata_gdpr AS UDATA
                INNER JOIN prod_swe_access.v_celonis_o2a_ace_udata AS O2A ON O2A.contact_id = CAST(UDATA.udata_value AS INTEGER)
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
            prod_swe_access.t_callguide_cg_hist_contact_gdpr AS CONTACT
            INNER JOIN all_contact_ids ON all_contact_ids.contact_id = CONTACT.contact_id
        GROUP BY
            all_contact_ids.first_contact_id,
            CONTACT.create_time,
            CONTACT.close_time
    ;