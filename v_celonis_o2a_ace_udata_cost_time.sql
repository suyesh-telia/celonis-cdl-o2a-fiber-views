/*

 2022-10-24 Inserted 5301743 row(s)
 2022-10-31 Inserted 5375861 row(s)

 */
DROP TABLE IF EXISTS prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata_cost_time;

CREATE TABLE prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata_cost_time AS
SELECT
    DISTINCT B.first_contact_id,
    CAST(B.first_start_time AS TIMESTAMP) AS first_start_time,
    CAST(B.last_stop_time AS TIMESTAMP) AS last_stop_time
FROM
    (
        SELECT
            CAST(O2A.contact_id AS INTEGER) AS first_contact_id,
            FIRST_VALUE(AGENT.start_time) OVER (
                PARTITION BY O2A.contact_id
                ORDER BY
                    AGENT.start_time RANGE BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS first_start_time,
            LAST_VALUE(AGENT.stop_time) OVER (
                PARTITION BY O2A.contact_id
                ORDER BY
                    AGENT.stop_time RANGE BETWEEN UNBOUNDED PRECEDING
                    AND UNBOUNDED FOLLOWING
            ) AS last_stop_time
        FROM
            t_callguide_cg_hist_agent_status_gdpr AS AGENT
            INNER JOIN prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata AS O2A ON O2A.contact_id = AGENT.contact_id
        GROUP BY
            O2A.contact_id,
            AGENT.start_time,
            AGENT.stop_time
    ) AS B;
