CREATE OR REPLACE VIEW prod_swe_access.V_CELONIS_O2A_RGW_HARDWARE_MILESTONES AS (
    WITH cte as(
        SELECT
        row_id,
        ts_hardware_milestone,
        FIRST_VALUE(orderline_last_updated_date) OVER (
                                                        PARTITION BY row_id, ts_hardware_milestone
                                                        ORDER BY orderline_last_updated_date
                                                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                      )
                                                      AS first_updated_date,
        LAST_VALUE(orderline_last_updated_date) OVER (
                                                        PARTITION BY row_id, ts_hardware_milestone
                                                        ORDER BY orderline_last_updated_date
                                                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                     ) AS last_updated_date
        FROM prod_swe_access.V_CELONIS_ORDER_LINE
        WHERE
            product_category = 'goods'
            AND
            product_sub_category = 'rgw'
            AND orderline_status <> 'Pending'
    )
    SELECT
        row_id,
        ts_hardware_milestone,
        first_updated_date,
        last_updated_date
    FROM cte
    GROUP BY
        row_id,
        ts_hardware_milestone,
        first_updated_date,
        last_updated_date
);