CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_siebel_service_requests AS

WITH status_dates AS (

    SELECT
        sr.row_id
        ,sr.status
        ,sr.sub_status
        ,MIN(sr.last_updated_date) AS status_date
    FROM prod_swe_base.t_siebel_service_request AS sr

    GROUP BY sr.row_id
            ,sr.status
            ,sr.sub_status
)

SELECT

    sr.row_id
    ,sr.ts_sales_order_id
    ,sr.ts_order_line_item_id
    ,sr.ticket_id
    ,sr.ticket_type
    ,sr.area
    ,sr.sub_area
    ,sr.priority
    ,sr.severity
    ,sr.tt_source
    ,sr.sr_rootcause
    ,sr.ts_owner_team
    ,sr.ts_service_type
    ,sr.created_date

    ,MAX(CASE WHEN status_dates.status = 'Open' AND status_dates.sub_status = 'Assigned' THEN CAST(status_dates.status_date AS TIMESTAMP) END)             AS open_assigned

    ,MAX(CASE WHEN status_dates.status = 'Resolved' THEN CAST(status_dates.status_date AS TIMESTAMP) END)              AS resolved_date
    ,MAX(CASE WHEN status_dates.status = 'Closed' THEN CAST(status_dates.status_date AS TIMESTAMP) END)                AS closed_date
    ,MAX(CASE WHEN status_dates.status = 'Cancelled' THEN CAST(status_dates.status_date AS TIMESTAMP) END)             AS cancelled_date
    ,MAX(CASE WHEN status_dates.status = 'Service Activation' THEN CAST(status_dates.status_date AS TIMESTAMP) END)    AS service_activation

    ,MAX(CASE WHEN status_dates.status = 'Open' AND status_dates.sub_status = 'Unassigned' THEN CAST(status_dates.status_date AS TIMESTAMP) END)           AS open_unassigned
    ,MAX(CASE WHEN status_dates.status = 'Open' AND status_dates.sub_status = 'Work In Progress' THEN CAST(status_dates.status_date AS TIMESTAMP) END)     AS open_work_in_progress
    ,MAX(CASE WHEN status_dates.status = 'Open' AND status_dates.sub_status = 'Waiting on Customer' THEN CAST(status_dates.status_date AS TIMESTAMP) END)  AS open_waiting_on_customer
    ,MAX(CASE WHEN status_dates.status = 'Open' AND status_dates.sub_status = 'Follow Up' THEN CAST(status_dates.status_date AS TIMESTAMP) END)            AS open_follow_up

FROM prod_swe_access.t_siebel_service_request_latest_state AS sr

INNER JOIN prod_swe_access.v_celonis_order_header AS header ON header.row_id = sr.ts_sales_order_id
INNER JOIN status_dates ON status_dates.row_id = sr.row_id

GROUP BY     sr.row_id
            ,sr.ts_sales_order_id
            ,sr.ts_order_line_item_id
            ,sr.ticket_id
            ,sr.ticket_type
            ,sr.area
            ,sr.sub_area
            ,sr.priority
            ,sr.severity
            ,sr.tt_source
            ,sr.sr_rootcause
            ,sr.ts_owner_team
            ,sr.ts_service_type
            ,sr.created_date
;

/*

SELECT
sr.area
, count(DISTINCT sr.row_id)
FROM prod_swe_base.t_siebel_service_request AS sr
GROUP BY sr.area
ORDER BY count(DISTINCT sr.row_id) DESC;

*/