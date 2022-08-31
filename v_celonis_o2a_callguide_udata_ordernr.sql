CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_callguide_udata_ordernr AS (

    SELECT
        udata.ordernr
        , udata.contact_id
        , udata.firstContactId
        , udata.firstContactSourceType
        , udata.origContactId
        , udata.origContactSourceType
        , udata.Inittarget
        , udata.Target
        , CAST(udata.routeReqTime AS TIMESTAMP) AS routeReqTime
        , udata.ivInteractionTimeStart

    FROM (
        SELECT

              id.contact_id
            , id.udata_value AS ordernr
            , MAX(CASE WHEN udata.udata_key = 'firstContactId'          THEN udata.udata_value END) AS firstContactId
            , MAX(CASE WHEN udata.udata_key = 'origContactId'           THEN udata.udata_value END) AS origContactId
            , MAX(CASE WHEN udata.udata_key = 'firstContactSourceType'  THEN udata.udata_value END) AS firstContactSourceType
            , MAX(CASE WHEN udata.udata_key = 'origContactSourceType'   THEN udata.udata_value END) AS origContactSourceType
            , MAX(CASE WHEN udata.udata_key = 'Inittarget'              THEN udata.udata_value END) AS Inittarget
            , MAX(CASE WHEN udata.udata_key = 'Target'                  THEN udata.udata_value END) AS Target
            , MAX(CASE WHEN udata.udata_key = 'routeReqTime'            THEN udata.udata_value END) AS routeReqTime
            , MAX(CASE WHEN udata.udata_key = 'ivInteractionTimeStart'  THEN udata.udata_value END) AS ivInteractionTimeStart
            , MAX(CASE WHEN udata.udata_key = 'pnr'                     THEN udata.udata_value END) AS pnr

        FROM prod_swe_access.t_callguide_cg_hist_udata_gdpr AS id

        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS udata ON udata.contact_id = id.contact_id

        WHERE id.orgarea_name = 'pri'
            AND id.udata_key = 'ordernr'
            AND id.udata_value IS NOT NULL
            AND id.dt_last_changed_time > '2021-06-01'

        GROUP BY
            id.contact_id
            , id.udata_value

    ) AS udata

    INNER JOIN prod_swe_access.v_celonis_order_header AS sb ON sb.order_number = udata.ordernr

    ORDER BY udata.contact_id
);