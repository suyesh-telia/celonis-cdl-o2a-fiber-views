CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_callguide_udata_tscid AS (
    SELECT
        udata.tscid
        , udata.contact_id
        , udata.firstContactId
        , udata.firstContactSourceType
        , udata.origContactId
        , udata.origContactSourceType
        , udata.Menuchoice
        , udata.asrAppCat
        , udata.Inittarget
        , udata.Target
        , CAST(udata.routeReqTime AS TIMESTAMP) AS routeReqTime
        , udata.ivInteractionTimeStart

        FROM (
        SELECT

              TSCid.contact_id
            , TSCid.udata_value AS tscid
            , MAX(CASE WHEN udata.udata_key = 'firstContactId'          THEN udata.udata_value END) AS firstContactId
            , MAX(CASE WHEN udata.udata_key = 'origContactId'           THEN udata.udata_value END) AS origContactId
            , MAX(CASE WHEN udata.udata_key = 'firstContactSourceType'  THEN udata.udata_value END) AS firstContactSourceType
            , MAX(CASE WHEN udata.udata_key = 'origContactSourceType'   THEN udata.udata_value END) AS origContactSourceType
            , MAX(CASE WHEN udata.udata_key = 'Menuchoice'              THEN udata.udata_value END) AS Menuchoice
            , MAX(CASE WHEN udata.udata_key = 'asrAppCat'               THEN udata.udata_value END) AS asrAppCat
            , MAX(CASE WHEN udata.udata_key = 'Inittarget'              THEN udata.udata_value END) AS Inittarget
            , MAX(CASE WHEN udata.udata_key = 'Target'                  THEN udata.udata_value END) AS Target
            , MAX(CASE WHEN udata.udata_key = 'routeReqTime'            THEN udata.udata_value END) AS routeReqTime
            , MAX(CASE WHEN udata.udata_key = 'ivInteractionTimeStart'  THEN udata.udata_value END) AS ivInteractionTimeStart
            , MAX(CASE WHEN udata.udata_key = 'pnr'                     THEN udata.udata_value END) AS pnr

        FROM prod_swe_access.t_callguide_cg_hist_udata_gdpr AS TSCid

        INNER JOIN prod_swe_access.t_callguide_cg_hist_udata_gdpr AS udata ON udata.contact_id = TSCid.contact_id

        WHERE TSCid.orgarea_name = 'pri'
            AND TSCid.udata_key = 'TSCid'
            AND TSCid.udata_value IS NOT NULL
            AND TSCid.cdl_ingest_time > '2021-06-01 00:00:00.748'

        GROUP BY
            TSCid.contact_id
            , TSCid.udata_value

        ) AS udata

        LEFT OUTER JOIN prod_swe_access.t_customer_permissions          AS permissions      ON udata.pnr = permissions.customer_identification_number
        LEFT OUTER JOIN prod_swe_base.t_blacklisted_cirrus_customers    AS blacklist        ON udata.tscid = blacklist.tscid

        WHERE permissions.cust_helix_pur1033 IS NULL
        AND blacklist.export_to_cloud IS NULL

        ORDER BY udata.contact_id

);