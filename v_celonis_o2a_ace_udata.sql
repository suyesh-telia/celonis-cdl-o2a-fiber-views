CREATE OR REPLACE VIEW prod_swe_access.v_celonis_o2a_ace_udata AS
SELECT
    ts_cid,
    contact_id,
    firstcontactsourcetype,
    origContactSourceType,
    menuchoice,
    inittarget,
    release_type,
    entrance_escpoint,
    Target,
    Entrance,
    CAST(create_time AS TIMESTAMP) AS create_time,
    CAST(delivering_time AS TIMESTAMP) AS delivering_time,
    CAST(contact_delivered_time AS TIMESTAMP) AS contact_delivered_time,
    CAST(contact_established_time AS TIMESTAMP) AS contact_established_time,
    CAST(close_time AS TIMESTAMP) AS close_time,
    concat_ws(",",collect_list(connected_by)) AS connected_by_id,
    concat_ws(",",collect_list(udata_key)) AS connected_by_keys
FROM
    prod_swe_access.v_celonis_o2a_ace_udata_powerbi
GROUP BY
    ts_cid,
    contact_id,
    firstcontactsourcetype,
    origcontactsourcetype,
    menuchoice,
    inittarget,
    release_type,
    entrance_escpoint,
    Target,
    Entrance,
    create_time,
    delivering_time,
    contact_delivered_time,
    contact_established_time,
    close_time
ORDER BY
    create_time;