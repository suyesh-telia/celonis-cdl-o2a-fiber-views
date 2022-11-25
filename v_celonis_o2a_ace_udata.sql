/*

 2022-10-11 Inserted 5421672 row(s)
 2022-10-24 Inserted 5560550 row(s)
 2022-10-31 Inserted 5639222 row(s)

 */
DROP TABLE IF EXISTS prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata;

CREATE TABLE prod_swe_sandboxes.chan10_v_celonis_o2a_callguide_udata AS
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
    group_concat(CAST(connected_by AS STRING), ', ') AS connected_by_id,
    group_concat(CAST(udata_key AS STRING), ', ') AS connected_by_keys
FROM
    prod_swe_sandboxes.chan10_v_cel_o2a_callguide_udata_powerbi
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