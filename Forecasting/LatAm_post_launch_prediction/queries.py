backfill_title_actives_query = '''
INSERT INTO max_dev.workspace.{table}
WITH dt_first_offered_dates AS (
--first offered by BTC
    select
      'BTC' as level,
      title_id,
      viewable_id,
      territory,
      brand,
      channel,
      first_offered_date,
      season_first_offered_date,
      title_first_offered_date,
      latest_asset_release_date,
      start_utc,
      end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'

    union all

    -- first offered by territory
    select
      'Territory' as level,
      title_id,
      viewable_id,
      territory,
      'Territory' as brand,
      'Territory' as channel,
      min(first_offered_date) as first_offered_date,
      min(season_first_offered_date) as season_first_offered_date,
      min(title_first_offered_date) as title_first_offered_date,
      max(latest_asset_release_date) as latest_asset_release_date,
      min(start_utc) as start_utc,
      max(end_utc) as end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'
    group by 1,2,3,4,5,6

    union all

    --first offered by all (HBO MAX?)
    select
      'HBO All' as level,
      title_id,
      viewable_id,
      'HBO All' as territory,
      'HBO All' as brand,
      'HBO All' as channel,
      min(first_offered_date) as first_offered_date,
      min(season_first_offered_date) as season_first_offered_date,
      min(title_first_offered_date) as title_first_offered_date,
      max(latest_asset_release_date) as latest_asset_release_date,
      min(start_utc) as start_utc,
      max(end_utc) as end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'
    group by 1,2,3,4,5,6
    )
            
SELECT title_name, match_id, content_category, available_date, days_on_hbo_max, local_country_date_date as end_date,
       count_hbo_uuid as actives, total_hours_viewed, record_count, count_first_viewing_accounts
FROM(
SELECT
    '{end_date}'  AS local_country_date_date,
    reporting_asset_dim."TITLE_NAME"  AS title_name,
    reporting_asset_dim."TITLE_ID"  AS title_id,
    case when reporting_asset_dim."CONTENT_CATEGORY" = 'series'
    then ifnull(season_number, 1)
    else season_number
        end as season_number_adj,
    coalesce(concat(reporting_asset_dim.series_id, '-', season_number_adj), reporting_asset_dim.viewable_id) as match_id,
    initcap(reporting_asset_dim."CONTENT_CATEGORY")  AS content_category,
    (TO_CHAR(TO_DATE(dt_first_offered_dates_territory."TITLE_FIRST_OFFERED_DATE" ), 'YYYY-MM-DD')) AS available_date,
    DATEDIFF('day', available_date, '{end_date}') as days_on_hbo_max,
    COUNT(DISTINCT ( uat_max_user_stream_heartbeat."HBO_UUID" )) AS count_hbo_uuid,
    COALESCE(SUM(( uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS"  )/60/60 ), 0) AS total_hours_viewed,
    COUNT(*) AS record_count,
    COUNT(DISTINCT subscription_first_content_watched.subscription_id) AS count_first_viewing_accounts
FROM viewership.max_user_stream_heartbeat  AS uat_max_user_stream_heartbeat
LEFT JOIN "CATALOG"."LIVE_SOCCER_SCHEDULE_REF"
     AS live_soccer_schedule_ref ON (uat_max_user_stream_heartbeat."VIEWABLE_ID") = (live_soccer_schedule_ref."VIEWABLE_ID")
LEFT JOIN "CATALOG"."REPORTING_ASSET_DIM" AS reporting_asset_dim ON (reporting_asset_dim."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
LEFT JOIN "CORE"."GEO_MAP"  AS geo_map ON (geo_map."COUNTRY_ISO_CODE") = (uat_max_user_stream_heartbeat."COUNTRY_ISO_CODE")
LEFT JOIN MAX_PROD.CATALOG.REPORTING_ASSET_OFFERING_DIM  AS reporting_asset_offering_dim ON (reporting_asset_offering_dim."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
        and (reporting_asset_offering_dim."BRAND") = 'HBO MAX'
        and (reporting_asset_offering_dim."CHANNEL") = (uat_max_user_stream_heartbeat."CHANNEL")
        and (reporting_asset_offering_dim."TERRITORY") = (geo_map."TERRITORY")

LEFT JOIN dt_first_offered_dates AS dt_first_offered_dates_territory ON (dt_first_offered_dates_territory."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
          and (dt_first_offered_dates_territory."CHANNEL") = 'Territory'
          and (dt_first_offered_dates_territory."TERRITORY") = (geo_map."TERRITORY")

LEFT JOIN
      bi_analytics.subscription_first_content_watched
      AS subscription_first_content_watched ON (uat_max_user_stream_heartbeat."HBO_UUID") = subscription_first_content_watched.hbo_uuid
                and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") = (subscription_first_content_watched."REQUEST_TIME_GMT")
                and (uat_max_user_stream_heartbeat."VIEWABLE_ID") = subscription_first_content_watched.viewable_id
                and (uat_max_user_stream_heartbeat."COUNTRY_ISO_CODE") = (subscription_first_content_watched."COUNTRY_ISO_CODE")
WHERE ((reporting_asset_dim."ASSET_TYPE") ) IN ('ELEMENT', 'FEATURE') 
  AND ( 1=1
    -- Define the date here    
    AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  >= available_date
    AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  < '{end_date}'
    AND available_date = '{start_date}'
    ------  End  
    AND ((CASE
          WHEN ((reporting_asset_dim."ASSET_RUN_TIME") < 180 AND (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%trailer%') THEN 'Promotional Asset'
          WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%A Test Series%' THEN 'Promotional Asset'
          WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%What To Watch%' THEN 'Promotional Asset'
          ELSE 'Main Asset'
          END)='Main Asset' )) 
  AND ((uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS" ) >= 120 
       -- Here Define Region
       AND (((UPPER(( geo_map."REGION"  )) = UPPER('LATAM'))) 
       -- End
            AND ((uat_max_user_stream_heartbeat."VIDEO_TYPE") = 'main' 
                 and  (uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS")>=120
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' 
                 then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= dateadd('minutes',-120,(reporting_asset_offering_dim."FIRST_OFFERED_DATE")) 
                 else (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= (reporting_asset_offering_dim."FIRST_OFFERED_DATE") end
      AND   1=1
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE'
               then dateadd('seconds',(uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS"),(uat_max_user_stream_heartbeat."REQUEST_TIME_GMT")) >= dateadd('minutes',-1*(live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= '2021-09-26'
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 + (live_soccer_schedule_ref."POST_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < '2021-09-26'
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 - (live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      )))

GROUP BY
1, 2, 3, 4, 5,6, 7)
'''



back_fill_base_query = '''
        INSERT INTO max_dev.workspace.{table}
-- sql for creating the total and/or determining pivot columns
WITH dt_first_offered_dates AS (--first offered by BTC
    select
      'BTC' as level,
      title_id,
      viewable_id,
      territory,
      brand,
      channel,
      first_offered_date,
      season_first_offered_date,
      title_first_offered_date,
      latest_asset_release_date,
      start_utc,
      end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'

    union all

    -- first offered by territory
    select
      'Territory' as level,
      title_id,
      viewable_id,
      territory,
      'Territory' as brand,
      'Territory' as channel,
      min(first_offered_date) as first_offered_date,
      min(season_first_offered_date) as season_first_offered_date,
      min(title_first_offered_date) as title_first_offered_date,
      max(latest_asset_release_date) as latest_asset_release_date,
      min(start_utc) as start_utc,
      max(end_utc) as end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'
    group by 1,2,3,4,5,6

    union all

    --first offered by all (HBO MAX?)
    select
      'HBO All' as level,
      title_id,
      viewable_id,
      'HBO All' as territory,
      'HBO All' as brand,
      'HBO All' as channel,
      min(first_offered_date) as first_offered_date,
      min(season_first_offered_date) as season_first_offered_date,
      min(title_first_offered_date) as title_first_offered_date,
      max(latest_asset_release_date) as latest_asset_release_date,
      min(start_utc) as start_utc,
      max(end_utc) as end_utc
    from
      max_prod.catalog.reporting_asset_offering_dim
    where
      brand = 'HBO MAX'
    group by 1,2,3,4,5,6
    )
    
SELECT
    '{start_date}' as start_date,
    '{end_date}' as end_date,  
    DATEDIFF('day', start_date, end_date),
    COUNT(DISTINCT ( uat_max_user_stream_heartbeat."HBO_UUID" )) AS "uat_max_user_stream_heartbeat.count_hbo_uuid",
    COALESCE(SUM(( uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS"  )/60/60 ), 0) AS "uat_max_user_stream_heartbeat.total_hours_viewed",
    COUNT(*) AS "uat_max_user_stream_heartbeat.count",
    COUNT(DISTINCT subscription_first_content_watched.subscription_id) AS "subscription_first_content_watched.count_first_viewing_accounts"
FROM viewership.max_user_stream_heartbeat  AS uat_max_user_stream_heartbeat
LEFT JOIN "CATALOG"."LIVE_SOCCER_SCHEDULE_REF"
     AS live_soccer_schedule_ref ON (uat_max_user_stream_heartbeat."VIEWABLE_ID") = (live_soccer_schedule_ref."VIEWABLE_ID")
LEFT JOIN "CATALOG"."REPORTING_ASSET_DIM" AS reporting_asset_dim ON (reporting_asset_dim."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
LEFT JOIN "CORE"."GEO_MAP"  AS geo_map ON (geo_map."COUNTRY_ISO_CODE") = (uat_max_user_stream_heartbeat."COUNTRY_ISO_CODE")
LEFT JOIN MAX_PROD.CATALOG.REPORTING_ASSET_OFFERING_DIM  AS reporting_asset_offering_dim ON (reporting_asset_offering_dim."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
        and (reporting_asset_offering_dim."BRAND") = 'HBO MAX'
        and (reporting_asset_offering_dim."CHANNEL") = (uat_max_user_stream_heartbeat."CHANNEL")
        and (reporting_asset_offering_dim."TERRITORY") = (geo_map."TERRITORY")

LEFT JOIN dt_first_offered_dates AS dt_first_offered_dates_territory ON (dt_first_offered_dates_territory."VIEWABLE_ID") = (uat_max_user_stream_heartbeat."VIEWABLE_ID")
          and (dt_first_offered_dates_territory."CHANNEL") = 'Territory'
          and (dt_first_offered_dates_territory."TERRITORY") = (geo_map."TERRITORY")

LEFT JOIN
      bi_analytics.subscription_first_content_watched
      AS subscription_first_content_watched ON (uat_max_user_stream_heartbeat."HBO_UUID") = subscription_first_content_watched.hbo_uuid
                and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") = (subscription_first_content_watched."REQUEST_TIME_GMT")
                and (uat_max_user_stream_heartbeat."VIEWABLE_ID") = subscription_first_content_watched.viewable_id
                and (uat_max_user_stream_heartbeat."COUNTRY_ISO_CODE") = (subscription_first_content_watched."COUNTRY_ISO_CODE")
WHERE ((reporting_asset_dim."ASSET_TYPE") ) IN ('ELEMENT', 'FEATURE') 
AND (1=1
    -- define dates here
    AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  >= '{start_date}'
    AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  < '{end_date}'
    -- END
     AND ((CASE
        WHEN ((reporting_asset_dim."ASSET_RUN_TIME") < 180 AND (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%trailer%') THEN 'Promotional Asset'
        WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%A Test Series%' THEN 'Promotional Asset'
        WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%What To Watch%' THEN 'Promotional Asset'
        ELSE 'Main Asset'
        END)='Main Asset' )) AND ((uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS" ) >= 120 AND (((UPPER(( geo_map."REGION"  )) = UPPER('LATAM'))) AND ((uat_max_user_stream_heartbeat."VIDEO_TYPE") = 'main' and  (uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS")>=120
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= dateadd('minutes',-120,(reporting_asset_offering_dim."FIRST_OFFERED_DATE")) else (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= (reporting_asset_offering_dim."FIRST_OFFERED_DATE") end
      AND   1=1
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE'
               then dateadd('seconds',(uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS"),(uat_max_user_stream_heartbeat."REQUEST_TIME_GMT")) >= dateadd('minutes',-1*(live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= '2021-09-26'
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 + (live_soccer_schedule_ref."POST_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < '2021-09-26'
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 - (live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      )))
GROUP BY
    1, 2
'''