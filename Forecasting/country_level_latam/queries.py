backfill_title_actives_query = '''
INSERT INTO max_dev.workspace.{table}
WITH dt_first_offered_dates AS
(
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
    -------------
    --AND title_id = 'GYZ9MrgqYWBq0wwEAAAAF'
    ------------
    group by 1,2,3,4,5,6
)

SELECT sub_region, title_name, match_id, content_category, available_date, days_on_hbo_max, local_country_date_date as end_date,count_hbo_uuid as actives
FROM (

SELECT
    '{end_date}'  AS local_country_date_date,
    INITCAP(geo_map."SUB_REGION") AS sub_region,
    reporting_asset_dim."TITLE_NAME" AS title_name,
    initcap(reporting_asset_dim."CONTENT_CATEGORY")  AS content_category,
    case when reporting_asset_dim."CONTENT_CATEGORY" = 'series'
        then ifnull(season_number, 1)
        else season_number
            end as season_number_adj,
    coalesce(concat(reporting_asset_dim.series_id, '-', season_number_adj), reporting_asset_dim.viewable_id) as match_id, 
    (TO_CHAR(TO_DATE(dt_first_offered_dates_territory.season_first_offered_date ), 'YYYY-MM-DD')) AS available_date,
    DATEDIFF('day', available_date, '{end_date}') as days_on_hbo_max,
    COUNT(DISTINCT ( uat_max_user_stream_heartbeat."HBO_UUID" )) AS count_hbo_uuid
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
WHERE reporting_asset_dim."ASSET_TYPE" IN ('ELEMENT', 'FEATURE')
AND 1=1
------- Define Date Here --------------------------
AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  >= '{start_date}'
AND uat_max_user_stream_heartbeat."LOCAL_COUNTRY_DATE"  < '{end_date}'
AND available_date = '{start_date}'
---------------------------------------------------
AND ((CASE
  WHEN ((reporting_asset_dim."ASSET_RUN_TIME") < 180 AND (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%trailer%') THEN 'Promotional Asset'
  WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%A Test Series%' THEN 'Promotional Asset'
  WHEN (initcap((reporting_asset_dim."ASSET_NAME"))) ILIKE '%What To Watch%' THEN 'Promotional Asset'
  ELSE 'Main Asset'
  END)='Main Asset' ) 
AND ((uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS" ) >= 120
        --------------- Define Region Here ------------
      AND (((UPPER(( geo_map."REGION"  )) = UPPER('LATAM')))
         -----------------------------------------------
      AND ((uat_max_user_stream_heartbeat."VIDEO_TYPE") = 'main' and  (uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS")>=120
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= dateadd('minutes',-120,(reporting_asset_offering_dim."FIRST_OFFERED_DATE")) else (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= (reporting_asset_offering_dim."FIRST_OFFERED_DATE") end
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (live_soccer_schedule_ref."MATCH_ID") is not null
               then dateadd('seconds',(uat_max_user_stream_heartbeat."STREAM_ELAPSED_PLAY_SECONDS"),(uat_max_user_stream_heartbeat."REQUEST_TIME_GMT")) >= dateadd('minutes',-1*(live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      AND case when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") >= '2021-09-26' and (live_soccer_schedule_ref."MATCH_ID") is not null
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 + (live_soccer_schedule_ref."POST_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               when upper((uat_max_user_stream_heartbeat."STREAM_TYPE")) = 'LIVE' and (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < '2021-09-26' and (live_soccer_schedule_ref."MATCH_ID") is not null
               then (uat_max_user_stream_heartbeat."REQUEST_TIME_GMT") < dateadd('minutes',(reporting_asset_dim."ASSET_RUN_TIME")/60 - (live_soccer_schedule_ref."PRE_GAME_DURATION_IN_MINUTES"),(live_soccer_schedule_ref."GAME_KICK_OFF_TIME"))
               else 1=1 end
      )))
GROUP BY
    1,2, 3, 4, 5, 6, 7
)

'''

back_fill_base_query = '''
INSERT INTO max_dev.workspace.{table}

with first as (
         SELECT distinct
              territory,
              title_id,
               'Series and Movies' as title_level,
              case when offering_start_date <='2021-06-29 09:00:00' THEN '2021-06-29 09:00:00' else offering_start_date end as offering_start_date,
              offering_end_date,
              case when content_category = 'series' then ifnull(season_number, 1) else season_number end as season_number_adj,
              coalesce(concat(b.series_id, '-', season_number_adj), b.viewable_id) as match_id,
              case when b.season_number is null then title_name else concat(b.series_title_long,' S',b.season_number) end as title
          from catalog.asset_offering_dim a
           left join catalog.reporting_asset_dim b on a.viewable_id=b.viewable_id
          where a.offering_end_date >='2021-06-29 09:00:00'
           and date_trunc('day',a.offering_start_date) <= current_date()
           and b.asset_type = 'FEATURE'
           and 1=1 -- no filter on 'new_title_release.territory'
  --------------------------------------- Filters Here ---------------------------------------------------
           and territory in ('ARGENTINA','BRAZIL','CARIBBEAN','COLOMBIA','MEXICO','LATAM','REST OF LATAM')
           and OFFERING_START_DATE >= '{start_date}' and OFFERING_START_DATE < dateadd(day, 1, '{start_date}')
  --------------------------------------------------------------------------------------------------------
      ),
      second as (
          SELECT
                 s1.territory,
                 s1.title_id,
                 s1.title,
                 s1.match_id,
                 s1.offering_start_date,
                 MIN(t1.offering_end_date) AS offering_end_date
          FROM first s1
          INNER JOIN first t1 ON s1.offering_start_date <= t1.offering_end_date and s1.title_id=t1.title_id and s1.title = t1.title and s1.territory = t1.territory
            AND NOT EXISTS(SELECT * FROM first t2
                           WHERE t1.offering_end_date >= t2.offering_start_date AND t1.offering_end_date < t2.offering_end_date  and t1.title_id=t2.title_id and t1.title = t2.title
                          and t1.territory = t2.territory)
          WHERE NOT EXISTS(SELECT * FROM first s2
                           WHERE s1.offering_start_date > s2.offering_start_date AND s1.offering_start_date <= s2.offering_end_date  and s1.title_id=s2.title_id and s1.title = s2.title
                          and s1.territory = s2.territory)
          GROUP BY 1,2,3,4,5
          order by 1,3
      ),
      third as (
           SELECT
           *,
           datediff(second, offering_start_date, offering_end_date) as window_length
           FROM second
      ),
      fourth as (
          SELECT
           territory,
           title_id,
           title,
           match_id,
           offering_start_date,
           offering_end_date,
           window_length,
           sum(window_length) over (partition by territory,title_id,title,match_id order by offering_start_date asc) as window_end
         FROM third
      ),
      max_release_date as (
           SELECT
             territory,
             title_id,
             title,
             match_id,
             offering_start_date,
             offering_end_date,
             window_end-window_length as window_start
           from fourth
      ), 
      denom_first_viewed as (
          select
            s.territory,
            s.title_id,
            s.title,
            s.match_id,
            hbo_uuid,
            offering_start_date,
            min(floor(((datediff(second, offering_start_date,request_time_gmt)+window_start)/24/3600))) as days_on_hbo_max
            from max_prod.viewership.max_user_stream_heartbeat mus
              join max_prod.catalog.reporting_asset_dim b on b.viewable_id = mus.viewable_id
              --------------------------------------------------------------------------------------------------------------------------
              join max_release_date s on  mus.request_time_gmt BETWEEN s.offering_start_date AND dateadd(day, 30, s.offering_start_date)
              --------------------------------------------------------------------------------------------------------------------------

              join max_prod.core.geo_map d
                  on mus.country_iso_code = d.country_iso_code
                  and s.territory = d.territory
              join max_prod.catalog.reporting_asset_offering_dim raod
                on raod.viewable_id = mus.viewable_id
                and raod.brand = 'HBO MAX'
                and raod.territory = d.territory
                and raod.channel = mus.channel
            where region = 'LATAM'
              and stream_elapsed_play_seconds >= 120
              and video_type = 'main'
              and request_time_gmt between s.offering_start_date and s.offering_end_date
              and b.asset_type = 'FEATURE'
              and request_time_gmt >= raod.first_offered_date
            group by 1,2,3,4,5,6
          ),
      
      
      denom_subs_count as (
        select
          territory,
          title_id,
          title,
          match_id,
          offering_start_date,
          days_on_hbo_max,
          count(hbo_uuid) as subs_count
        from denom_first_viewed
        group by 1,2,3,4,5,6
      )
      select
        territory,
        title,
        match_id,
        offering_start_date,
        days_on_hbo_max+1 as days_on_hbo_max,
        sum(subs_count) over(partition by territory,title_id,title,match_id order by days_on_hbo_max rows between unbounded preceding and current row) as cumulative_viewing_subs_denom
      from denom_subs_count
      order by 1,2,3,5 asc
      





'''