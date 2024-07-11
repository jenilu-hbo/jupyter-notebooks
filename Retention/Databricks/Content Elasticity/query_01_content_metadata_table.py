# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_recency_title_season_level_metadata
with series_first as (
    SELECT
    a.ckg_series_id
    , b.country_code
    , min(CONVERT_TIMEZONE('UTC', geo_map.TIME_ZONE_DESC, first_offered_date)::date) as series_window_start
    , max(CONVERT_TIMEZONE('UTC', geo_map.TIME_ZONE_DESC, first_offered_date)::date) as series_window_end
    FROM bolt_dai_ckg_prod.gold.reporting_asset_dim_combined a 
    JOIN bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
        on a.ckg_program_id = b.ckg_program_id 
    LEFT JOIN bolt_dai_ckg_prod.gold.geo_map geo_map on geo_map.country_iso_code = b.country_code
    where asset_type = 'FEATURE' 
    group by all
)

, metadata as (
    SELECT 
    rad.series_title_long as  title_name
    , rad.ckg_series_id
    , COALESCE(rad.season_number, 0) as season_number
    , b.country_code
    , geo_map.region
    , CASE WHEN rad.series_type LIKE ('%series%') THEN 'series'
        WHEN rad.series_type IN ('movie', 'standalone') THEN 'movie'
        ELSE rad.series_type
        END AS series_type
    , mode(rad.PRIMARY_GENRE) as primary_genre
    , mode(rad.program_type) as program_type
    , rad.EVER_POPCORN_TITLE AS is_popcorn
    , CASE WHEN THEATRICAL_RELEASE_DATE IS NOT NULL THEN 1 ELSE 0 END AS is_pay_1
    , sum(ASSET_RUN_TIME/3600.00) as RUN_TIME_HOURS
    , min(CONVERT_TIMEZONE('UTC', geo_map.TIME_ZONE_DESC, first_offered_date)::date) AS season_window_start
    , max(CONVERT_TIMEZONE('UTC', geo_map.TIME_ZONE_DESC, first_offered_date)::date) AS season_window_end
     FROM bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined rad
     JOIN bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
        on rad.ckg_program_id = b.ckg_program_id 
     LEFT JOIN bolt_dai_ckg_prod.gold.geo_map geo_map on geo_map.country_iso_code = b.country_code
     where asset_type = 'FEATURE'
     and series_title_long is not null
     GROUP BY ALL)

, medal_data as (
  SELECT title_id as ckg_series_id
  , season_number
  , medal_us as medal
  , lob
  , mode(category) as category
  , imdb_id
  FROM bolt_cus_dev.gold.delphi_titles
  group by all
) 

SELECT m.title_name
, m.ckg_series_id
, m.season_number
, m.series_type
, m.primary_genre
, m.program_type
, m.is_popcorn
, m.is_pay_1
, m.run_time_hours
, m.season_window_start
, m.season_window_end
, s.series_window_start
, s.series_window_end
, m.country_code
, m.region
, CASE WHEN medal = 'WB | Same Day Premiere' then 'Platinum'
       WHEN medal = 'A' then 'Gold'
       WHEN medal = 'B' then 'Silver'
       WHEN medal = 'C' then 'Bronze'
       ELSE medal
  END AS medal
, medal.lob
, medal.category
, medal.imdb_id
FROM metadata m
JOIN series_first s ON s.ckg_series_id = m.ckg_series_id and s.country_code = m.country_code
LEFT JOIN medal_data medal 
    on medal.ckg_series_id = m.ckg_series_id
    and medal.season_number = m.season_number
''')

# COMMAND ----------

spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_recency_title_series_level_new_library_indicator

WITH calendar_season AS (
    SELECT t.date as request_date, ckg_series_id, season_number, region, country_code
    , case when t.date between m.season_window_start and dateadd(DAY, 30, m.season_window_end) THEN 1 ELSE 0 END AS in_season_window
    FROM bolt_cus_dev.bronze.cip_daily_date_tracker t
    LEFT JOIN  bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m
    WHERE t.date >= m.season_window_start
    and t.date <= GETDATE()
)

, calendar_series AS (
    SELECT request_date, ckg_series_id, region, country_code
           , MAX(in_season_window) as in_series_window
    FROM calendar_season
    GROUP BY ALL
)

SELECT ss.*, s.in_series_window
FROM calendar_season ss
JOIN calendar_series s
ON s.ckg_series_id = ss.ckg_series_id
and s.region = ss.region
and s.country_code = ss.country_code
and s.request_date = ss.request_date
''')

# COMMAND ----------

qc = spark.sql('''
               SELECT ckg_series_id, season_number, country_code, count(*) as record_count
               FROM bolt_cus_dev.bronze.cip_recency_title_season_level_metadata
               GROUP BY ALL
               HAVING  record_count > 1
               ''').toPandas()
assert len(qc) == 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bolt_cus_dev.gold.delphi_titles
# MAGIC WHERE title_id = 'a8484031-f244-4661-9fb7-0932bd1ba872'
# MAGIC -- and country_code = 'US'
