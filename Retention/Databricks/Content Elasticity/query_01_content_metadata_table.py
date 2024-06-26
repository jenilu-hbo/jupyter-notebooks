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
        and b.country_code = 'US' 
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
    , rad.SERIES_TYPE
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
        and b.country_code = 'US' 
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
, CASE WHEN medal = 'WB | Same Day Premiere' then 'Platinum'
       WHEN medal = 'A' then 'Gold'
       WHEN medal = 'B' then 'Silver'
       WHEN medal = 'C' then 'Bronze'
       ELSE medal
  END AS medal
, medal.lob
, medal.category
FROM metadata m
JOIN series_first s ON s.ckg_series_id = m.ckg_series_id and s.country_code = s.country_code
LEFT JOIN medal_data medal 
    on medal.ckg_series_id = m.ckg_series_id
    and medal.season_number = m.season_number
''')

# COMMAND ----------

qc = spark.sql('''
               SELECT ckg_series_id, season_number, count(*) as record_count
               FROM bolt_cus_dev.bronze.cip_recency_title_season_level_metadata
               GROUP BY ALL
               HAVING  record_count > 1
               ''').toPandas()
assert len(qc) == 0
