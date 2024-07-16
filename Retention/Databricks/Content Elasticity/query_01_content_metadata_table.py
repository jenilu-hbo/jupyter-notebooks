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

qc = spark.sql('''
               SELECT ckg_series_id, season_number, country_code, count(*) as record_count
               FROM bolt_cus_dev.bronze.cip_recency_title_season_level_metadata
               GROUP BY ALL
               HAVING  record_count > 1
               ''').toPandas()
assert len(qc) == 0

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table bolt_cus_dev.bronze.cip_recency_title_offering_table_season_level 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (request_date)
# MAGIC
# MAGIC with first as (
# MAGIC SELECT
# MAGIC     a.ckg_series_id
# MAGIC     , a.season_number
# MAGIC     , country_code
# MAGIC     ,max(first_offered_date) as window_end
# MAGIC FROM bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined a 
# MAGIC inner join bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
# MAGIC     on a.ckg_program_id = b.ckg_program_id 
# MAGIC where asset_type = 'FEATURE' 
# MAGIC     and (case when content_category = 'episode' and episode_number_in_season is null then 0
# MAGIC     else 1 end) = 1
# MAGIC group by all
# MAGIC ),
# MAGIC second as
# MAGIC (select  
# MAGIC     series_title_long 
# MAGIC     , a.ckg_series_id 
# MAGIC     , content_category 
# MAGIC     , a.season_number 
# MAGIC     , a.ckg_program_id 
# MAGIC     , a.legacy_hbomax_viewable_id
# MAGIC     , episode_number_in_season 
# MAGIC     --using title_first_offered for movies, using individual start_dates for all other categories 
# MAGIC     , release_year
# MAGIC     , b.country_code
# MAGIC     , coalesce(season_first_offered_date,title_first_offered_date) as start_date 
# MAGIC     , dateadd(day, 30, window_end) as recency_window_end 
# MAGIC     , median(release_year) over (partition by a.ckg_series_id, a.season_number)::int as median_release_year 
# MAGIC     , median_release_year+1 as release_year_plusone 
# MAGIC from bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined a 
# MAGIC inner join bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
# MAGIC     on a.ckg_program_id = b.ckg_program_id 
# MAGIC left join first c 
# MAGIC     on a.ckg_series_id=c.ckg_series_id 
# MAGIC     and ifnull(a.season_number,0)=ifnull(c.season_number,0)
# MAGIC     and c.country_code = b.country_code
# MAGIC where asset_type = 'FEATURE' 
# MAGIC     and title_first_offered_date is not null 
# MAGIC     and (case when content_category = 'episode' and episode_number_in_season is null then 0 else 1 end) = 1
# MAGIC group by 1,2,3,4,5,6,7,8,9,10,11)
# MAGIC ,third as
# MAGIC (SELECT
# MAGIC     b.series_title_long 
# MAGIC     , b.ckg_series_id 
# MAGIC     , b.content_category 
# MAGIC     , b.season_number 
# MAGIC     , b.ckg_program_id 
# MAGIC     , b.legacy_hbomax_viewable_id
# MAGIC     , b.episode_number_in_season 
# MAGIC     , b.country_code
# MAGIC     , a.start_date 
# MAGIC     , a.recency_window_end 
# MAGIC     , a.median_release_year 
# MAGIC     , a.release_year_plusone     
# MAGIC FROM second a
# MAGIC inner join second b 
# MAGIC     on a.ckg_series_id=b.ckg_series_id 
# MAGIC     and a.season_number>b.season_number 
# MAGIC     and a.start_date<>b.start_date
# MAGIC     and a.country_code = b.country_code
# MAGIC group by all
# MAGIC )
# MAGIC , fourth as ( 
# MAGIC SELECT 
# MAGIC     series_title_long 
# MAGIC     , ckg_series_id 
# MAGIC     , content_category 
# MAGIC     , season_number 
# MAGIC     , ckg_program_id 
# MAGIC     , legacy_hbomax_viewable_id
# MAGIC     , episode_number_in_season 
# MAGIC     , country_code
# MAGIC     , start_date 
# MAGIC     , recency_window_end 
# MAGIC     , median_release_year 
# MAGIC     , release_year_plusone     
# MAGIC FROM second
# MAGIC UNION ALL
# MAGIC SELECT * FROM third
# MAGIC )
# MAGIC , fifth as (
# MAGIC SELECT
# MAGIC     date as request_date
# MAGIC FROM bolt_cus_dev.bronze.cip_daily_date_tracker
# MAGIC where date < current_date()
# MAGIC group by all)
# MAGIC SELECT distinct
# MAGIC     a.request_date
# MAGIC     , b.ckg_series_id
# MAGIC     , b.season_number
# MAGIC     , b.country_code
# MAGIC FROM fifth a
# MAGIC left join fourth b 
# MAGIC     on a.request_date between to_date(b.start_date) and to_date(recency_window_end) 
# MAGIC     and year(a.request_date) between median_release_year and release_year_plusone
# MAGIC where 1=1
# MAGIC group by all
