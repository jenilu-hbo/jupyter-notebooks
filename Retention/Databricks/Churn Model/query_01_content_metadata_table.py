# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_recency_title_season_level_metadata
# MAGIC with series_first as (
# MAGIC     SELECT
# MAGIC     a.ckg_series_id
# MAGIC     , min(first_offered_date) as series_window_start
# MAGIC     , max(first_offered_date) as series_window_end
# MAGIC     FROM bolt_dai_ckg_prod.gold.reporting_asset_dim_combined a 
# MAGIC     JOIN bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
# MAGIC         on a.ckg_program_id = b.ckg_program_id 
# MAGIC         and b.country_code = 'US' 
# MAGIC     where asset_type = 'FEATURE' 
# MAGIC     group by all
# MAGIC )
# MAGIC
# MAGIC , metadata as (
# MAGIC     SELECT 
# MAGIC     rad.series_title_long as  title_name
# MAGIC     , rad.ckg_series_id
# MAGIC     , COALESCE(rad.season_number, 0) as season_number
# MAGIC     , rad.SERIES_TYPE
# MAGIC     , mode(rad.PRIMARY_GENRE) as primary_genre
# MAGIC     , mode(rad.program_type) as program_type
# MAGIC     , rad.EVER_POPCORN_TITLE AS is_popcorn
# MAGIC     , CASE WHEN THEATRICAL_RELEASE_DATE IS NOT NULL THEN 1 ELSE 0 END AS is_pay_1
# MAGIC     , sum(ASSET_RUN_TIME/3600.00) as RUN_TIME_HOURS
# MAGIC     , MIN(first_offered_date) AS season_window_start
# MAGIC     , MAX(first_offered_date) AS season_window_end
# MAGIC      FROM bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined rad
# MAGIC      JOIN bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
# MAGIC         on rad.ckg_program_id = b.ckg_program_id 
# MAGIC         and b.country_code = 'US' 
# MAGIC      where asset_type = 'FEATURE'
# MAGIC      and series_title_long is not null
# MAGIC      GROUP BY ALL)
# MAGIC
# MAGIC , medal_data as (
# MAGIC   SELECT title_id as ckg_series_id
# MAGIC   , season_number
# MAGIC   , medal_us as medal
# MAGIC   , lob
# MAGIC   , mode(category) as category
# MAGIC   FROM bolt_cus_dev.gold.delphi_titles
# MAGIC   group by all
# MAGIC ) 
# MAGIC
# MAGIC SELECT m.title_name
# MAGIC , m.ckg_series_id
# MAGIC , m.season_number
# MAGIC , m.series_type
# MAGIC , m.primary_genre
# MAGIC , m.program_type
# MAGIC , m.is_popcorn
# MAGIC , m.is_pay_1
# MAGIC , m.run_time_hours
# MAGIC , m.season_window_start
# MAGIC , m.season_window_end
# MAGIC , s.series_window_start
# MAGIC , s.series_window_end
# MAGIC , CASE WHEN medal.medal IS NULL THEN 'Bronze' ELSE medal.medal END AS medal
# MAGIC , medal.lob
# MAGIC , medal.category
# MAGIC FROM metadata m
# MAGIC JOIN series_first s ON s.ckg_series_id = m.ckg_series_id
# MAGIC LEFT JOIN medal_data medal 
# MAGIC     on medal.ckg_series_id = m.ckg_series_id
# MAGIC     and medal.season_number = m.season_number

# COMMAND ----------


