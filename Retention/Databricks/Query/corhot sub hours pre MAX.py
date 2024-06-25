# Databricks notebook source
# !pip install snowflake-connector-python
# dbutils.library.restartPython()

# COMMAND ----------

import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd
import io
# import snowflake.connector
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Series level

# COMMAND ----------

# spark.sql('''
#           CREATE OR REPLACE table bolt_cus_dev.bronze.user_title_hours_watched_subs_legacy (        
#               hurley_user_id STRING
#             , hurley_profile_id STRING
#             , request_date STRING
#             , ckg_series_id STRING
#             , hours_viewed float
#           )
#             PARTITIONED BY (request_date)
#           ''')

# COMMAND ----------

# qry = '''
#         insert into bolt_cus_dev.bronze.{table}
#             with streaming_subset as ( 
#             SELECT 
#             v.request_date_pst as request_date,
#             v.HBOMAX_USER_ID as hurley_user_id,
#             v.HBOMAX_PROFILE_ID as hurley_profile_id,
#             asset.ckg_series_id, 
#             CONTENT_MINUTES_WATCHED/60 AS hours_viewed
#             FROM bolt_dai_ce_prod.gold.combined_video_stream  v
#             JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined asset 
#                 on v.PROGRAM_ID_OR_VIEWABLE_ID = ASSET.ckg_program_id
#             join bolt_cus_dev.bronze.legacy_user_profile_dim_current up 
#                 ON v.HBOMAX_PROFILE_ID::STRING = up.HURLEY_PROFILE_ID::STRING
#             WHERE 1=1
#             and v.PROGRAM_ID_OR_VIEWABLE_ID is not null
#             and v.CONTENT_MINUTES_WATCHED >= 15
#             and v.VIDEO_TYPE = 'main'
#             AND v.TERRITORY = 'HBO MAX DOMESTIC'
#             AND REQUEST_DATE_PST<='2023-05-23'
#             AND REQUEST_DATE_PST between '{start_date}' and '{end_date}'
#             AND up.IS_PRIMARY_PROFILE=1
#             )

#             SELECT 
#                     ss.hurley_user_id
#                     , ss.hurley_profile_id
#                     , ss.request_date
#                     , ss.ckg_series_id
#                     , sum(hours_viewed) as hours_viewed
#                 FROM streaming_subset ss
#                 JOIN (
#                         SELECT DISTINCT tt.hurley_user_id
#                         FROM bolt_cus_dev.bronze.user_retain_churn_list_test tt
#                         where '{end_date}' >= cycle_start_date 
#                         and '{start_date}' <= cycle_expire_date) c
#                 ON   c.hurley_user_id::STRING = ss.hurley_user_id::STRING
#                 group by all


#         '''

# COMMAND ----------

# table = 'user_title_hours_watched_subs_legacy'
# t =  pd.to_datetime('2020-05-01')

# while (t>=pd.to_datetime('2020-05-01') and t<=pd.to_datetime('2023-05-23')):
#     print (t)
#     query = qry.format(
#                     start_date=t.strftime('%Y-%m-%d'),
#                     end_date = (t + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d'),
#                     table = table
#                     )
#     print (query)
#     df = spark.sql(query)

#     t=t+ pd.DateOffset(months=1)
#     # break

# COMMAND ----------

spark.sql('SELECT MAX(REQUEST_DATE) FROM bolt_cus_dev.bronze.user_title_hours_watched_subs_legacy').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BY Season level

# COMMAND ----------

# spark.sql('''
#           CREATE OR REPLACE table bolt_cus_dev.bronze.user_title_hours_watched_season_legacy (        
#               hurley_user_id STRING
#             , hurley_profile_id STRING
#             , request_date STRING
#             , ckg_series_id STRING
#             , season_number INT
#             , offer_start_date STRING
#             , DAYS_SINCE_PREMIERE INT
#             , hours_viewed float
#           )
#             PARTITIONED BY (request_date)
#           ''')

# COMMAND ----------

# qry = '''
#         insert into bolt_cus_dev.bronze.{table}
#             with streaming_subset as ( 
#             SELECT 
#             v.request_date_pst as request_date,
#             v.HBOMAX_USER_ID as hurley_user_id,
#             v.HBOMAX_PROFILE_ID as hurley_profile_id,
#             asset.ckg_series_id, 
#             asset.season_number,
#             COALESCE(v.SEASON_OFFERING_PLAYABLE_START_DATE_PST, v.SERIES_OFFERING_PLAYABLE_START_DATE_PST) as offer_start_date,
#             v.DAYS_SINCE_SEASON_PREMIERE_DATE_PST as DAYS_SINCE_PREMIERE,
#             v.CONTENT_MINUTES_WATCHED/60 AS hours_viewed
#             FROM bolt_dai_ce_prod.gold.combined_video_stream  v
#             JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined asset 
#                 on v.PROGRAM_ID_OR_VIEWABLE_ID = ASSET.ckg_program_id
#             join bolt_cus_dev.bronze.legacy_user_profile_dim_current up 
#                 ON v.HBOMAX_PROFILE_ID::STRING = up.HURLEY_PROFILE_ID::STRING
#             WHERE 1=1
#             and v.PROGRAM_ID_OR_VIEWABLE_ID is not null
#             and v.CONTENT_MINUTES_WATCHED >= 15
#             and v.VIDEO_TYPE = 'main'
#             AND v.TERRITORY = 'HBO MAX DOMESTIC'
#             AND REQUEST_DATE_PST<='2023-05-23'
#             AND REQUEST_DATE_PST between '{start_date}' and '{end_date}'
#             AND up.IS_PRIMARY_PROFILE=1
#             )

#             SELECT 
#                     ss.hurley_user_id
#                     , ss.hurley_profile_id
#                     , ss.request_date
#                     , ss.ckg_series_id
#                     , ss.season_number
#                     , ss.offer_start_date
#                     , ss.DAYS_SINCE_PREMIERE
#                     , sum(hours_viewed) as hours_viewed
#                 FROM streaming_subset ss
#                 JOIN (
#                         SELECT DISTINCT tt.hurley_user_id
#                         FROM bolt_cus_dev.bronze.user_retain_churn_list_test tt
#                         where '{end_date}' >= cycle_start_date 
#                         and '{start_date}' <= cycle_expire_date) c
#                 ON   c.hurley_user_id::STRING = ss.hurley_user_id::STRING
#                 group by all


#         '''

# COMMAND ----------

# spark.sql('SELECT MIN(request_date) FROM bolt_cus_dev.bronze.user_title_hours_watched_season_legacy').show()

# COMMAND ----------

# table = 'user_title_hours_watched_season_legacy'
# t =  pd.to_datetime('2023-01-01')

# while (t>=pd.to_datetime('2020-05-01') and t<pd.to_datetime('2023-05-23')):
#     print (t)
#     query = qry.format(
#                     start_date=t.strftime('%Y-%m-%d'),
#                     end_date = (t + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d'),
#                     table = table
#                     )
#     print (query)
#     df = spark.sql(query)

#     t=t+ pd.DateOffset(months=1)
#     # break

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bolt_cus_dev.bronze.user_retain_churn_list_test LIMIT 10  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct user_id) FROM bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max
# MAGIC where cycle_expire_date between '2024-01-01' and '2024-01-31'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_title_hours_watched_season_legacy_avod_svod AS (
# MAGIC with subs as(
# MAGIC select c.hurley_user_id, up.hurley_profile_id
# MAGIC , is_cancel, sub_month, is_voluntary, c.sku
# MAGIC , cycle_start_date
# MAGIC , cycle_expire_date
# MAGIC from bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c 
# MAGIC LEFT join bolt_cus_dev.bronze.legacy_user_profile_dim_current up 
# MAGIC     on c.hurley_user_id = up.hurley_user_id
# MAGIC where up.IS_PRIMARY_PROFILE=1
# MAGIC --LIMIT 3000000
# MAGIC )
# MAGIC
# MAGIC , streaming_subset as
# MAGIC (
# MAGIC select
# MAGIC     hb.request_date_pst as request_date
# MAGIC     , hb.WBD_MAX_OR_HBOMAX_USER_ID as user_id
# MAGIC     , hb.WBD_MAX_PROFILE_ID as profile_id
# MAGIC     , hb.HBOMAX_USER_ID as hurley_user_id
# MAGIC     , hb.HBOMAX_PROFILE_ID as hurley_profile_id
# MAGIC     , ss.is_cancel
# MAGIC     , is_voluntary
# MAGIC     , sku
# MAGIC     , ss.sub_month
# MAGIC     , hb.PROGRAM_ID_OR_VIEWABLE_ID as ckg_program_id
# MAGIC     , hb.CONTENT_MINUTES_WATCHED/60 as hours_viewed
# MAGIC from subs ss
# MAGIC left join bolt_dai_ce_prod.gold.combined_video_stream hb
# MAGIC     on hb.HBOMAX_PROFILE_ID::STRING = ss.HURLEY_PROFILE_ID::STRING
# MAGIC where 1=1
# MAGIC     and hb.request_date_pst between ss.cycle_start_date and ss.cycle_expire_date
# MAGIC     and hb.request_date_pst >= DATEADD(MONTH, -1, ss.cycle_expire_date)
# MAGIC     and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
# MAGIC     and hb.CONTENT_MINUTES_WATCHED >= 15
# MAGIC     and hb.video_type = 'main'
# MAGIC     and hb.territory = 'HBO MAX DOMESTIC'
# MAGIC     and hb.request_date_pst <= '2023-05-23'
# MAGIC     and hb.request_date_pst >= '2023-01-01'
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC       s.request_date
# MAGIC     , s.user_id
# MAGIC     , s.profile_id
# MAGIC     , s.hurley_user_id
# MAGIC     , s.hurley_profile_id
# MAGIC     , s.is_cancel
# MAGIC     , s.is_voluntary
# MAGIC     , s.sku
# MAGIC     , s.sub_month
# MAGIC     , rad.ckg_series_id
# MAGIC     , COALESCE(rad.season_number, 0) as season_number
# MAGIC     , s.ckg_program_id
# MAGIC     , sum(s.hours_viewed) as hours_viewed
# MAGIC from streaming_subset s
# MAGIC join bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad
# MAGIC      on s.ckg_program_id = rad.ckg_program_id
# MAGIC where rad.asset_type!='PROMOTION'
# MAGIC group by ALL
# MAGIC )
