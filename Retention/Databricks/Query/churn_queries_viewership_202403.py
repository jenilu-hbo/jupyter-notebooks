# Databricks notebook source
import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd
import io

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

sql = '''
--CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_svod_avod AS (
INSERT INTO TABLE bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_svod_avod
with subs as(
select c.user_id, c.hurley_user_id, c.sub_id, up.profile_id
, is_cancel, sub_month, is_voluntary
, cycle_start_date
, cycle_expire_date
from bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c 
LEFT join bolt_dai_subs_prod.gold.max_profile_dim_current up
    on c.user_id = up.USER_ID
where up.default_profile_ind = True
and cycle_expire_date between '{target_month}' and '{target_month_end}'
--LIMIT 3000000
)

, streaming_subset as
(
select
      ss.user_id
    , ss.hurley_user_id
    , ss.profile_id
    , ss.is_cancel
    , is_voluntary
    , ss.sub_month
    , hb.PROGRAM_ID_OR_VIEWABLE_ID as ckg_program_id
    , hb.CONTENT_MINUTES_WATCHED/60 as hours_viewed
from subs ss
join bolt_dai_ce_prod.gold.combined_video_stream hb
    on hb.WBD_MAX_PROFILE_ID = ss.profile_id
    AND hb.sub_id = ss.sub_id
where DATEDIFF(DAY, hb.request_date_pst, ss.cycle_expire_date) <= 60
    and DATEDIFF(DAY, hb.request_date_pst, ss.cycle_expire_date) >= 0
    and hb.request_date_pst::DATE between DATEADD(MONTH, -2, '{target_month}'::DATE) and '{target_month_end}'
    and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
    and hb.CONTENT_MINUTES_WATCHED >= 15
    and hb.video_type = 'main'
    and hb.territory = 'HBO MAX DOMESTIC'
)

select 
    '{target_month}' as expiration_month
    , s.user_id
    , s.profile_id
    , s.is_cancel
    , is_voluntary
    , s.sub_month
    , rad.ckg_series_id
    , rad.ckg_program_id
    , sum(s.hours_viewed) as hours_viewed
from streaming_subset s
left join bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad
     on s.ckg_program_id = rad.ckg_program_id
where rad.asset_type!='PROMOTION'
group by ALL

UNION

select
    '{target_month}' as expiration_month
    , s.user_id
    , s.profile_id
    , s.is_cancel
    , is_voluntary
    , s.sub_month
    , NULL AS ckg_series_id
    , NULL AS ckg_program_id
    , NULL AS hours_viewed
from subs s
where s.user_id not in (SELECT DISTINCT user_id FROM streaming_subset)
--)                                                           
'''

# COMMAND ----------

# spark.sql('TRUNCATE TABLE bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_svod_avod')

# COMMAND ----------

# target_month = '2023-08-01'
# target_month_end = (pd.to_datetime(target_month) + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d')
# table_name = target_month.replace('-', '')

# while target_month <= '2024-04-01':
#     target_month_end = (pd.to_datetime(target_month) + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d')
#     table_name = target_month.replace('-', '')

#     queries = sql.format(table_name = table_name,
#            target_month = target_month,
#            target_month_end = target_month_end
#            )
#     print(queries)
#     spark.sql(queries)
#     target_month = (pd.to_datetime(target_month) + pd.DateOffset(months=2)).strftime('%Y-%m-%d')
    

# COMMAND ----------

#### Viewership Generation Queries #########
# user_stream_60d_genpop_all_months = spark.sql('''
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_savod AS (
# with subs as(
# select c.user_id, c.hurley_user_id, c.sub_id, up.profile_id
# , is_cancel, sub_month, is_voluntary, sku
# , cycle_start_date
# , cycle_expire_date
# , date_trunc('MONTH', cycle_expire_date) as expiration_month
# from bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c 
# LEFT join bolt_dai_subs_prod.gold.max_profile_dim_current up
#     on c.user_id = up.USER_ID
# where up.default_profile_ind = True
# --LIMIT 3000000
# )

# , streaming_subset as
# (
# select
#     ss.expiration_month
#     , ss.user_id
#     , ss.hurley_user_id
#     , ss.profile_id
#     , ss.is_cancel
#     , is_voluntary
#     , sku
#     , ss.sub_month
#     , hb.PROGRAM_ID_OR_VIEWABLE_ID as ckg_program_id
#     , hb.CONTENT_MINUTES_WATCHED/60 as hours_viewed
# from subs ss
# left join bolt_dai_ce_prod.gold.combined_video_stream hb
#     on hb.WBD_MAX_PROFILE_ID = ss.profile_id
#     AND hb.sub_id = ss.sub_id
# where DATEDIFF(DAY, hb.request_date_pst, ss.cycle_expire_date) <= 60
#     and DATEDIFF(DAY, hb.request_date_pst, ss.cycle_expire_date) >= 0
#     -- and hb.request_date_pst between DATEADD(MONTH, -2, '{target_month}'::DATE) and '{target_month_end}'
#     and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
#     and hb.CONTENT_MINUTES_WATCHED >= 15
#     and hb.video_type = 'main'
#     and hb.territory = 'HBO MAX DOMESTIC'
# )

# select
#       s.expiration_month
#     , s.user_id
#     , s.profile_id
#     , s.is_cancel
#     , s.is_voluntary
#     , s.sku
#     , s.sub_month
#     , rad.ckg_series_id
#     , rad.ckg_program_id
#     , sum(s.hours_viewed) as hours_viewed
# from streaming_subset s
# left join bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad
#      on s.ckg_program_id = rad.ckg_program_id
# where rad.asset_type!='PROMOTION'
# group by ALL

# UNION

# select DISTINCT
#       ss.expiration_month
#     , s.user_id
#     , s.profile_id
#     , s.is_cancel
#     , s.is_voluntary
#     , s.sku
#     , s.sub_month
#     , NULL AS ckg_series_id
#     , NULL AS ckg_program_id
#     , NULL AS hours_viewed
# from subs s
# left join streaming_subset ss ON s.expiration_month = ss.expiration_month and s.user_id = ss.user_id
# where ss.user_id IS NULL
# )                                                           
# '''.format(table_name = table_name,
#            target_month = target_month,
#            target_month_end = target_month_end
#            ))

# COMMAND ----------

spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_20231001 LIMIT 10').show()

# COMMAND ----------

audience_segement = spark.sql('''
                              SELECT a.*, b.entertainment_segment_lifetime
                              FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_20231201 a
                              LEFT JOIN 
                                        (SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
                                         FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
                                         GROUP BY all
                                        ) b
                              ON a.profile_id = b.PROFILE_ID
                              ''')
# audience_segement = audience_segement.toPandas()

# COMMAND ----------

audience_segement.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.churn_user_stream60d_segmented_20231201")

# COMMAND ----------

user_stream_60d_genpop.show()

# COMMAND ----------

display(
    spark.sql('''
SELECT * FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_new_lib_series_level_202312 where user_id = '1681250818211238493'
          '''
          )
)

# COMMAND ----------


