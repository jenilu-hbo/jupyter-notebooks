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

# spark.sql('''
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.calendar_date_tracker
# SELECT explode(sequence(DATE'2020-05-01', DATE'2024-12-01', INTERVAL 1 DAY)) as start_date,
#        explode(sequence(start_date::DATE, DATE'2024-12-01', INTERVAL 1 DAY)) as end_date,
#        DATEDIFF(DAY, start_date, end_date::DATE) as DAYS_ON_HBO_MAX
#           ''').show()

# COMMAND ----------

# spark.sql('''
#          CREATE OR REPLACE TABLE bolt_cus_dev.bronze.user_title_hours_watched_subs_count_legacy (
#         start_date STRING
#         , end_date STRING
#         , DAYS_ON_HBO_MAX INT
#         , entertainment_segment_lifetime  STRING
#         , subs BIGINT
#          ) 
#           ''')

# COMMAND ----------

qry = '''
        INSERT INTO bolt_cus_dev.bronze.{table}
        
        SELECT dt.start_date 
        , dt.end_date
        , DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
        , entertainment_segment_lifetime
        , count(distinct c.hurley_user_id) as subs
        FROM bolt_cus_dev.bronze.calendar_date_tracker dt
        JOIN bolt_cus_dev.bronze.legacy_user_profile_dim_current up 
        JOIN bolt_cus_dev.bronze.user_retain_churn_list_test c
            on c.hurley_user_id = up.hurley_user_id
            and c.cycle_expire_date >= dt.start_date
            and c.cycle_start_date <= dt.end_date
        JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global map
                ON map.hurley_profile_id = up.hurley_profile_id
        LEFT JOIN (SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
                FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
                GROUP BY all
        ) seg
                ON seg.profile_id = map.profile_id
        WHERE END_DATE <= LEAST('{end_date}', current_date()::STRING)
        and START_DATE = '{start_date}'
        AND MOD(DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE), 30) = 0
        --AND (DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 60 OR DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 30)
        and up.IS_PRIMARY_PROFILE=1
        GROUP BY all
                '''

# COMMAND ----------

table = 'user_title_hours_watched_subs_count_legacy'
t =  pd.to_datetime('2020-05-01')

while (t>=pd.to_datetime('2020-05-01') and t<pd.to_datetime('2021-01-01')):
    print (t)
    query = qry.format(
                    start_date= t.strftime('%Y-%m-%d'),
                    end_date = (t + pd.DateOffset(days=365)).strftime('%Y-%m-%d') , 
                    table = table
                    )
    print (query)
    df = spark.sql(query)

    t=t+ pd.DateOffset(days=1)
    # break

# COMMAND ----------


