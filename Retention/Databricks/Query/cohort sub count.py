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

qry = '''
        INSERT INTO bolt_cus_dev.bronze.{table}
        
        SELECT dt.start_date 
        , dt.end_date
        , DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
        , entertainment_segment_lifetime
        , count(distinct c.user_id) as subs
        FROM bolt_cus_dev.bronze.calendar_date_tracker dt
        JOIN bolt_dai_subs_prod.gold.max_profile_dim_current up 
        JOIN bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c
            on c.user_id = up.user_id
            and c.cycle_expire_date >= dt.start_date
            and c.cycle_start_date <= dt.end_date
        LEFT JOIN (SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
                                         FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
                                         GROUP BY all
                                        ) seg
                ON seg.profile_id = up.profile_id
        WHERE END_DATE <= LEAST('{end_date}', current_date()::STRING)
        and START_DATE = '{start_date}'
        AND up.default_profile_ind=True
        AND MOD(DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE), 30) = 0
        --AND (DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 60 OR DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 30)
        GROUP BY all
                '''

# COMMAND ----------

# spark.sql('''
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.calendar_date_tracker
# SELECT explode(sequence(DATE'2020-05-01', DATE'2024-12-01', INTERVAL 1 DAY)) as start_date,
#        explode(sequence(start_date::DATE, DATE'2024-12-01', INTERVAL 1 DAY)) as end_date,
#        DATEDIFF(DAY, start_date, end_date::DATE) as DAYS_ON_HBO_MAX
#           ''').show()

# COMMAND ----------

# spark.sql('''
#          CREATE OR REPLACE TABLE bolt_cus_dev.bronze.user_title_hours_watched_subs_count_wbd (
#         start_date STRING
#         , end_date STRING
#         , DAYS_ON_HBO_MAX INT
#         , entertainment_segment_lifetime  STRING
#         , subs BIGINT
#          ) 
#           ''')

# COMMAND ----------

table = 'user_title_hours_watched_subs_count_wbd'
t =  pd.to_datetime('2023-05-01')

# COMMAND ----------

while (t>=pd.to_datetime('2020-05-01') and t<pd.to_datetime('2023-05-23')):
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

spark.sql('''
          SELECT * FROM bolt_cus_dev.bronze.user_title_hours_watched_subs_count_wbd WHERE START_DATE = END_DATE
          ''').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_title_hours_watched_subs_count_wbd_avod_svod (
# MAGIC         
# MAGIC         SELECT dt.start_date 
# MAGIC         , dt.end_date
# MAGIC         , DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
# MAGIC         , entertainment_segment_lifetime
# MAGIC         , count(distinct c.user_id) as subs
# MAGIC         FROM bolt_cus_dev.bronze.calendar_date_tracker dt
# MAGIC         JOIN bolt_dai_subs_prod.gold.max_profile_dim_current up 
# MAGIC         JOIN bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c
# MAGIC             on c.user_id = up.user_id
# MAGIC             and c.cycle_expire_date >= dt.start_date
# MAGIC             and c.cycle_start_date <= dt.end_date
# MAGIC         LEFT JOIN (SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
# MAGIC                                          FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
# MAGIC                                          GROUP BY all
# MAGIC                                         ) seg
# MAGIC                 ON seg.profile_id = up.profile_id
# MAGIC         WHERE 1=1
# MAGIC         and START_DATE >= '2023-05-23'
# MAGIC         AND up.default_profile_ind=True
# MAGIC         AND (DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 28
# MAGIC             or DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 60
# MAGIC             or DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 90
# MAGIC         )
# MAGIC         GROUP BY all
# MAGIC )
