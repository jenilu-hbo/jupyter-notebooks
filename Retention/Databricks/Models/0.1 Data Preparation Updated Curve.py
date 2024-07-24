# Databricks notebook source
import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd
import io
from scipy.optimize import curve_fit
# import snowflake.connector
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.8f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_30d
# WITH viewership AS (
#         SELECT coalesce(sub_id_max, sub_id_legacy) as sub_id
#               , title_name
#               , ckg_series_id
#               , season_number
#               , medal
#               , series_type
#               , primary_genre
#               , program_type
#               , season_window_start as offering_start_date
#               , datediff(DAY, season_window_start, request_date) as days_on_hbo_max
#               , mode(entertainment_segment_lifetime) as entertainment_segment_lifetime
#               , sum(hours_viewed) as hours_viewed
#         FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric hb
#         LEFT JOIN (SELECT DISTINCT user_id, profile_id                -- to get default profile id
#                   FROM bolt_dai_subs_prod.gold.max_profile_dim_current
#                   where default_profile_ind = true) mp
#               ON mp.user_id = hb.user_id
#         LEFT JOIN (SELECT DISTINCT hurley_user_id, hurley_profile_id   -- to get default legacy profile id
#                   FROM bolt_cus_dev.bronze.legacy_user_profile_dim_current
#                   where is_primary_profile = 1) lp 
#               ON lp.hurley_user_id = hb.hurley_user_id                 -- to map legacy profile id to max profile id
#         LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global mapping 
#               ON mapping.hurley_profile_id = lp.hurley_profile_id
#         LEFT JOIN  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
#               ON coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) = seg.profile_id
#         GROUP BY ALL
# )

# SELECT      title_name
#             , ckg_series_id 
#             , season_number
#             , offering_start_date
#             , medal
#             , series_type
#             , primary_genre
#             , program_type
#             , v.entertainment_segment_lifetime
#             , sum(v.hours_viewed) as hours_viewed
#             , count(distinct v.sub_id) as viewing_sub
#             , sum(v.hours_viewed)/denom.subs as percent_hours_viewed
#             , count(distinct v.sub_id)/denom.subs as percent_viewing_subs
# FROM viewership v
# JOIN bolt_cus_dev.bronze.cip_user_stream_subscription_base denom
#             ON v.offering_start_date::DATE = denom.start_date::DATE
#             AND v.entertainment_segment_lifetime = denom.entertainment_segment_lifetime
#         WHERE 1=1
#         AND v.days_on_hbo_max <=30
#         AND denom.days_on_hbo_max = 30
#         GROUP BY 1,2,3,4,5,6,7,8,9,subs

# COMMAND ----------


hours_df_30 = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_30d').toPandas()

# COMMAND ----------

hours_df_30.head()

# COMMAND ----------

content_category = pd.read_csv(file_path + 'Content Strategy Category Tags.csv')

# COMMAND ----------

content_category = content_category.rename(columns = {'1. Names and IDs CKG Series ID':'ckg_series_id', '1. Names and IDs Title Name (Series-Level)':'title_name', 'Content Strategy Category':'content_strategy_category'})
content_category.columns = [i.replace(" ", "_") for i in content_category.columns]

# COMMAND ----------

content_category.head()

# COMMAND ----------

content_category_df = spark.createDataFrame(content_category)
content_category_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bolt_cus_dev.bronze.cip_content_strategy_category_tags")

# COMMAND ----------

df = hours_df_30.merge(content_category[['ckg_series_id', 'content_category']], 
                       on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

content_pillar = pd.read_csv(file_path + '2024.07.18 - Latest Pillar Mapping to Past Titles.csv')

# COMMAND ----------

content_pillar = content_pillar.rename(columns = {'1. Names and IDs CKG Series ID':'ckg_series_id',
                                                  '1. Names and IDs Title Name (Series-Level)':'title_name',
                                                      'ROI Pillar':'content_pillar',
                                                      'ROI Sub-Pillar':'content_sub_pillar'})
content_pillar.columns = [i.replace(" ", "_") for i in content_pillar.columns]

# COMMAND ----------

content_pillar.head()

# COMMAND ----------

content_pillar = content_pillar[(content_pillar['content_sub_pillar'].notnull())
                                &(content_pillar['content_sub_pillar']!= '0')].copy()
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Flagship'), 'rank'] = 1
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Engagement'), 'rank'] = 2
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Segment'), 'rank'] = 3
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('New'), 'rank'] = 4
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Library'), 'rank'] = 5
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Experimentation'), 'rank'] = 6
content_pillar.loc[content_pillar['Pillar_x_Content_Strategy_Category'].str.contains('Brand Halo'), 'rank'] = 7

# COMMAND ----------

content_pillar.head()

# COMMAND ----------

content_pillar_df = spark.createDataFrame(content_pillar)
content_pillar_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bolt_cus_dev.bronze.cip_content_pillar_designation_for_past_titles")

# COMMAND ----------

df = df.merge(content_pillar[['ckg_series_id', 'content_pillar', 'content_sub_pillar']], 
                       on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

content_churn_curve = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_churn_curve_parameters_v2').toPandas()
content_churn_curve = content_churn_curve[content_churn_curve['agg_level'] == 'new']

# COMMAND ----------

content_churn_curve.head() #0.01168905	-0.44522673	0.0411347

# COMMAND ----------

df = df.merge(content_churn_curve, on='entertainment_segment_lifetime', how='left')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

df.loc[df['series_type'].str.contains('live'), 'series_type'] = 'livesports'
content_category_onehot = pd.get_dummies(df['series_type'], prefix='series_type')
title_series_info=pd.concat([df, content_category_onehot], axis = 1)

# COMMAND ----------

program_type_onehot = pd.get_dummies(title_series_info['program_type'], prefix='program_type')
title_series_info=pd.concat([title_series_info, program_type_onehot], axis = 1)

# COMMAND ----------

# genre_onehot = pd.get_dummies(title_series_info['genre'], prefix='genre')
# title_series_info=pd.concat([title_series_info, genre_onehot], axis = 1)

# COMMAND ----------

# Medal Data
title_series_info.loc[title_series_info['medal'].isin(['Direct-to-MAX', 'WB Pay-1 | Short Window']), 'medal'] = 'Gold'
title_series_info.loc[title_series_info['title_name'] == 'House of the Dragon'] = 'Platinum'
medal_dict = {'Silver':2, 'Bronze':3, 'Gold':1 , 'Platinum':0, 'None':'Bronze'}
title_series_info['medal'] = title_series_info['medal'].fillna('Bronze')
title_series_info['medal_number'] = title_series_info['medal'].replace(medal_dict)

# COMMAND ----------

# title_series_info['age_of_content'] = (pd.to_datetime(title_series_info['offering_start_date'])-
#                       pd.to_datetime(title_series_info['air_date']))/np.timedelta64(1, 'Y')

# COMMAND ----------

title_series_info['offering_start_date'] = title_series_info['offering_start_date'].astype(str)

# COMMAND ----------

data_df = spark.createDataFrame(title_series_info)
# spark.sql('drop table bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_avod_svod')
# data_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_20240710")
data_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_20240715")
