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


hours_df_60 = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_30d').toPandas()

# COMMAND ----------

hours_df_60.head()

# COMMAND ----------

content_category = pd.read_csv(file_path + 'Content Strategy Category Tags.csv')
content_category.head()

# COMMAND ----------

content_category = content_category.rename(columns = {'1. Names and IDs CKG Series ID':'ckg_series_id',
                                                      'Content Strategy Category':'content_category'})

# COMMAND ----------

df = hours_df_60.merge(content_category[['ckg_series_id', 'content_category']], 
                       on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

content_pillar = pd.read_csv(file_path + '2024.07.03 - Past Titles Mapped to Pillar.csv')
content_pillar.head()

# COMMAND ----------

content_pillar = content_pillar.rename(columns = {'1. Names and IDs CKG Series ID':'ckg_series_id',
                                                      'ROI Pillar':'content_pillar',
                                                      'ROI Sub-Pillar':'content_sub_pillar'})

# COMMAND ----------

df = df.merge(content_pillar[['ckg_series_id', 'content_pillar', 'content_sub_pillar']], 
                       on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

title_info.loc[title_info['content_category'].str.contains('series'), 'content_category'] = 'series'
title_info.loc[title_info['content_category'].str.contains('live'), 'content_category'] = 'live'
title_info.loc[title_info['content_category']=='standalone', 'content_category'] = 'movie'

content_category_onehot = pd.get_dummies(title_info['content_category'], prefix='content_category')
title_series_info=pd.concat([title_info, content_category_onehot], axis = 1)

# COMMAND ----------


program_type_onehot = pd.get_dummies(title_series_info['program_type'], prefix='program_type')
title_series_info=pd.concat([title_series_info, program_type_onehot], axis = 1)

# COMMAND ----------

# title_series_info.loc[title_series_info['genre'] == 'Action', 'genre'] = 'action'
# title_series_info.loc[title_series_info['genre'] == 'Comedy', 'genre'] = 'comedy'
# title_series_info.loc[title_series_info['genre'] == 'Drama', 'genre'] = 'drama'
# title_series_info.loc[~title_series_info['genre'].isin(['action', 'comedy', 'drama']), 'genre'] = 'other'

genre_onehot = pd.get_dummies(title_series_info['genre'], prefix='genre')
title_series_info=pd.concat([title_series_info, genre_onehot], axis = 1)

# COMMAND ----------

# Medal Data
medal_dict = {'Silver':2, 'Bronze':3, 'Gold':1 , 'Platinum':0, 'None':'Bronze'}
title_series_info['medal'] = title_series_info['medal'].fillna('Bronze')
title_series_info['medal_number'] = title_series_info['medal'].replace(medal_dict)

# COMMAND ----------

title_series_info['is_new_content'] = 0

### Air date and Release date < 1 year
title_series_info.loc[(pd.to_datetime(title_series_info['offering_start_date'])-
                      pd.to_datetime(title_series_info['air_date']))/np.timedelta64(1, 'Y') <= 1,
                      'is_new_content']=1

# COMMAND ----------

title_series_info['age_of_content'] = (pd.to_datetime(title_series_info['offering_start_date'])-
                      pd.to_datetime(title_series_info['air_date']))/np.timedelta64(1, 'Y')

# COMMAND ----------

data = hours_df_60[['ckg_series_id', 'season_number','entertainment_segment_lifetime', 'percent_hours_viewed', 'percent_viewing_subs']].merge(
       title_series_info, on = ['ckg_series_id', 'season_number'])
data['pct_hours_viewed_per_runtime'] = data['percent_hours_viewed'].astype(float)/data['asset_run_time_hours'].astype(float)

# COMMAND ----------

data[data['offering_start_date'] == '2023-05-23']

# COMMAND ----------

data.columns = [i.replace(' ', '').replace('&', '_') for i in data.columns]
for i in data.columns:
    data[i] = data[i].astype(str)

# COMMAND ----------

# data.columns = [i.replace(' ', '').replace('&', '_') for i in data.columns]
data_df = spark.createDataFrame(data)
spark.sql('drop table bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_avod_svod')
data_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_avod_svod")

# COMMAND ----------

# MAGIC %md
# MAGIC Add Churn Curves

# COMMAND ----------

import matplotlib.pyplot as plt 
import seaborn as sns

# COMMAND ----------

META_COLS = ['title_name', 'title_id', 'season_number', 'offering_start_date', 'asset_run_time_hours']
FEATURE_COLS = ['is_pay_1', 'is_popcorn', 'is_new_content', 
                'medal_number', 'age_of_content', 
               'budget', 'content_category_live', 'content_category_movie',
               'content_category_series',
               'program_type_Acquired', 'program_type_Original', ''
               'genre_Action', 'genre_Adult Animation', 'genre_Adventure & Survival',
               'genre_Comedy', 'genre_Documentaries', 'genre_Drama', 'genre_Events',
               'genre_Fantasy & Sci-Fi', 'genre_Food & Home', 'genre_Horror', 'genre_Kids & Family'
               ,'genre_News', 'genre_Sports', 'entertainment_segment_lifetime'
               ]

# COMMAND ----------

for i in FEATURE_COLS:
    if i not in (['budget', 'entertainment_segment_lifetime', 'age_of_content']):
        try:
            data[i] = data[i].astype(int, errors='ignore')
        except Exception as e:
            print (i)
            print (e)
    

# COMMAND ----------


TARGET_COL = ['percent_viewing_subs'] #cumulative_hours_viewed

# COMMAND ----------

np.bool = np.bool_

# COMMAND ----------

plot_data=data[data['entertainment_segment_lifetime'] == 'Drama Originals Watchers'][FEATURE_COLS+TARGET_COL]
corr = plot_data.corr()[TARGET_COL]
mask=np.zeros_like(corr, dtype=np.bool)
corr.loc['dummy_value'] = -1
# corr.sort_values(by = TARGET_COL)

# COMMAND ----------

corr['abs_value'] = np.abs(corr[TARGET_COL])
corr.sort_values(by = 'abs_value', ascending=False).head(12)

# COMMAND ----------


f, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10, as_cmap=True),
            square=True, ax=ax)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

data.columns = [i.replace(' ', '').replace('&', '_') for i in data.columns]
data_df = spark.createDataFrame(data)
# spark.sql('drop table bolt_cus_dev.bronze.cip_title_segment_viewership_training_data')
# data_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_segment_viewership_training_data")

# COMMAND ----------


