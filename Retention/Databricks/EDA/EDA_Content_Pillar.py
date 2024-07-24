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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_content_pillar_by_segments
# MAGIC with viewership as (
# MAGIC SELECT
# MAGIC  expiration_month
# MAGIC , hb.ckg_series_id
# MAGIC , hb.title_name
# MAGIC , pillar.Pillar_x_Content_Strategy_Category as content_pillar
# MAGIC , pillar.rank as pillar_rank
# MAGIC , case when is_recent = 1 then 'new' else 'library' end as new_library
# MAGIC , entertainment_segment_lifetime
# MAGIC , count(distinct coalesce(sub_id_max, sub_id_legacy)) as sub_count
# MAGIC FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_us hb
# MAGIC LEFT JOIN (SELECT DISTINCT user_id, profile_id                -- to get default profile id
# MAGIC           FROM bolt_dai_subs_prod.gold.max_profile_dim_current
# MAGIC           where default_profile_ind = true) mp
# MAGIC       ON mp.user_id = hb.user_id
# MAGIC LEFT JOIN (SELECT DISTINCT hurley_user_id, hurley_profile_id   -- to get default legacy profile id
# MAGIC            FROM bolt_cus_dev.bronze.legacy_user_profile_dim_current
# MAGIC            where is_primary_profile = 1) lp 
# MAGIC       ON lp.hurley_user_id = hb.hurley_user_id                 -- to map legacy profile id to max profile id
# MAGIC LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global mapping 
# MAGIC       ON mapping.hurley_profile_id = lp.hurley_profile_id
# MAGIC LEFT JOIN  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
# MAGIC       ON coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) = seg.profile_id
# MAGIC LEFT JOIN bolt_cus_dev.bronze.cip_content_pillar_designation_for_past_titles pillar
# MAGIC       ON hb.ckg_series_id = pillar.ckg_series_id
# MAGIC WHERE 1=1
# MAGIC GROUP BY ALL
# MAGIC )
# MAGIC
# MAGIC SELECT v.*, denom.subs as denom_subs, sub_count/denom.subs as percent_viewing_subs
# MAGIC FROM viewership v
# MAGIC JOIN bolt_cus_dev.bronze.cip_user_stream_subscription_base_by_segments denom
# MAGIC             ON DATEADD(MONTH, -1, v.expiration_month::DATE)::DATE = denom.start_date::DATE
# MAGIC             AND v.entertainment_segment_lifetime = denom.entertainment_segment_lifetime
# MAGIC             AND denom.days_on_hbo_max = 30
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_content_pillar
# MAGIC with viewership as (
# MAGIC SELECT
# MAGIC  expiration_month
# MAGIC , hb.ckg_series_id
# MAGIC , hb.title_name
# MAGIC , pillar.Pillar_x_Content_Strategy_Category as content_pillar
# MAGIC , pillar.rank as pillar_rank
# MAGIC , case when is_recent = 1 then 'new' else 'library' end as new_library
# MAGIC , count(distinct coalesce(sub_id_max, sub_id_legacy)) as sub_count
# MAGIC FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_us hb
# MAGIC LEFT JOIN bolt_cus_dev.bronze.cip_content_pillar_designation_for_past_titles pillar
# MAGIC       ON hb.ckg_series_id = pillar.ckg_series_id
# MAGIC WHERE 1=1
# MAGIC GROUP BY ALL
# MAGIC )
# MAGIC
# MAGIC SELECT v.*, denom.subs as denom_subs, sub_count/denom.subs as percent_viewing_subs
# MAGIC FROM viewership v
# MAGIC JOIN (SELECT expiration_month, count(distinct coalesce(sub_id_max, sub_id_legacy)) as subs
# MAGIC      FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_us
# MAGIC      GROUP BY ALL) denom
# MAGIC      ON v.expiration_month::DATE = denom.expiration_month::DATE

# COMMAND ----------

content_viewership = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_content_pillar').toPandas()

# COMMAND ----------

content_viewership = content_viewership[content_viewership['content_pillar']!='0'].copy() 

# COMMAND ----------

content_viewership_average = content_viewership.groupby(['expiration_month', 'content_pillar', 'pillar_rank'])['percent_viewing_subs'].mean().reset_index()

# COMMAND ----------

content_churn_curve = spark.sql('''SELECT * FROM bolt_cus_dev.bronze.cip_churn_curve_parameters_v2
                                WHERE entertainment_segment_lifetime='genpop'
                                ''').toPandas()

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

# COMMAND ----------

content_viewership[(content_viewership['expiration_month'].astype(str) == '2023-06-01')
                        &(content_viewership['content_pillar'] == 'Flagship Anchor - Comedy Series (OP)')]

# COMMAND ----------

content_viewership_final = content_viewership.groupby(['expiration_month', 'content_pillar', 'pillar_rank','denom_subs', 'new_library',])\
    .agg({'percent_viewing_subs':'sum', 'ckg_series_id':'count'}).reset_index()

# COMMAND ----------

content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].replace('None', np.nan)
content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].astype('float')
content_viewership_final = content_viewership_final.merge(content_churn_curve, left_on = ['new_library'], right_on = ['agg_level'])

# COMMAND ----------

content_viewership_final['average churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay(0, x.a, x.b, x.c), axis = 1)
content_viewership_final['new churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay((x['percent_viewing_subs']),x.a, x.b, x.c), axis = 1)
content_viewership_final['absolute churn reduction'] = content_viewership_final['average churn']  - content_viewership_final['new churn']
content_viewership_final['number of users impacted'] = content_viewership_final['absolute churn reduction']*content_viewership_final['denom_subs']

# COMMAND ----------

content_viewership_final[(content_viewership_final['agg_level']=='new')
                        &(content_viewership_final['content_pillar'] == 'Flagship Anchor - Comedy Series (OP)')] # Only sex and city

# COMMAND ----------

content_viewership_final.head()

# COMMAND ----------

content_viewership_final = content_viewership_final.groupby(['expiration_month','content_pillar', 'pillar_rank', 'entertainment_segment_lifetime' ])[['absolute churn reduction', 'number of users impacted', 'ckg_series_id']].sum()

# COMMAND ----------

content_viewership_final = content_viewership_average.merge(content_viewership_final, on = ['expiration_month','content_pillar', 'pillar_rank', ])

# COMMAND ----------

content_viewership_final.to_csv('content_viewership_final.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # By Segments

# COMMAND ----------

content_viewership = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_content_pillar_by_segments').toPandas()

# COMMAND ----------

content_viewership = content_viewership[content_viewership['content_pillar']!='0'].copy()

# COMMAND ----------

content_viewership.head()

# COMMAND ----------

# content_viewership.to_csv('content_viewership.csv')

# COMMAND ----------

content_viewership_average = content_viewership.groupby(['expiration_month', 'content_pillar', 'pillar_rank', 'entertainment_segment_lifetime'])['percent_viewing_subs'].mean().reset_index()

# COMMAND ----------

content_churn_curve = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_churn_curve_parameters_v2').toPandas()

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

# COMMAND ----------

content_viewership_final = content_viewership.groupby(['expiration_month', 'content_pillar', 'pillar_rank', 'entertainment_segment_lifetime','denom_subs', 'new_library',])[['percent_viewing_subs']].sum().reset_index()

# COMMAND ----------

content_viewership[(content_viewership['expiration_month'].astype(str) == '2023-06-01')
                        &(content_viewership['content_pillar'] == 'Flagship Anchor - Comedy Series (OP)')
                        &(content_viewership['entertainment_segment_lifetime'] == 'Adult Animation Watchers')]

# COMMAND ----------

content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].replace('None', np.nan)
content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].astype('float')
content_viewership_final = content_viewership_final.merge(content_churn_curve, left_on = ['entertainment_segment_lifetime', 'new_library'], right_on = ['entertainment_segment_lifetime','agg_level'])

# COMMAND ----------

content_viewership_final['average churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay(0, x.a, x.b, x.c), axis = 1)
content_viewership_final['new churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay((x['percent_viewing_subs']),x.a, x.b, x.c), axis = 1)
content_viewership_final['absolute churn reduction'] = content_viewership_final['average churn']  - content_viewership_final['new churn']
content_viewership_final['number of users impacted'] = content_viewership_final['absolute churn reduction']*content_viewership_final['denom_subs']

# COMMAND ----------

content_viewership_final.head()

# COMMAND ----------

content_viewership_final = content_viewership_final.groupby(['expiration_month','content_pillar', 'pillar_rank', 'entertainment_segment_lifetime' ])[['absolute churn reduction', 'number of users impacted']].sum()

# COMMAND ----------

content_viewership_final = content_viewership_average.merge(content_viewership_final, on = ['expiration_month','content_pillar', 'pillar_rank', 'entertainment_segment_lifetime' ])

# COMMAND ----------

content_viewership_final.to_csv('content_viewership_final.csv')

# COMMAND ----------


