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

content_viewership = spark.sql('''
          SELECT * FROM bolt_cus_dev.bronze.cip_title_segment_viewership_training_data_avod_svod
          ''').toPandas()

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------

content_viewership.head()

# COMMAND ----------

content_viewership[content_viewership['offering_start_date'] == '2023-05-23']

# COMMAND ----------

content_viewership_genpop = content_viewership[[i for i in content_viewership.columns if i not in ['entertainment_segment_lifetime', 'percent_hours_viewed', 'percent_viewing_subs', 'pct_hours_viewed_per_runtime']]].drop_duplicates()

# COMMAND ----------

content_viewership_genpop[(content_viewership_genpop['ckg_series_id'] == '0016010d-5220-4bfc-89b0-511b48b5b77a')
                          &(content_viewership_genpop['season_number'] == '0')]

# COMMAND ----------

content_viewership_genpop['entertainment_segment_lifetime'] = 'gen_pop'
content_viewership_genpop['percent_hours_viewed'] = np.nan
content_viewership_genpop['percent_viewing_subs'] = content_viewership_genpop['percent_cumulative_viewing_subscribers']
content_viewership_genpop['pct_hours_viewed_per_runtime'] = np.nan

# COMMAND ----------

content_viewership = pd.concat([content_viewership, content_viewership_genpop[content_viewership.columns]], axis=0)

# COMMAND ----------

content_churn_curve = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_churn_curve_parameters').toPandas()

# COMMAND ----------

segment_info = spark.sql('''
SELECT a.entertainment_segment_lifetime, count(distinct a.user_id) as user_count
FROM  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table a
join bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max b
ON a.user_id = b.user_id 
where cycle_expire_date between '2024-01-01' and '2024-01-31'
group by all
order by user_count desc        
                  ''').toPandas()

# COMMAND ----------

genpop_info = spark.sql('''
SELECT 'gen_pop' as entertainment_segment_lifetime
, count(distinct user_id) as user_count
FROM  bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max
where cycle_expire_date between '2024-01-01' and '2024-01-31'
group by all
order by user_count desc        
                  ''').toPandas()

# COMMAND ----------

segment_info = pd.concat([segment_info, genpop_info], axis = 0)

# COMMAND ----------

content_churn_curve = content_churn_curve.merge(segment_info, on = ['entertainment_segment_lifetime'])

# COMMAND ----------

content_churn_curve[content_churn_curve['entertainment_segment_lifetime'] == 'gen_pop']

# COMMAND ----------

content_category = pd.read_csv(file_path + 'Content Strategy Category Tags.csv')
content_category.head()

# COMMAND ----------

content_category = content_category.rename(columns= {'1. Names and IDs CKG Series ID':'ckg_series_id'})

# COMMAND ----------

content_viewership = content_viewership.merge(content_churn_curve
                                              , on = ['entertainment_segment_lifetime'])

# COMMAND ----------

content_viewership_final = content_viewership.merge(content_category[['ckg_series_id', 'Content Strategy Category', 'Observed Medal', 
                                                                      'Performance Medal', 'Reporting Brand', 'Reporting Net Studio', 'Reporting Franchise']],
                                                    how = 'left')

# COMMAND ----------

content_viewership_final.head()

# COMMAND ----------

content_viewership_final[content_viewership_final['entertainment_segment_lifetime'] == 'gen_pop']

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

# COMMAND ----------

content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].replace('None', np.nan)
content_viewership_final['percent_viewing_subs'] = content_viewership_final['percent_viewing_subs'].astype('float')
content_viewership_final['mean'] = content_viewership_final['mean'].astype('float')

# COMMAND ----------

content_viewership_final['average churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay(x['mean'], x.a, x.b, x.c), axis = 1)
content_viewership_final['new churn'] = content_viewership_final.apply(lambda x: 
    exponential_decay((x['mean']+x['percent_viewing_subs']),x.a, x.b, x.c), axis = 1)
content_viewership_final['absolute churn reduction'] = content_viewership_final['average churn']  - content_viewership_final['new churn']
content_viewership_final['number of users impacted'] = content_viewership_final['absolute churn reduction']*content_viewership_final['user_count']

# COMMAND ----------

content_viewership_final['production_cost'] = content_viewership_final['production_cost'].astype(float)
content_viewership_final['budget'] =content_viewership_final['budget'].replace('None', np.nan)
content_viewership_final['budget'] = content_viewership_final['budget'].astype(float)
content_viewership_final['cost'] = content_viewership_final['production_cost'].combine_first(content_viewership_final.budget)
content_viewership_final[['ckg_series_id', 'production_cost', 'budget', 'cost']].isnull().sum()/len(content_viewership_final)

# COMMAND ----------

# content_viewership_final_df = spark.createDataFrame(content_viewership_final)
# content_viewership_final_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_content_viewership_churn_20240603")

# COMMAND ----------

# content_viewership_final.to_csv(file_path + 'content_viewership_final.csv')

# COMMAND ----------

# !pip install boto3
import boto3

bucket_name = "dcp-cd-cus-data-us-east-1-dev"
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)

def read_from_s3(filename, input_bucket = bucket_name):
    for obj in bucket.objects.filter(Prefix='content_data_science/'+filename):
            key = obj.key 
            body = obj.get()['Body']
            print('Reading {0} features'.format(key))
            df = pd.read_csv(body, na_values = [r'\\\\N'])
    return df

def write_to_sf(df, file_name):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index = False)
    content = csv_buffer.getvalue()
    filename = 'content_data_science/cip/{}.csv'.format(file_name)
    client = boto3.client('s3')
    client.put_object(Bucket=bucket_name, Key=filename, Body=content)


# COMMAND ----------

write_to_sf(content_viewership_final, 'content_viewership_final_20240607')

# COMMAND ----------


