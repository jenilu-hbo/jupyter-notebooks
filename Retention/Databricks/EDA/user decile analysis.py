# Databricks notebook source
# !pip install snowflake-connector-python
# !pip install sympy
# dbutils.library.restartPython()

# COMMAND ----------

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
pd.options.display.float_format = '{:,.3f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------

expiration_month = '2023-12-01'
df_60_00 = spark.sql('''
     WITH new_library AS (
      SELECT s.*
                     , CASE WHEN r.recency_window_end >= expiration_month
                        THEN 'current'
                        ELSE 'library'
                        END AS old_new
      FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_savod s
      left join bolt_cus_dev.bronze.cip_recency_title_series_level_offering_table r
         on s.ckg_program_id = r.ckg_program_id
      where s.expiration_month = '{expiration_month}'
      )

      SELECT s.user_id ::STRING
            , s.profile_id ::STRING
            --, max(is_cancel) AS is_cancel
            --, max(is_voluntary) as is_voluntary
            , is_cancel
            , is_voluntary
            , sku
            , mode(seg.entertainment_segment_lifetime) as entertainment_segment_lifetime
            , min(sub_month)::STRING as sub_month
            , sum(hours_viewed)::STRING as hours_viewed 
            , count(distinct ckg_series_id) as titles_viewed
            , count(distinct (CASE WHEN old_new = 'current' THEN ckg_series_id else null END))  as new_titles_viewed
            , count(distinct (CASE WHEN old_new = 'library' THEN ckg_series_id else null END))  as library_titles_viewed
            , case when is_voluntary = 1 and is_cancel = 1 then 1 else 0 end as is_cancel_vol
            , case when is_voluntary = 0 and is_cancel = 1 then 1 else 0 end as is_cancel_invol
      FROM new_library s
      LEFT JOIN bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
                        on seg.PROFILE_ID = s.profile_id
      GROUP BY ALL
'''.format(expiration_month = expiration_month)
                     ).toPandas()

# COMMAND ----------

#### Problem yet to be resolved??? Why a profile has 2 different sub_month? ########
#### Example user_id = 1680197285172035360

# COMMAND ----------


from scipy.stats import percentileofscore


def get_df_test(df_test, metric_cols):
    df_test['tenure_months'] = df_test['sub_month']
    for i in metric_cols:
        df_test['monthly_'+i] = np.where(df_test['tenure_months']>1, df_test[i]/2, df_test[i])
    df_test['monthly_title_viewed'] = df_test['monthly_titles_viewed'] 
    user_total = df_test.groupby(['user_id'])['monthly_title_viewed'].transform('sum')
    df_test['frc'] = df_test['monthly_title_viewed'] / user_total

    # df_test = df_test[df_test.tenure_months>1]
    df_test = df_test.fillna(0)
    return(df_test)

def get_df_60_h(list_df):
    df_list=[]
    num=0
    for df_test in list_df:
        df_test['num_df'] = num
        df_list.append(df_test)
        num=num+1
    return(df_list)

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

def exponential_decay_slope(x, a, b):
    return a * b*np.exp(b * x)

def fit_exponential(x_data, y_data, p0, param_bounds):
    x_fit = np.linspace(0, x_data.max(), 100)   
    params, _ = curve_fit(exponential_decay, np.array(x_data), y_data, p0, bounds=param_bounds)
    return x_fit, params


def get_equal_churn_bin(df_in, grpby):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    # nbins = int(df.monthly_title_viewed.max())
    # df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], np.linspace(0,nbins,2*nbins+1))
    
    bins =[-0.01]
    bins = bins + np.arange(0, 12.5, 0.5).tolist()
    bins = bins + np.arange(13, 17, 1.0).tolist()
    bins = bins + [19.0, df.monthly_title_viewed.max()]

    df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], bins,include_lowest =True)
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_title_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    return(df_bin)

def get_churn_bin(df_in, grpby, nbins = 100):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    df['title_viewed_bin_bucket'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,nbins), duplicates='drop')    
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket'] +grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    return(df_bin)

# COMMAND ----------

metric_cols = ['hours_viewed', 'titles_viewed', 'new_titles_viewed', 'library_titles_viewed']
for m in metric_cols:
    df_60_00[m] = df_60_00[m].astype(float)
df_60_00['sub_month'] = df_60_00['sub_month'].astype(int)

# COMMAND ----------

df_60_00=get_df_test(df_60_00, metric_cols)
df_list = get_df_60_h([df_60_00]) #, df_60_0, df_60_1, df_60_2])
df_60 = pd.concat(df_list)
df_60.head()

# COMMAND ----------

df_60 = df_60[df_60['sub_month']>1].copy()
df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60['sub_month'] = df_60['sub_month'].astype('int')

# COMMAND ----------

df_60.user_id.nunique() #10678914

# COMMAND ----------

df_t = df_60.groupby(by=['user_id','sub_month'] +['is_cancel']).sum().reset_index()
df_t[df_t['titles_viewed']<5].groupby(['titles_viewed'])[['user_id']].nunique()

# COMMAND ----------

df = df_60.copy()#df_60[df_60['titles_viewed']>0]

# COMMAND ----------

display(df[df['titles_viewed']<=5].groupby(['titles_viewed'])[['user_id']].nunique())

# COMMAND ----------

bins_titles =[0.4999, 0.5, 1, 1.5, 2.5, 3, 4, 5, 7, 10.5, 189.5]
bins_hours = [0.124, 1.089, 2.352, 3.961, 6.065, 8.881, 12.793, 18.678, 28.601, 49.727, 2038.859]

# COMMAND ----------

df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], bins_titles,include_lowest =True)
df['hours_watched_bin_bucket'] = pd.cut(df['monthly_hours_viewed'], bins_hours,include_lowest =True)


# COMMAND ----------

# df['title_viewed_bin_bucket'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,11), duplicates='drop')  
# df['hours_watched_bin_bucket'] = pd.qcut(df['monthly_hours_viewed'], np.linspace(0,1,11), duplicates='drop')  

df['title_viewed_bin'] = df['title_viewed_bin_bucket'].apply(lambda x: x.right)
df['hours_watched_bin'] = df['hours_watched_bin_bucket'].apply(lambda x: x.right)


df['title_viewed_decile_rank'] = df['title_viewed_bin'].rank(method='dense', ascending=True)
df['hours_watched_decile_rank'] = df['hours_watched_bin'].rank(method='dense', ascending=True)

# COMMAND ----------

df.hours_watched_decile_rank.max()

# COMMAND ----------

# MAGIC %md
# MAGIC # Statistics

# COMMAND ----------

df.is_cancel.unique()

# COMMAND ----------

df_group = df.groupby(['title_viewed_bin_bucket', 'title_viewed_bin', 'title_viewed_decile_rank', 'hours_watched_bin_bucket', 'hours_watched_bin' , 'hours_watched_decile_rank']).agg({'is_cancel':'mean', 'user_id':'count','is_cancel_vol':'mean',}).reset_index()

df_group['composition'] = df_group['user_id']/df.user_id.count()

df_group = df_group[df_group['user_id']>0]

# COMMAND ----------

df.groupby(['title_viewed_decile_rank'])[['is_cancel_vol']].mean()

# COMMAND ----------

df.groupby(['hours_watched_decile_rank'])[['is_cancel_vol']].mean()

# COMMAND ----------

df_group.to_csv(file_path+'df_group.csv')

# COMMAND ----------


