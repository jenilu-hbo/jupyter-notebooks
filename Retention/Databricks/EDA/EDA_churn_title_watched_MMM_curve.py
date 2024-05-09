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
pd.options.display.float_format = '{:,.2f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------

df_60_00 = pd.read_parquet(file_path+'churn_user_stream60d_segmented_20240103.parquet')

# COMMAND ----------

def get_df_test(df_test):
    df_test['tenure_months'] = df_test['sub_month']
    df_test['monthly_title_viewed'] = np.where(df_test['tenure_months']>1, df_test['titles_viewed']/2, df_test['titles_viewed'])
    df_test['monthly_hours_viewed'] = np.where(df_test['tenure_months']>1, df_test['hours_viewed']/2, df_test['hours_viewed'])
    user_total = df_test.groupby(['user_id'])['monthly_title_viewed'].transform('sum')
    df_test['frc'] = df_test['monthly_title_viewed'] / user_total
    
    df_test['program_type'] = np.where((df_test.program_type=='original') & (df_test.old_new=='library'), 'acquired', df_test.program_type)
    df_test = df_test[df_test.tenure_months>2]
    df_test = df_test.fillna(0)
    return(df_test)

# COMMAND ----------

df_60_00=get_df_test(df_60_00)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

# COMMAND ----------

def hill_function_decay(x, k, b):
    return x/((x + k)**b)

# def exponential_decay_slope(x, a, b):
#     return a * b*np.exp(b * x)

def fit_exponential(x_data, y_data):
    x_fit = np.linspace(0, x_data.max(), 100)   
    params, _ = curve_fit(hill_function_decay, np.array(x_data), y_data)
    return x_fit, params


def get_equal_churn_bin(df_in, grpby):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    nbins = int(df.monthly_title_viewed.max())
    df['title_viewed_bin'] = pd.cut(df['monthly_title_viewed'], 
                                    np.linspace(0,nbins,2*nbins+1))
    df['title_viewed_bin'] = df['title_viewed_bin'].apply(lambda x: x.right)
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_title_viewed':'sum'}).reset_index()
    return(df_bin)

def get_churn_bin(df_in, grpby):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    nbins = 100
    df['title_viewed_bin'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,nbins), duplicates='drop')
    df['title_viewed_bin'] = df['title_viewed_bin'].apply(lambda x: (x.left+x.right)/2)
    df['title_viewed_bin'] = df['title_viewed_bin'].astype('float')
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    return(df_bin)

# COMMAND ----------

df_60 = df_60_00.copy()
df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, [])

# COMMAND ----------

df_60_s.head()

# COMMAND ----------

param_bounds = ([0, 1], [1, np.inf])
p0 = [0.5, 0.01] 

# COMMAND ----------

def hill_function(x, a, b, bounds = param_bounds):
    return (a+b-1)/(a+b+x-1)
popt, pcov = curve_fit(hill_function, df_60_s.title_viewed_bin, df_60_s.churn)

# COMMAND ----------

popt

# COMMAND ----------

x_fit = np.linspace(0, df_60_s.title_viewed_bin.max(), 100)   
y_fit = hill_function(x_fit, *popt)

# COMMAND ----------

plt.plot(df_60_s.title_viewed_bin, df_60_s.churn, 'b-', label='data')
# plt.plot(x_fit, y_fit, 'r-',)
plt.legend()
plt.show()

# COMMAND ----------


