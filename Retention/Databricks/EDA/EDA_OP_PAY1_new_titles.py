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

engagement = spark.sql('''SELECT * 
                       FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_US
                       where expiration_month = '2023-12-01'
                       ''').toPandas()

# COMMAND ----------

content_category = pd.read_csv(file_path + 'Content Strategy Category Tags.csv')
content_category = content_category.rename(columns = {'1. Names and IDs CKG Series ID':'ckg_series_id',
                                                      'Content Strategy Category':'content_category'})
content_category.head()

# COMMAND ----------

content_category_df = spark.createDataFrame(content_category[['ckg_series_id', 'content_category']])
content_category_df.write.mode('overwrite').saveAsTable('bolt_cus_dev.bronze.cip_content_strategy_category_tags')

# COMMAND ----------

content_category.content_category.unique()

# COMMAND ----------

engagement = engagement.merge(content_category[['ckg_series_id', 'content_category']], 
                       on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

engagement.head()

# COMMAND ----------

engagement.
