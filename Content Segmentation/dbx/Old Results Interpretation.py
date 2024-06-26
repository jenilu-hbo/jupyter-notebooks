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

clustering_results = pd.read_csv('clustering_results.csv')

# COMMAND ----------

clustering_results = clustering_results[['consolidated_id', 'acquiring_power', 'cluster']].rename(columns = {'consolidated_id':'title_id'})

# COMMAND ----------

id_mapping = spark.sql('''
                       SELECT DISTINCT LEGACY_HBOMAX_SERIES_VIEWABLE_ID AS title_id,
                       SERIES_TITLE_LONG as title_name
                       FROM bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined
                       ''').toPandas()

# COMMAND ----------

clustering_results = clustering_results[['title_id', 'cluster', 'acquiring_power']].merge(id_mapping, on = ['title_id'])

# COMMAND ----------

clustering_results.head()

# COMMAND ----------

clustering_results[clustering_results['title_id'].isnull()]

# COMMAND ----------

clustering_results.to_csv('final_clustering_results.csv')

# COMMAND ----------


