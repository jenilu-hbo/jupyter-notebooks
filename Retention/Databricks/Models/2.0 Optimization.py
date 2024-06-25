# Databricks notebook source
# MAGIC %md
# MAGIC # Research Python
# MAGIC ####### Pacakge that can be used:
# MAGIC ####### - Scipy SLSQP https://stackoverflow.com/questions/21765794/python-constrained-non-linear-optimization, https://paulminogue.com/posts/a0d8c837-a40d-4b17-9d30-e0bd36a6befc
# MAGIC ####### - Scipy Fsolver https://www.geeksforgeeks.org/how-to-solve-a-pair-of-nonlinear-equations-using-python/
# MAGIC ####### - Gekko https://stackoverflow.com/questions/52905072/integer-decision-variable-in-non-linear-programming, https://scicomp.stackexchange.com/questions/19870/python-solvers-for-mixed-integer-nonlinear-constrained-optimization, https://apmonitor.com/wiki/index.php/Main/IntegerBinaryVariables
# MAGIC ####### pyipopt https://github.com/xuy/pyipopt
# MAGIC ####### CVXPY https://www.cvxpy.org/examples/#basic

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
pd.options.display.float_format = '{:,.4f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

# MAGIC %md
# MAGIC # 0 - Get data

# COMMAND ----------

### 1. Cost data

# COMMAND ----------

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

content_viewership_final = read_from_s3('content_viewership_final_20240607')

# COMMAND ----------

# content_viewership_final[content_viewership_final['offering_start_date']=='2023-05-23']

# COMMAND ----------

content_viewership_final.rename(columns = {'Content Strategy Category':'content_strategy_category'}, inplace=True)

content_viewership_final = content_viewership_final[(content_viewership_final['title_name']!= 'Quiet on Set: The Dark Side of Kids TV')
                                                    &(content_viewership_final['agg_level'] == 'new')
                                                    # &(content_viewership_final['entertainment_segment_lifetime']!='gen_pop')
                                                    &(~content_viewership_final['content_strategy_category'].isin(['News', 'Sports']))
                                                    &(~content_viewership_final['title_name'].isin(['Walker: Independence', 'Sesame Street']))
                                                    &(content_viewership_final['offering_start_date']!='2023-05-23')
                                                    ].copy()

# COMMAND ----------

content_cost = pd.read_csv(file_path+'content_cost.csv')
content_cost = content_cost.rename(columns = {'TITLE_ID':'LEGACY_ID', 'SEASON_NUMBER':'season_number'})
content_cost = content_cost[content_cost['LEGACY_ID'].notnull()]

# COMMAND ----------

id_mapping = spark.sql(
'''
SELECT distinct LEGACY_ID, MAX_ID as ckg_series_id
FROM bolt_cus_dev.silver.forecasting_rad 
where LEGACY_ID is not null
''').toPandas()

# COMMAND ----------

content_cost[content_cost['TITLE'] == 'The Last of Us']

# COMMAND ----------

content_viewership_final = content_viewership_final.merge(id_mapping,on = ['ckg_series_id'], how = 'left')

content_viewership_final = content_viewership_final.merge(content_cost[['LEGACY_ID', 'season_number', 'CONTENT_COST']]
                                                          , on = ['LEGACY_ID', 'season_number'], how = 'left')

content_viewership_final = content_viewership_final.merge(content_cost[['LEGACY_ID', 'season_number', 'CONTENT_COST']]\
                                                          .rename(columns = {'CONTENT_COST':'content_cost_2',
                                                                             'LEGACY_ID':'ckg_series_id'})
                                                          , on = ['ckg_series_id', 'season_number'], how = 'left')

# COMMAND ----------

content_cost_series = content_cost.groupby(['LEGACY_ID'])[['CONTENT_COST']].mean().reset_index()
content_cost_series = content_cost_series.rename(columns = {'CONTENT_COST':'content_cost_series'})

content_viewership_final = content_viewership_final.merge(content_cost_series, on = ['LEGACY_ID'], how = 'left')
content_viewership_final = content_viewership_final.merge(content_cost_series.rename(columns = \
                                                            {'content_cost_series':'content_cost_series_2','LEGACY_ID':'ckg_series_id'})
                                                          , on = ['ckg_series_id'], how = 'left')

# COMMAND ----------

content_viewership_final['content_cost'] = content_viewership_final.CONTENT_COST.combine_first(content_viewership_final.content_cost_2).combine_first(content_viewership_final.content_cost_series).combine_first(content_viewership_final.content_cost_series_2)#.combine_first(content_viewership_final.cost)

# COMMAND ----------

content_viewership_final['content_cost'] = content_viewership_final['content_cost'].astype(float)

# COMMAND ----------

content_viewership_final[(content_viewership_final['entertainment_segment_lifetime'] == 'gen_pop')
                        # &(content_viewership_final['content_strategy_category'] == 'Adult Animation Series (Acq)')
                        #  &(content_viewership_final['medal'] == 'Bronze')
                        #  &(content_viewership_final['content_cost'].notnull())
                        # &(content_viewership_final['title_name'] == 'Avatar')
                         ]\
                         [['ckg_series_id', 'season_number', 'title_name', 'content_strategy_category', 'medal','content_cost']].sort_values(by = ['ckg_series_id']).to_csv('content_cost_details.csv')

# COMMAND ----------

content_viewership_final = content_viewership_final[['ckg_series_id', 'season_number', 'title_name', 'entertainment_segment_lifetime', 'percent_viewing_subs', 'medal', 'content_strategy_category', 'content_cost', 'user_count', 'a', 'b', 'c', 'mean']]

metadata_cols = ['ckg_series_id', 'season_number', 'title_name', 'entertainment_segment_lifetime', 'content_strategy_category', 'medal']
for i in content_viewership_final.columns:
    if i not in metadata_cols:
        content_viewership_final[i] = content_viewership_final[i].astype(float)

# COMMAND ----------

####### CREATE SEGMENT LEVEL INFO TABLE #############
seg_viewership = content_viewership_final[content_viewership_final['entertainment_segment_lifetime']!='gen_pop']\
    .groupby(['entertainment_segment_lifetime', 'medal', 'content_strategy_category'])[['percent_viewing_subs']].mean().reset_index()

seg_table = content_viewership_final[content_viewership_final['entertainment_segment_lifetime']!='gen_pop']\
    .groupby(['entertainment_segment_lifetime'])[['a', 'b', 'c', 'mean', 'user_count']].mean().reset_index()

# COMMAND ----------

content_viewership_final = pd.concat([content_viewership_final, content_viewership_final['content_strategy_category'].str.split("(", expand=True).rename(columns = {1:'op_acq'})], axis = 1)

# COMMAND ----------

content_viewership_final['op_acq'] = content_viewership_final['op_acq'].fillna('None')

# COMMAND ----------

########## CREATE COST TABLE ################
cost_table = content_viewership_final[content_viewership_final['entertainment_segment_lifetime']=='gen_pop']\
    .groupby([ 'content_strategy_category', 'medal', 'op_acq'])[['percent_viewing_subs', 'content_cost']].median().reset_index()

cost_medal = content_viewership_final[content_viewership_final['entertainment_segment_lifetime']=='gen_pop']\
    .groupby([ 'medal', 'op_acq'])[['content_cost']].median().reset_index()
cost_medal.rename(columns={'content_cost':'medal_cost'}, inplace = True)

cost_table = cost_table.merge(cost_medal, on = ['medal', 'op_acq'])

cost_table['content_cost'] = cost_table['content_cost'].combine_first(cost_table.medal_cost)
cost_table = cost_table.sort_values(by = ['content_strategy_category', 'medal', ])

# COMMAND ----------

cost_table.sort_values(by = ['content_strategy_category', 'medal', ]).to_csv('cost_table.csv')
seg_table.to_csv(file_path+'seg_table.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimize using Gekko

# COMMAND ----------

!pip install --upgrade gekko
!sudo chmod -R a+rwx /local_disk0/.ephemeral_nfs

# COMMAND ----------

import threading
import time, random
from gekko import GEKKO
from math import e

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * pow(e, (b * x)) + c

# COMMAND ----------

def geometric_progression_sum(n, q):
    return (1-pow(q, n))/(1-q)

# COMMAND ----------

budget = 4.5*1000000000     #/18 ####/

# COMMAND ----------


m.cleanup()
n = len(cost_table)
m = GEKKO(remote=True,server='https://byu.apmonitor.com')
m.solver_options = ['minlp_gap_tol 1.0e-4',\
                    'minlp_maximum_iterations 15000',\
                    'minlp_max_iter_with_int_sol 15000',
                    'minlp_gap_tol 0.01'
                    ]

x = m.Array(m.Var,n,lb=1, integer=True)

for i in range(n):
    if i == 2: # Silver Adult Animation
        x[i] = m.Array(m.Var,1,lb=1, ub = 5, integer=True)[0]
    if i == 20: # Platinum Drama Series (OP)
        x[i] = m.Array(m.Var,1,lb=1, ub = 3, integer=True)[0]
    elif i == 26:  # Platinum Movies (Acq)
        x[i] = m.Array(m.Var,1,lb=1, ub = 10, integer=True)[0]
    # else:


cost_table['variable'] = x

# COMMAND ----------

viewership = cost_table[['content_strategy_category', 'medal', 'variable']]\
            .merge(seg_viewership, on = ['content_strategy_category', 'medal'])
viewership['decay'] = viewership.apply(lambda x: geometric_progression_sum(x.variable, 0.9), axis = 1)
viewership['total_viewership'] = viewership['percent_viewing_subs'] * viewership['decay'] #*viewership['decay']

p = [0 for i in range(len(viewership))]
for i in range(len(viewership)):
    p[i] = m.Intermediate(viewership.total_viewership.values[i])
viewership['intermedia_p'] = p

viewership_total = viewership[['entertainment_segment_lifetime', 'intermedia_p']].rename(columns = 
                            {'intermedia_p':'total_viewership'})\
                    .groupby(['entertainment_segment_lifetime'])[['total_viewership']].sum().reset_index()


viewership_churn = viewership_total.merge(seg_table, on = ['entertainment_segment_lifetime'])

p = [0 for i in range(len(viewership_churn))]
for i in range(len(viewership_churn)):
    p[i] = m.Intermediate(viewership_churn.total_viewership.values[i])
viewership_churn['intermedia_p'] = p


viewership_churn['mean'] = viewership_churn['mean'].astype(float)
viewership_churn['start_point'] = viewership_churn.apply(lambda x: exponential_decay(x['mean'],x.a,x.b,x.c), axis = 1)
viewership_churn['churn_new'] = viewership_churn.apply(lambda x: exponential_decay(x.intermedia_p,
                                                                                   x.a,x.b,x.c), axis = 1)
viewership_churn['abs_churn_reduction'] = viewership_churn['start_point'] - viewership_churn['churn_new'] 
viewership_churn['no_users_impacted'] = viewership_churn['abs_churn_reduction']*viewership_churn['user_count']

viewership_total_sum = m.Intermediate(viewership_churn.total_viewership.sum())
abs_churn_reduction = m.Intermediate(viewership_churn.abs_churn_reduction.sum())
no_users_impacted = m.Intermediate(viewership_churn.no_users_impacted.sum())
total_cost = m.Intermediate((cost_table['content_cost']*cost_table['variable']).sum())

# COMMAND ----------

m.Equation(total_cost <= budget)
m.Maximize(no_users_impacted)
m.options.SOLVER = 1
m.solve(debug=0)

# COMMAND ----------

df = pd.DataFrame([x[i].value for i in range(n)])
df[0] = df[0].astype(int)
df.to_csv('final_results.csv')
# df

# COMMAND ----------

df

# COMMAND ----------

print(f'Objective: {0-m.options.objfcnval:.2f}')

# COMMAND ----------


