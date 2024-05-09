# Databricks notebook source
# !pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd

import snowflake.connector
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format

# COMMAND ----------

from utils_sf import run_query

# COMMAND ----------

run_query('SELECT * FROM max_prod.content_analytics.cumulative_content_viewership_pst LIMIT 10')

# COMMAND ----------

data = read_from_s3('Cannibalization Analysis.csv')
data_ca_title = read_from_s3('cannibalization_analysis_detailed_view.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # Causal

# COMMAND ----------

!pip install cdt
!pip install torch

# COMMAND ----------

import cdt
import networkx as nx
import matplotlib.pyplot as plt

# COMMAND ----------

data['medal_number'] = data['medal_number'].fillna(3)

# COMMAND ----------

df = data[['ahvr', 'medal_Platinum_count', 'medal_same_count', 'medal_number']]

# COMMAND ----------

df_msc_avg = df.groupby(['medal_number'])['medal_same_count'].mean().reset_index().rename(columns = {'medal_same_count':'medal_same_count_avg'})
df = df.merge(df_msc_avg, on = ['medal_number'])
df['medal_same_count_bigger_than_average'] = 0
df.loc[df['medal_same_count'] > df['medal_same_count_avg'], 'medal_same_count_bigger_than_average'] = 1

# COMMAND ----------

df_msc_avg = df.groupby(['medal_number'])['medal_Platinum_count'].mean().reset_index().rename(columns = {'medal_Platinum_count':'medal_Platinum_count_avg'})
df = df.merge(df_msc_avg, on = ['medal_number'])
df['medal_Platinum_count_bigger_than_average'] = 0
df.loc[df['medal_Platinum_count'] > df['medal_Platinum_count_avg'], 'medal_Platinum_count_bigger_than_average'] = 1

# COMMAND ----------

# ### Setting different threshold
# df['medal_Platinum_count_avg'] = 0.98
# df['medal_Platinum_count_bigger_than_average'] = 0
# df.loc[df['medal_Platinum_count'] > df['medal_Platinum_count_avg'], 'medal_Platinum_count_bigger_than_average'] = 1

# COMMAND ----------

df.tail()

# COMMAND ----------

# Get skeleton graph
# initialize graph lasso
glasso = cdt.independence.graph.Glasso()
# apply graph lasso to data
skeleton = glasso.predict(df)
# visualize network
fig = plt.figure(figsize=(15,10))
nx.draw_networkx(skeleton, font_size=18, font_color='r')

# COMMAND ----------

# # Use causal discovery to get causal models
# # PC algorithm
# model_pc = cdt.causality.graph.PC()
# graph_pc = model_pc.predict(df, skeleton)

# # visualize network
# fig=plt.figure(figsize=(15,10))
# nx.draw_networkx(graph_pc, font_size=18, font_color='r')

# COMMAND ----------

!pip install econml
!pip install dowhy

# COMMAND ----------

import econml
import dowhy
from dowhy import CausalModel

# COMMAND ----------

df.columns

# COMMAND ----------

G = nx.DiGraph()
G.add_nodes_from(['ahvr', 'medal_Platinum_count', 'medal_same_count', 'medal_number'])
G.add_edges_from([
                  ("medal_number", "ahvr"), 
                  ("medal_number", "medal_same_count"), 
                  ("medal_number", "medal_Platinum_count"), 
                  ("medal_same_count", "medal_same_count_bigger_than_average"), 
                  ("medal_Platinum_count", "medal_Platinum_count_bigger_than_average"), 
                  ("medal_same_count_bigger_than_average", "ahvr"), 
                  ("medal_Platinum_count_bigger_than_average", "ahvr")
                ])
# G.add_edges_from([
#                   ("medal_number", "ahvr"), 
# #                   ("medal_number", "medal_same_count"), 
# #                   ("medal_number", "medal_Platinum_count"), 
# #                   ("medal_same_count", "medal_same_count_bigger_than_average"), 
# #                   ("medal_Platinum_count", "medal_Platinum_count_bigger_than_average"), 
#                   ("medal_same_count_bigger_than_average", "ahvr"), 
#                   ("medal_Platinum_count_bigger_than_average", "ahvr")
#                 ])

# COMMAND ----------

graph = nx.generate_gml(G)
graph = ''.join(graph)

# COMMAND ----------

# With graph
model=CausalModel(
        data = df,
        treatment="medal_Platinum_count_bigger_than_average",
        outcome="ahvr",
        graph = graph
        )

# COMMAND ----------

model.view_model() 

# COMMAND ----------

# Generate estimand
identified_estimand= model.identify_effect(proceed_when_unidentifiable=True)
print(identified_estimand)

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# Compute causal effect using metalearner
identified_estimand_experiment = model.identify_effect(proceed_when_unidentifiable=True)

from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import TweedieRegressor

metalearner_estimate = model.estimate_effect(identified_estimand_experiment,                             
                                             method_name="backdoor.econml.metalearners.TLearner",
                                             confidence_intervals=False,
                                             method_params={
                                                         "init_params":{'models': RandomForestRegressor()},
                                                         "fit_params":{}
                                                                  })
print(metalearner_estimate)

# COMMAND ----------

# Print histogram of causal effects
plt.hist(metalearner_estimate.cate_estimates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## By Tier

# COMMAND ----------

# MAGIC %md
# MAGIC ### Platinum

# COMMAND ----------

# With graph
model=CausalModel(
        data = df[df['medal_number'] == 0],
        treatment="medal_Platinum_count_bigger_than_average",
        outcome="ahvr",
        graph = graph
        )

# COMMAND ----------

identified_estimand_experiment = model.identify_effect(proceed_when_unidentifiable=True)

metalearner_estimate = model.estimate_effect(identified_estimand_experiment,                             
                                             method_name="backdoor.econml.metalearners.TLearner",
                                             confidence_intervals=False,
                                             method_params={
                                                         "init_params":{'models': TweedieRegressor()},
                                                         "fit_params":{}
                                                                  })
print(metalearner_estimate)

# COMMAND ----------

# Print histogram of causal effects
plt.hist(metalearner_estimate.cate_estimates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

df_gold = df[df['medal_number'] == 1]
# df_gold['medal_same_count_avg'] = 3 # 2.78
df_gold['medal_Platinum_count_bigger_than_average'] = 0
df_gold.loc[df_gold['medal_Platinum_count'] > df_gold['medal_Platinum_count_avg'], 'medal_Platinum_count_bigger_than_average'] = 1

# COMMAND ----------

df_gold.head()

# COMMAND ----------

# With graph
model=CausalModel(
        data = df_gold,
        treatment="medal_Platinum_count_bigger_than_average",
        outcome="ahvr",
        graph = graph
        )

# COMMAND ----------

identified_estimand_experiment = model.identify_effect(proceed_when_unidentifiable=True)

metalearner_estimate = model.estimate_effect(identified_estimand_experiment,                             
                                             method_name="backdoor.econml.metalearners.TLearner",
                                             confidence_intervals=False,
                                             method_params={
                                                         "init_params":{'models': RandomForestRegressor()},
                                                         "fit_params":{}
                                                                  })
print(metalearner_estimate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# With graph
model=CausalModel(
        data = df[df['medal_number'] == 2],
        treatment="medal_Platinum_count_bigger_than_average",
        outcome="ahvr",
        graph = graph
        )

# COMMAND ----------

df[df['medal_number'] == 2].head()

# COMMAND ----------

identified_estimand_experiment = model.identify_effect(proceed_when_unidentifiable=True)

metalearner_estimate = model.estimate_effect(identified_estimand_experiment,                             
                                             method_name="backdoor.econml.metalearners.TLearner",
                                             confidence_intervals=False,
                                             method_params={
                                                         "init_params":{'models': RandomForestRegressor()},
                                                         "fit_params":{}
                                                                  })
print(metalearner_estimate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze

# COMMAND ----------

# With graph
model=CausalModel(
        data = df[df['medal_number'] >= 2],
        treatment="medal_Platinum_count_bigger_than_average",
        outcome="ahvr",
        graph = graph
        )

# COMMAND ----------

identified_estimand_experiment = model.identify_effect(proceed_when_unidentifiable=True)

metalearner_estimate = model.estimate_effect(identified_estimand_experiment,                             
                                             method_name="backdoor.econml.metalearners.TLearner",
                                             confidence_intervals=False,
                                             method_params={
                                                         "init_params":{'models': RandomForestRegressor()},
                                                         "fit_params":{}
                                                                  })
print(metalearner_estimate)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# !pip install causalinference

# COMMAND ----------

from causalinference import CausalModel
from causalinference.utils import random_data
#Y is the outcome, D is treatment status, and X is the independent variable
causal = CausalModel(df['ahvr'], 
                     df['medal_Platinum_count_bigger_than_average'], 
                     df[['medal_number', 'medal_Platinum_count_bigger_than_average', 'medal_same_count_bigger_than_average', 
                         'medal_Platinum_count', 'medal_same_count']])

# COMMAND ----------

print(causal.summary_stats)

# COMMAND ----------

causal.est_via_ols()
print(causal.estimates)

# COMMAND ----------

# ATE, ATC, and ATT stand for Average Treatment Effect, Average Treatment Effect for Control and Average Treatment Effect for Treated, respectively. 
# Using this information, we could assess whether the treatment has an effect compared to the control.

# COMMAND ----------


