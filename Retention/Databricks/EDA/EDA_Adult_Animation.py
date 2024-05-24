# Databricks notebook source
content_viewership = spark.sql('''
          SELECT *
          FROM bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_60d t
          LEFT JOIN bolt_cus_dev.bronze.cip_title_series_level_metadata m
          ON t.ckg_series_id = m.ckg_series_id
          and t.season_number = m.season_number
          ''').toPandas()

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------

content_viewership.head()

# COMMAND ----------

content_viewership[content_viewership['primary_genre'] == 'Adult Animation'].ckg_series_id.nunique()

# COMMAND ----------

plot_data = content_viewership[content_viewership['primary_genre'] == 'Adult Animation']\
            .groupby(['entertainment_segment_lifetime']).mean().reset_index().sort_values(by = ['percent_viewing_subs'], ascending = False)
fig = px.scatter(plot_data,x='entertainment_segment_lifetime', y='percent_viewing_subs', 
                 title='average Adult Animation 60-Day percent view',
                 width=1000, height=400)
fig.show()

# COMMAND ----------

content_viewership[(content_viewership['primary_genre'] == 'Adult Animation')
                   &(content_viewership['entertainment_segment_lifetime'] == 'DC Superfans')].sort_values(by=['percent_viewing_subs'], ascending = False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


