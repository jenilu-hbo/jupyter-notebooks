# Databricks notebook source
import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hours and title counts check

# COMMAND ----------

hours_df = spark.sql('''
          SELECT date_trunc('month',a.request_date) as date_month
          ,seg.entertainment_segment_lifetime, sum(a.hours_viewed) as total_hours_viewed
          FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
          JOIN bolt_cus_dev.bronze.cip_user_title_hours_watched_season_wbd_avod_svod a
                ON a.profile_id = seg.PROFILE_ID
          WHERE a.request_date >= '2023-05-23'
          GROUP BY ALL

          UNION 
          
          SELECT date_trunc('month',a.request_date) as date_month
          ,seg.entertainment_segment_lifetime, sum(a.hours_viewed) as total_hours_viewed
          FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
          JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global b
                ON b.profile_id = seg.PROFILE_ID
          JOIN bolt_cus_dev.bronze.cip_user_title_hours_watched_season_legacy_avod_svod a 
                ON b.hurley_profile_id = a.hurley_profile_id
          WHERE a.request_date < '2023-05-23'
          GROUP BY ALL
          ''')

# COMMAND ----------

plot_data = hours_df.toPandas()

# COMMAND ----------

fig = px.scatter(plot_data, x="request_date", y="total_hours_viewed", color="entertainment_segment_lifetime",
                 )
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Subs Check

# COMMAND ----------

subs_df = spark.sql('''
          SELECT start_date, entertainment_segment_lifetime, subs
          FROM bolt_cus_dev.bronze.cip_user_title_hours_watched_subs_count_wbd_avod_svod
          WHERE DAYS_ON_HBO_MAX = 28
          AND start_date >= '2023-05-23'

          UNION 

          SELECT start_date, entertainment_segment_lifetime, subs
          FROM bolt_cus_dev.bronze.cip_user_title_hours_watched_subs_count_legacy_avod_svod
          WHERE DAYS_ON_HBO_MAX = 28
          AND start_date < '2023-05-23'
          ''')

# COMMAND ----------

plot_data = subs_df.toPandas()

# COMMAND ----------

fig = px.scatter(plot_data, x="start_date", y="subs", color="entertainment_segment_lifetime",
                 )
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the hours and title query

# COMMAND ----------

test_df = spark.sql('''
          SELECT seg.entertainment_segment_lifetime, sum(a.hours_viewed) as total_hours_viewed, count(DISTINCT a.user_id)
          FROM (
        SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
        FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
        GROUP BY all
    ) seg
          JOIN bolt_cus_dev.bronze.user_title_hours_watched_subs_wbd a
                ON a.profile_id = seg.PROFILE_ID
          WHERE a.request_date='2023-07-03'
          and seg.entertainment_segment_lifetime = 'Adult Animation Watchers'
          and a.ckg_series_id = 'ab553cdc-e15d-4597-b65f-bec9201fd2dd'
          GROUP BY ALL
          ''').show()

# COMMAND ----------



# COMMAND ----------


