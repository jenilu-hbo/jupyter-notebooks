# Databricks notebook source
title_info = spark.sql('''
with meta_data as (
    SELECT
    rad.series_title_long as  title_name
    , rad.ckg_series_id
    , COALESCE(rad.season_number, 0) as season_number
    , rad.SERIES_TYPE AS content_category
    , rad.PRIMARY_GENRE
    , rad.program_type
    , rad.EVER_POPCORN_TITLE AS is_popcorn
    , rad.IMDB_SERIES_ID as imdb_id
    , CASE WHEN THEATRICAL_RELEASE_DATE IS NOT NULL THEN 1 ELSE 0 END AS is_pay_1
    , sum(ASSET_RUN_TIME/3600.00) as ASSET_RUN_TIME_HOURS
     FROM bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined rad
     where asset_type = 'FEATURE'
     and rad.PROGRAM_ID_SOURCE not in ('HBO_MAX')
     and series_title_long not null
     GROUP BY ALL)


, offerings as (
    SELECT 
    rad.ckg_series_id
    , COALESCE(rad.season_number, 0) as season_number
    , MIN(COALESCE(SEASON_FIRST_OFFERED_DATE, TITLE_FIRST_OFFERED_DATE)) AS offering_start_date
    , MIN(AIR_DATE) AS air_date
    FROM bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_dim_combined rad
    LEFT JOIN bolt_dcp_brd_prod.gold.content_metadata_reporting_asset_offering_dim_combined aod
    ON aod.CKG_PROGRAM_ID = rad.CKG_PROGRAM_ID
     where asset_type = 'FEATURE'
     and aod.COUNTRY_CODE = 'US'
    GROUP BY ALL

)

, budget as (
    SELECT title_id as imdb_id, max(bdg.AMOUNT) as budget
    FROM bolt_dai_ckg_prod.gold.imdb_boxoffice_title_budget bdg
    GROUP BY ALL
)

,offerings_by_date as (
select
     meta_data.title_name 
    , meta_data.ckg_series_id
    , meta_data.season_number
    , meta_data.content_category
    , meta_data.program_type
    , meta_data.primary_genre
    , meta_data.is_popcorn
    , meta_data.is_pay_1
    , meta_data.asset_run_time_hours
    , meta_data.imdb_id
    , case when to_date(offerings.offering_start_date) < '2020-05-27' THEN to_date('2020-05-27')
           ELSE to_date(offerings.offering_start_date)
      end as offering_start_date
    , air_date
    , budget.budget AS budget 
FROM meta_data
JOIN offerings 
    on meta_data.ckg_series_id = offerings.ckg_series_id and meta_data.season_number = offerings.season_number
LEFT JOIN budget 
    on budget.imdb_id = meta_data.imdb_id
WHERE 1=1
-- offerings.OFFERING_START_DATE > '2020-05-01'
-- and CURRENT_DATE() between (OFFERING_START_DATE) and (OFFERING_END_DATE)
GROUP BY ALL
)

SELECT * FROM offerings_by_date
''').toPandas()

# COMMAND ----------

medal = spark.sql('''
                  WITH viewership AS (
                  SELECT DISTINCT
                  WBD_MAX_SERIES_ID_OR_HBOMAX_TITLE_ID as ckg_series_id
                  , COALESCE(c.season_number, 0) as season_number
                  , MAX(c.cumulative_viewing_subscribers/p.viewing_subscribers*100) as percent_cumulative_viewing_subscribers
                  , MAX(c.cumulative_subscription_first_views/p.subscription_first_views*100) as percent_cumulative_first_views
                  FROM bolt_dai_ce_prod.gold.cumulative_content_viewership_pst c
                  JOIN bolt_cus_dev.bronze.cumulative_platform_viewership_pst p
                    ON date_format(c.OFFERING_START_DATE_PST, 'yyyy-MM-dd')= p.start_date::DATE
                    and c.geo_value = p.geo_value
                    and c.days_on_hbo_max_and_wbd_max = p.days_since_release
                  WHERE 1 = 1
                  and title_level = 'Seasons and Movies'
                  and c.geo_value = 'HBO MAX DOMESTIC'
                  and offering_window_num = 1 
                  and days_on_hbo_max_and_wbd_max = 28
                  GROUP BY ALL
                  )
                
                  SELECT ckg_series_id
                  , season_number
                  , case when percent_cumulative_viewing_subscribers > 20 or percent_cumulative_first_views > 10 then 'Platinum'
                        when percent_cumulative_viewing_subscribers > 10 or percent_cumulative_first_views > 5 then 'Gold'
                        when percent_cumulative_viewing_subscribers > 5 or percent_cumulative_first_views > 2.5 then 'Silver'
                        else 'Bronze'
                        end as observed_medal
                  FROM viewership
                  ''').toPandas()

# COMMAND ----------

medal['season_number'] = medal['season_number'].astype(int)
title_info['season_number'] = title_info['season_number'].astype(int)
title_info = title_info.merge(medal, on = ['ckg_series_id', 'season_number'], how = 'left')

# COMMAND ----------

# title_info_df = spark.createDataFrame(title_info)
# spark.sql('DROP TABLE bolt_cus_dev.bronze.cip_title_series_level_metadata')
# title_info_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_series_level_metadata")

# COMMAND ----------

title_info = spark.sql('''
                       SELECT *
                       FROM bolt_cus_dev.bronze.cip_title_series_level_metadata
                       ''').toPandas()

# COMMAND ----------

### SANITY CHECK ####
title_test = title_info.groupby(['ckg_series_id', 'season_number']).count()
title_test[title_test['title_name'] == 2]

# COMMAND ----------

spark.sql('''SELECT * FROM 
          bolt_dai_subs_prod.gold.max_legacy_user_mapping_global
          LIMIT 10''')

# COMMAND ----------

# ###### union the viewership first and then aggregate #######
# hours_df_60 = spark.sql('''
# WITH viewership AS (
#         --------- POST_MAX LAUNCH -------------------------
#         SELECT m.ckg_series_id
#             , COALESCE(m.season_number, 0) as season_number
#             , m.offering_start_date
#             , h.user_id
#             , h.profile_id
#             , seg.entertainment_segment_lifetime
#             , h.request_date
#             , date_diff(DAY, m.offering_start_date, h.request_date) AS days_on_hbo_max
#             , h.hours_viewed
#         FROM bolt_cus_dev.bronze.cip_title_series_level_metadata m
#         JOIN bolt_cus_dev.bronze.user_title_hours_watched_season_wbd h 
#             ON m.ckg_series_id = h.ckg_series_id
#             AND COALESCE(m.season_number, 0) = COALESCE(h.season_number, 0)
#         JOIN bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
#             ON seg.profile_id = h.profile_id
#         WHERE 1=1
#         AND h.request_date::DATE >= '2023-05-23'

#         UNION 

#         --------- PRE_MAX LAUNCH -------------------------
#         SELECT m.ckg_series_id
#             , COALESCE(m.season_number, 0) as season_number
#             , m.offering_start_date
#             , COALESCE(u_map.user_id, h.hurley_user_id) as user_id
#             , COALESCE(p_map.profile_id, h.hurley_profile_id) as profile_id
#             , seg.entertainment_segment_lifetime
#             , h.request_date
#             , date_diff(DAY, m.offering_start_date, h.request_date) AS days_on_hbo_max
#             , h.hours_viewed
#         FROM bolt_cus_dev.bronze.cip_title_series_level_metadata m
#         JOIN bolt_cus_dev.bronze.user_title_hours_watched_season_legacy h 
#             ON m.ckg_series_id = h.ckg_series_id
#             AND COALESCE(m.season_number, 0) = COALESCE(h.season_number, 0)
#             AND date_diff(DAY, m.offering_start_date, h.request_date) < 60
#         LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global p_map 
#             ON h.hurley_profile_id::STRING = p_map.HURLEY_PROFILE_ID::STRING
#         LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_user_mapping_global u_map
#             ON h.hurley_user_id::STRING = u_map.hurley_user_id::STRING
#         JOIN bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
#             ON seg.profile_id = p_map.profile_id
#         WHERE 1=1
#         AND h.request_date::DATE < '2023-05-23'
# )
# ,denom AS (
#     SELECT *
#     FROM bolt_cus_dev.bronze.user_title_hours_watched_subs_count_wbd
#     WHERE start_date >= '2023-05-23'
#     UNION
#     SELECT *
#     FROM bolt_cus_dev.bronze.user_title_hours_watched_subs_count_legacy
#     WHERE start_date < '2023-05-23'
# )

# SELECT      ckg_series_id 
#             , season_number
#             , offering_start_date
#             , v.entertainment_segment_lifetime
#             , sum(v.hours_viewed) as hours_viewed
#             , count(distinct v.user_id) as viewing_sub
#             , sum(v.hours_viewed)/denom.subs as percent_hours_viewed
#             , viewing_sub/denom.subs as percent_viewing_subs
# FROM viewership v
# JOIN denom
#             ON v.offering_start_date::DATE = denom.start_date::DATE
#             AND V.entertainment_segment_lifetime = denom.entertainment_segment_lifetime
#         WHERE 1=1
#         AND v.days_on_hbo_max < 60
#         AND denom.days_on_hbo_max = 60
#         GROUP BY 1, 2, 3, 4, subs
# ''')

# # hours_df_60.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_60d")

# COMMAND ----------


hours_df_60 = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_title_hours_watched_season_segment_agg_60d').toPandas()

# COMMAND ----------

hours_df_60.offering_start_date.min()

# COMMAND ----------

title_info[title_info['ckg_series_id'] == '93ba22b1-833e-47ba-ae94-8ee7b9eefa9a']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

title_info = title_info.rename(columns = {'observed_medal':'medal', 'primary_genre':'genre'})
title_info = title_info[title_info['title_name'].notnull()]

# COMMAND ----------


title_info[['asset_run_time_hours', 'content_category', 'program_type', 'air_date',
                   'medal', 'genre', 'is_pay_1', 'is_popcorn', 'budget']]\
.isnull().sum()/len(title_info)

# COMMAND ----------

title_info.loc[title_info['content_category'].str.contains('series'), 'content_category'] = 'series'
title_info.loc[title_info['content_category'].str.contains('live'), 'content_category'] = 'live'
title_info.loc[title_info['content_category']=='standalone', 'content_category'] = 'movie'

content_category_onehot = pd.get_dummies(title_info['content_category'], prefix='content_category')
title_series_info=pd.concat([title_info, content_category_onehot], axis = 1)

# COMMAND ----------


program_type_onehot = pd.get_dummies(title_series_info['program_type'], prefix='program_type')
title_series_info=pd.concat([title_series_info, program_type_onehot], axis = 1)

# COMMAND ----------

# title_series_info.loc[title_series_info['genre'] == 'Action', 'genre'] = 'action'
# title_series_info.loc[title_series_info['genre'] == 'Comedy', 'genre'] = 'comedy'
# title_series_info.loc[title_series_info['genre'] == 'Drama', 'genre'] = 'drama'
# title_series_info.loc[~title_series_info['genre'].isin(['action', 'comedy', 'drama']), 'genre'] = 'other'

genre_onehot = pd.get_dummies(title_series_info['genre'], prefix='genre')
title_series_info=pd.concat([title_series_info, genre_onehot], axis = 1)

# COMMAND ----------

# Medal Data
medal_dict = {'Silver':2, 'Bronze':3, 'Gold':1 , 'Platinum':0, 'None':np.NaN}
title_series_info['medal_number'] = title_series_info['medal'].replace(medal_dict)

# COMMAND ----------

title_series_info['is_new_content'] = 0

### Air date and Release date < 1 year
title_series_info.loc[(pd.to_datetime(title_series_info['offering_start_date'])-
                      pd.to_datetime(title_series_info['air_date']))/np.timedelta64(1, 'Y') <= 1,
                      'is_new_content']=1

# COMMAND ----------

title_series_info['age_of_content'] = (pd.to_datetime(title_series_info['offering_start_date'])-
                      pd.to_datetime(title_series_info['air_date']))/np.timedelta64(1, 'Y')

# COMMAND ----------

data = hours_df_60[['ckg_series_id', 'season_number','entertainment_segment_lifetime', 'percent_hours_viewed', 'percent_viewing_subs']].merge(
       title_series_info, on = ['ckg_series_id', 'season_number'])
data['pct_hours_viewed_per_runtime'] = data['percent_hours_viewed'].astype(float)/data['asset_run_time_hours'].astype(float)

# COMMAND ----------

data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC CORRELATION

# COMMAND ----------

import matplotlib.pyplot as plt 
import seaborn as sns

# COMMAND ----------

META_COLS = ['title_name', 'title_id', 'season_number', 'offering_start_date', 'asset_run_time_hours']
FEATURE_COLS = ['is_pay_1', 'is_popcorn', 'is_new_content', 
                'medal_number', 'age_of_content', 
               'budget', 'content_category_live', 'content_category_movie',
               'content_category_series',
               'program_type_Acquired', 'program_type_Original', ''
               'genre_Action', 'genre_Adult Animation', 'genre_Adventure & Survival',
               'genre_Comedy', 'genre_Documentaries', 'genre_Drama', 'genre_Events',
               'genre_Fantasy & Sci-Fi', 'genre_Food & Home', 'genre_Horror', 'genre_Kids & Family'
               ,'genre_News', 'genre_Sports', 'entertainment_segment_lifetime'
               ]

# COMMAND ----------

for i in FEATURE_COLS:
    if i not in (['budget', 'entertainment_segment_lifetime', 'age_of_content']):
        try:
            data[i] = data[i].astype(int, errors='ignore')
        except Exception as e:
            print (i)
            print (e)
    

# COMMAND ----------


TARGET_COL = ['percent_viewing_subs'] #cumulative_hours_viewed

# COMMAND ----------

np.bool = np.bool_

# COMMAND ----------

plot_data=data[data['entertainment_segment_lifetime'] == 'Drama Originals Watchers'][FEATURE_COLS+TARGET_COL]
corr = plot_data.corr()[TARGET_COL]
mask=np.zeros_like(corr, dtype=np.bool)
corr.loc['dummy_value'] = -1
# corr.sort_values(by = TARGET_COL)

# COMMAND ----------

corr['abs_value'] = np.abs(corr[TARGET_COL])
corr.sort_values(by = 'abs_value', ascending=False).head(12)

# COMMAND ----------


f, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr, mask=np.zeros_like(corr, dtype=np.bool), cmap=sns.diverging_palette(220, 10, as_cmap=True),
            square=True, ax=ax)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to table

# COMMAND ----------

data.columns = [i.replace(' ', '').replace('&', '_') for i in data.columns]
data_df = spark.createDataFrame(data)
spark.sql('drop table bolt_cus_dev.bronze.cip_title_segment_viewership_training_data')
data_df.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_title_segment_viewership_training_data")

# COMMAND ----------


