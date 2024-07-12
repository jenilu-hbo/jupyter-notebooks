# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_base
USING DELTA
PARTITIONED BY (start_date)

SELECT dt.start_date 
      , dt.end_date
      , DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
      , entertainment_segment_lifetime
      , count(distinct coalesce(u.sub_id_max, u.sub_id_legacy)) as subs
FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
JOIN bolt_cus_dev.bronze.calendar_date_tracker dt
    ON u.period_end_ts >= dt.start_date
    and u.period_start_ts <= dt.end_date
LEFT JOIN (SELECT DISTINCT user_id, profile_id                -- to get default profile id
          FROM bolt_dai_subs_prod.gold.max_profile_dim_current
          where default_profile_ind = true) mp
      ON mp.user_id = u.user_id
LEFT JOIN (SELECT DISTINCT hurley_user_id, hurley_profile_id   -- to get default legacy profile id
           FROM bolt_cus_dev.bronze.legacy_user_profile_dim_current
           where is_primary_profile = 1) lp 
      ON lp.hurley_user_id = u.hurley_user_id                 -- to map legacy profile id to max profile id
LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global mapping 
      ON mapping.hurley_profile_id = lp.hurley_profile_id
LEFT JOIN  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
      ON coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) = seg.profile_id
WHERE 1=1
and u.provider = 'Direct'
and u.payment_period = 'PERIOD_MONTH'
and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
and u.region = 'NORTH AMERICA'
and dt.end_date <= '2024-07-01'
and dt.end_date >= '2022-01-01'
AND (DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 28
or DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 30
or DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 60
or DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) = 90
        )
GROUP BY ALL
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC --------------- OLDER VERSION ---------------
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (request_date)
# MAGIC
# MAGIC WITH VIEWERSHIP AS (
# MAGIC SELECT hb.request_date_local, hb.request_time_local
# MAGIC , hb.WBD_MAX_USER_ID, hb.HBOMAX_USER_ID
# MAGIC , hb.WBD_MAX_PROFILE_ID, hb.HBOMAX_PROFILE_ID
# MAGIC , hb.SUB_ID, hb.HBOMAX_SUBSCRIPTION_ID
# MAGIC ,PROGRAM_ID_OR_VIEWABLE_ID,hb.CONTENT_MINUTES_WATCHED 
# MAGIC FROM bolt_dai_ce_prod.gold.combined_video_stream hb
# MAGIC WHERE 1=1
# MAGIC and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
# MAGIC and hb.CONTENT_MINUTES_WATCHED >= 2
# MAGIC and hb.video_type = 'main'
# MAGIC and hb.territory = 'HBO MAX DOMESTIC'
# MAGIC and hb.CATEGORY = 'retail'
# MAGIC and hb.request_date_local >= '2022-01-01'
# MAGIC --and hb.request_date_local = '2023-06-01'
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC u.sub_id_max
# MAGIC , u.sub_id_legacy
# MAGIC , u.user_id 
# MAGIC , u.hurley_user_id
# MAGIC , u.sub_paid_start as subscription_start_date
# MAGIC , u.period_start_ts as cycle_start_date
# MAGIC , u.period_end_ts as cycle_expire_date
# MAGIC , date_trunc('MONTH', u.period_end_ts)::DATE as expiration_month
# MAGIC , u.seamless_paid_tenure_months as tenure
# MAGIC , u.sub_number as number_of_reconnects
# MAGIC , CASE WHEN u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_cancel
# MAGIC , CASE WHEN u.vol_cancel_ind and u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_vol_cancel
# MAGIC , u.provider
# MAGIC , u.ad_strategy as sku
# MAGIC , u.payment_period
# MAGIC , u.signup_offer
# MAGIC , hb.request_date_local as request_date
# MAGIC -- , u.region
# MAGIC -- , hb.session_country as country
# MAGIC , m.title_name
# MAGIC , m.ckg_series_id
# MAGIC , m.season_number
# MAGIC , m.medal
# MAGIC , sum(hb.CONTENT_MINUTES_WATCHED)/60 as hours_viewed
# MAGIC , CASE WHEN m.series_type LIKE ('%series%') THEN 'series'
# MAGIC         WHEN m.series_type IN ('movie', 'standalone') THEN 'movie'
# MAGIC         ELSE m.series_type
# MAGIC         END AS series_type
# MAGIC , m.primary_genre
# MAGIC , m.program_type
# MAGIC , m.is_popcorn
# MAGIC , m.is_pay_1
# MAGIC , m.run_time_hours
# MAGIC , m.season_window_start::DATE
# MAGIC , m.season_window_end::DATE
# MAGIC , m.series_window_start::DATE
# MAGIC , m.series_window_end::DATE
# MAGIC , m.lob
# MAGIC , m.category
# MAGIC , case when hb.request_date_local between m.season_window_start and dateadd(DAY, 30, m.season_window_end) THEN 1 ELSE 0 END AS in_season_window
# MAGIC , case when hb.request_date_local between m.series_window_start and dateadd(DAY, 30, m.series_window_end) THEN 1 ELSE 0 END AS in_series_window
# MAGIC FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
# MAGIC LEFT JOIN VIEWERSHIP hb                             -- JOIN with viewership data
# MAGIC     ON hb.request_time_local between u.period_start_ts and u.period_end_ts
# MAGIC     AND (hb.hbomax_subscription_id = u.sub_id_legacy or hb.sub_id = u.sub_id_max)
# MAGIC LEFT JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad  -- map program to series and season level
# MAGIC     ON hb.PROGRAM_ID_OR_VIEWABLE_ID = rad.ckg_program_id
# MAGIC LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m  --title season level metadata
# MAGIC     ON rad.ckg_series_id = m.ckg_series_id 
# MAGIC     AND coalesce(rad.season_number, 0) = m.season_number
# MAGIC WHERE 1=1
# MAGIC and u.provider = 'Direct'
# MAGIC and u.payment_period = 'PERIOD_MONTH'
# MAGIC and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
# MAGIC and u.region = 'NORTH AMERICA'
# MAGIC and date_trunc('MONTH', u.period_end_ts)::DATE >= '2022-01-01'
# MAGIC GROUP BY ALL

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_dec2023

# SELECT 
#   u.sub_id_max
# , u.sub_id_legacy
# , u.user_id 
# , u.hurley_user_id
# , u.sub_paid_start as subscription_start_date
# , u.period_start_ts as cycle_start_date
# , u.period_end_ts as cycle_expire_date
# , date_trunc('MONTH', u.period_end_ts)::DATE as expiration_month
# , u.seamless_paid_tenure_months as tenure
# , u.sub_number as number_of_reconnects
# , CASE WHEN u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_cancel
# , CASE WHEN u.vol_cancel_ind and u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_vol_cancel
# , u.provider
# , u.ad_strategy as sku
# , u.payment_period
# , u.signup_offer
# , hb.request_date_local as request_date
# , u.region
# , hb.session_country as country
# , m.title_name
# , m.ckg_series_id
# , m.season_number
# , m.medal
# , sum(hb.CONTENT_MINUTES_WATCHED)/60 as hours_viewed
# , CASE WHEN m.series_type LIKE ('%series%') THEN 'series'
#         WHEN m.series_type IN ('movie', 'standalone') THEN 'movie'
#         ELSE m.series_type
#         END AS series_type
# , m.primary_genre
# , m.program_type
# , m.is_popcorn
# , m.is_pay_1
# , m.run_time_hours
# , m.season_window_start::DATE
# , m.season_window_end::DATE
# , m.series_window_start::DATE
# , m.series_window_end::DATE
# , m.lob
# , m.category
# , case when hb.request_date_local between m.season_window_start and dateadd(DAY, 30, m.season_window_end) THEN 1 ELSE 0 END AS in_season_window
# , case when hb.request_date_local between m.series_window_start and dateadd(DAY, 30, m.series_window_end) THEN 1 ELSE 0 END AS in_series_window
# FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
# LEFT JOIN bolt_dai_ce_prod.gold.combined_video_stream hb                             -- JOIN with viewership data
#     ON hb.request_date_local between u.period_start_ts::DATE and u.period_end_ts::DATE
#     AND hb.request_date_local >= DATEADD(MONTH, -1, u.period_end_ts::DATE)
#     AND (hb.hbomax_subscription_id = u.sub_id_legacy or hb.sub_id = u.sub_id_max)
#     -- AND hb.region = u.region
#     AND hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
#     AND hb.CONTENT_MINUTES_WATCHED >= 2
#     AND hb.video_type = 'main'
#     AND hb.request_date_local between '2023-10-01' and '2023-12-31'
#     AND hb.region = 'NORTH AMERICA'
#     -- AND SESSION_COUNTRY = 'US'
# LEFT JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad  -- map program to series and season level
#     ON hb.PROGRAM_ID_OR_VIEWABLE_ID = rad.ckg_program_id
# LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m --title season level metadata
#     ON rad.ckg_series_id = m.ckg_series_id 
#     AND coalesce(rad.season_number, 0) = m.season_number
#     -- AND m.region = u.region
#     AND m.country_code = hb.session_country
# WHERE 1=1
# and u.provider = 'Direct'
# and u.payment_period = 'PERIOD_MONTH'
# and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
# and u.region = 'NORTH AMERICA'
# and date_trunc('MONTH', u.period_end_ts)::DATE = '2023-12-01'
# GROUP BY ALL

# COMMAND ----------


