# Databricks notebook source
# MAGIC %sql
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
# MAGIC -- and hb.request_date_local >= '2022-01-01'
# MAGIC and hb.request_date_local = '2023-06-01'
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   u.sub_id_max
# MAGIC , u.sub_id_legacy
# MAGIC , u.user_id 
# MAGIC , u.hurley_user_id
# MAGIC , u.period_start_ts as cycle_start_date
# MAGIC , u.period_end_ts as cycle_expire_date
# MAGIC , date_trunc('MONTH', u.period_end_ts)::DATE as expiration_month
# MAGIC , u.seamless_paid_tenure_months as tenure
# MAGIC , u.sub_number as number_of_reconnects
# MAGIC , CASE WHEN u.cancelled_ind THEN 1 ELSE 0 END AS is_cancel
# MAGIC , CASE WHEN u.vol_cancel_ind and u.cancelled_ind THEN 1 ELSE 0 END AS is_vol_cancel
# MAGIC , u.provider
# MAGIC , u.ad_strategy as sku
# MAGIC , hb.request_date_local as request_date
# MAGIC , m.title_name
# MAGIC , m.ckg_series_id
# MAGIC , m.season_number
# MAGIC , CASE WHEN m.medal = 'WB | Same Day Premiere' then 'Platinum'
# MAGIC        WHEN m.medal = 'A' then 'Gold'
# MAGIC        WHEN m.medal = 'B' then 'Silver'
# MAGIC        WHEN m.medal = 'C' then 'Bronze'
# MAGIC        ELSE 'Bronze'
# MAGIC   END AS medal
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
# MAGIC GROUP BY ALL
