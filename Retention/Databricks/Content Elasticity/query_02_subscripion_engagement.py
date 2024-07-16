# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric
USING DELTA
PARTITIONED BY (request_date)

SELECT 
  u.sub_id_max
, u.sub_id_legacy
, u.user_id 
, u.hurley_user_id
, u.sub_paid_start as subscription_start_date
, u.period_start_ts as cycle_start_date
, u.period_end_ts as cycle_expire_date
, date_trunc('MONTH', u.period_end_ts)::DATE as expiration_month
, u.seamless_paid_tenure_months as tenure
, u.sub_number as number_of_reconnects
, CASE WHEN u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_cancel
, CASE WHEN u.vol_cancel_ind and u.terminated_ind_imputed THEN 1 ELSE 0 END AS is_vol_cancel
, u.provider
, u.ad_strategy as sku
, u.payment_period
, u.signup_offer
, hb.request_date_local as request_date
, u.region
, hb.session_country as country
, m.title_name
, m.ckg_series_id
, m.season_number
, m.medal
, sum(hb.CONTENT_MINUTES_WATCHED)/60 as hours_viewed
, CASE WHEN m.series_type LIKE ('%series%') THEN 'series'
        WHEN m.series_type IN ('movie', 'standalone') THEN 'movie'
        ELSE m.series_type
        END AS series_type
, m.primary_genre
, m.program_type
, m.is_popcorn
, m.is_pay_1
, m.run_time_hours
, m.season_window_start::DATE
, m.season_window_end::DATE
, m.series_window_start::DATE
, m.series_window_end::DATE
, m.lob
, m.category
, case when recency.ckg_series_id IS NULL then 1 else 0 end as is_recent
FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
LEFT JOIN bolt_dai_ce_prod.gold.combined_video_stream hb                             -- JOIN with viewership data
    ON hb.request_date_local between u.period_start_ts::DATE and u.period_end_ts::DATE
    AND hb.request_date_local >= DATEADD(MONTH, -1, u.period_end_ts::DATE)
    AND (hb.hbomax_subscription_id = u.sub_id_legacy or hb.sub_id = u.sub_id_max)
    AND hb.region = u.region
    AND hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
    AND hb.CONTENT_MINUTES_WATCHED >= 2
    AND hb.video_type = 'main' 
    and hb.region = 'NORTH AMERICA'
LEFT JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad  -- map program to series and season level
    ON hb.PROGRAM_ID_OR_VIEWABLE_ID = rad.ckg_program_id
LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m --title season level metadata
    ON rad.ckg_series_id = m.ckg_series_id 
    AND coalesce(rad.season_number, 0) = m.season_number
    -- AND m.region = u.region
    and m.region = 'NORTH AMERICA'
    AND m.country_code = hb.session_country
LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_offering_table_season_level recency --title season level recency indicator
    ON rad.ckg_series_id = recency.ckg_series_id 
    AND coalesce(rad.season_number, 0) = recency.season_number
    -- AND recency.region = u.region
    AND recency.country_code = hb.session_country
    AND hb.request_date_local = recency.request_date
WHERE 1=1
-- and u.provider = 'Direct'
-- and u.payment_period = 'PERIOD_MONTH'
-- and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
and u.region = 'NORTH AMERICA'
-- and date_trunc('MONTH', u.period_end_ts)::DATE >= '2022-01-01'
GROUP BY ALL
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC --------------- US ONLY ---------------
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_US
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (request_date)
# MAGIC
# MAGIC SELECT 
# MAGIC   u.sub_id_max
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
# MAGIC , u.region
# MAGIC , hb.session_country as country
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
# MAGIC , case when recency.ckg_series_id IS NULL then 1 else 0 end as is_recent
# MAGIC FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
# MAGIC LEFT JOIN bolt_dai_ce_prod.gold.combined_video_stream hb                             -- JOIN with viewership data
# MAGIC     ON hb.request_date_local between u.period_start_ts::DATE and u.period_end_ts::DATE
# MAGIC     AND hb.request_date_local >= DATEADD(MONTH, -1, u.period_end_ts::DATE)
# MAGIC     AND (hb.hbomax_subscription_id = u.sub_id_legacy or hb.sub_id = u.sub_id_max)
# MAGIC     -- AND hb.region = u.region
# MAGIC     and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
# MAGIC     and hb.CONTENT_MINUTES_WATCHED >= 2
# MAGIC     and hb.video_type = 'main'
# MAGIC     and hb.request_date_local >= '2022-01-01'
# MAGIC     and hb.region = 'NORTH AMERICA'
# MAGIC LEFT JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad  -- map program to series and season level
# MAGIC     ON hb.PROGRAM_ID_OR_VIEWABLE_ID = rad.ckg_program_id
# MAGIC LEFT JOIN 
# MAGIC     bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m --title season level metadata
# MAGIC     ON rad.ckg_series_id = m.ckg_series_id 
# MAGIC     AND coalesce(rad.season_number, 0) = m.season_number
# MAGIC     -- AND m.region = u.region
# MAGIC     AND m.country_code = hb.session_country
# MAGIC LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_offering_table_season_level recency --title season level recency indicator
# MAGIC     ON rad.ckg_series_id = recency.ckg_series_id 
# MAGIC     AND coalesce(rad.season_number, 0) = recency.season_number
# MAGIC     -- AND recency.region = u.region
# MAGIC     AND recency.country_code = hb.session_country
# MAGIC     AND hb.request_date_local = recency.request_date
# MAGIC WHERE 1=1
# MAGIC and u.provider = 'Direct'
# MAGIC and u.payment_period = 'PERIOD_MONTH'
# MAGIC and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
# MAGIC and u.region = 'NORTH AMERICA'
# MAGIC and date_trunc('MONTH', u.period_end_ts)::DATE >= '2022-01-01'
# MAGIC GROUP BY ALL

# COMMAND ----------


