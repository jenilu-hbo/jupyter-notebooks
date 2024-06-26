# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric
USING DELTA
PARTITIONED BY (request_date)

WITH VIEWERSHIP AS (
SELECT hb.request_date_local, hb.request_time_local
, hb.WBD_MAX_USER_ID, hb.HBOMAX_USER_ID
, hb.WBD_MAX_PROFILE_ID, hb.HBOMAX_PROFILE_ID
, hb.SUB_ID, hb.HBOMAX_SUBSCRIPTION_ID
,PROGRAM_ID_OR_VIEWABLE_ID,hb.CONTENT_MINUTES_WATCHED 
FROM bolt_dai_ce_prod.gold.combined_video_stream hb
WHERE 1=1
and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
and hb.CONTENT_MINUTES_WATCHED >= 2
and hb.video_type = 'main'
and hb.territory = 'HBO MAX DOMESTIC'
and hb.CATEGORY = 'retail'
and hb.request_date_local >= '2022-01-01'
--and hb.request_date_local = '2023-06-01'
)

SELECT 
  u.sub_id_max
, u.sub_id_legacy
, u.user_id 
, u.hurley_user_id
, u.period_start_ts as cycle_start_date
, u.period_end_ts as cycle_expire_date
, date_trunc('MONTH', u.period_end_ts)::DATE as expiration_month
, u.seamless_paid_tenure_months as tenure
, u.sub_number as number_of_reconnects
, CASE WHEN u.cancelled_ind THEN 1 ELSE 0 END AS is_cancel
, CASE WHEN u.vol_cancel_ind and u.cancelled_ind THEN 1 ELSE 0 END AS is_vol_cancel
, u.provider
, u.ad_strategy as sku
, hb.request_date_local as request_date
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
, case when hb.request_date_local between m.season_window_start and dateadd(DAY, 30, m.season_window_end) THEN 1 ELSE 0 END AS in_season_window
, case when hb.request_date_local between m.series_window_start and dateadd(DAY, 30, m.series_window_end) THEN 1 ELSE 0 END AS in_series_window
FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
LEFT JOIN VIEWERSHIP hb                             -- JOIN with viewership data
    ON hb.request_time_local between u.period_start_ts and u.period_end_ts
    AND (hb.hbomax_subscription_id = u.sub_id_legacy or hb.sub_id = u.sub_id_max)
LEFT JOIN bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad  -- map program to series and season level
    ON hb.PROGRAM_ID_OR_VIEWABLE_ID = rad.ckg_program_id
LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_season_level_metadata m  --title season level metadata
    ON rad.ckg_series_id = m.ckg_series_id 
    AND coalesce(rad.season_number, 0) = m.season_number
WHERE 1=1
and u.provider = 'Direct'
and u.payment_period = 'PERIOD_MONTH'
and (u.signup_offer is null or u.signup_offer = 'no_free_trial')
and u.region = 'NORTH AMERICA'
and date_trunc('MONTH', u.period_end_ts)::DATE >= '2022-01-01'
GROUP BY ALL
''')
