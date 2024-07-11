# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg
USING DELTA
PARTITIONED BY (expiration_month)

SELECT
coalesce(sub_id_max, sub_id_legacy) as sub_id
, coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) as profile_id
, coalesce(hb.user_id, hb.hurley_user_id) as user_id
, cycle_expire_date
, expiration_month
, tenure
, number_of_reconnects
, is_cancel
, is_vol_cancel
, provider
, sku
, mode(entertainment_segment_lifetime) as entertainment_segment_lifetime
, count(distinct weekofyear(request_date)) as number_of_weeks_viewed
, count(distinct ckg_series_id) as titles_viewed
, count(distinct case when in_series_window = 1 then ckg_series_id else null end) as new_titles_viewed
, count(distinct case when in_series_window = 0 then ckg_series_id else null end) as library_titles_viewed
, count(distinct case when medal = 'Platinum' then ckg_series_id else null end) as platinum_titles_viewed
, count(distinct case when medal = 'Gold' then ckg_series_id else null end) as gold_titles_viewed
, count(distinct case when medal = 'Silver' then ckg_series_id else null end) as silver_titles_viewed
, count(distinct case when series_type = 'series' then ckg_series_id else null end) as series_viewed
, count(distinct case when series_type = 'movie' then ckg_series_id else null end) as movie_viewed
, count(distinct case when series_type IN ('livesports', 'live') then ckg_series_id else null end) as livesports_viewed
, sum(hours_viewed) as hours_viewed
, sum(case when in_series_window = 1 then hours_viewed else null end) as new_titles_hours_viewed
, sum(case when in_series_window = 0 then hours_viewed else null end) as library_titles_hours_viewed
, sum(case when medal = 'Platinum' then hours_viewed else null end) as platinum_hours_viewed
, sum(case when medal = 'Gold' then hours_viewed else null end) as gold_hours_viewed
, sum(case when medal = 'Silver' then hours_viewed else null end) as silver_hours_viewed
, sum(case when series_type = 'series' then hours_viewed else null end) as series_hours_viewed
, sum(case when series_type = 'movie' then hours_viewed else null end) as movie_hours_viewed
, sum(case when series_type IN ('livesports', 'live') then hours_viewed else null end) as livesports_hours_viewed
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric hb
LEFT JOIN (SELECT DISTINCT user_id, profile_id                -- to get default profile id
          FROM bolt_dai_subs_prod.gold.max_profile_dim_current
          where default_profile_ind = true) mp
      ON mp.user_id = hb.user_id
LEFT JOIN (SELECT DISTINCT hurley_user_id, hurley_profile_id   -- to get default legacy profile id
           FROM bolt_cus_dev.bronze.legacy_user_profile_dim_current
           where is_primary_profile = 1) lp 
      ON lp.hurley_user_id = hb.hurley_user_id                 -- to map legacy profile id to max profile id
LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global mapping 
      ON mapping.hurley_profile_id = lp.hurley_profile_id
LEFT JOIN  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
      ON coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) = seg.profile_id
WHERE 1=1
-- and hb.provider = 'Direct'
-- and hb.payment_period = 'PERIOD_MONTH'
-- and (hb.signup_offer is null or hb.signup_offer = 'no_free_trial')
-- and hb.region = 'NORTH AMERICA'
-- and expiration_month >= '2022-01-01'
GROUP BY ALL
''')

# COMMAND ----------


