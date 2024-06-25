# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (expiration_month)
# MAGIC
# MAGIC SELECT
# MAGIC coalesce(sub_id_max, sub_id_legacy) as sub_id
# MAGIC , coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) as profile_id
# MAGIC , coalesce(hb.user_id, hb.hurley_user_id) as user_id
# MAGIC , cycle_expire_date
# MAGIC , expiration_month
# MAGIC , tenure
# MAGIC , number_of_reconnects
# MAGIC , is_cancel
# MAGIC , is_vol_cancel
# MAGIC , provider
# MAGIC , sku
# MAGIC , mode(entertainment_segment_lifetime) as entertainment_segment_lifetime
# MAGIC , count(distinct ckg_series_id) as titles_viewed
# MAGIC , count(distinct case when in_series_window = 1 then ckg_series_id else null end) as new_titles_viewed
# MAGIC , count(distinct case when in_series_window = 0 then ckg_series_id else null end) as library_titles_viewed
# MAGIC , sum(hours_viewed) as hours_viewed
# MAGIC , sum(case when in_series_window = 1 then hours_viewed else null end) as new_titles_hours_viewed
# MAGIC , sum(case when in_series_window = 0 then hours_viewed else null end) as library_titles_hours_viewed
# MAGIC , count(distinct weekofyear(request_date)) as number_of_weeks_viewed
# MAGIC , count(distinct case when medal = 'Platinum' then ckg_series_id else null end) as platinum_titles_viewed
# MAGIC , count(distinct case when medal = 'Gold' then ckg_series_id else null end) as gold_titles_viewed
# MAGIC , count(distinct case when medal = 'Silver' then ckg_series_id else null end) as silver_titles_viewed
# MAGIC , count(distinct case when series_type = 'series' then ckg_series_id else null end) as series_viewed
# MAGIC , count(distinct case when series_type = 'movie' then ckg_series_id else null end) as movie_viewed
# MAGIC , count(distinct case when series_type = 'livesports' then ckg_series_id else null end) as livesports_viewed
# MAGIC FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_dec2023 hb
# MAGIC LEFT JOIN (SELECT DISTINCT user_id, profile_id                -- to get default profile id
# MAGIC           FROM bolt_dai_subs_prod.gold.max_profile_dim_current
# MAGIC           where default_profile_ind = true) mp
# MAGIC       ON mp.user_id = hb.user_id
# MAGIC LEFT JOIN (SELECT DISTINCT hurley_user_id, hurley_profile_id   -- to get default legacy profile id
# MAGIC            FROM bolt_cus_dev.bronze.legacy_user_profile_dim_current
# MAGIC            where is_primary_profile = 1) lp 
# MAGIC       ON lp.hurley_user_id = hb.hurley_user_id                 -- to map legacy profile id to max profile id
# MAGIC LEFT JOIN bolt_dai_subs_prod.gold.max_legacy_profile_mapping_global mapping 
# MAGIC       ON mapping.hurley_profile_id = lp.hurley_profile_id
# MAGIC LEFT JOIN  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
# MAGIC       ON coalesce(mp.profile_id, mapping.profile_id, lp.hurley_profile_id) = seg.profile_id
# MAGIC GROUP BY ALL
# MAGIC

# COMMAND ----------


