# Databricks notebook source
spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_base_by_segments
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

spark.sql('''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_base

SELECT dt.start_date 
      , dt.end_date
      , DATEDIFF(DAY, dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
      , count(distinct coalesce(u.sub_id_max, u.sub_id_legacy)) as subs
FROM bolt_cus_dev.silver.max_ltv_period_combined u  --Subcription level metric
JOIN bolt_cus_dev.bronze.calendar_date_tracker dt
    ON u.period_end_ts >= dt.start_date
    and u.period_start_ts <= dt.end_date
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


