# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM bolt_dai_subs_prod.gold.max_sub_fact_daily_to_dim_sub_mapping_table LIMIT 10

# COMMAND ----------

spark.sql('''
create
or replace table bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max as (
  -- basic table
  with crs_sub_balance as (
    select
      a.user_id,
      b.hurley_user_id,
      a.sub_id,
      a.PROVIDER,
      PERIOD_START_DT_LOCALTZ as subscription_start_date,
      PERIOD_END_DT_LOCALTZ as subscription_expire_date,
      sku,
      CASE
        WHEN SUB_CATEGORY = 'direct_billing' THEN 1
        ELSE 0
      END AS is_direct_to_paid,
      CASE
        WHEN SUBSCRIPTION_EVENT_TYPE = 'churn_event' then 1
        else 0
      end as is_cancel,
      CASE
        WHEN TERMINATION_TYPE = 'VOLUNTARY' THEN 1
        ELSE 0
      END AS is_voluntary,
      a.RECONNECT_IND AS is_reconnect,
      a.sub_number as sub_session_ind,
      SEAMLESS_PAID_START_TS_LOCALTZ :: DATE AS sub_session_start_timestamp,
      SEAMLESS_PAID_TENURE_MONTHS as sub_month
    from
      bolt_analytics_prod.gold.subscription_wbd_max_period_events a
      LEFT JOIN bolt_dai_subs_prod.gold.max_sub_fact_daily_to_dim_sub_mapping_table b on a.user_id = b.user_id
      and a.sub_id = b.sub_id
    where
      1 = 1
      and lower(a.provider) in (
        'apple',
        'direct',
        'google',
        'samsung',
        'amazon',
        'roku'
      ) 
      --   and a.PRODUCT  IN ('Premium', 'Legacy HBO MAX SVOD', 'Ad-Free', 'Legacy HBO MAX AVOD', 'Ad-Lite')
      # and a.territory = 'US' 
      and a.region = 'NORTH AMERICA'
      and a.CATEGORY = 'retail'
      and a.user_id is not null
      and PERIOD_START_DT_LOCALTZ is not null
      and (
        a.SIGNUP_OFFER IS NULL
        or a.SIGNUP_OFFER = 'no_free_trial'
      )
      and SKU in (
        'ultimate-monthly',
        'ad-free-monthly',
        'ad-lite-monthly'
      )
      and SEAMLESS_IN_PAYING_BALANCE_EVER_IND = True
      and datediff(
        day,
        PERIOD_START_DT_LOCALTZ,
        PERIOD_END_DT_LOCALTZ
      ) > 7
  ),
  -- combine churn and purchase event
  final_base AS (
    SELECT
      USER_ID,
      HURLEY_USER_ID,
      SUB_ID,
      is_direct_to_paid,
      is_reconnect,
      sub_session_ind,
      sub_session_start_timestamp,
      sku,
      subscription_start_date,
      subscription_expire_date,
      sub_month,
      MAX(is_cancel) as is_cancel,
      MAX(is_voluntary) AS is_voluntary
    FROM
      crs_sub_balance
    group by
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11
  ),
  sub_mon_clean as (
    select
      user_id,
      hurley_user_id,
      sub_id,
      sub_session_ind,
      sub_month,
      sku,
      max(is_direct_to_paid) as is_direct_to_paid,
      max(is_cancel) as is_cancel,
      max(is_reconnect) as is_reconnect,
      max(is_voluntary) as is_voluntary,
      min(sub_session_start_timestamp) as sub_session_start_timestamp_clean,
      min(subscription_start_date) as cycle_start_date,
      max(subscription_expire_date) as cycle_expire_date
    from
      final_base
    where
      1 = 1
      and sub_month > 0
    group by
      1,
      2,
      3,
      4,
      5,
      6
  )
  select
    *,
    lag(cycle_expire_date, 1) over (
      partition by user_id,
      sub_id
      order by
        sub_session_ind asc,
        sub_month asc
    ) as prev_session_expire_date
  from
    sub_mon_clean as f
  where
    1 = 1
  order by
    user_id,
    sub_id,
    sub_session_ind,
    cycle_start_date
)
          ''')
