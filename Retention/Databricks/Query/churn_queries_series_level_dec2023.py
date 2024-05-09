# Databricks notebook source
import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd
import io

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

############### CREATE NEW/LIBRARY CONTENT TABLE ###############
spark.sql('''
create or replace table bolt_cus_dev.bronze.cip_recency_title_series_level_offering_table as ( 
with first as
(SELECT
a.ckg_series_id,
max(first_offered_date) as window_end
FROM bolt_dai_ckg_prod.gold.reporting_asset_dim_combined a 
inner join bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
    on a.ckg_program_id = b.ckg_program_id 
    and b.country_code = 'US' 
where asset_type = 'FEATURE' 
group by all
)

select  
    series_title_long 
    , a.ckg_series_id 
    , content_category 
    , a.season_number 
    , a.ckg_program_id 
    , episode_number_in_season 
    --using title_first_offered for movies, using individual start_dates for all other categories 
    , coalesce(season_first_offered_date,title_first_offered_date) as start_date 
    , dateadd(day, 365, window_end) as recency_window_end 
    , release_year 
    --some metadata has inconsistent release year's within a series (abbott S1), using median to mask that, and converting to an int to perform conditions on 
    , median(release_year) over (partition by a.ckg_series_id, a.season_number)::int as median_release_year 
    , median_release_year+1 as release_year_plusone 
from bolt_dai_ckg_prod.gold.reporting_asset_dim_combined a 
inner join bolt_dai_ckg_prod.gold.reporting_asset_offering_dim_combined b 
    on a.ckg_program_id = b.ckg_program_id 
    and b.country_code = 'US' 
left join first c on a.ckg_series_id=c.ckg_series_id
where asset_type = 'FEATURE' 
    and title_first_offered_date is not null 
group by 1,2,3,4,5,6,7,8,9
) 

          ''')

# COMMAND ----------

target_month = '2023-12-01'
target_month_end = (pd.to_datetime(target_month) + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d')

# COMMAND ----------

user_stream_60d_genpop ='''
CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_new_lib_series_level_202312 AS (
with subs as(
select c.user_id, c.hurley_user_id, up.profile_id
, is_cancel, sub_month, is_voluntary
, cycle_start_date
, cycle_expire_date
from bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max c 
LEFT join bolt_dai_subs_prod.gold.max_profile_dim_current up
    on c.user_id = up.USER_ID
where up.default_profile_ind = True
and cycle_expire_date between '{target_month}' and '{target_month_end}'
--LIMIT 3000000
)

, streaming_subset as
(
select
      ss.user_id
    , ss.hurley_user_id
    , ss.profile_id
    , ss.is_cancel
    , is_voluntary
    , ss.sub_month
    , hb.PROGRAM_ID_OR_VIEWABLE_ID as ckg_program_id
    , hb.CONTENT_MINUTES_WATCHED/60 as hours_viewed
from subs ss
left join bolt_dai_ce_prod.gold.combined_video_stream hb
    on hb.WBD_MAX_PROFILE_ID = ss.profile_id
where DATEDIFF(DAY, hb.request_date_pst, ss.cycle_expire_date) <= 60
    and hb.request_date_pst::DATE between DATEADD(MONTH, -2, '{target_month}'::DATE)
                            and '{target_month_end}'
    and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
    and hb.CONTENT_MINUTES_WATCHED >= 15
    and hb.video_type = 'main'
    and hb.territory = 'HBO MAX DOMESTIC'
)

select
      s.user_id
    , s.profile_id
    , s.is_cancel
    , is_voluntary
    , s.sub_month
    , rad.program_type as program_type
    , rad.content_category as content_category
    , rad.REPORTING_PRIMARY_GENRE as genre
    , CASE WHEN r.recency_window_end >= '{target_month}' 
      THEN 'current'
      ELSE 'library'
      END AS old_new
    , sum(s.hours_viewed) as hours_viewed
    , count(distinct rad.ckg_series_id) as titles_viewed
from streaming_subset s
left join bolt_analytics_prod.gold.v_r_content_metadata_reporting_asset_dim_combined rad 
    on s.ckg_program_id = rad.ckg_program_id
left join bolt_cus_dev.bronze.cip_recency_title_series_level_offering_table r
    on s.ckg_program_id = r.ckg_program_id
where rad.asset_type!='PROMOTION'
group by 1,2,3,4,5,6,7,8,9 
)                                                              
'''.format(
    target_month = target_month,
    target_month_end = target_month_end
    )
# )

# COMMAND ----------

print(user_stream_60d_genpop)

# COMMAND ----------

print('bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_new_lib_series_level_{target_month}'.format(target_month = target_month))

# COMMAND ----------

# user_stream_60d_genpop.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_new_lib_20231201")

# COMMAND ----------

audience_segement = spark.sql('''
                              SELECT a.*, b.entertainment_segment_lifetime
                              FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_new_lib_20231201 a
                              LEFT JOIN 
                                        (SELECT USER_ID, PROFILE_ID, MAX(entertainment_segment_lifetime) as entertainment_segment_lifetime
                                         FROM bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table
                                         GROUP BY all
                                        ) b
                              ON a.profile_id = b.PROFILE_ID
                              ''')
# audience_segement = audience_segement.toPandas()

# COMMAND ----------

audience_segement.write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.churn_user_stream60d_segmented_20231201")

# COMMAND ----------

user_stream_60d_genpop.show()

# COMMAND ----------


