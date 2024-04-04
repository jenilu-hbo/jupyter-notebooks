daily_total_views_query = '''
                            select *, datediff(day, start_date, end_date)+1 as days_after_launch
                            from max_dev.workspace.actives_base_first_view
                            where days_after_launch<= 28
                            order by start_date
                            '''

title_actives_query = '''
                with max_release_date_base as (
                select
                  distinct
                  rad.title_id,
                  case
                    when rad.season_number is null
                        then title_name
                    else concat(rad.series_title_long,' S',rad.season_number)
                  end as title,
                  'Seasons and Movies' as title_level,
                  first_value(raod.first_offered_date) over (partition by title order by raod.first_offered_date asc) as first_release_date,
                  case
                    when aod.offering_start_date <='2020-05-27 07:00:00' THEN '2020-05-27 07:00:00'
                    else aod.offering_start_date
                  end as offering_start_date,
                  aod.offering_end_date
                  , case when content_category = 'series'
                    then ifnull(season_number, 1)
                    else season_number
                        end as season_number_adj
                , coalesce(concat(rad.series_id, '-', season_number_adj), rad.viewable_id) as match_id
                from
                  max_prod.catalog.reporting_asset_dim rad
                join max_prod.catalog.asset_offering_dim aod
                    on rad.viewable_id = aod.viewable_id
                        and aod.offering_end_date>='2020-05-27 07:00:00'
                        AND aod.brand = 'HBO MAX'
                join max_prod.catalog.reporting_asset_offering_dim raod
                  on aod.viewable_id = raod.viewable_id
                    AND aod.channel = raod.channel
                    AND aod.brand = raod.brand
                    AND raod.territory = aod.territory -- leave as HBO MAX domestic because no MUS?
                where
                  raod.first_offered_date is not null
                  and rad.asset_type = 'FEATURE'
                  and offering_start_date<=sysdate()
                  and aod.territory='HBO MAX DOMESTIC'
              ),
              second as (
                SELECT
                   s1.title_id,
                   s1.match_id,
                   s1.title,
                   s1.title_level,
                   s1.first_release_date,
                   s1.offering_start_date,
                   MIN(t1.offering_end_date) AS offering_end_date
                FROM max_release_date_base s1
                  INNER JOIN max_release_date_base t1 ON s1.offering_start_date <= t1.offering_end_date and s1.title_id=t1.title_id and s1.title=t1.title and s1.title_level=t1.title_level
                    AND NOT EXISTS(SELECT * FROM max_release_date_base t2
                                   WHERE t1.offering_end_date >= t2.offering_start_date AND t1.offering_end_date < t2.offering_end_date
                                    and t1.title_id=t2.title_id and t1.title=t2.title
                                  )
                WHERE NOT EXISTS(SELECT * FROM max_release_date_base s2
                                   WHERE s1.offering_start_date > s2.offering_start_date AND s1.offering_start_date <= s2.offering_end_date
                                    and s1.title_id=s2.title_id and s1.title=s2.title
                                  )
                GROUP BY 1,2,3,4,5, 6
                order by 1,2,3,4,5, 6
              ),
              third as
                (SELECT
                title_id,
                title,
                title_level,
                match_id,
                max(offering_end_date) as end_date
                from second
                group by 1,2,3,4),
              fourth as
                (SELECT
                 title_id,
                title,
                title_level,
                match_id,
                case when end_date>=sysdate() then FALSE
                else TRUE end as is_inactive
                from third),
               fifth as
                 (SELECT
                 title,
                 title_id,
                 title_level,
                 max(days_on_hbo_max) as total_days
                from content_intelligence.new_title_release_days_on_platform
                group by 1,2,3)

             SELECT c.match_id, a.title, a.first_release_date, a.DAILY_VIEWING_SUBS, a.CUMULATIVE_VIEWING_SUBS, 
                     a.days_on_hbo_max, a.title_id
               FROM content_intelligence.new_title_release_days_on_platform a
                left join fifth b on initcap(a.title)=initcap(b.title) and a.title_id=b.title_id and a.title_level=b.title_level
                left join fourth c on initcap(a.title)=initcap(c.title) and a.title_id=c.title_id and a.title_level=c.title_level
              where (days_on_hbo_max between 1 and 28)

'''

back_fill_base_query = '''
        INSERT INTO max_dev.workspace.{table}
        WITH date AS
            (
            select seq_date as date
            from "MAX_PROD"."STAGING"."DATE_RANGE" 
            where seq_date <= CURRENT_DATE()
            and seq_date >= '2020-05-27'
            and seq_date = '{date}'
            ), 

         denom_first_viewed as (
              select
                hbo_uuid,
                dr.date as start_date,
                min(LOCAL_REQUEST_DATE) as request_date
                from viewership.max_user_stream_distinct_user_dates_heartbeat udh
                join date dr ON udh.LOCAL_REQUEST_DATE between dr.date and dateadd(day, 28, dr.date)
                group by 1,2
              ),

          denom_subs_count as (
                select
                    start_date, request_date as end_date,
                    count(hbo_uuid) as subs_count
              from denom_first_viewed
               group by 1,2
            )
          
            select
              start_date, end_date,
              subs_count as daily_viewing_subs_denom,
              sum(subs_count) over(partition by start_date order by end_date rows between unbounded preceding and current row) as cumulative_viewing_subs_denom
            from denom_subs_count
            group by 1, 2, 3
'''