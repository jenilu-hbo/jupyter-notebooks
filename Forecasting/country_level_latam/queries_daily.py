back_fill_base_query = '''
INSERT INTO max_dev.workspace.{table}
with first as (
         SELECT distinct
              territory,
              title_id,
               'Seasons and Movies' as title_level,
              case when offering_start_date <='2021-06-29 09:00:00' THEN '2021-06-29 09:00:00' else offering_start_date end as offering_start_date,
              offering_end_date,
              case when content_category = 'series' then ifnull(season_number, 1) else season_number end as season_number_adj,
              coalesce(concat(b.series_id, '-', season_number_adj), b.viewable_id) as match_id,
              case when b.season_number is null then title_name else concat(b.series_title_long,' S',b.season_number) end as title
          from catalog.asset_offering_dim a
           left join catalog.reporting_asset_dim b on a.viewable_id=b.viewable_id
          where a.offering_end_date >='2021-06-29 09:00:00'
           and date_trunc('day',a.offering_start_date) <= current_date()
           and b.asset_type = 'FEATURE'
           and 1=1 -- no filter on 'new_title_release.territory'
  --------------------------------------- Filters Here ---------------------------------------------------
           and territory in ('ARGENTINA','BRAZIL','CARIBBEAN','COLOMBIA','MEXICO','LATAM','REST OF LATAM')
           and OFFERING_START_DATE >= '{start_date}' and OFFERING_START_DATE < dateadd(day, 1, '{start_date}')
  --------------------------------------- ----------------------------------------------------------------
      ),
      second as (
          SELECT
                 s1.territory,
                 s1.title_id,
                 s1.title,
                 s1.match_id,
                 s1.offering_start_date,
                 MIN(t1.offering_end_date) AS offering_end_date
          FROM first s1
          INNER JOIN first t1 ON s1.offering_start_date <= t1.offering_end_date and s1.title_id=t1.title_id and s1.title = t1.title and s1.territory = t1.territory
            AND NOT EXISTS(SELECT * FROM first t2
                           WHERE t1.offering_end_date >= t2.offering_start_date AND t1.offering_end_date < t2.offering_end_date  and t1.title_id=t2.title_id and t1.title = t2.title
                          and t1.territory = t2.territory)
          WHERE NOT EXISTS(SELECT * FROM first s2
                           WHERE s1.offering_start_date > s2.offering_start_date AND s1.offering_start_date <= s2.offering_end_date  and s1.title_id=s2.title_id and s1.title = s2.title
                          and s1.territory = s2.territory)
          GROUP BY 1,2,3,4,5
          order by 1,3
      ),
      third as (
           SELECT
           *,
           datediff(second, offering_start_date, offering_end_date) as window_length
           FROM second
      ),
      fourth as (
          SELECT
           territory,
           title_id,
           title,
           match_id,
           offering_start_date,
           offering_end_date,
           window_length,
           sum(window_length) over (partition by territory,title_id,title,match_id order by offering_start_date asc) as window_end
         FROM third
      ),
      max_release_date as (
           SELECT
             territory,
             title_id,
             title,
             match_id,
             offering_start_date,
             offering_end_date,
             window_end-window_length as window_start
           from fourth
      ), 
      denom_first_viewed as (
          select
            s.territory,
            s.title_id,
            s.title,
            s.match_id,
            hbo_uuid,
            offering_start_date,
            min(floor(((datediff(second, offering_start_date,request_time_gmt)+window_start)/24/3600))) as days_on_hbo_max
            from max_prod.viewership.max_user_stream_heartbeat mus
              join max_prod.catalog.reporting_asset_dim b on b.viewable_id = mus.viewable_id
              ------------------------------------------------------------------------------------------------------------------------
              join max_release_date s on  mus.request_time_gmt >= s.offering_start_date 
                                      AND mus.request_time_gmt < dateadd(day, {days_on_hbo_max}, s.offering_start_date)
              ------------------------------------------------------------------------------------------------------------------------
              join max_prod.core.geo_map d
                  on mus.country_iso_code = d.country_iso_code
                  and s.territory = d.territory
              join max_prod.catalog.reporting_asset_offering_dim raod
                on raod.viewable_id = mus.viewable_id
                and raod.brand = 'HBO MAX'
                and raod.territory = d.territory
                and raod.channel = mus.channel
            where region = 'LATAM'
              and stream_elapsed_play_seconds >= 120
              and video_type = 'main'
              and request_time_gmt between s.offering_start_date and s.offering_end_date
              and b.asset_type = 'FEATURE'
              and request_time_gmt >= raod.first_offered_date
            group by 1,2,3,4,5,6
          ),
          denom_subs_count as (
            select
              territory,
              title_id,
              title,
              match_id,
              days_on_hbo_max,
              count(hbo_uuid) as subs_count
            from denom_first_viewed
            group by 1,2,3,4,5
          ),

          cum_subs as (
          select
            territory,
            title_id,
            title,
            match_id,
            days_on_hbo_max+1 as days_on_hbo_max,
            sum(subs_count) over(partition by territory,title_id,title,match_id order by days_on_hbo_max rows between unbounded preceding and current row) as cumulative_viewing_subs_denom
          from denom_subs_count

            )

          select * from cum_subs 
          where days_on_hbo_max = {days_on_hbo_max}

'''