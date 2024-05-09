import os
import sys
from utils import *
import snowflake.connector
from datetime import timedelta

class SnowflakeConnector(BaseConnector):
    def __init__(self, credentials: Credentials):
        keys = credentials.get_keys()
        self._secrets = json.loads(keys.get('SecretString', "{}"))

    def connect(self, dbname: str, schema: str = 'DEFAULT'):
        ctx = snowflake.connector.connect(
            user=self._secrets['login_name'],
            password=self._secrets['login_password'],
            account=self._secrets['account'],
            warehouse=self._secrets['warehouse'],
            database=dbname,
            schema=schema
        )

        return ctx
    
## Credentials
SF_CREDS = 'datascience-max-dev-sagemaker-notebooks'

## Snowflake connection 
conn=SnowflakeConnector(SSMPSCredentials(SF_CREDS))
ctx=conn.connect("MAX_PROD","DATASCIENCE_STAGE")

def run_query(query):
    cursor = ctx.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
    df.columns= df.columns.str.lower()
    return df
    
    
qry = '''
        INSERT INTO max_dev.workspace.{table}
        WITH date AS
            (
            select seq_date from "MAX_PROD"."STAGING"."DATE_RANGE" 
            where seq_date <= CURRENT_DATE()
            and seq_date > '2020-05-23'
            ), 

        date_range AS 
            (SELECT tad.*,
                d1.seq_date as start_date,
                d2.seq_date as end_date
            from date d1
            join date d2
                ON d2.seq_date >= d1.seq_date
                and d2.seq_date <= dateadd(day, 28, d1.seq_date) 
            join max_dev.workspace.title_available_date tad on tad.available_date = d1.seq_date
            where d2.seq_date = '{date}'

             )

            SELECT 
                b.title_name,
                dr.season_number,
                dr.match_id,
                dr.available_date, 
                dr.end_date as real_date,
                cast(datediff('day',cast(dr.available_date as timestamp), cast(dr.end_date as timestamp)) as int) as days_after_launch,
                count(distinct hbo_uuid) as total_viewing_accounts,
                count(*) as total_plays,
                sum(stream_elapsed_play_seconds) as total_seconds
            from max_prod.viewership.max_user_stream_heartbeat a
             join max_prod.catalog.reporting_asset_dim b on a.viewable_id=b.viewable_id
             join max_prod.core.geo_map c on a.country_iso_code=c.country_iso_code
             join max_prod.catalog.reporting_asset_offering_dim d on a.viewable_id=d.viewable_id and brand='HBO MAX' and c.territory=d.territory and d.channel='HBO MAX SUBSCRIPTION'
             join date_range dr on (dr.series_id = b.series_id or dr.match_id = b.viewable_id) 
                                and a.request_date between dr.start_date and dr.end_date
            where a.stream_elapsed_play_seconds>=120
             and first_offered_date<=a.request_time_gmt
             and region='NORTH AMERICA'
             and b.asset_type='FEATURE'
             and video_type='main'
             and a.request_date between dr.start_date and dr.end_date
            group by 1, 2, 3, 4, 5
'''

table_name = 'ds_content_actives'
t=date(2021,1,1)

while (t>=date(2020,5,24) and t<=date(2021,9,20)):
    print (t)
    
    ct = run_query('''
        SELECT count(*) as ct FROM max_dev.workspace.{table} 
        WHERE real_date = '{date}'
        '''.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = table_name
                        ))
    if ct.ct[0] == 0:
        query = qry.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = table_name
                        )
        print (query)
        df = run_query(query)
    else:
        pass
    t=t+ timedelta(days=1)