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
            and seq_date >= '2020-05-23'
            ), 

        date_range AS 
            (SELECT 
                d1.seq_date as start_date,
                d2.seq_date as end_date
            from date d1
            join date d2
            ON d2.seq_date >= d1.seq_date
            and d2.seq_date <= dateadd(day, 28, d1.seq_date)
            where d1.seq_date = '{date}'
             )

        SELECT 
            dr.start_date, dr.end_date,
            count(distinct hbo_uuid) as total_viewing_accounts
        from max_prod.viewership.max_user_stream_heartbeat a
         left join date_range dr on 1=1
         left join max_prod.catalog.reporting_asset_dim b on a.viewable_id=b.viewable_id
         left join max_prod.core.geo_map c on a.country_iso_code=c.country_iso_code
         left join max_prod.catalog.reporting_asset_offering_dim d on a.viewable_id=d.viewable_id and brand='HBO MAX' and c.territory=d.territory and d.channel='HBO MAX SUBSCRIPTION'
        where a.stream_elapsed_play_seconds>=120
         and first_offered_date<=a.request_time_gmt
         and region='NORTH AMERICA'
         and asset_type='FEATURE'
         and video_type='main'
         and a.request_date between dr.start_date and dr.end_date
        group by 1, 2
        order by 1, 2;
'''

t=date(2021,1,1)

while (t>=date(2020,5,23) and t<date(2021,9,20)):
    print (t)
    ct = run_query('''
        SELECT count(*) as ct FROM max_dev.workspace.{table} 
        WHERE start_date = '{date}'
        '''.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = 'activse_base_28days'
                        ))
    if ct.ct[0] == 0:
        query = qry.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = 'activse_base_28days'
                        )
        print (query)
        df = run_query(query)
    else:
        pass
    t=t+ timedelta(days=1)