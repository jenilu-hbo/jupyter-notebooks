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

start_date = run_query('''
                SELECT max(start_date) as max_date FROM max_dev.workspace.{table}
                '''.format(
                                table = 'actives_base_first_view'
                                ))
max_date = pd.to_datetime(start_date.max_date.values[0]) - timedelta(days=28)
print ('delete unfinished dates')
print ('curret_date: ' + str(start_date.max_date[0]))
print ('start_date: ' + str(max_date))
# run_query('''
#         DELETE FROM max_dev.workspace.{table}
#         WHERE start_date >= '{max_date}'
#         '''.format(
#                         table = 'actives_base_first_view',
#                         max_date = max_date
#                         ))
end_date = pd.to_datetime("now") - timedelta(days=1)

t=max_date

while (t>=max_date and t<=end_date):
    print (t)
    ct = run_query('''
        SELECT count(*) as ct FROM max_dev.workspace.{table} 
        WHERE start_date = '{date}'
        '''.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = 'actives_base_first_view'
                        ))
    if ct.ct[0] == 0:
        query = qry.format(
                        date=t.strftime('%Y-%m-%d'),
                        table = 'actives_base_first_view'
                        )
        print (query)
        df = run_query(query)
    else:
        pass
    t=t+ timedelta(days=1)