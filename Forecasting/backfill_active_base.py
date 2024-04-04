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
insert into {database}.{schema}.actives_base_first_view
    WITH denom_first_viewed as (
      select
        '{start_date}' as start_date,
        '{end_date}' as end_date,
        count (distinct hbo_uuid) as subs_count
        from max_prod.viewership.max_user_stream_distinct_user_dates_heartbeat udh
        WHERE udh.LOCAL_REQUEST_DATE >= '{start_date}' and udh.LOCAL_REQUEST_DATE < dateadd(day, 1, date('{end_date}'))
        group by 1,2
      ),

   yesterday as (
   select start_date, cumulative_viewing_subs_denom as yesterday_denom
   from max_dev.workspace.actives_base_first_view
   where start_date = '{start_date}'
   and end_date  = dateadd(day, -1, date('{end_date}'))
   )

    select
      d.start_date, end_date,
      subs_count - (case when yesterday_denom is null then 0 else yesterday_denom end) as daily_viewing_subs_denom,
      subs_count as cumulative_viewing_subs_denom
    from denom_first_viewed d
    left join yesterday y
    on d.start_date = y.start_date
'''


query_windows = '''SELECT max(end_date) as max_date
                   FROM {database}.{schema}.actives_base_first_view'''\
                .format(database='max_dev'
                           ,schema='workspace')

max_end_date = run_query(query_windows)

max_end_date = pd.to_datetime(max_end_date.max_date.values[0])
max_date = pd.to_datetime("now")

t=max_end_date

while (t.strftime('%Y-%m-%d')>=max_end_date.strftime('%Y-%m-%d') \
   and t.strftime('%Y-%m-%d')< max_date.strftime('%Y-%m-%d')):
    for i in range(0, 29):
        start_date = (t - timedelta(days=i)).strftime('%Y-%m-%d')
        print ('start_date:' + start_date)
        end_date = t.strftime('%Y-%m-%d')
        print ('end_date:' + end_date)
        query_count = '''SELECT count(*) as ct FROM {database}.{schema}.actives_base_first_view
                                    WHERE start_date = '{start_date}'
                                    and end_date = '{end_date}'
                      '''.format(start_date=start_date,
                                 end_date=end_date,
                                 database='max_dev',
                                 schema='workspace')

        ct = run_query(query_count)

        if ct.ct[0] == 0:
            print (qry.format(start_date=start_date,
                                 end_date=end_date,
                                 database='max_dev',
                                 schema='workspace'))
            run_query(qry.format(start_date=start_date,
                                 end_date=end_date,
                                 database='max_dev',
                                 schema='workspace'))
        else:
            pass
    t=t+ timedelta(days=1)