import boto3
import datetime as dt
from pandas import DateOffset
import json
import numpy as np
import pandas as pd
import snowflake.connector
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format

from abc import ABCMeta, abstractmethod

class Credentials(metaclass=ABCMeta):
    pass
    
    
class SSMPSCredentials(Credentials):
    def __init__(self, secretid: str):
        self._secretid = secretid
        self._secrets = {}
        
    def get_keys(self):
        """
        credential fetching 
        """
        _aws_sm_args = {'service_name': 'secretsmanager', 'region_name': 'us-east-1'}
        secrets_client = boto3.client(**_aws_sm_args)
        get_secret_value_response = secrets_client.get_secret_value(SecretId=self._secretid)
        return get_secret_value_response
    
    
class BaseConnector(metaclass=ABCMeta):
    @abstractmethod
    def connect(self):
        raise NotImplementedError
    

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
ctx=conn.connect("MAX_DEV","WORKSPACE")

def run_query(query):
    cursor = ctx.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
    df.columns= df.columns.str.lower()
    return df
    
qry = '''
        INSERT INTO max_dev.workspace.{table}
        with streaming_subset as
        (   SELECT
              request_date
            , hb.hbo_uuid
            , hb.viewable_id
            , sum(hb.stream_elapsed_play_seconds/3600) as hours_viewed
        from max_prod.viewership.max_user_stream_heartbeat hb
        where 1=1
            and hb.request_date between '{start_date}' and '{end_date}'
            and hb.viewable_id IS NOT NULL 
            and hb.stream_elapsed_play_seconds >= 900
            and hb.video_type = 'main'
            and hb.channel = 'HBO MAX SUBSCRIPTION'
            and hb.country_iso_code in ('US')
            and hb.is_primary_profile=1
        group by 1,2,3
        )

        SELECT 
              request_date
            , ss.viewable_id
            , sum(hours_viewed) as hours_viewed
        FROM streaming_subset ss
        JOIN (
             SELECT DISTINCT tt.hbo_uuid
             FROM max_dev.workspace.user_retain_churn_list_test tt
             where '{end_date}' >= cycle_start_date 
             and '{start_date}' <= cycle_expire_date) c
        ON   c.hbo_uuid = ss.hbo_uuid
        group by 1, 2
'''

table = 'user_title_hours_watched_test2'
t =  pd.to_datetime('2022-01-01')

while (t>=pd.to_datetime('2020-05-01') and t<=pd.to_datetime('2023-03-06')):
    print (t)
    query = qry.format(
                    start_date=t.strftime('%Y-%m-%d'),
                    end_date = (t + pd.DateOffset(months=1)- pd.DateOffset(days=1)).strftime('%Y-%m-%d'),
                    table = table
                    )
    print (query)
    df = run_query(query)

    t=t+ pd.DateOffset(months=1)