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
        
        SELECT dt.start_date, dt.end_date
        ,DATEDIFF('DAY', dt.start_date::DATE, dt.end_date::DATE) as DAYS_ON_HBO_MAX
        , count(distinct up.hbo_uuid) as subs
        FROM MAX_PROD.CONTENT_ANALYTICS.CALENDAR_DATE_TRACKER dt
        JOIN max_prod.identity.user_profile_dim_current up 
        JOIN max_dev.workspace.user_retain_churn_list_test c
            on c.hurley_user_id = up.hurley_user_id
            and c.cycle_expire_date >= dt.start_date
            and c.cycle_start_date <= dt.end_date
        WHERE END_DATE <= LEAST('{end_date}', '2023-05-01')
        and START_DATE = '{start_date}'
        and MOD(DATEDIFF('DAY', dt.start_date::DATE, dt.end_date::DATE), 30) = 0
        AND DATEDIFF('DAY', dt.start_date::DATE, dt.end_date::DATE) > 182
        GROUP BY 1,2,3
        
'''

table = 'user_title_hours_watched_subs'
t =  pd.to_datetime('2022-10-04')

while (t>=pd.to_datetime('2020-05-01') and t<=pd.to_datetime('2023-03-06')):
    print (t)
    query = qry.format(
                    start_date= t.strftime('%Y-%m-%d'),
                    end_date = (t + pd.DateOffset(days=821)).strftime('%Y-%m-%d') , 
                    table = table
                    )
    print (query)
    df = run_query(query)

    t=t+ pd.DateOffset(days=1)