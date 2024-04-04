import os
import sys
import logging
import boto3
import itertools as it
import io
from utils import *
import snowflake.connector
import datetime
from datetime import timedelta
import numpy as np
from scipy.optimize import curve_fit

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', None)
pd.options.mode.chained_assignment = None  # default='warn'

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

def upload_to_sf(df): 

    # Write to S3
    
    bucket_name = 'hbo-outbound-datascience-content-dev'
    table_name = 'imdb_title_contributor_billing_logged'
    filename ='talent_data/' + table_name + '.csv'

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index = False)
    content = csv_buffer.getvalue()
    client = boto3.client('s3')
    client.put_object(Bucket=bucket_name, Key=filename, Body=content)
    print ('Write to S3 finished')
    
    run_query('''
        copy into max_dev.workspace.{table_name}
            from(
                select
                      $1, $2, $3, $4, $5, $6
                from @HBO_OUTBOUND_DATASCIENCE_CONTENT_DEV/talent_data/{table_name}.csv
                )
            file_format = (type = csv null_if=(''))
            on_error = 'CONTINUE'
        '''.format(table_name = table_name))
    print ('Uploaded to SF')
    

cast_order = run_query('''
SELECT titleid, category, billing as billingOrder, name, nameid
FROM max_dev.workspace.imdb_title_contributor_billing
where BILLING is not null
''')

def func(x, a, b, c):
    return a * np.exp(-b * x) + c

x = [1.0, 2.0, 6.0]
y = [1, 0.5, 0.1]

popt, pcov = curve_fit(func, x, y)

print (cast_order.head())

cast_order['logged_order'] = func(cast_order['billingorder'], *popt)

upload_to_sf(cast_order)