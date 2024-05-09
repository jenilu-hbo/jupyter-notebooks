import sys
import logging
import boto3
import itertools as it
import io
from utils import *
import snowflake.connector
import os

import gzip
import json

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

input_bucket = 'hbo-ingest-content-metadata'
s3 = boto3.resource('s3')
bucket = s3.Bucket(input_bucket)

def upload_to_sf(df): 

    # Write to S3
    
    bucket_name = 'hbo-outbound-datascience-content-dev'
    table_name = 'imdb_title_contributor_billing_2'
    filename ='talent_data/' + table_name + '.csv'

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index = False)
    content = csv_buffer.getvalue()
    client = boto3.client('s3')
    client.put_object(Bucket=bucket_name, Key=filename, Body=content)
    
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
#     print ('Uploaded to SF')


# def load_data(obj, ln_num):
#     key = obj.key
#     body = obj.get()['Body']
#     count = 1
#     with gzip.GzipFile(fileobj = body) as gzipfile:
#         try:
#             for line in gzipfile:
#                 json_file = json.loads(line)
#                 if count >= ln_num and 'filmography' in json_file.keys():
#                     df = pd.DataFrame(json_file['filmography'])
#                     df['name'] = json_file['name']
#                     df['nameId'] = json_file['nameId']
#                     df['imdbUrl'] = json_file['imdbUrl']
#                     if 'billing' not in df.columns:
#                         df['billing'] = np.nan
#                     if len(df)>0:
#                         df = df[['titleId', 'category', 'billing', 'name', 'nameId', 'imdbUrl']]
#                         print (count)
#                         upload_to_sf(df)
#                 count += 1
               
#         except Exception as e:
#             error_line = count
#             print (e)
#             print ('Error at line: ')
#             print (count)
#             return error_line
#     return -1
    
# for obj in bucket.objects.filter(Prefix='IMDB_Essential/imdb_name_essentials/yr=2022/mo=01/dt=2022-01-04/name_essential_v1_complete.jsonl.gz'):

#     file_ln = 10000
#     finish = 1

#     while 1==1:
#         finish = load_data(obj, file_ln)
#         if finish == -1:
#             break
#         else:
#             file_ln = finish
    
for obj in bucket.objects.filter(Prefix='IMDB_Essential/imdb_name_essentials/yr=2022/mo=01/dt=2022-01-04/name_essential_v1_complete.jsonl.gz'):    
    body = obj.get()['Body']
    count = 1  
    final_df = pd.DataFrame()
    
    with gzip.GzipFile(fileobj = body) as gzipfile:
        try:
            for line in gzipfile:
                line = line
                json_file = json.loads(line)

                if count >= 7060000 and 'filmography' in json_file.keys():

#                     ct = run_query('''
#                         SELECT COUNT(*) AS count_num
#                         FROM max_dev.workspace.imdb_title_contributor_billing_2
#                         where name = $${}$$
#                         and titleId = '{}'
#                         '''.format(json_file['name'],
#                                   json_file['filmography'][0]['titleId']))
#                     if ct.count_num[0] == 0:   
                    df = pd.DataFrame(json_file['filmography'])
                    df['name'] = json_file['name']
                    df['nameId'] = json_file['nameId']
                    df['imdbUrl'] = json_file['imdbUrl']
                    if 'billing' not in df.columns:
                        df['billing'] = np.nan
                    if len(df)>0:
                        df = df[['titleId', 'category', 'billing', 'name', 'nameId', 'imdbUrl']]
                        final_df = pd.concat([final_df, df])
#                     print ('new record at: ' + str(count))
                if count%10000 == 0 and len(final_df) > 0:
#                     print (count) 
                    print (final_df.shape)
                    upload_to_sf(final_df)
                    final_df = pd.DataFrame()
                count += 1
        except:
            raise Exception('Errors at line: ' + str(count))