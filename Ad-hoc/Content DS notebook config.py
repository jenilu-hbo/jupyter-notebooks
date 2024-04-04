#!/usr/bin/env python
# coding: utf-8

# # Sagemaker notebook Git, Snowflake and S3 set up

# ## Git
1. Generate a SSH public key on sagemaker & add it on github to allow access. 
    a. If there's no ~/.ssh/id_rsa, run `ssh-keygen -o`.  Navigate to ~./ssh and copy the key `cat id_rsa.pub`
    b. On github, go to settings-> SSH/GPG keys. Add new SSH key and paste in the key from a.
2. On your notebook instance, open terminal and clone your git repo (git clone ...). 
# ## S3
Search S3 on AWS and make a folder under /datascience-hbo-users/users. You can access this folder on Sagemaker as 's3://datascience-hbo-users/' directory. You can also use aws s3 commands for general file management ('aws s3 rm', 'aws s3 cp', etc. 

e.g. 
Read file :  pd.read_csv('s3://datascience-hbo-users/users/tjung/test_s.csv')
Delete file:  !aws s3 rm s3://datascience-hbo-users/users/tjung/test.csv
# ## Snowflake

# In[1]:


## Run the following pip install commands and restart the notebook kernel 
get_ipython().system('pip install snowflake --user')
get_ipython().system('pip install snowflake-connector-python --user')


# In[2]:


###### import pandas as pd
import json
import snowflake.connector
from abc import ABCMeta, abstractmethod
import boto3

## Limit Size of Returned Records
MAX_QUERY_RETURN_SIZE = 1000000

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
    


# In[3]:


## Credentials
SF_CREDS = 'datascience-max-dev-sagemaker-notebooks'

## Snowflake connection 
conn=SnowflakeConnector(SSMPSCredentials(SF_CREDS))
ctx=conn.connect("MAX_PROD","DATASCIENCE_STAGE")
cur = ctx.cursor()


# In[5]:


# Execute a statement that will generate a result set.
querystr='''
    select *
    from max_prod.content_intelligence.future_programming_schedule
    limit 2
'''
cur.execute(querystr)
# Fetch the result set from the cursor and deliver it as the Pandas DataFrame.

colstring = ','.join([col[0] for col in cur.description])
df = pd.DataFrame(cur.fetchall(), columns =colstring.split(","))
display(df)

df.to_csv('test.csv')

