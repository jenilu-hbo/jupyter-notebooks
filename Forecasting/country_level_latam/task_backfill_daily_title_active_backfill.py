import os
import sys
from utils import *
from queries import backfill_title_actives_query
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

class backfill_daily_active_base:
    def __init__(self):
        pass

    def run_query(self, query):
        SF_CREDS = 'datascience-max-dev-sagemaker-notebooks'

        conn=SnowflakeConnector(SSMPSCredentials(SF_CREDS))
        ctx=conn.connect("MAX_PROD","DATASCIENCE_STAGE")
        cursor = ctx.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
        df.columns= df.columns.str.lower()
        return df


    def run(self):
        table_name = 'latam_country_title_level_actives_2'
#         max_end_date = self.run_query('''
#                         SELECT max(end_date) as max_date 
#                         FROM max_dev.workspace.{table}
#                         '''.format(
#                                         table = table_name
#                                         ))
        max_end_date = '2021-08-20'
        max_date = '2021-11-15'

        t=pd.to_datetime(max_end_date)
        
        while (t.strftime('%Y-%m-%d')>=max_end_date) and (t.strftime('%Y-%m-%d')<max_date.strftime('%Y-%m-%d')):
            for i in range(1, 30):
                start_date = (t - timedelta(days=i)).strftime('%Y-%m-%d')
                print ('start_date:' + start_date)
                end_date = t.strftime('%Y-%m-%d')
                print ('end_date:' + end_date)
                if start_date >= '2021-06-29':
                    ct = self.run_query('''
                        SELECT count(*) as ct 
                        FROM max_dev.workspace.{table}
                        WHERE available_date = '{start_date}'
                        and end_date = '{end_date}'
                        '''.format(
                                        table = table_name,
                                        start_date = start_date,
                                        end_date = end_date
                                        ))
                    if ct.ct[0] == 0:
                        query = backfill_title_actives_query.format(
                                        start_date = start_date,
                                        end_date = end_date,
                                        table = table_name
                                        )
                        print (query)
                        df = self.run_query(query)
                    else:
                        pass
            t=t+ timedelta(days=1)
            
            
if __name__ == '__main__':
    run_task = backfill_daily_active_base()
    run_task.run()