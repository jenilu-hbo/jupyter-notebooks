import os
import sys
from utils import *
from queries_daily import back_fill_base_query
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
        table_name = 'latam_country_metrics_base'
#         max_end_date = self.run_query('''
#                         SELECT max(offering_start_date) as max_date 
#                         FROM max_dev.workspace.{table}
#                         where offering_start_date < '2021-11-15'
#                         '''.format(
#                                         table = table_name
#                                         ))
#         max_end_date = max_end_date.max_date.values[0]
        max_end_date = pd.to_datetime('2021-08-28')

        max_date = pd.to_datetime('2021-09-25')
        t=pd.to_datetime(max_end_date)
        
        while (t.strftime('%Y-%m-%d')>=max_end_date.strftime('%Y-%m-%d')) and (t.strftime('%Y-%m-%d')<max_date.strftime('%Y-%m-%d')):
            for i in range(1, 30):
                start_date = (t - timedelta(days=i)).strftime('%Y-%m-%d')
                print ('start_date:' + start_date)
                end_date = t.strftime('%Y-%m-%d')
                print ('end_date:' + end_date)
                if start_date >= '2021-06-29':
                    ct = self.run_query('''
                        SELECT count(*) as ct 
                        FROM max_dev.workspace.{table}
                        WHERE offering_start_date = '{start_date}'
                        and days_on_hbo_max = {days_on_hbo_max}
                        '''.format(
                                        table = table_name,
                                        start_date = start_date,
                                        days_on_hbo_max = i
                                        ))
                    if ct.ct[0] == 0:
                        query = back_fill_base_query.format(
                                        start_date = start_date,
                                        days_on_hbo_max = i,
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