import os
import sys
from utils import *
from queries import back_fill_base_query
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
        start_date = self.run_query('''
                        SELECT max(start_date) as max_date FROM max_dev.workspace.{table}
                        '''.format(
                                        table = 'actives_base_first_view'
                                        ))
        max_date = pd.to_datetime(start_date.max_date.values[0]) - timedelta(days=28)
        print ('delete unfinished dates')
        print ('curret_date: ' + str(start_date.max_date[0]))
        print ('start_date: ' + str(max_date))
        self.run_query('''
                DELETE FROM max_dev.workspace.{table}
                WHERE start_date >= '{max_date}'
                '''.format(
                                table = 'actives_base_first_view',
                                max_date = max_date
                                ))
        end_date = pd.to_datetime("now") - timedelta(days=1)

        t=max_date

        while (t.strftime('%Y-%m-%d')>=max_date.strftime('%Y-%m-%d') and t.strftime('%Y-%m-%d')<end_date.strftime('%Y-%m-%d')):
            print (t)
            ct = self.run_query('''
                SELECT count(*) as ct FROM max_dev.workspace.{table}
                WHERE start_date = '{date}'
                '''.format(
                                date=t.strftime('%Y-%m-%d'),
                                table = 'actives_base_first_view'
                                ))
            if ct.ct[0] == 0:
                query = back_fill_base_query.format(
                                date=t.strftime('%Y-%m-%d'),
                                table = 'actives_base_first_view'
                                )
                print (query)
                df = self.run_query(query)
            else:
                pass
            t=t+ timedelta(days=1)