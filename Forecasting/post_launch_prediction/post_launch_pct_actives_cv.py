import os
import sys
import io
import logging
from utils import *
from queries import *
from model import *
from task_backfill_daily_active_base import backfill_daily_active_base
import snowflake.connector
from datetime import timedelta
from sklearn.model_selection import GroupKFold
import datetime

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

class post_launch_cross_validation:
    def __init__(self):
        self.metadata_feature = None
        self.popcorn_titles = None
        self.pct_actives = None


    def run_query(self, query, dbname, schema):
        SF_CREDS = 'datascience-max-dev-sagemaker-notebooks'

        conn=SnowflakeConnector(SSMPSCredentials(SF_CREDS))
        ctx=conn.connect(dbname,schema)
        cursor = ctx.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
        df.columns= df.columns.str.lower()
        return df
    
    def cvdf_to_snowflake(self, df, table_name):
        stage = '@HBO_OUTBOUND_DATASCIENCE_CONTENT_DEV'
        output_bucket = "hbo-outbound-datascience-content-dev"
        filename ='pct_actives_prediction/' + table_name + '.csv'
        dbname, schema = 'MAX_DEV', 'WORKSPACE'
        
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index = False)
        content = csv_buffer.getvalue()
        client = boto3.client('s3')
        client.put_object(Bucket=output_bucket, Key=filename, Body=content)
        
        print ('Create Table: ' + table_name)
        self.run_query('''
        create or replace table {table_name}(
        match_id varchar,
        title varchar,
        title_id varchar, 
        available_date varchar,
        originals_type varchar,
        content_category varchar,
        prediction_start_date varchar,
        real_date varchar,
        prediction_start_day int,
        days_after_launch int,
        actuals float,
        prediction float
        )
        '''.format(table_name = table_name), dbname, schema)
        
        print ('Begin Uploading')
        self.run_query('''
        insert into MAX_DEV.workspace.{table_name}
        
        select 
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        from {stage}/pct_actives_prediction/{file_name}
        
         (FILE_FORMAT => csv_v2)

        '''.format(stage = stage, table_name = table_name,
                  file_name = table_name+'.csv')
                , dbname, schema)
        
        print ('Finish Uploading')    
    
    
    def get_metadata(self):
        logger = logging.getLogger()
        logger.info(f'Loading inputs')
        data_list ={}

        s3 = boto3.resource('s3')
        bucket = s3.Bucket('hbo-ingest-datascience-content-dev')

        for obj in bucket.objects.filter(Prefix='input_percent_view'):
            key = obj.key
            logger.info('Loading csv file {}'.format(key))
            body = obj.get()['Body']
            var_name = key.split('.')[0].split('/')[1]
            print('Reading {0} features'.format(var_name))
            df = pd.read_csv(body, na_values = [np.NaN])
            df.columns = df.columns.str.lower()

            df = df.loc[:, df.isnull().sum()!=df.shape[0]]

            df = df.loc[df['match_id_platform'].\
            isin(['1-GYGQBcwsaCIW2XgEAAAAL', '0-GYGQBcwsaCIW2XgEAAAAL'])==False,:]\
            .reset_index(drop = True)

            data_list[var_name] = df
        metadata_feature=data_list['metadata_feature']
        self.metadata_feature = metadata_feature

        popcorn_titles = self.run_query('''
                                        SELECT * FROM MAX_PROD.CATALOG.POPCORN_TITLES
                                        ''', 
                                        "MAX_PROD","DATASCIENCE_STAGE")
        self.popcorn_titles = popcorn_titles


    def get_active_data(self):
        daily_total_views = self.run_query(daily_total_views_query, "MAX_PROD", "DATASCIENCE_STAGE")
        title_actives = self.run_query(title_actives_query, "MAX_PROD", "DATASCIENCE_STAGE")

        title_actives.drop_duplicates(inplace = True)
        title_actives['available_date'] = title_actives['first_release_date'].astype(str).str[0:10:1]

        pct_actives = pd.merge(title_actives[['match_id', 'title', 'title_id', 'days_on_hbo_max', 'available_date', 'cumulative_viewing_subs']],
                      daily_total_views[['start_date', 'end_date', 'cumulative_viewing_subs_denom', 'days_after_launch']],
                      left_on = ['available_date', 'days_on_hbo_max'], right_on = ['start_date', 'days_after_launch'],
                      how = 'inner')
        pct_actives['pct_actives'] = pct_actives['cumulative_viewing_subs']/pct_actives['cumulative_viewing_subs_denom']*100     

        metadata_feature = self.metadata_feature.groupby(['match_id']).first().reset_index()
        pct_actives['match_id'] = pct_actives['match_id'].astype(str)
        metadata_feature['match_id'] = metadata_feature['match_id'].astype(str)
        pct_actives=pd.merge(pct_actives,
                      metadata_feature.rename(columns = {'title_name':'id'}),
                      on = ['match_id'],how = 'left')

        recent_originals = pct_actives[(pct_actives['program_type'] == 'original')
                         &(pct_actives['prod_release_year'] >= 2020)
                         &(pct_actives['platform_name'] == 1)
                         ].copy()

        popcorn_titles = pd.merge(pct_actives,  self.popcorn_titles[['viewable_id']],
                         left_on = ['match_id'], right_on = ['viewable_id']).copy()

        recent_originals['originals_after_launch'] = 1
        popcorn_titles['popcorn_titles'] = 1
        recent_originals.drop_duplicates(inplace = True)
        popcorn_titles.drop_duplicates(inplace = True)
        pct_actives = pd.merge(pct_actives, recent_originals[['match_id', 'originals_after_launch', 'days_on_hbo_max', 'available_date']],
                        on = ['match_id', 'days_on_hbo_max', 'available_date'], how = 'left')
        pct_actives = pd.merge(pct_actives, popcorn_titles[['match_id', 'popcorn_titles', 'days_on_hbo_max', 'available_date']],
                                on = ['match_id', 'available_date', 'days_on_hbo_max'], how = 'left')

        pct_actives.loc[pct_actives['originals_after_launch'] == 1, 'originals_type'] = 'originals_after_launch'
        pct_actives.loc[pct_actives['popcorn_titles'] == 1, 'originals_type'] = 'popcorn_titles'
        pct_actives['originals_type'] = pct_actives['originals_type'].fillna(pct_actives['program_type'])
        pct_actives = pct_actives.drop(['originals_after_launch', 'popcorn_titles'], axis = 1)
        pct_actives['real_date'] = (pd.to_datetime(pct_actives['available_date']) +
                                    pd.to_timedelta(pct_actives['days_on_hbo_max'], unit='D'))
        self.pct_actives = pct_actives

    def run(self, kpi):
        print ('backfill base task')
#         pda = backfill_daily_active_base()
#         pda.run()
        print ("get metadata task")
        self.get_metadata()
        print ("get pct_actives data task")
        self.get_active_data()
        print ("data size: " + str(self.pct_actives.shape[0]))

        kpi = kpi

        pct_actives = self.pct_actives
        data_train_all = pct_actives[(pct_actives['originals_type'] == 'originals_after_launch')
                               |(pct_actives['originals_type'] == 'popcorn_titles')].copy()

        data_train_all.rename(columns={"days_after_launch": "prediction_start_day"}, inplace=True)
        data_train_all['real_date'] = data_train_all['real_date'].map(str).map(lambda x: x[:10])
        data_train_all['available_date'] = data_train_all['available_date'].map(str).map(lambda x: x[:10])

        # logger.info("nrow(features): " + str(len(data_train_all.index)))
        print("nrow(features): " + str(len(data_train_all.index)))

        validation_set = pd.DataFrame()

        num_folds = len(data_train_all['match_id'].unique())
        group_kfold = GroupKFold(n_splits=num_folds)
        print (group_kfold)

        # Train
        for train_index, test_index in group_kfold.split(data_train_all, groups=data_train_all['match_id'].values):


            train_df, test_df = data_train_all.iloc[train_index], data_train_all.iloc[test_index]


            avail_date = test_df['available_date'].values[0]
            train_df = train_df[(train_df['available_date'] <= avail_date)]

            print("Validation Title: " + test_df['id'].values[0])

            # fit_predict decay model
            decay_model = DecayModel(kpi=kpi)
            decay_model.fit(train_df)
            pred = decay_model.predict(test_df)
            validation_set = pd.concat((validation_set, pred))

        validation_set = validation_set[validation_set['days_after_launch'].notnull()]
        validation_set.reset_index(drop=True, inplace=True)
        # post-process
        validation_set.rename(columns={'real_date': 'prediction_start_date'}, inplace=True)
        validation_set['real_date'] = pd.to_datetime(validation_set['available_date']
                                                    ).add(
            validation_set['days_after_launch'].map(lambda x: datetime.timedelta(x))
            ).map(str).map(lambda x: x[:10])

        validation_set = pd.merge(validation_set,
                                  data_train_all[
                                      ['match_id', 'real_date', 'season_number_adj'] + [TRACKING_COLUMN[kpi]]],
                                  on=['match_id', 'real_date'],
                                  how='left')

        validation_set.rename(columns={TRACKING_COLUMN[kpi]: 'actuals'}, inplace=True)

        validation_set = validation_set[['match_id',
                                         'title',
                                         'title_id',
                                         'available_date',
                                         'originals_type',
                                         'content_category',
                                         'prediction_start_date',
                                         'real_date',
                                         'prediction_start_day',
                                         'days_after_launch',
                                         'actuals',
                                         'prediction']]
#         validation_set.to_csv('validation_set.csv')

        self.cvdf_to_snowflake(validation_set, 'pct_actives_cross_validation')
        


if __name__ == '__main__':
    cv_task = post_launch_cross_validation()
    cv_task.run(kpi = 'pct_actives')

