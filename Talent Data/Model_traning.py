import os
import sys
import logging
import boto3
import itertools as it
import io
from utils import *
import snowflake.connector

from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
from sklearn.model_selection import GroupKFold
import datetime
from datetime import timedelta
import scipy.stats as st

from sklearn import datasets, linear_model
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor

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

imdb_votes = run_query('''
 WITH
      imdb_contributor AS (
          SELECT distinct name as contributor_name, 
                 t.title_id, c.category,
                 t.ORIGINAL_TITLE, t.TITLE_TYPE,
                 production_release_date,
                 YEAR as release_year,
                c.billingorder, c.logged_order,
                 max(number_of_votes) as number_of_votes,
                 max(imdb_rating) as imdb_rating
          FROM max_prod.workspace.imdb_title_contributor_billing_logged c
          JOIN "ENTERPRISE_DATA"."CATALOG"."IMDB_TITLE" t  ON c.titleid = t.title_id
          where 1=1
          and title_type IN ('tvEpisode', 'movie')
          and category NOT IN ('archive_footage')

          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
      ),
      yearly_sum as (
      select YEAR as release_year, sum(number_of_votes) as yearly_sum
      from
      "ENTERPRISE_DATA"."CATALOG"."IMDB_TITLE"
      GROUP BY YEAR
      having yearly_sum > 0
      )

      SELECT c.contributor_name, c.ORIGINAL_TITLE as title_name, c.category, c.title_type,
             number_of_votes, yearly_sum,
             number_of_votes/yearly_sum * 10000 as standardized_votes,
             imdb_rating,
             production_release_date,
             c.release_year,
             billingOrder,
             number_of_votes/yearly_sum *logged_order * 10000  AS standardized_score
      FROM imdb_contributor c
      JOIN yearly_sum s ON c.release_year = s. release_year
      order by STANDARDIZED_SCORE desc

''')

train_data = imdb_votes[['contributor_name','title_name' ,'release_year','billingorder', 'standardized_score']]
data = train_data.copy()
data['release_year'] = data['release_year'].astype(int)
data['billingorder'] = data['billingorder'].astype(str)
data = data.groupby(['contributor_name', 'release_year']).sum().reset_index()
data1 = data[data['release_year'] > 2000]
data2 = data[data['release_year'] > 1997]
data2.columns = ['contributor_name', 'release_year_before', 'standardized_score_before_']
data = pd.merge(data1, data2, on = ['contributor_name'], how = 'outer')
data=data[(data['release_year'] > data['release_year_before'])
         &(data['release_year'] - 3 <= data['release_year_before'])]
data['years_before'] = data['release_year_before'] - data['release_year']
data['years_before'] = data['years_before'].astype(int).astype(str)
data = data.pivot(index=['contributor_name', 'release_year', 'standardized_score'], 
           columns='years_before', values=['standardized_score_before_']).reset_index()
data.columns = [''.join(col).strip() for col in data.columns.values]
data_order = train_data.copy()
data_order = data_order.groupby(['contributor_name', 'release_year', 'billingorder']).count().reset_index()
data_order = data_order.drop(['title_name'], axis = 1).rename(columns = {'standardized_score': 'count_billingorder_'})
data_order['billingorder'] = data_order['billingorder'].astype(str)
data_order = data_order[(data_order['billingorder'].astype(int)<=3)].pivot(index=['contributor_name', 'release_year'], 
           columns='billingorder', values=['count_billingorder_']).reset_index()
data_order.columns = [''.join(col).strip() for col in data_order.columns.values]
data_order['release_year'] = data_order['release_year'].astype(int)
final_data = pd.merge(data, data_order, how = 'left', on = ['contributor_name', 'release_year'])
final_data = final_data.fillna(0)



year = 2020
feature_cols = ['standardized_score_before_-1', 'standardized_score_before_-2', 'standardized_score_before_-3', 
                'count_billingorder_1', 'count_billingorder_2', 'count_billingorder_3']
target_col = ['standardized_score']

train_X = final_data[final_data['release_year'] < year][feature_cols]
train_Y = final_data[final_data['release_year'] < year][target_col]
test_X = final_data[final_data['release_year'] == year][feature_cols]
test_Y = final_data[final_data['release_year'] == year][target_col]


def predict_score(train, test):
    train_X = train[feature_cols]
    train_Y = train[target_col]
    test_X = test[feature_cols]
    test_Y = test[target_col]
    
    model_rf = RandomForestRegressor(n_estimators = 1000, random_state = 42)
    model_rf.fit(train_X, train_Y)
    prediction_rf = model_rf.predict(test_X)
    
    test['prediction'] = prediction_rf
    test['mape'] = abs(test['prediction']-test['standardized_score'])/test['standardized_score']
    return test

start_year = final_data.release_year.astype(int).min()+3
end_year = final_data.release_year.astype(int).max() + 1

train_df = pd.DataFrame()
for year in range(start_year, end_year):
    print ('cross validation for year: ' + str(year))
    train = final_data[(final_data['release_year'] < year)
                      &(final_data['release_year'] >= year - 3)]
    test = final_data[final_data['release_year'] == year]
    
    output = predict_score(train, test)
    train_df = pd.concat([train_df, output])
    
train_df.to_csv('train_df.csv')
