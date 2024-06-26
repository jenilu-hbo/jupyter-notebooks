# Databricks notebook source
# general
import datetime as dt
import json
import numpy as np
import pandas as pd

# COMMAND ----------

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)

# COMMAND ----------

pd.options.display.float_format = '{:,.4f}'.format
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Read Training Data

# COMMAND ----------

validation_data = spark.sql("SELECT * FROM bolt_cus_dev.bronze.cip_title_segment_viewership_training_data").toPandas()

# COMMAND ----------

###### Manually add hit_series tag #########
manual_pop_title = ['Euphoria', 'House of the Dragon', 'Peacemaker', 'The Last of Us']

validation_data.loc[validation_data['title_name'].isin(manual_pop_title), 'medal_number'] = 0
validation_data['hit_series'] = 0
validation_data.loc[validation_data['title_name'].isin(manual_pop_title), 'hit_series'] = 1
validation_data.loc[(validation_data['title_name'] == 'Euphoria')&(validation_data['season_number'] == 1),
                   'hit_series'] = 0
validation_data.loc[(validation_data['title_name'] == 'Euphoria')&(validation_data['season_number'] == 1),
                   'medal_number'] = 3

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Modeling - Cross Validation

# COMMAND ----------

META_COLS = ['title_name', 'ckg_series_id', 'season_number', 'offering_start_date', 'air_date','asset_run_time_hours', 'content_category', 'program_type', 'medal', 'genre']

# COMMAND ----------

validation_data.head(2)

# COMMAND ----------

FEATURE_COLS = [
                'is_pay_1', 
                # 'is_popcorn', 
                # 'hit_series',
                'is_new_content', 
                'medal_number', 
                'age_of_content', 
#                 'content_cost', #XX
                # 'budget', 
                'content_category_live', 
                'content_category_movie', 
                'content_category_series', 
                'program_type_Acquired', 
                'program_type_Original', 
                'genre_Action','genre_AdultAnimation', 'genre_Adventure_Survival', 'genre_Comedy', 'genre_Crime',
                'genre_Fantasy_Sci-Fi', 'genre_Drama', 'genre_Events', 'genre_Documentaries', 'genre_Horror', 
                'genre_Food_Home','genre_Kids_Family', 'genre_News', 'genre_NotAGenre', 'genre_Paranormal', 'genre_Reality', 'genre_Science_Nature', 'genre_Sports'
               ]

# COMMAND ----------

def cal_error(validation_set):
    error_col = ((validation_set['pred']-validation_set[TARGET_COL[0]]).abs()/validation_set[TARGET_COL[0]]).abs()
    return error_col.mean()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 percent_viewing_subs

# COMMAND ----------

# !pip install pygam
# !pip install xgboost

# COMMAND ----------

import xgboost  
from xgboost import XGBRegressor
from sklearn.model_selection import GroupKFold

# COMMAND ----------

training_data.sort_values(by = ['percent_viewing_subs'], ascending=False).head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.1 Significant title classifier

# COMMAND ----------

seg = 'Action Movies Watchers'

# COMMAND ----------

training_data = validation_data.copy()

# COMMAND ----------

training_data = training_data[training_data['entertainment_segment_lifetime'] == seg]

# COMMAND ----------

training_data[training_data['title_name'] == 'Peacemaker']

# COMMAND ----------

training_data['is_significant_title'] = 1
training_data.loc[training_data['percent_viewing_subs'] < 0.05, 'is_significant_title'] = 0

# COMMAND ----------

n_sample = len(training_data[training_data['is_significant_title'] == 1])
down_sample = training_data[training_data['is_significant_title'] == 0].sample(n_sample)
training_data = pd.concat([training_data[training_data['is_significant_title']== 1], down_sample], axis = 0)

# COMMAND ----------

n_sample

# COMMAND ----------

len(training_data[training_data['is_significant_title'] == 1])/len(training_data)

# COMMAND ----------

training_data[FEATURE_COLS].isnull().sum()/len(training_data)
## Checking if there are any nulls in the feature columns

# COMMAND ----------

training_data['medal_number'] = training_data['medal_number'].fillna(3)
training_data['budget'] = training_data['budget'].fillna(0)
training_data['age_of_content'] = training_data['age_of_content'].fillna(100)

# COMMAND ----------

TARGET_COL = ['is_significant_title']

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

# COMMAND ----------

num_fold = 10
group_kfold = GroupKFold(n_splits=num_fold)

# COMMAND ----------

validation_set = pd.DataFrame()

for train_index, test_index in group_kfold.split(training_data, groups=training_data['ckg_series_id'].values):
    train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
#     print ("Validation Title: " + test_df['title'].values[0])
    try:
        ## fit_predict prelaunch model|
        X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
        y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]

        clf = LogisticRegression(random_state=0).fit(X_train, y_train)
        pred = clf.predict(X_test)
        pred = pd.DataFrame(pred)
        pred.columns = ['pred']
        test_df.reset_index(inplace = True, drop = True)
        test_df = pd.concat([test_df, pred], axis = 1)
        validation_set = pd.concat([validation_set, test_df], axis = 0)
        print (clf.score(X_train, y_train))

    except Exception as e:
        print (e)
        raise 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1.2 Modeling

# COMMAND ----------

### Impacting more than 5% of the audience would have a churn impact. ###### ====> Impacting at least one user
training_data = validation_data[(validation_data['percent_viewing_subs']>=0.05)]
# training_data = validation_data[(validation_data['content_category'].isin(['movies', 'series', 'live']))]
training_data = training_data[training_data['entertainment_segment_lifetime'] == seg]

# COMMAND ----------

len(training_data.ckg_series_id.unique())

# COMMAND ----------

from pygam import LinearGAM, s, PoissonGAM

# COMMAND ----------

training_data['log_percent_viewing_subs'] = np.log2(training_data['percent_viewing_subs'])

# COMMAND ----------

TARGET_COL = ['log_percent_viewing_subs']
ORIG_COL = ['percent_viewing_subs']

# COMMAND ----------

training_data[FEATURE_COLS].isnull().sum()/len(training_data)
## Checking if there are any nulls in the feature columns

# COMMAND ----------

training_data['medal_number'] = training_data['medal_number'].fillna(3)
# training_data['budget'] = training_data['budget'].fillna(0)

# COMMAND ----------

num_fold = 10
group_kfold = GroupKFold(n_splits=num_fold)

# COMMAND ----------

for i in FEATURE_COLS:
    if i not in (['budget', 'entertainment_segment_lifetime', 'age_of_content']):
        try:
            training_data[i] = training_data[i].astype(int, errors='ignore')
        except Exception as e:
            print (i)
            print (e)
    else:
        training_data[i] = training_data[i].astype(float, errors='ignore')

# COMMAND ----------

### BENCHMARK XGBOOST ####
validation_set = pd.DataFrame()
feature_importances = pd.DataFrame()
model = XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8)

for train_index, test_index in group_kfold.split(training_data, groups=training_data['ckg_series_id'].values):
    train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
#     print ("Validation Title: " + test_df['title'].values[0])
    try:
        ## fit_predict prelaunch model|
        X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
        y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]

        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        pred = pd.DataFrame(pred)
        pred.columns = ['pred']
        test_df.reset_index(inplace = True, drop = True)
        test_df = pd.concat([test_df, pred], axis = 1)
        validation_set = pd.concat([validation_set, test_df], axis = 0)

        fi = model.feature_importances_
        fi_df = pd.DataFrame([fi])
        fi_df.columns = FEATURE_COLS
        feature_importances=pd.concat([feature_importances, fi_df], axis = 0)

    except Exception as e:
        print (e)
        raise 


# COMMAND ----------

# ### DOWNSIDE --- DO NOT TAKE NULL ####
# validation_set = pd.DataFrame()
# # feature_importances = pd.DataFrame()
# # gam = LinearGAM(s(0))

# for train_index, test_index in group_kfold.split(training_data, groups=training_data['ckg_series_id'].values):
#     train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
# #     print ("Validation Title: " + test_df['title'].values[0])
#     try:
#         ## fit_predict prelaunch model|
#         X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
#         y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]

#         gam = LinearGAM(n_splines=20).fit(X_train, y_train)
#         pred = gam.predict(X_test)
#         pred = pd.DataFrame(pred)
#         pred.columns = ['pred']
#         test_df.reset_index(inplace = True, drop = True)
#         test_df = pd.concat([test_df, pred], axis = 1)
#         validation_set = pd.concat([validation_set, test_df], axis = 0)

#     except Exception as e:
#         print (e)
#         raise 


# COMMAND ----------

feature_importances.head()

# COMMAND ----------

validation_set = validation_set[META_COLS+FEATURE_COLS+ORIG_COL+TARGET_COL+['pred']]
validation_set['pred_antilog'] = (2**(validation_set['pred']))

# COMMAND ----------

cal_error(validation_set)

# COMMAND ----------

def cal_true_error(validation_set):
    error_col = (validation_set['pred_antilog']-validation_set[ORIG_COL[0]]).abs()/validation_set[ORIG_COL[0]]
    return error_col.mean()

# COMMAND ----------

cal_true_error(validation_set) #0.617

# COMMAND ----------

validation_set['error'] = (validation_set['pred_antilog']-validation_set[ORIG_COL[0]])
validation_set['pct_error'] = (validation_set['pred_antilog']-validation_set[ORIG_COL[0]])/validation_set[ORIG_COL[0]]
validation_set['abs_pct_error'] = (validation_set['pred_antilog']-validation_set[ORIG_COL[0]]).abs()/validation_set[ORIG_COL[0]]

# COMMAND ----------

validation_set[validation_set['title_name'] == 'Peacemaker']

# COMMAND ----------

validation_set[validation_set['abs_pct_error'] > 1]

# COMMAND ----------

X = training_data[FEATURE_COLS]
y = training_data[TARGET_COL]
gam = LinearGAM(n_splines=10).fit(X, y)

# COMMAND ----------

gam.summary()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

plt.rcParams['figure.figsize'] = (20, 8)
fig, axs = plt.subplots(1, 9)
titles = FEATURE_COLS
for i, ax in enumerate(axs):
    XX = gam.generate_X_grid(term=i)
    pdep, confi = gam.partial_dependence(term=i, width=.95)

    ax.plot(XX[:, i], pdep)
    ax.plot(XX[:, i], confi, c='r', ls='--')
    ax.set_title(titles[i]);

# COMMAND ----------

validation_set[validation_set['title_name'] == 'The Last of Us']

# COMMAND ----------

validation_set['offering_start_date'] = pd.to_datetime(validation_set['offering_start_date'])

# COMMAND ----------

import matplotlib.pyplot as plt
from matplotlib.pyplot import figure

# COMMAND ----------

figure(figsize=(16, 6), dpi=80)
for c in validation_set.medal.unique():
    plot_data = validation_set[(validation_set['medal'] == c)]
    plt.scatter(plot_data['offering_start_date'], plot_data['pct_error'], label = c, s = plot_data['ahvr']*500)
plt.title('errors V.S. first offer date')
plt.xlabel('first offer date')
plt.ylabel('pct error rate')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to S3/SF

# COMMAND ----------

validation_set.rename(columns = {'ahvr':'average_hours_viewed', 'log_ahvr':'log_average_hours_viewed', 
                                 'pred':'pred_logged', 'pred_antilog':'prediction'}, inplace = True)

# COMMAND ----------

# validation_set.to_csv('validation_set_prediction_model.csv')

# COMMAND ----------

filename = 'cross_validation_results_'+pd.Timestamp.today().strftime('%Y-%m-%d')
write_to_sf(validation_set, filename)

# COMMAND ----------

write_to_input(validation_set, 'validation_set_prediction_model')

# COMMAND ----------

# run_query('''
# create or replace table max_dev.workspace.user_title_hours_watched_crossvalidation 
# (
# title_name	VARCHAR,
# title_id	VARCHAR,
# season_number	int,
# offering_start_date	VARCHAR,
# air_date	VARCHAR,
# asset_run_time_hours	float,
# content_category	VARCHAR,
# program_type	VARCHAR,
# medal	VARCHAR,
# pillar_genre	VARCHAR,
# is_pay_1	int,
# is_popcorn	int,
# medal_number	int,
# age_of_content	float,
# content_cost	bigint,
# average_hours_viewed	float,
# log_average_hours_viewed	float,
# pred_logged	float,
# prediction	float,
# pct_error	float
# );

# ''')

# COMMAND ----------

# run_query('''
# copy into max_dev.workspace.user_title_hours_watched_crossvalidation
#     from(
#         select
#               $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,$13, $14, $15, $16, $17, $18, $19, $20
#         from @HBO_OUTBOUND_DATASCIENCE_CONTENT_DEV/title_hours_viewed_retention/cross_validation_results.csv
#         )
#     file_format = (type = csv null_if=('') skip_header = 1  FIELD_OPTIONALLY_ENCLOSED_BY='"')
#     on_error = 'CONTINUE';

# ''')

# COMMAND ----------

# MAGIC %md
# MAGIC # Appendix - OTHER MODELS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Random Forest CV

# COMMAND ----------

training_data = validation_data[(validation_data['ahvr']>0.005)]
# training_data = training_data[META_COLS+FEATURE_COLS+TARGET_COL]

# COMMAND ----------

# training_data = training_data.sort_values(by = ['offering_start_date']).reset_index(drop = True)

# COMMAND ----------

num_fold = 10
group_kfold = GroupKFold(n_splits=num_fold)

# COMMAND ----------

TARGET_COL = ['ahvr']

# COMMAND ----------

len(training_data)

# COMMAND ----------

validation_set = pd.DataFrame()
feature_importances = pd.DataFrame()
model = XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8)

for train_index, test_index in group_kfold.split(training_data, groups=training_data['title_id'].values):
    train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
    
#     offer_date = test_df['offering_start_date'].min()
#     train_df = train_df[train_df['offering_start_date']<=offer_date]
#     if offer_date == '2020-05-27':
#         print ('pass')
#         continue
    n_record = len(train_df.title_id.unique())
    print ("n of Validation Title: " + str(n_record))
    n_record = len(test_df.title_id.unique())
    print ("n of Test Title: " + str(n_record))
#     print (test_df.offering_start_date.unique())
    try:
        ## fit_predict prelaunch model
        X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
        y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]
        
        

        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        pred = pd.DataFrame(pred)
        pred.columns = ['pred']
        test_df.reset_index(inplace = True, drop = True)
        test_df = pd.concat([test_df, pred], axis = 1)
        validation_set = pd.concat([validation_set, test_df], axis = 0)

#         fi = model.feature_importances_
#         fi_df = pd.DataFrame([fi])
#         fi_df.columns = FEATURE_COLS
#         fi_df['match_id'] = test_df['match_id'].values[0]
#         fi_df['title_name'] = test_df['title_name'].values[0]
#         fi_df['days_on_hbo_max'] = day
#         feature_importances=pd.concat([feature_importances, fi_df], axis = 0)

#     except (KeyboardInterrupt, SystemExit):
#         raise

    except Exception as e: print(e)

# COMMAND ----------

cal_error(validation_set)

# COMMAND ----------

validation_set['pct_error'] = (validation_set['pred']-validation_set[TARGET_COL[0]]).abs()/validation_set[TARGET_COL[0]]

# COMMAND ----------

validation_set[validation_set['title_name']=='Wonder Woman 1984']

# COMMAND ----------

validation_set[validation_set['pct_error']>2]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Bayesian Regression CV

# COMMAND ----------

# !pip install pygam

# COMMAND ----------

from pygam import LinearGAM, s

# COMMAND ----------

training_data = validation_data#[(validation_data['ahvr'] > 0.03)]
# training_data = training_data[META_COLS+FEATURE_COLS+TARGET_COL]

# COMMAND ----------

len(training_data.title_id.unique())

# COMMAND ----------

META_COLS = ['title_name', 'title_id', 'season_number', 'offering_start_date', 'air_date','asset_run_time_hours',
            'content_category', 'program_type', 'medal', 'pillar_genre']

# COMMAND ----------

FEATURE_COLS = ['is_pay_1', 
                'is_popcorn', 
#                 'is_new_content', 
                'medal_number', 
                'age_of_content', 
#                 'content_cost', #XX
                'budget', 
#                 'content_category_livesports', 
                'content_category_movies', 
                'content_category_series', 
#                 'content_category_special', 
                'program_type_acquired', 
                'program_type_original', 
                'genre_Action/Adventure ', 
#                 'Dominant_Topic_7.0'    XX
                'genre_Classics',  
                'genre_Horror '
               ]

# COMMAND ----------

TARGET_COL = ['ahvr'] # cumulative_first_views

# COMMAND ----------

training_data[FEATURE_COLS].isnull().sum()/len(training_data)

# COMMAND ----------

training_data.sort_values(by = ['offering_start_date'], inplace = True)

# COMMAND ----------

training_data['medal_number'] = training_data['medal_number'].fillna(3)
# training_data['content_cost'] = training_data['content_cost'].fillna(0)
training_data['budget'] = training_data['budget'].fillna(0)
training_data['age_of_content'] = training_data['age_of_content'].fillna(100)

# COMMAND ----------

### DOWNSIDE --- DO NOT TAKE NULL ####
validation_set = pd.DataFrame()
# feature_importances = pd.DataFrame()
# gam = LinearGAM(s(0))

for train_index, test_index in group_kfold.split(training_data, groups=training_data['offering_start_date'].values):
    train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
#     print ("Validation Title: " + test_df['title'].values[0])
    try:
        ## fit_predict prelaunch model
        X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
        y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]

        gam = LinearGAM(n_splines=10).fit(X_train, y_train)
        pred = gam.predict(X_test)
        pred = pd.DataFrame(pred)
        pred.columns = ['pred']
        test_df.reset_index(inplace = True, drop = True)
        test_df = pd.concat([test_df, pred], axis = 1)
        validation_set = pd.concat([validation_set, test_df], axis = 0)

    except Exception as e:
        print (e)
        raise 

# COMMAND ----------

cal_error(validation_set)

# COMMAND ----------

X = training_data[FEATURE_COLS]
y = training_data[TARGET_COL]
gam = LinearGAM(n_splines=10).fit(X, y)

# COMMAND ----------

gam.summary()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

plt.rcParams['figure.figsize'] = (20, 8)
fig, axs = plt.subplots(1, 7)
titles = FEATURE_COLS
for i, ax in enumerate(axs):
    XX = gam.generate_X_grid(term=i)
    pdep, confi = gam.partial_dependence(term=i, width=.95)

    ax.plot(XX[:, i], pdep)
    ax.plot(XX[:, i], confi, c='r', ls='--')
    ax.set_title(titles[i]);

# COMMAND ----------



# COMMAND ----------

validation_set = validation_set[META_COLS+FEATURE_COLS+TARGET_COL+['pred']]

# COMMAND ----------

validation_set['pct_error'] = (validation_set['pred']-validation_set[TARGET_COL[0]]).abs()/validation_set[TARGET_COL[0]]

# COMMAND ----------

validation_set[validation_set['title_name']=='House of the Dragon']

# COMMAND ----------

validation_set[validation_set['pct_error']>2]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Scale AVHR and Feature

# COMMAND ----------

from sklearn.preprocessing import QuantileTransformer
scaler = QuantileTransformer()

# COMMAND ----------

training_data = validation_data[(validation_data['ahvr']>0.03)]

# COMMAND ----------

len(training_data.title_id.unique())

# COMMAND ----------

from pygam import LinearGAM, s, PoissonGAM

# COMMAND ----------

training_data['quantile_ahvr'] = scaler.fit_transform(training_data['ahvr'].values.reshape(-1, 1))

# COMMAND ----------

TARGET_COL = ['quantile_ahvr']

# COMMAND ----------

FEATURE_COLS = ['is_pay_1', 
                'is_popcorn', 
#                 'is_new_content', 
                'medal_number', 
                'age_of_content', 
                'content_cost', #XX
#                 'budget', 
#                 'content_category_livesports', 
                'content_category_movies', 
                'content_category_series', 
#                 'content_category_special', 
                'program_type_acquired', 
                'program_type_original', 
                'genre_Action/Adventure ', 
#                 'Dominant_Topic_7.0'    XX
                'genre_Classics',  
                'genre_Horror '
               ]

# COMMAND ----------

training_data[FEATURE_COLS].isnull().sum()/len(training_data)
## Checking if there are any nulls in the feature columns

# COMMAND ----------

# training_data['content_cost_scaled'] = scaler.fit_transform(training_data['content_cost'].values.reshape(-1, 1))

# COMMAND ----------

training_data['medal_number'] = training_data['medal_number'].fillna(4)
training_data['content_cost'] = training_data['content_cost'].fillna(0)
training_data['age_of_content'] = training_data['age_of_content'].fillna(0)

# COMMAND ----------

num_fold = 10
group_kfold = GroupKFold(n_splits=num_fold)

# COMMAND ----------

### DOWNSIDE --- DO NOT TAKE NULL ####
validation_set = pd.DataFrame()
# feature_importances = pd.DataFrame()
# gam = LinearGAM(s(0))

for train_index, test_index in group_kfold.split(training_data, groups=training_data['title_id'].values):
    train_df, test_df = training_data.iloc[train_index], training_data.iloc[test_index]
#     print ("Validation Title: " + test_df['title'].values[0])
    try:
        ## fit_predict prelaunch model|
        X_train, X_test = train_df[FEATURE_COLS], test_df[FEATURE_COLS]
        y_train, y_test = train_df[TARGET_COL], test_df[TARGET_COL]

        gam = LinearGAM(n_splines=10).fit(X_train, y_train)
        pred = gam.predict(X_test)
        pred = pd.DataFrame(pred)
        pred.columns = ['pred']
        test_df.reset_index(inplace = True, drop = True)
        test_df = pd.concat([test_df, pred], axis = 1)
        validation_set = pd.concat([validation_set, test_df], axis = 0)

    except Exception as e:
        print (e)
        raise 

# COMMAND ----------

cal_error(validation_set) #

# COMMAND ----------

validation_set = validation_set[META_COLS+FEATURE_COLS+['ahvr']+TARGET_COL+['pred']]

# COMMAND ----------

validation_set['pred_transform'] = scaler.transform(validation_set['pred'].values.reshape(-1, 1))

# COMMAND ----------

def cal_true_error(validation_set):
    error_col = (validation_set['pred_transform']-validation_set['ahvr']).abs()/validation_set['ahvr']
    return error_col.mean()

# COMMAND ----------

cal_true_error(validation_set)

# COMMAND ----------

validation_set['pct_error'] = (validation_set['pred_transform']-validation_set['ahvr']).abs()/validation_set['ahvr']

# COMMAND ----------

X = training_data[FEATURE_COLS]
y = training_data[TARGET_COL]
gam = LinearGAM(n_splines=10).fit(X, y)

# COMMAND ----------

gam.summary()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

plt.rcParams['figure.figsize'] = (20, 8)
fig, axs = plt.subplots(1, 7)
titles = FEATURE_COLS
for i, ax in enumerate(axs):
    XX = gam.generate_X_grid(term=i)
    pdep, confi = gam.partial_dependence(term=i, width=.95)

    ax.plot(XX[:, i], pdep)
    ax.plot(XX[:, i], confi, c='r', ls='--')
    ax.set_title(titles[i]);

# COMMAND ----------

validation_set.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 
