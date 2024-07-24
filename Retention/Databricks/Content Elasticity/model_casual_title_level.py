# Databricks notebook source
import datetime as dt
import json
import numpy as np
import pandas as pd

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)

pd.options.display.float_format = '{:,.4f}'.format
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg_tbbt
# SELECT    coalesce(sub_id_max, sub_id_legacy) as sub_id
# , coalesce(hb.user_id, hb.hurley_user_id) as user_id
# , cycle_expire_date
# , expiration_month
# , tenure
# , number_of_reconnects
# , is_cancel
# , is_vol_cancel
# , provider
# , sku
# , count(distinct weekofyear(request_date)) as number_of_weeks_viewed
# , count(distinct ckg_series_id) as titles_viewed
# , sum(hours_viewed) as hours_viewed
# , count(distinct case when in_series_window = 1 then ckg_series_id else null end) as new_titles_viewed
# , count(distinct case when in_series_window = 0 then ckg_series_id else null end) as library_titles_viewed
# , count(distinct case when medal = 'Platinum' then ckg_series_id else null end) as platinum_titles_viewed
# , count(distinct case when medal = 'Gold' then ckg_series_id else null end) as gold_titles_viewed
# , count(distinct case when medal = 'Silver' then ckg_series_id else null end) as silver_titles_viewed
# , count(distinct case when series_type = 'series' then ckg_series_id else null end) as series_viewed
# , count(distinct case when series_type = 'movie' then ckg_series_id else null end) as movie_viewed
# , count(distinct case when series_type IN ('livesports', 'live') then ckg_series_id else null end) as livesports_viewed
# , CASE WHEN COUNT(distinct case when title_name = 'Friends' then 1 else null end) >=1
#         THEN 1 ELSE 0 END AS is_friends_watcher
# , CASE WHEN COUNT(distinct case when title_name = 'The Big Bang Theory' then 1 else null end) >=1
#         THEN 1 ELSE 0 END AS is_tbbt_watcher
# , CASE WHEN COUNT(distinct case when title_name IN ('The Big Bang Theory', 'Friends') then 1 else null end) >=1
#         THEN 1 ELSE 0 END AS is_friends_tbbt_watcher
# , CASE WHEN COUNT(distinct case when title_name = 'House of the Dragon' then 1 else null end) >=1
#         THEN 1 ELSE 0 END AS is_hotd_watcher
# FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_us hb
# GROUP BY ALL


# COMMAND ----------

sub_data = spark.sql('''
(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg_tbbt
WHERE is_vol_cancel = 1
and expiration_month >='2023-06-01'
and is_hotd_watcher = 1
ORDER BY rand()
limit 500000)

UNION

(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg_tbbt
WHERE is_vol_cancel = 1
and expiration_month >='2023-06-01'
and is_hotd_watcher = 0
ORDER BY rand()
limit 500000)

UNION

(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg_tbbt
WHERE is_vol_cancel = 0
and expiration_month >='2023-06-01'
and is_hotd_watcher = 1
ORDER BY rand()
limit 500000)

UNION

(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg_tbbt
WHERE is_vol_cancel = 0
and expiration_month >='2023-06-01'
and is_hotd_watcher = 0
ORDER BY rand()
limit 500000)

'''
).toPandas()

# COMMAND ----------

sub_data[sub_data['is_hotd_watcher'] == 1].sub_id.nunique()/sub_data.sub_id.nunique()

# COMMAND ----------

# META_COLS = ['sub_id', 'profile_id', 'user_id', 'cycle_expire_date', 'entertainment_segment_lifetime' ]

# COMMAND ----------

TARGET_COL = ['is_vol_cancel'] # is_vol_cancel

# COMMAND ----------

sub_data.head()

# COMMAND ----------

FEATURE_COLS = ['tenure', 'number_of_reconnects', 'titles_viewed', 'number_of_weeks_viewed', 'platinum_titles_viewed', 'gold_titles_viewed', 'silver_titles_viewed', 'series_viewed', 'movie_viewed', 'livesports_viewed', 'hours_viewed'] #'new_titles_viewed', 'library_titles_viewed', , 'hours_viewed'
CAT_FEATURE_COLS = ['provider', 'sku']

# COMMAND ----------

sub_data.loc[sub_data['tenure'] >= 12, 'tenure'] = 12

# COMMAND ----------

sub_data.tenure.max()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

ONEHOT_COLS = []

# COMMAND ----------

for c in CAT_FEATURE_COLS:
    onehot = pd.get_dummies(sub_data[c], prefix=c)
    sub_data=pd.concat([sub_data, onehot], axis = 1)
    ONEHOT_COLS = ONEHOT_COLS+onehot.columns.to_list()

# COMMAND ----------

ONEHOT_COLS

# COMMAND ----------

for i in FEATURE_COLS:
    sub_data[i] = sub_data[i].astype(float)

# COMMAND ----------

# Normalize Data
from sklearn import preprocessing
FINAL_FEATURE_COL = []
for i in FEATURE_COLS+ONEHOT_COLS:
    min_max_scaler = preprocessing.MinMaxScaler()
    scaled_col = min_max_scaler.fit_transform(sub_data[[i]])
    sub_data[i+'_scaled'] = scaled_col
    FINAL_FEATURE_COL.append(i+'_scaled')

# COMMAND ----------

sub_data['hours_viewed_scaled'] = sub_data['hours_viewed_scaled'].fillna(0)
sub_data['hours_viewed'] = sub_data['hours_viewed'].fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation

# COMMAND ----------

import matplotlib.pyplot as plt 
import seaborn as sns

# COMMAND ----------

plot_data=sub_data[FINAL_FEATURE_COL+TARGET_COL]
corr = plot_data.corr()[TARGET_COL]

plt.figure(figsize=(5, 10))
sns.heatmap(corr,
            vmin=-1, vmax=1, annot=True, cmap='coolwarm')
plt.title('Correlation Heatmap', fontsize=14, weight='bold');

# COMMAND ----------

# Plot correlation heatmap
plot_data=sub_data[FINAL_FEATURE_COL]
corr = plot_data.corr()[FINAL_FEATURE_COL]

plt.figure(figsize=(14, 10))
sns.heatmap(corr,
            vmin=-1, vmax=1, annot=True, cmap='coolwarm')
plt.title('Correlation Heatmap', fontsize=14, weight='bold');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Causal Logistics

# COMMAND ----------

import xgboost  
from xgboost import XGBRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GroupKFold

# COMMAND ----------

import statsmodels.formula.api as smf
import statsmodels.api as sm

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------


X = sm.add_constant(sub_data[['tenure_scaled',
                                'number_of_reconnects_scaled',
                                'titles_viewed_scaled',
                                # 'new_titles_viewed_scaled',
                                # 'library_titles_viewed_scaled',
                                'number_of_weeks_viewed_scaled',
                                'platinum_titles_viewed_scaled',
                                'gold_titles_viewed_scaled',
                                'silver_titles_viewed_scaled',
                                'series_viewed_scaled',
                                'movie_viewed_scaled',
                                'livesports_viewed_scaled',
                                # 'hours_viewed_scaled',
                                # 'provider_Direct_scaled',
                                'sku_ad_free_scaled',
                                'sku_ad_lite_scaled',
                                'sku_premium_ad_free_scaled']])
model_logit = sm.OLS(sub_data[TARGET_COL], X).fit()
model_logit.summary()

# COMMAND ----------



# COMMAND ----------

def get_casual_impact(lower, upper, sub_data, treatment_col):
    level_data = sub_data[(sub_data[treatment_col]>=lower)
                         &(sub_data[treatment_col]<=upper)]

    if len(level_data) <= 30 or level_data[treatment_col].nunique() < 2:
        return np.NaN, np.NaN, np.NaN

    ############ Scale the new dataset ###########
    for i in FEATURE_COLS+ONEHOT_COLS:
        min_max_scaler = preprocessing.MinMaxScaler()
        scaled_col = min_max_scaler.fit_transform(level_data[[i]])
        # level_data[i+'_scaled'] = scaled_col
    
    # scaled_treatment_col = treatment_col
    ############ Debiasing Step ###########
    model = LogisticRegression(penalty='none', max_iter=400)
    debiasing_model = model.fit(level_data[confounding_factor], level_data[treatment_col])
    titles_deb = level_data.assign(
        titles_res= level_data[treatment_col] - debiasing_model.predict_proba(level_data[confounding_factor])[:, 0])

    # model = LinearRegression()
    # debiasing_model = model.fit(level_data[confounding_factor], level_data[treatment_col])
    # titles_deb = level_data.assign(
    #     titles_res= level_data[treatment_col] - debiasing_model.predict(level_data[confounding_factor]))

    ############ Denoise Step ###########
    model = LogisticRegression(penalty='none', max_iter=400)
    denoising_model = model.fit(titles_deb[confounding_factor], titles_deb[TARGET_COL])
    data_denoise = titles_deb.assign(
        cancel_res= titles_deb[TARGET_COL[0]] - denoising_model.predict_proba(level_data[confounding_factor])[:, 0])
    

    # model = LinearRegression()
    # denoising_model = model.fit(titles_deb[confounding_factor], titles_deb[TARGET_COL])
    # titles_deb['prediction'] = denoising_model.predict(titles_deb[confounding_factor])

    # data_denoise = titles_deb.assign(
    #     cancel_res= titles_deb[TARGET_COL[0]] - titles_deb['prediction']
    # )

    ############ Denoise Step ###########
    # model = LinearRegression()
    # model.fit(data_denoise[['titles_res']], data_denoise['cancel_res'])
    # return model.coef_[0], model.intercept_, model.conf_int(0.05)
    model = sm.OLS(data_denoise['cancel_res'], sm.add_constant(data_denoise[['titles_res']])).fit()

    # print(model.summary())
    return model.params[1], model.params[0], model.bse[1]

# COMMAND ----------

def convert_odds_to_prob(y):
    return np.exp(y)/(1 + np.exp(y))

def convert_odds_change_to_prob_change(coef, intercept):
    return (convert_odds_to_prob(coef+intercept) - convert_odds_to_prob(intercept))

# COMMAND ----------

confounding_factor = ['tenure_scaled',
                        'number_of_reconnects_scaled',
                        'titles_viewed_scaled',
                        # 'new_titles_viewed_scaled',
                        # 'library_titles_viewed_scaled',
                        # 'number_of_weeks_viewed_scaled',
                        'platinum_titles_viewed_scaled',
                        'gold_titles_viewed_scaled',
                        'silver_titles_viewed_scaled',
                        'series_viewed_scaled',
                        'movie_viewed_scaled',
                        'livesports_viewed_scaled',
                        'hours_viewed_scaled',
                        # 'provider_Direct_scaled',
                        'sku_ad_free_scaled',
                        'sku_ad_lite_scaled',
                        'sku_premium_ad_free_scaled']

# COMMAND ----------

# get_casual_impact(0, 1, sub_data, 'new_titles_viewed', 0.0525)

# COMMAND ----------

# sub_data_total = sub_data.copy()
# sub_data = sub_data[sub_data['tenure']>=2].copy()

# COMMAND ----------

treatment_col = 'is_friends_watcher'

base_churn = 0.0525 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn]]  #,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn',])  # 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, int(sub_data[treatment_col].max()+1), 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    # churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    # churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn]]         # , churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn'])  # , 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_new = casual_df

# COMMAND ----------

casual_df_new

# COMMAND ----------

treatment_col = 'is_tbbt_watcher'

base_churn = 0.0525 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn]]  #,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn',])  # 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, int(sub_data[treatment_col].max()+1), 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    # churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    # churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn]]         # , churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn'])  # , 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_new = casual_df

# COMMAND ----------

casual_df_new

# COMMAND ----------

treatment_col = 'is_friends_tbbt_watcher'

base_churn = 0.0525 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn]]  #,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn',])  # 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, int(sub_data[treatment_col].max()+1), 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    # churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    # churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn]]         # , churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn'])  # , 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_new = casual_df

# COMMAND ----------

casual_df_new

# COMMAND ----------

sub_data.head()

# COMMAND ----------

treatment_col = 'is_hotd_watcher'

base_churn = 0.0525 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn]]  #,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn',])  # 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, int(sub_data[treatment_col].max()+1), 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    # churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    # churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn]]         # , churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn'])  # , 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_new = casual_df

# COMMAND ----------

casual_df_new

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Churn Curves

# COMMAND ----------

content_churn_curve = spark.sql('SELECT * FROM bolt_cus_dev.bronze.cip_churn_curve_parameters_v2').toPandas()

# COMMAND ----------

content_churn_curve=content_churn_curve[content_churn_curve['agg_level'] == 'library'].reset_index()

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

def fit_exponential(x_data, y_data, p0, param_bounds):
    params, _ = curve_fit(exponential_decay, np.array(x_data), y_data, p0, bounds=param_bounds)
    return params

# COMMAND ----------

def plot_curve_and_mean(a,b,c,mean, seg):
    print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a, b, c))

    # x = casual_df.index_upper
    # y = casual_df.churn
    x_fit = np.linspace(0, 15, 100) 
    y_fit = exponential_decay(x_fit, a, b, c)
    x_med = mean
    y_med = exponential_decay(x_med, a, b, c)


    # fig = px.scatter(x=x, y=y,  title=seg, width=1000, height=400)
    fig = px.scatter(x=x_fit, y=y_fit,  title=seg, width=1000, height=400)
    # fig.add_scatter(x=x_fit, y=y_fit)
    fig.add_scatter(x=[x_med], y=[y_med],mode='markers',marker=dict(size=14, color='red', line=dict(color='black', width=2)))

    fig.update_layout(
        template='simple_white',
        showlegend=False,
        xaxis=dict(range=[0.25,15]),
        annotations=[
        dict(
            x=x_med+0.2,  # x-coordinate for the text
            y=y_med+0.01,  # y-coordinate for the text
            text='{:.2f}, {:.2f}'.format(x_med, y_med),  # the text to display
            showarrow=False,  # disable arrow for the annotation
            xanchor='left',
            font=dict(
                family='Arial',  # specify font family
                size=18,  # specify font size
                color='black'  # specify font color
            )
        )
    ]
) 
    fig.show()
    # return fig


# COMMAND ----------

a,b,c,mean = content_churn_curve.loc[0]['a'], content_churn_curve.loc[0]['b'], content_churn_curve.loc[0]['c'], content_churn_curve.loc[0]['mean']
plot_curve_and_mean(a,b,c,mean, 'library')

# COMMAND ----------

# pd.options.display.float_format = '{:,.6f}'.format
(exponential_decay(mean, a, b, c) - exponential_decay(mean-0.1397, a, b, c))

# COMMAND ----------

