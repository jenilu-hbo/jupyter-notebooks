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

sub_data = spark.sql('''
(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg
WHERE is_vol_cancel = 1
ORDER BY rand()
limit 1000000)

UNION

(SELECT *
FROM bolt_cus_dev.bronze.cip_user_stream_subscription_metric_agg
WHERE is_vol_cancel = 0
ORDER BY rand()
limit 1000000)

'''
).toPandas()

# COMMAND ----------

sub_data.sub_id.nunique()

# COMMAND ----------

META_COLS = ['sub_id', 'profile_id', 'user_id', 'cycle_expire_date', 'entertainment_segment_lifetime' ]

# COMMAND ----------

TARGET_COL = ['is_vol_cancel'] # is_vol_cancel

# COMMAND ----------

FEATURE_COLS = ['tenure', 'number_of_reconnects', 'titles_viewed', 'new_titles_viewed', 'library_titles_viewed', 'number_of_weeks_viewed', 'platinum_titles_viewed', 'gold_titles_viewed', 'silver_titles_viewed', 'series_viewed', 'movie_viewed', 'livesports_viewed', 'hours_viewed']
CAT_FEATURE_COLS = ['provider', 'sku']

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
# MAGIC ## Logistic regression

# COMMAND ----------

import xgboost  
from xgboost import XGBRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GroupKFold

# COMMAND ----------

sub_data['hours_viewed'] = sub_data['hours_viewed'].fillna(0)
sub_data['hours_viewed_scaled'] = sub_data['hours_viewed_scaled'].fillna(0)

# COMMAND ----------

x_train = sub_data[FINAL_FEATURE_COL]
y_train = sub_data[TARGET_COL]

# COMMAND ----------

clf = LogisticRegression()  
clf.fit(x_train,y_train)  

weight = clf.coef_  

# COMMAND ----------

coef_df = pd.DataFrame(
    {'feature': FINAL_FEATURE_COL,
     'coef': weight[0]
    })

# COMMAND ----------

coef_df = coef_df.set_index(coef_df.feature).drop(['feature'], axis = 1)

# COMMAND ----------

coef_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linear Regression

# COMMAND ----------

clf = LinearRegression()  
clf.fit(x_train,y_train)  

weight = clf.coef_  

# COMMAND ----------

coef_df = pd.DataFrame(
    {'feature': FINAL_FEATURE_COL,
     'coef': weight[0]
    })

# COMMAND ----------

coef_df = coef_df.set_index(coef_df.feature).drop(['feature'], axis = 1)

# COMMAND ----------

coef_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Causal Logistics

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
                                'new_titles_viewed_scaled',
                                'library_titles_viewed_scaled',
                                'number_of_weeks_viewed_scaled',
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
                                'sku_premium_ad_free_scaled']])
model_logit = sm.OLS(sub_data[TARGET_COL], X).fit()
model_logit.summary()

# COMMAND ----------

model_logit.bse[1]

# COMMAND ----------

def get_casual_impact(lower, upper, sub_data, treatment_col, base_churn):
    level_data = sub_data[(sub_data[treatment_col]>=lower)
                         &(sub_data[treatment_col]<=upper)]

    if len(level_data) == 0 or level_data[treatment_col].nunique() < 2:
        return np.NaN, np.NaN

    ############ Scale the new dataset ###########
    for i in FEATURE_COLS+ONEHOT_COLS:
        min_max_scaler = preprocessing.MinMaxScaler()
        scaled_col = min_max_scaler.fit_transform(level_data[[i]])
        level_data[i+'_scaled'] = scaled_col
    
    scaled_treatment_col = treatment_col+'_scaled'
    ############ Debiasing Step ###########
    model = LogisticRegression(penalty='none', max_iter=400)
    debiasing_model = model.fit(level_data[confounding_factor], level_data[scaled_treatment_col])
    titles_deb = level_data.assign(
        titles_res= level_data[scaled_treatment_col] - debiasing_model.predict_proba(level_data[confounding_factor])[:, 0])

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

    model = sm.OLS(data_denoise['cancel_res'], sm.add_constant(data_denoise[['titles_res']])).fit()

    # return model.coef_[0], model.intercept_, model.conf_int(0.05)
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
                        'library_titles_viewed_scaled',
                        'number_of_weeks_viewed_scaled',
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

get_casual_impact(0, 1, sub_data, 'new_titles_viewed', 0.0525)

# COMMAND ----------

treatment_col = 'new_titles_viewed'

base_churn = 0.0525 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, 15, 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col, base_churn)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn, churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_new = casual_df

# COMMAND ----------

casual_df_new

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

# Define the range for g
g = np.linspace(-100, 100, 400)

# Compute x using the new equation
x = -0.0659*g+0.1190		

# Compute the logistic function y
y = np.exp(x) / (1 + np.exp(x))

# Plot the relationship between y and g
plt.figure(figsize=(8, 5))
plt.plot(g, y, label=r'$p = \frac{e^{(-0.0659X + 0.1190	)}}{1 + e^{(-0.0659X + 0.1190)}}$')
plt.title('Relationship between p and X')
plt.xlabel('g')
plt.ylabel('y')
plt.grid(True)
plt.legend()
plt.show()

# COMMAND ----------

confounding_factor = ['tenure_scaled',
                        'number_of_reconnects_scaled',
                        'titles_viewed_scaled',
                        'new_titles_viewed_scaled',
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
                        'sku_premium_ad_free_scaled']

# COMMAND ----------

treatment_col = 'library_titles_viewed'

base_churn = 0.0559 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, 15, 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col, base_churn)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn, churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_lib = casual_df

# COMMAND ----------

casual_df_lib

# COMMAND ----------

confounding_factor = ['tenure_scaled',
                        'number_of_reconnects_scaled',
                        # 'titles_viewed_scaled',
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
                        'sku_premium_ad_free_scaled']

# COMMAND ----------

treatment_col = 'titles_viewed'

base_churn = 0.0593 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn,  np.NaN, np.NaN]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 

new_churn = base_churn

for i in range(0, 15, 1):
    coef, intercept, se = get_casual_impact(i, i+1, sub_data, treatment_col, base_churn)
    intercept = intercept - np.log((0.95/0.05)*(1))
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    churn_diff_low = convert_odds_change_to_prob_change(coef-1.96*se, intercept)
    churn_diff_upper = convert_odds_change_to_prob_change(coef+1.96*se, intercept)

    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn, churn_diff_low, churn_diff_upper]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept' , 'churn_diff', 'churn', 'churn_diff_low', 'churn_diff_upper']) 
    casual_df = casual_df.append(df)

casual_df_total = casual_df

# COMMAND ----------

casual_df_new.head()

# COMMAND ----------

fig = px.scatter(title='chur difference vs titles viewed',
                  width=800, height=800)

color_discrete_sequence=px.colors.qualitative.D3

# fig.add_scatter(x=casual_df_total['index_upper']
#                 , y=casual_df_total['churn_diff']
#                 , showlegend = True, name = 'total')

i = 0
fig.add_scatter(x=casual_df_new['index_upper']
                , y=casual_df_new['churn_diff']
                , showlegend = True, name = 'new', line = dict(color = color_discrete_sequence[i]))

fig.add_scatter(x=casual_df_new['index_upper']
                , y=casual_df_new['churn_diff']
                , error_y = dict(type = 'data', array = casual_df_new['churn_diff_upper'] - casual_df_new['churn_diff'], visible = True)
                , showlegend=False, mode='markers', marker = dict(color = color_discrete_sequence[i]))

i = i + 1
fig.add_scatter(x=casual_df_lib['index_upper']
                , y=casual_df_lib['churn_diff']
                , showlegend = True, name = 'lib', line = dict(color = color_discrete_sequence[i]))

fig.add_scatter(x=casual_df_lib['index_upper']
                , y=casual_df_lib['churn_diff']
                , error_y = dict(type = 'data', array = casual_df_lib['churn_diff_upper'] - casual_df_lib['churn_diff'], visible = True)
                , showlegend=False,mode='markers', marker = dict(color = color_discrete_sequence[i]))

fig.update_layout(
    template='simple_white',
    showlegend=True,
    xaxis=dict(range=[0,10]),
) 

# COMMAND ----------

np.abs(casual_df_new.churn_diff).mean()/np.abs(casual_df_lib.churn_diff).mean()
# weighted average 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Churve Fitting

# COMMAND ----------

from scipy.optimize import curve_fit

# COMMAND ----------

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

def fit_exponential(x_data, y_data, p0, param_bounds):
    x_fit = np.linspace(0, x_data.max(), 100)   
    params, _ = curve_fit(exponential_decay, np.array(x_data), y_data, p0, bounds=param_bounds)
    return x_fit, params

# COMMAND ----------

def fit_churn_curve(df_i):
    x_var = df_i.index_upper
    y_data = df_i.churn
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -np.inf, 0], [np.inf,0, np.inf])


    x_fit, params = fit_exponential(x_var, y_data, p0, param_bounds)
    a_fit, b_fit, c_fit = params
    print(a_fit, b_fit, c_fit)
    y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
    return x_fit, y_fit

# COMMAND ----------

fig = px.scatter(title='chur difference vs titles viewed Exponential Fit',
                  width=1000, height=400)


color_discrete_sequence=px.colors.qualitative.D3

i = 0
fig.add_scatter(x=casual_df_total['index_upper']
                , y=casual_df_total['churn'], mode='markers'
                , showlegend = True, name = 'total', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_total)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])

i = i+1
fig.add_scatter(x=casual_df_new['index_upper']
                , y=casual_df_new['churn'], mode='markers'
                , showlegend = True, name = 'new', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_new)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])

i = i+1
fig.add_scatter(x=casual_df_lib['index_upper']
                , y=casual_df_lib['churn'], mode='markers'
                , showlegend = True, name = 'lib', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_lib)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])


fig.update_layout(
    template='simple_white',
    showlegend=True,
    xaxis=dict(range=[0,15]),
    # yaxis=dict(range=[0.2,0.65]),
) 

# COMMAND ----------

from scipy import interpolate
import scipy.interpolate as inter

# COMMAND ----------

fig = px.scatter(title='chur difference vs titles viewed Spline fit',
                  width=1000, height=400)

color_discrete_sequence=px.colors.qualitative.D3

i = 0
fig.add_scatter(x=casual_df_total['index_upper']
                , y=casual_df_total['churn'], mode='markers'
                , showlegend = True, name = 'total', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_total)
y_fit = inter.UnivariateSpline(casual_df_total.index_upper, casual_df_total.churn, s = 0.1)(x_fit)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])

i = i+1
fig.add_scatter(x=casual_df_new['index_upper']
                , y=casual_df_new['churn'], mode='markers'
                , showlegend = True, name = 'new', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_new)
y_fit = inter.UnivariateSpline(casual_df_new.index_upper, casual_df_new.churn, s = 0.1)(x_fit)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])

i = i+1
fig.add_scatter(x=casual_df_lib['index_upper']
                , y=casual_df_lib['churn'], mode='markers'
                , showlegend = True, name = 'lib', line_color = color_discrete_sequence[i])
x_fit, y_fit = fit_churn_curve(casual_df_lib)
y_fit = inter.UnivariateSpline(casual_df_lib.index_upper, casual_df_lib.churn, s = 0.1)(x_fit)
fig.add_scatter(x=x_fit, y=y_fit
                , showlegend = False, name = 'total', line_color = color_discrete_sequence[i])


fig.update_layout(
    template='simple_white',
    showlegend=True,
    xaxis=dict(range=[0,15]),
    # yaxis=dict(range=[0.2,0.65]),
) 

# COMMAND ----------

casual_df_new

# COMMAND ----------

casual_df_lib

# COMMAND ----------

casual_df_total

# COMMAND ----------



# COMMAND ----------

confounding_factor = ['tenure_scaled',
                        'number_of_reconnects_scaled',
                        'titles_viewed_scaled',
                        # 'new_titles_viewed_scaled',
                        # 'library_titles_viewed_scaled',
                        'number_of_weeks_viewed_scaled',
                        # 'platinum_titles_viewed_scaled',
                        'gold_titles_viewed_scaled',
                        'silver_titles_viewed_scaled',
                        'series_viewed_scaled',
                        'movie_viewed_scaled',
                        'livesports_viewed_scaled',
                        # 'hours_viewed_scaled',
                        # 'provider_Direct_scaled',
                        'sku_ad_free_scaled',
                        'sku_ad_lite_scaled',
                        'sku_premium_ad_free_scaled']

# COMMAND ----------

treatment_col = 'platinum_titles_viewed'

base_churn = 0.0593 #sub_data[sub_data[treatment_col] == 0][TARGET_COL[0]].mean()

casual_df = pd.DataFrame([[0, 0, np.NaN,  np.NaN, np.NaN, base_churn]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn']) 

new_churn = base_churn

for i in range(0, 15, 1):
    coef, intercept = get_casual_impact(i, i+1, sub_data, treatment_col, base_churn)
    churn_diff = convert_odds_change_to_prob_change(coef, intercept)
    new_churn = new_churn+churn_diff
    df = pd.DataFrame([[i, i+1, coef, intercept, churn_diff,new_churn]]
    , columns=['index_lower', 'index_upper', 'coef', 'intercept', 'churn_diff', 'churn']) 
    casual_df = casual_df.append(df)

casual_df

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# level_data = sub_data[sub_data['new_titles_viewed']<=1]

# for i in FEATURE_COLS+ONEHOT_COLS:
#     min_max_scaler = preprocessing.MinMaxScaler()
#     scaled_col = min_max_scaler.fit_transform(level_data[[i]])
#     level_data[i+'_scaled'] = scaled_col

# # Debiasing Step
# model = LogisticRegression(penalty='none', max_iter=400)
# debiasing_model = model.fit(level_data[confounding_factor], level_data['new_titles_viewed_scaled'])
# titles_deb = level_data.assign(
# # for visualization, avg(T) is added to the residuals
# titles_res= level_data['new_titles_viewed_scaled'] - debiasing_model.predict_proba(level_data[confounding_factor])[:, 0]
# )

# model = LogisticRegression(penalty='none', max_iter=400)
# denoising_model = model.fit(titles_deb[confounding_factor], titles_deb[TARGET_COL])
# data_denoise = titles_deb.assign(cancel_res= titles_deb[TARGET_COL[0]] - denoising_model.predict_proba(level_data[confounding_factor])[:, 0])

# # Final Model
# model = LinearRegression()
# model.fit(data_denoise[['titles_res']], data_denoise['cancel_res'])

# COMMAND ----------

# debiasing_model = sm.Logit(level_data[['new_titles_viewed_scaled']], 
#                             level_data[confounding_factor]).fit(method='bfgs')
# titles_deb = level_data.assign(
# # for visualization, avg(T) is added to the residuals
# titles_res=debiasing_model.resid_generalized + level_data["new_titles_viewed_scaled"].mean()
# )

############ Denoise Step ###########
# denoising_model = sm.Logit(titles_deb[TARGET_COL], 
#                        titles_deb[confounding_factor]).fit(method='bfgs')
# data_denoise = titles_deb.assign(
# cancel_res=denoising_model.resid_generalized + titles_deb["is_vol_cancel"].mean()
# )
