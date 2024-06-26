# Databricks notebook source
# !pip install snowflake-connector-python
# !pip install sympy
# dbutils.library.restartPython()

# COMMAND ----------

import boto3
import datetime as dt
import json
import numpy as np
import pandas as pd
import io
from scipy.optimize import curve_fit
# import snowflake.connector
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.options.display.float_format = '{:,.2f}'.format
file_path = '/Workspace/Users/jeni.lu@wbd.com/Retention/files/'

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------


from scipy.stats import percentileofscore


def get_df_test(df_test, metric_cols):
    df_test['tenure_months'] = df_test['sub_month']
    for i in metric_cols:
        df_test['monthly_'+i] = np.where(df_test['tenure_months']>1, df_test[i]/2, df_test[i])
    df_test['monthly_title_viewed'] = df_test['monthly_titles_viewed'] 
    user_total = df_test.groupby(['user_id'])['monthly_title_viewed'].transform('sum')
    df_test['frc'] = df_test['monthly_title_viewed'] / user_total

    # df_test = df_test[df_test.tenure_months>1]
    df_test = df_test.fillna(0)
    return(df_test)

def get_df_60_h(list_df):
    df_list=[]
    num=0
    for df_test in list_df:
        df_test['num_df'] = num
        df_list.append(df_test)
        num=num+1
    return(df_list)

def exponential_decay(x, a, b,c):
    return a * np.exp(b * x) + c

def exponential_decay_slope(x, a, b):
    return a * b*np.exp(b * x)

def fit_exponential(x_data, y_data, p0, param_bounds):
    x_fit = np.linspace(0, x_data.max(), 100)   
    params, _ = curve_fit(exponential_decay, np.array(x_data), y_data, p0, bounds=param_bounds)
    return x_fit, params


def get_equal_churn_bin(df_in, grpby):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    # nbins = int(df.monthly_title_viewed.max())
    # df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], np.linspace(0,nbins,2*nbins+1))
    
    bins =[-0.01]
    bins = bins + np.arange(0, 12.5, 0.5).tolist()
    bins = bins + np.arange(13, 17, 1.0).tolist()
    bins = bins + [19.0, df.monthly_title_viewed.max()]

    df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], bins,include_lowest =True)
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_title_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    return(df_bin)

def get_churn_bin(df_in, grpby, nbins = 100):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    df['title_viewed_bin_bucket'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,nbins), duplicates='drop')    
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket'] +grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    return(df_bin)

# COMMAND ----------

# DBTITLE 1,Plot By Content Category
def get_simple_plot_multiple(df_plt, x, y, x_fit, y_fit, params, title=''):
    if title=='':
        
        title = f'{y} vs {x}'
       
    a_fit, b_fit, c_fit = params
    annotation_x_loc = 50
    annotation_y_loc = y_fit.min() +(y_fit.max()  - y_fit.min() )/2 
        
    fig = px.scatter(df_plt,
                  x=x, 
                  y=y, 
                  title=title,
                  width=1000, height=400)
    fig.add_scatter( 
              x=x_fit, 
              y=y_fit)

    fig.update_layout(
        template='simple_white',
        showlegend=False,
        xaxis=dict(range=[0,50]),
        annotations=[
        dict(
            x=annotation_x_loc,  # x-coordinate for the text
            y=annotation_y_loc,  # y-coordinate for the text
            text='y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit),  # the text to display
            showarrow=False,  # disable arrow for the annotation
            xanchor='right',
            font=dict(
                family='Arial',  # specify font family
                size=18,  # specify font size
                color='black'  # specify font color
            )
        )
    ]
) 
    fig.show()
    return 

def get_simple_plot_multiple_dot(df_plt, x, y, x_fit, y_fit, params, x_med, y_med, title=''):
    if title=='':
        
        title = f'{y} vs {x}'
       
    a_fit, b_fit, c_fit = params
    print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
    print('y= {:.3f} * e^({:.2f} * title_viewed)'.format(a_fit*b_fit,b_fit))
    annotation_x_loc = 50
    annotation_y_loc = y_fit.min() +(y_fit.max()  - y_fit.min() )/2 
        
    fig = px.scatter(df_plt,
                  x=x, 
                  y=y, 
                  title=title,
                  width=500, height=400)
    fig.add_scatter( 
              x=x_fit, 
              y=y_fit)
    
    fig.add_scatter( 
              x=x_med, 
              y=y_med,
                mode='markers',
            marker=dict(size=14, color='red', line=dict(color='black', width=2)))

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
    return fig



def get_churn_plot_simple(df_i, title, param_dic, x_med=0):
    # df_i = df_i[df_i.is_cancel>=10]
#         display(df_i.tail(5))

    x_var = df_i.title_viewed_bin
    y_data = df_i.churn
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    x_fit, params = fit_exponential(x_var, y_data, p0, param_bounds)
    a_fit, b_fit, c_fit = params
    y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
    
    if x_med==0:
        fig = get_simple_plot_multiple(df_i, 'title_viewed_bin', 'churn', x_fit, y_fit, params, f'{title}')
    else:
        y_med = exponential_decay(x_med, a_fit, b_fit, c_fit)
        y_med_slope = exponential_decay_slope(x_med, a_fit, b_fit)
        print(x_med)
        print('average churn: ' + str('{:.3f}'.format(y_med)))
        print('slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
        fig = get_simple_plot_multiple_dot(df_i, 'title_viewed_bin', 'churn', x_fit, y_fit, params, x_med, np.array(y_med), f'{title}')
    # display(df_i.head())
    param_dic[title] = params
    return fig, params



def get_simple_plot_dot(df_plt, x, y, x_fit, y_fit, params, x_med, y_med, title=''):
    if title=='':
        
        title = f'{y} vs {x}'
       
    a_fit, b_fit, c_fit = params
    print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
    print('y= {:.3f} * e^({:.2f} * title_viewed)'.format(a_fit*b_fit,b_fit))
    annotation_x_loc = 50
    annotation_y_loc = y_fit.min() +(y_fit.max()  - y_fit.min() )/2 
        
    fig = px.line(x=x_fit, 
                  y=y_fit, 
                  title=title,
                  width=500, height=400)
    fig.add_scatter( 
              x=x_med, 
              y=y_med,
                mode='markers',
            marker=dict(size=14, color='red', line=dict(color='black', width=2)))

    fig.update_layout(
        template='simple_white',
        showlegend=False,
        xaxis=dict(range=[0.25,15]),
        xaxis_title= "title_viewed_bin",
        yaxis_title= "Change in churn rate (slope)",
        annotations=[
        dict(
            x=x_med+0.25,  # x-coordinate for the text
            y=y_med+0.0005,  # y-coordinate for the text
            text='{:.2f}, {:.4f}'.format(x_med, y_med),  # the text to display
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
    return fig

def get_churn_slope_plot_simple(df_i, title, params, x_med=0):
    df_i = df_i[df_i.is_cancel>=20]
#         display(df_i.tail(5))

    x_var = df_i.title_viewed_bin
    x_fit = np.linspace(0, x_var.max(), 100)   
    a_fit, b_fit, c_fit = params
    y_fit = exponential_decay_slope(x_fit, a_fit, b_fit)
    
    y_med = exponential_decay_slope(x_med, a_fit, b_fit)
    print(x_med)
    print(y_med)
    fig = get_simple_plot_dot(df_i, 'title_viewed_bin', 'churn', x_fit, y_fit, params, x_med, np.array(y_med), f'{title}')
    display(df_i.head())
    param_dict[title] = params
    return fig


# COMMAND ----------

expiration_month = '2023-12-01'
df_60_00 = spark.sql('''
     WITH new_library AS (
      SELECT s.*
                     , CASE WHEN r.recency_window_end >= expiration_month
                        THEN 'current'
                        ELSE 'library'
                        END AS old_new
      FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_savod s
      left join bolt_cus_dev.bronze.cip_recency_title_series_level_offering_table r
         on s.ckg_program_id = r.ckg_program_id
      where s.expiration_month = '{expiration_month}'
      )

      SELECT s.user_id ::STRING
            , s.profile_id ::STRING
            , is_cancel
            , is_voluntary
            , sku
            , mode(seg.entertainment_segment_lifetime) as entertainment_segment_lifetime
            , min(sub_month)::STRING as sub_month
            , sum(hours_viewed)::STRING as hours_viewed 
            , count(distinct ckg_series_id) as titles_viewed
            , count(distinct (CASE WHEN old_new = 'current' THEN ckg_series_id else null END))  as new_titles_viewed
            , count(distinct (CASE WHEN old_new = 'library' THEN ckg_series_id else null END))  as library_titles_viewed
            , case when is_voluntary = 1 and is_cancel = 1 then 1 else 0 end as is_cancel_vol
            , case when is_voluntary = 0 and is_cancel = 1 then 1 else 0 end as is_cancel_invol
      FROM new_library s
      LEFT JOIN bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
                        on seg.PROFILE_ID = s.profile_id
      GROUP BY ALL
'''.format(expiration_month = expiration_month)
                     ).toPandas()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bolt_cus_dev.bronze.cip_user_title_hours_vs_no_of_weeks_watched AS (
# MAGIC with subs as(
# MAGIC select c.user_id, c.sub_id, up.profile_id
# MAGIC , cancelled_ind as is_cancel
# MAGIC , seamless_paid_tenure_months as sub_month
# MAGIC , period_start_ts as cycle_start_date
# MAGIC , period_end_ts AS cycle_expire_date
# MAGIC , date_trunc('MONTH', period_end_ts) as expiration_month
# MAGIC from bolt_cus_dev.silver.max_ltv_period_combined c 
# MAGIC LEFT join bolt_dai_subs_prod.gold.max_profile_dim_current up
# MAGIC     on c.user_id = up.USER_ID
# MAGIC where up.default_profile_ind = True
# MAGIC and date_trunc('MONTH', period_end_ts) = '2023-12-01'
# MAGIC --LIMIT 3000000
# MAGIC )
# MAGIC
# MAGIC , streaming_subset as
# MAGIC (
# MAGIC select
# MAGIC     hb.request_date_local
# MAGIC     , ss.user_id
# MAGIC     , ss.profile_id
# MAGIC     , ss.is_cancel
# MAGIC     , ss.sub_month
# MAGIC     , hb.PROGRAM_ID_OR_VIEWABLE_ID as ckg_program_id
# MAGIC     , hb.CONTENT_MINUTES_WATCHED/60 as hours_viewed
# MAGIC     , weekofyear(hb.request_date_local) as week_of_year
# MAGIC     , ss.expiration_month
# MAGIC from subs ss
# MAGIC left join bolt_dai_ce_prod.gold.combined_video_stream hb
# MAGIC     on hb.WBD_MAX_PROFILE_ID = ss.profile_id
# MAGIC     AND hb.sub_id = ss.sub_id
# MAGIC where DATEDIFF(DAY, hb.request_time_local, ss.cycle_expire_date) <= 30
# MAGIC     and DATEDIFF(DAY, hb.request_time_local, ss.cycle_expire_date) >= 0
# MAGIC     -- and hb.request_date_pst between DATEADD(MONTH, -2, '{target_month}'::DATE) and '{target_month_end}'
# MAGIC     and hb.PROGRAM_ID_OR_VIEWABLE_ID IS NOT NULL 
# MAGIC     and hb.CONTENT_MINUTES_WATCHED >= 2
# MAGIC     and hb.video_type = 'main'
# MAGIC     and hb.territory = 'HBO MAX DOMESTIC'
# MAGIC     and request_date_local between '2023-10-01' and '2023-12-31'
# MAGIC )
# MAGIC select * from streaming_subset
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bolt_cus_dev.silver.max_ltv_period_combined
# MAGIC WHERE USER_ID = '1681254412181324076'
# MAGIC and date_trunc('MONTH', period_end_ts) = '2023-12-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bolt_cus_dev.bronze.cip_user_title_hours_vs_no_of_weeks_watched
# MAGIC WHERE USER_ID = '1681254412181324076'

# COMMAND ----------

### Viewership Generation Queries #########
df = spark.sql('''
SELECT s.user_id ::STRING
            , s.profile_id ::STRING
            , is_cancel
            -- , is_voluntary
            -- , sku
            , mode(seg.entertainment_segment_lifetime) as entertainment_segment_lifetime
            , min(sub_month)::STRING as sub_month
            , sum(hours_viewed)::STRING as hours_viewed 
            , count(distinct ckg_series_id) as titles_viewed
            , CASE WHEN r.recency_window_end >= expiration_month THEN 'current' ELSE 'library' END AS old_new
            -- , count(distinct (CASE WHEN old_new = 'current' THEN ckg_series_id else null END))  as new_titles_viewed
            -- , count(distinct (CASE WHEN old_new = 'library' THEN ckg_series_id else null END))  as library_titles_viewed
            -- , case when is_voluntary = 1 and is_cancel = 1 then 1 else 0 end as is_cancel_vol
            -- , case when is_voluntary = 0 and is_cancel = 1 then 1 else 0 end as is_cancel_invol
            , count(distinct week_of_year) AS number_of_weeks
      FROM bolt_cus_dev.bronze.cip_user_title_hours_vs_no_of_weeks_watched s
      LEFT JOIN bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table seg
                        on seg.PROFILE_ID = s.profile_id
      LEFT JOIN bolt_cus_dev.bronze.cip_recency_title_series_level_offering_table r
         on s.ckg_program_id = r.ckg_program_id
      GROUP BY ALL                                                        
''').toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Correlation

# COMMAND ----------

df = df[df['number_of_weeks']<=5]
df['titles_viewed'].corr(df['number_of_weeks'])

# COMMAND ----------

#### Problem yet to be resolved??? Why a profile has 2 different sub_month? ########
#### Example user_id = 1680197285172035360

# COMMAND ----------

df[df['number_of_weeks'] == 9].head()

# COMMAND ----------

df.head()

# COMMAND ----------

nbins = 30
df_test = df[df['titles_viewed']<=20].copy()
df_test['title_viewed_bin_bucket'] = pd.qcut(df_test['titles_viewed'], np.linspace(0,1,nbins), duplicates='drop')    
df_test['churn'] = 1*df_test['is_cancel']  

df_bin = df_test.groupby(['title_viewed_bin_bucket']).agg({'churn':'mean', 'user_id':'count',
                                                        'number_of_weeks':'mean'}).reset_index()    
df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')

# COMMAND ----------

df_bin

# COMMAND ----------

df.head()

# COMMAND ----------

fig = px.scatter(df[df['number_of_weeks']<=5].sample(frac=0.1, replace=False, random_state=1),
                  x='number_of_weeks', 
                  y='titles_viewed', 
                  title='titles viewed bin VS number of weeks',
                  width=1000, height=400)
fig.show()

# COMMAND ----------

fig = px.scatter(df_bin,
                  x='title_viewed_bin', 
                  y='number_of_weeks', 
                  title='titles viewed bin VS average number of weeks',
                  width=1000, height=400)
fig.show()

# COMMAND ----------



# COMMAND ----------

metric_cols = ['hours_viewed', 'titles_viewed', 'new_titles_viewed', 'library_titles_viewed']
for m in metric_cols:
    df_60_00[m] = df_60_00[m].astype(float)
df_60_00['sub_month'] = df_60_00['sub_month'].astype(int)

# COMMAND ----------

df_60_00=get_df_test(df_60_00, metric_cols)
df_list = get_df_60_h([df_60_00]) #, df_60_0, df_60_1, df_60_2])
df_60 = pd.concat(df_list)
df_60.head()

# COMMAND ----------

df_60 = df_60[df_60['sub_month']>1]
df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60['sub_month'] = df_60['sub_month'].astype('int')

# COMMAND ----------

df_60.user_id.nunique() #10678914

# COMMAND ----------

param_dict = {}
med_dict = {}

# COMMAND ----------

seg_name = 'gen_pop'

df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()

# COMMAND ----------

df_60_s = get_churn_bin(df_60_t, [])
## Get median 
med_x= df_60_t.monthly_title_viewed.median()

# Plot the Churve 
fig, params = get_churn_plot_simple(df_60_s[(df_60_s['title_viewed_bin']<15)], 
                                    seg_name, param_dict, np.array(med_x))
med_dict[seg_name] = med_x

# COMMAND ----------

param_dict

# COMMAND ----------

# df_final = pd.DataFrame()
# for i in param_dict.keys():
#     df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
#     df_i['agg_level'] = i
#     df_i['mean'] = med_dict[i]
#     df_i['expiration_month'] = expiration_month
#     df_i['entertainment_segment_lifetime'] = seg_name
#     df_final = pd.concat([df_final, df_i])

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution

# COMMAND ----------

df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month',])\
            [['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, ['sub_month'])

user_total = df_60_s.groupby(['sub_month'])['user_id'].transform('sum')
df_60_s['Composition'] = df_60_s['user_id']/user_total

# df_60_s[df_60_s['title_viewed_bin_mid']<10].Composition.sum()/6 ##  91% people watched < 10 titles

# COMMAND ----------

fig = px.bar(df_60_s[(df_60_s['title_viewed_bin']<10) & (df_60_s['sub_month']<10) & (df_60_s['sub_month']>1)], 
             x="title_viewed_bin", y="Composition",
             color='sub_month', barmode='group',
             height=400)
fig.layout.yaxis.tickformat = ',.0%'
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # By Segment

# COMMAND ----------

def get_equal_churn_bin(df_in, grpby):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    # nbins = int(df.monthly_title_viewed.max())
    # df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], np.linspace(0,nbins,2*nbins+1))
    
    bins =[-0.01]
    if df.monthly_title_viewed.max() <=19:
        bins = bins + np.arange(0, df.monthly_title_viewed.max() + 0.5, 0.5).tolist()
    else:
        bins = bins + np.arange(0, 12.5, 0.5).tolist()
        bins = bins + np.arange(13, 17, 1.0).tolist()
        bins = bins + [19.0, df.monthly_title_viewed.max()]

    df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], bins,include_lowest =True)
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_title_viewed':'sum',
                                                         'total_monthly_title_viewed':'mean'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')

    return df_bin

# COMMAND ----------

def churn_plot_new_library(df_60, m, exclusive = False):
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    ########## add seg total ####################
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_'+i for i in metric_cols]].sum().reset_index()

    fig = px.scatter(title = m)

    ##### ADD BY Category ######
    for i in ['monthly_titles_viewed', 'monthly_new_titles_viewed', 'monthly_library_titles_viewed']:
        
        df_60_t_amw= df_60_t.copy()
        df_60_t_amw['monthly_title_viewed'] = df_60_t_amw[i]

        if exclusive == True:
            print( 'Mutually Exclusive')
            if i == 'monthly_library_titles_viewed':
                df_60_t_amw = df_60_t_amw[df_60_t_amw['user_id'].isin(df_library.user_id)]
            if i == 'monthly_new_titles_viewed':
                df_60_t_amw = df_60_t_amw[~df_60_t_amw['user_id'].isin(df_library.user_id)]

        df_60_s = get_churn_bin(df_60_t_amw, [])
        df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
        df_60_s= df_60_s[df_60_s['churn'].notnull()]
        df_60_s = df_60_s[df_60_s['title_viewed_bin']<=15]
        med_x= df_60_t_amw.monthly_title_viewed.median()

        try:
            x_fit, params = fit_exponential(df_60_s.title_viewed_bin, df_60_s.churn, p0, param_bounds)
            a_fit, b_fit, c_fit = params
            x_fit = np.linspace(0, 15, 100)
            y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
            y_med = exponential_decay(med_x, a_fit, b_fit, c_fit)
            y_med_slope = exponential_decay_slope(med_x, a_fit, b_fit)

            print(med_x)
            print(i+' average churn: ' + str('{:.3f}'.format(y_med)))
            print(i+' slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
            print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
        
            if i == 'monthly_new_titles_viewed': 
                i = 'new'
                color = 'pink'
            elif i == 'monthly_library_titles_viewed':
                color = 'red'
                i = 'library'
            else:
                color = 'blue'
                i = 'total'
            fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = i+' content', line_color = color))
            fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, mode='markers', name = i+'_raw', line_color = color)

            param_dict[i] = params
            med_dict[i] = med_x
         
        except Exception as e:
            print(m+'/' + i)
            print(e)
            print(df_60_s)


    fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0.00,15]),
    # yaxis=dict(range=[0.00,0.08])
    ) 
    fig.show()


# COMMAND ----------

############ Gen pop ####################
df_final = pd.DataFrame()
seg_name = 'gen_pop'

param_dict = {}
med_dict = {}
df_seg_amw= df_60.copy()
churn_plot_new_library(df_seg_amw,seg_name, exclusive = False)



for i in param_dict.keys():
    df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
    df_i['agg_level'] = i
    df_i['mean'] = med_dict[i]
    df_i['expiration_month'] = expiration_month
    df_i['entertainment_segment_lifetime'] = seg_name
    df_final = pd.concat([df_final, df_i])

# COMMAND ----------

df_final

# COMMAND ----------

# df_final = pd.DataFrame()
# m = 'tenure 2+'

# for seg_name in df_60[df_60['entertainment_segment_lifetime']!=0]['entertainment_segment_lifetime'].unique():

#     param_dict = {}
#     med_dict = {}
#     df_seg_amw= df_60[df_60['entertainment_segment_lifetime'] == seg_name]
#     churn_plot_new_library(df_seg_amw,seg_name, exclusive = False)
    
#     for i in param_dict.keys():
#         df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
#         df_i['agg_level'] = i
#         df_i['mean'] = med_dict[i]
#         df_i['expiration_month'] = expiration_month
#         df_i['entertainment_segment_lifetime'] = seg_name
#         df_final = pd.concat([df_final, df_i])

#     # break
        

# COMMAND ----------

######### Look at voL Churn oNLY ########
m = 'tenure 2+'
df_temp = df_60[df_60['entertainment_segment_lifetime']!=0].copy()
df_temp['is_cancel'] = df_temp['is_cancel_vol']

for seg_name in df_temp['entertainment_segment_lifetime'].unique():

    param_dict = {}
    med_dict = {}
    df_seg_amw= df_temp[df_temp['entertainment_segment_lifetime'] == seg_name]
    churn_plot_new_library(df_seg_amw,seg_name, exclusive = False)
    
    for i in param_dict.keys():
        df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
        df_i['agg_level'] = i
        df_i['mean'] = med_dict[i]
        df_i['expiration_month'] = expiration_month
        df_i['entertainment_segment_lifetime'] = seg_name
        df_final = pd.concat([df_final, df_i])

    # break
        

# COMMAND ----------

# df_final[df_final['entertainment_segment_lifetime'] == 'gen_pop']
df_final.entertainment_segment_lifetime.unique()

# COMMAND ----------

spark.createDataFrame(df_final).write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_churn_curve_parameters")

# COMMAND ----------

######### Look at InvoL Churn oNLY ########
df_final = pd.DataFrame()
m = 'tenure 2+'
df_temp = df_60[df_60['entertainment_segment_lifetime']!=0].copy()
df_temp['is_cancel'] = df_temp['is_cancel_invol']

for seg_name in df_temp['entertainment_segment_lifetime'].unique():

    param_dict = {}
    med_dict = {}
    df_seg_amw= df_temp[df_temp['entertainment_segment_lifetime'] == seg_name]
    churn_plot_new_library(df_seg_amw,seg_name, exclusive = False)
    
    for i in param_dict.keys():
        df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
        df_i['agg_level'] = i
        df_i['mean'] = med_dict[i]
        df_i['expiration_month'] = expiration_month
        df_i['entertainment_segment_lifetime'] = seg_name
        df_final = pd.concat([df_final, df_i])

    # break
        

# COMMAND ----------

df_60[df_60['is_cancel_vol'] == 1].titles_viewed.sum()/df_60[df_60['is_cancel'] == 1].titles_viewed.sum()

# COMMAND ----------

# MAGIC %md
# MAGIC # BY TENURE

# COMMAND ----------

df_60_final = df_60.copy()

# COMMAND ----------

mapping_dict = {1:'month_1_to_3', 2:'month_1_to_3', 3:'month_1_to_3', 4:'month_4_to_6', 5:'month_4_to_6', 6:'month_4_to_6', 7:'month_7_to_12', 8:'month_7_to_12', 9:'month_7_to_12', 10:'month_7_to_12', 11:'month_7_to_12', 12:'month_7_to_12'}
df_60_final['tenure_bucket'] = df_60_final['sub_month'].map(mapping_dict).fillna('month_13+')

# COMMAND ----------

(df_60_final[df_60_final['new_titles_viewed']>0].groupby(['tenure_bucket'])['user_id'].nunique()/df_60_final.groupby(['tenure_bucket'])['user_id'].nunique())
# Wathced at lease 1 new content:

# COMMAND ----------

df_60_final[df_60_final['library_titles_viewed']>0].groupby(['tenure_bucket'])['user_id'].nunique()/df_60_final.groupby(['tenure_bucket'])['user_id'].nunique()
# Wathced at lease 1 library content:

# COMMAND ----------

# ####
# df_60_00 = spark.sql('SELECT * FROM bolt_cus_dev.bronze.churn_user_stream60d_segmented_20231201').toPandas()
# df_60_00['tenure_months'] = df_60_00['sub_month']
# df_60_00['monthly_title_viewed'] = np.where(df_60_00['tenure_months']>1, df_60_00['titles_viewed']/2, df_60_00['titles_viewed'])
# df_60_00['monthly_hours_viewed'] = np.where(df_60_00['tenure_months']>1, df_60_00['hours_viewed']/2, df_60_00['hours_viewed'])

# df_60_00['monthly_hours_viewed']  = df_60_00['monthly_hours_viewed'].astype(float)

# COMMAND ----------

def get_churn_bin(df_in, grpby, nbins = 100):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id']+ grpby +['is_cancel']).sum().reset_index()
    df['title_viewed_bin_bucket'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,nbins), duplicates='drop')    
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket', 'sub_month'] +grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    return(df_bin)

# COMMAND ----------

param_tenure_dict = {}
med_tenure_dict = {}
m = 'tenure month 2+'

# df_60= df_60[df_60['sub_month'] > 1]

df_60_t = df_60.groupby(by=['user_id', 'sub_month','is_cancel'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()

# COMMAND ----------

df_60_s = get_churn_bin(df_60_t, [])

med_x= df_60_t.monthly_title_viewed.median()
fig, params = get_churn_plot_simple(df_60_s[df_60_s['title_viewed_bin']<15], 
                                    m, param_tenure_dict, np.array(med_x))
med_tenure_dict[m] = med_x
    # break

# COMMAND ----------

# MAGIC %md
# MAGIC # Tenure by Library/new Content

# COMMAND ----------

df_60['tenure_bucket'] = 'tenure month 2+'
df_60.new_titles_viewed.sum()/df_60.groupby(['tenure_bucket']).titles_viewed.sum()

# COMMAND ----------

['monthly_'+i for i in metric_cols]

# COMMAND ----------

['monthly_'+i for i in metric_cols]

# COMMAND ----------

def churn_plot_new_library(df_60, m, exclusive = False):
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    ########## add seg total ####################
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_'+i for i in metric_cols]].sum().reset_index()

    ##### ADD BY Category ######
    for i in ['monthly_titles_viewed', 'monthly_new_titles_viewed', 'monthly_library_titles_viewed']:
        
        df_60_t_amw= df_60_t.copy()
        df_60_t_amw['monthly_title_viewed'] = df_60_t_amw[i]

        if exclusive == True:
            print( 'Mutually Exclusive')
            if i == 'monthly_library_titles_viewed':
                df_60_t_amw = df_60_t_amw[df_60_t_amw['user_id'].isin(df_library.user_id)]
            if i == 'monthly_new_titles_viewed':
                df_60_t_amw = df_60_t_amw[~df_60_t_amw['user_id'].isin(df_library.user_id)]

        df_60_s = get_churn_bin(df_60_t_amw, [])
        df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
        df_60_s = df_60_s[df_60_s['title_viewed_bin']<=15]
        med_x= df_60_t_amw.monthly_title_viewed.median()

        x_fit, params = fit_exponential(df_60_s.title_viewed_bin, df_60_s.churn, p0, param_bounds)
        a_fit, b_fit, c_fit = params
        x_fit = np.linspace(0, 15, 100)
        y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
        y_med = exponential_decay(med_x, a_fit, b_fit, c_fit)
        y_med_slope = exponential_decay_slope(med_x, a_fit, b_fit)

        print(med_x)
        print(i+' average churn: ' + str('{:.3f}'.format(y_med)))
        print(i+' slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
        print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
    
        if i == 'monthly_new_titles_viewed': 
            i = 'new'
            color = 'pink'
        elif i == 'monthly_library_titles_viewed':
            color = 'red'
            i = 'library'
        else:
            color = 'blue'
            i = 'total'
        fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = i+' content', line_color = color))
        fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, mode='markers', name = i+'_raw', line_color = color)
        # fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=10, color='red', line=dict(color='black', width=1)), showlegend=False)
        # fig.add_scatter(x=df_60_s.title_viewed_bin, y=exponential_decay(df_60_s.title_viewed_bin, a_fit, b_fit, c_fit), 
        #                 showlegend=False, error_y = dict(type = 'data', array = df_60_s.sdv, visible = True),
        #                 name = i+' error bars', mode='markers', marker = dict(color = color))

        param_dict[i] = params
        med_dict[i] = med_x


    fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0.01,15]),
    # yaxis=dict(range=[0.00,0.08])
    ) 
    fig.show()


# COMMAND ----------

# df_60 = df_60[df_60['monthly_title_viewed']>0]
# df_test = df_60.groupby(by=['user_id'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
# df_test = df_test[df_test['monthly_title_viewed'] == 1]
# df_1 = df_60[df_60['user_id'].isin(df_test.user_id)]
# df_1 = df_60.groupby(['user_id', 'old_new'])[['monthly_hours_viewed']].sum()/df_60.groupby(['user_id'])[['monthly_hours_viewed']].sum()
# df_1 = df_1.reset_index()
# df_library = df_1[(df_1['old_new']=='library') & (df_1['monthly_hours_viewed']>=0.85)] #80% of the population

# COMMAND ----------

param_dict = {}
med_dict = {}
m = 'tenure 2+'
churn_plot_new_library(df_60,m, exclusive = False)

# COMMAND ----------

med_dict['new']

# COMMAND ----------

df_final = pd.DataFrame()
for i in param_dict.keys():
    df_i = pd.DataFrame([param_dict[i]], columns = ['a', 'b', 'c'])
    df_i['agg_level'] = i
    df_i['mean'] = med_dict[i]
    df_i['expiration_month'] = expiration_month
    df_final = pd.concat([df_final, df_i])

# COMMAND ----------

spark.createDataFrame(df_final).write.mode("overwrite").saveAsTable("bolt_cus_dev.bronze.cip_churn_curve_parameters")

# COMMAND ----------

# churn_plot_new_library(df_60, m, groupby_col, exclusive = True)

# COMMAND ----------

def get_equal_churn_bin(df_in, grpby):
    # df = df_in[df_in.monthly_hours_viewed<=60]
    df = df_in.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    # nbins = int(df.monthly_title_viewed.max())
    # df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], np.linspace(0,nbins,2*nbins+1))
    
    bins =[-0.01]
    if df.monthly_title_viewed.max() <=19:
        bins = bins + np.arange(0, df.monthly_title_viewed.max() + 0.5, 0.5).tolist()
    else:
        bins = bins + np.arange(0, 12.5, 0.5).tolist()
        bins = bins + np.arange(13, 17, 1.0).tolist()
        bins = bins + [19.0, df.monthly_title_viewed.max()]

    df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], bins,include_lowest =True)
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_title_viewed':'sum',
                                                         'total_monthly_title_viewed':'mean'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')

    return df_bin

# COMMAND ----------

df_60_00[df_60_00['user_id'] == 1680197285172035360]

# COMMAND ----------

df_60_test =  df_60.groupby(['user_id'])['monthly_title_viewed'].count().reset_index()
df_60_test[df_60_test['monthly_title_viewed']>1]

# COMMAND ----------

def churn_plot_new_library_ouputtable(df_60, m, exclusive = False):

    # ########## add seg total ####################
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
    
    ##### ADD BY Category ######
    for i in ['monthly_title_viewed', 'monthly_new_titles_viewed', 'monthly_library_titles_viewed']:
        
        df_seg_amw= df_60_t.copy()
        df_seg_amw['total_monthly_title_viewed'] = df_seg_amw['monthly_title_viewed']
        df_seg_amw['monthly_title_viewed'] = df_seg_amw[i]
        df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed', 'total_monthly_title_viewed']].sum().reset_index()
        df_60_s = get_equal_churn_bin(df_60_t, [])
        output_df[i] = df_60_s


# COMMAND ----------

df_60.head()

# COMMAND ----------

groupby_col = 'old_new'
m = 'tenure 2+'
output_df = {}
df_test = df_60[df_60['sub_month']>1].copy()
df_test.loc[(df_test['titles_viewed'] == 5) &(df_test['is_cancel'] == 1), 'is_cancel_5'] = df_test['is_cancel']
df_test['is_cancel_5'] = df_test['is_cancel_5'].fillna(0)
df_test['is_cancel'] = df_test['is_cancel_5']
churn_plot_new_library_ouputtable(df_test,m, exclusive = False)

# COMMAND ----------

output_df.keys()

# COMMAND ----------

output_df['total'].head(10)

# COMMAND ----------

output_df['monthly_library_titles_viewed'][['title_viewed_bin_bucket', 'title_viewed_bin', 'churn', 'user_dist', 'user_id', 'is_cancel','total_monthly_title_viewed']]

# COMMAND ----------

# MAGIC %md
# MAGIC Invol V.S Vol

# COMMAND ----------

df_60.head()

# COMMAND ----------

param_tenure_dict = {}
med_tenure_dict = {}
groupby_col = 'old_new'
p0 = [0.5, -0.1, 0.01] 
param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

# add seg total
df_60_t = df_60.groupby(by=['user_id','is_cancel','is_voluntary'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, ['is_voluntary'])
df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
df_60_s = df_60_s[df_60_s['title_viewed_bin']<=15]
med_x= df_60_t.monthly_title_viewed.median()

x_fit, params = fit_exponential(df_60_s.title_viewed_bin, df_60_s.churn, p0, param_bounds)
a_fit, b_fit, c_fit = params
y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
y_med = exponential_decay(med_x, a_fit, b_fit, c_fit)
y_med_slope = exponential_decay_slope(med_x, a_fit, b_fit)

print(med_x)
print('average churn: ' + str('{:.3f}'.format(y_med)))
print('slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))


fig = px.scatter(title=m, width=600, height=400)
# fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=False)
fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = 'segment total'))
fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=14, color='red', line=dict(color='black', width=2)), showlegend=False)


##### ADD BY Category ######
for i in df_60[groupby_col].unique():
    
    df_seg_amw= df_60[(df_60['tenure_bucket'] == m) & (df_60[groupby_col] == i)]
    df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])
    df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
    df_60_s = df_60_s[df_60_s['title_viewed_bin']<=15]
    med_x= df_60_t.monthly_title_viewed.median()

    x_fit, params = fit_exponential(df_60_s.title_viewed_bin, df_60_s.churn, p0, param_bounds)
    a_fit, b_fit, c_fit = params
    x_fit = np.linspace(0, 15, 100)
    y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
    y_med = exponential_decay(med_x, a_fit, b_fit, c_fit)
    y_med_slope = exponential_decay_slope(med_x, a_fit, b_fit)

    print(med_x)
    print(i+' average churn: ' + str('{:.3f}'.format(y_med)))
    print(i+' slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
    print('y= {:.2f} * e^({:.2f} * title_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
   
    if i == 'current': i = 'new'
    fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = i+' content'))
    # fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=False)
    fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=10, color='red', line=dict(color='black', width=1)), showlegend=False)


fig.update_layout(
template='simple_white',
# showlegend=True,
xaxis=dict(range=[0,15]),
) 
fig.show()

# break

# COMMAND ----------

# MAGIC %md
# MAGIC # Take Away Content

# COMMAND ----------

df_60.head()

# COMMAND ----------

# Invol 
df_60[df_60['monthly_library_titles_viewed']>0][(df_60['is_voluntary'] == 0)&(df_60['is_cancel'] == 1)].groupby(['entertainment_segment_lifetime']).user_id.nunique()/df_60[df_60['monthly_library_titles_viewed']>0][(df_60['is_cancel'] == 1)].groupby(['entertainment_segment_lifetime']).user_id.nunique()

# COMMAND ----------


