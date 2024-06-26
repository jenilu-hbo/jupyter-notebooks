# Databricks notebook source
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
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
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
        xaxis={'range':[0.25,15]},
        # xaxis = {tickvals: [0.5, 5, 10, 15]},
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
        xaxis=dict(range=[1,15]),
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

target_month = '2023-12-01'

# COMMAND ----------

df_60_00 = spark.sql('''
      WITH new_library AS (
      SELECT s.*
                     , CASE WHEN r.start_date <= '{target_month}' and r.recency_window_end >= '{target_month}' and 
                        (year('{target_month}') = r.median_release_year OR year('{target_month}') = r.release_year_plusone)
                        THEN 'current'
                        ELSE 'library'
                        END AS old_new
      FROM bolt_cus_dev.bronze.cip_churn_user_stream60d_genpop_20231201 s
      left join bolt_cus_dev.bronze.cip_recency_title_offering_table r
         on s.ckg_program_id = r.ckg_program_id
      )
      SELECT user_id, profile_id, is_cancel, is_voluntary, sub_month
            , sum(hours_viewed) as hours_viewed
            , count(distinct ckg_series_id) as titles_viewed
            , count(distinct (CASE WHEN old_new = 'current' THEN ckg_series_id else null END)) 
               as new_titles_viewed
            , count(distinct (CASE WHEN old_new = 'library' THEN ckg_series_id else null END)) 
               as library_titles_viewed
      FROM new_library
      GROUP BY ALL
                     '''.format(target_month = target_month)).toPandas()
# cip_churn_user_stream60d_genpop_new_lib_202401
# cip_churn_user_stream60d_genpop_new_lib_202310

# COMMAND ----------

metric_cols = ['hours_viewed', 'titles_viewed', 'new_titles_viewed', 'library_titles_viewed']
df_60_00=get_df_test(df_60_00, metric_cols)
df_list = get_df_60_h([df_60_00]) #, df_60_0, df_60_1, df_60_2])
df_60 = pd.concat(df_list)
display(df_60.head())

# COMMAND ----------

df_60.user_id.nunique() #8716058

# COMMAND ----------

df_60 = df_60[df_60['titles_viewed'] > 0].copy()
df_60 = df_60[df_60.tenure_months>1]

# COMMAND ----------

df_60[(df_60['is_voluntary'] == 0)&(df_60['is_cancel'] == 1)].user_id.nunique()/df_60[(df_60['is_cancel'] == 1)].user_id.nunique()
###### Invol churn distribution #####

# COMMAND ----------

param_dict = {}
med_dict = {}

# COMMAND ----------

seg_name = 'gen_pop'
df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()

# COMMAND ----------

df_60_s = get_equal_churn_bin(df_60_t, [])
# # df_60_s[['title_viewed_bin_bucket', 'title_viewed_bin', 'churn', 'user_dist', 'user_id', 'is_cancel', 'monthly_title_viewed']]

# COMMAND ----------

# df_60_s = get_churn_bin(df_60_t, [])
# ## Get median 
# med_x= df_60_t.monthly_title_viewed.median()

# Plot the Churve 
fig, params = get_churn_plot_simple(df_60_s[(df_60_s['title_viewed_bin']<15)], 
                                    seg_name, param_dict, np.array(med_x))
med_dict[seg_name] = med_x

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution

# COMMAND ----------

df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')
df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month',])\
            [['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, [])

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
# MAGIC # BY TENURE

# COMMAND ----------

# ####
# df_60_00 = spark.sql('SELECT * FROM bolt_cus_dev.bronze.churn_user_stream60d_segmented_20231201').toPandas()
# df_60_00['tenure_months'] = df_60_00['sub_month']
# df_60_00['monthly_title_viewed'] = np.where(df_60_00['tenure_months']>1, df_60_00['titles_viewed']/2, df_60_00['titles_viewed'])
# df_60_00['monthly_hours_viewed'] = np.where(df_60_00['tenure_months']>1, df_60_00['hours_viewed']/2, df_60_00['hours_viewed'])

# df_60_00['monthly_hours_viewed']  = df_60_00['monthly_hours_viewed'].astype(float)

# COMMAND ----------

df_60_final = df_60[df_60['sub_month']>1].copy()

# COMMAND ----------

mapping_dict = {1:'month_1_to_3', 2:'month_1_to_3', 3:'month_1_to_3', 4:'month_4_to_6', 5:'month_4_to_6', 6:'month_4_to_6', 7:'month_7_to_12', 8:'month_7_to_12', 9:'month_7_to_12', 10:'month_7_to_12', 11:'month_7_to_12', 12:'month_7_to_12'}
df_60_final['tenure_bucket'] = df_60_final['sub_month'].map(mapping_dict).fillna('month_13+')

# COMMAND ----------

(df_60_final[df_60_final['new_titles_viewed']>0].groupby(['tenure_bucket'])['user_id'].nunique()/df_60_final.groupby(['tenure_bucket'])['user_id'].nunique())+0.2

# COMMAND ----------

df_60_final[df_60_final['library_titles_viewed']>0].groupby(['tenure_bucket'])['user_id'].nunique()/df_60_final.groupby(['tenure_bucket'])['user_id'].nunique()

# COMMAND ----------

param_dict = {}
med_dict = {}

for i in df_60_final['tenure_bucket'].unique():
    df_seg_amw= df_60_final[df_60_final['tenure_bucket'] == i]

    df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])

    med_x= df_60_t.monthly_title_viewed.median()
    fig, params = get_churn_plot_simple(df_60_s[df_60_s['title_viewed_bin']<15], 
                                        i, param_dict, np.array(med_x))
    
    med_dict[i] = med_x

# COMMAND ----------

fig = go.Figure()
i = 0
for m in df_60_final['tenure_bucket'].unique():
    color_discrete_sequence=["red", "green", "blue", "goldenrod", "magenta", "orange", "purple"]+px.colors.qualitative.Plotly
    color_cur = color_discrete_sequence[i]

    df_seg_amw= df_60_final[df_60_final['tenure_bucket'] == m]
    df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed', 'monthly_title_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])
    med_x= df_60_t.monthly_title_viewed.median()

    fig.add_trace(
        go.Scatter(
            mode='lines',
            x=df_60_s.title_viewed_bin,
            y=df_60_s.churn,
            marker=dict(
                color=color_cur,
                size=20,
                line=dict(
                    color=color_cur,
                    width=2
                )
            ),
            showlegend=True,
            name = 'month '+ m
        )
    )   
    
    i = i+1

fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0,45]),
    
) 
fig.show()

# COMMAND ----------

df_60_s ## >20, 11000/284593

# COMMAND ----------

# MAGIC %md
# MAGIC # Tenure by Library/new Content

# COMMAND ----------

df_60['tenure_bucket'] = 'tenure month 2+'
# df_60 = df_60[df_60['sub_month'] > 1]
df_60.new_titles_viewed.sum()/df_60.groupby(['tenure_bucket']).titles_viewed.sum()

# COMMAND ----------

df_60_t = df_60.groupby(by=['user_id','tenure_bucket'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
df_60_t_content = df_60.groupby(by=['user_id','is_cancel', 'tenure_bucket', 'old_new'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()

# COMMAND ----------

df_final = df_60_t_content.merge(df_60_t.rename(columns = {'monthly_title_viewed':'total_title_viewed', 'monthly_hours_viewed':'total_hours_viewed'}), 
                                on = ['user_id','tenure_bucket'])
df_final['frac'] = df_final['monthly_title_viewed']/df_final['total_title_viewed']

# COMMAND ----------

df_final.head()

# COMMAND ----------

df_final = df_final[df_final['frac'].notnull()]

# COMMAND ----------

df_final[df_final['frac'] < 1]['user_id'].nunique()/df_final['user_id'].nunique() # 53% users watched mixed content

# COMMAND ----------

df_final[(df_final['frac'] < 1) & (df_final['old_new'] == 'current')]['monthly_title_viewed'].sum()/df_final[df_final['frac'] < 1]['monthly_title_viewed'].sum() ### OUT OF THE MIXED CONTENT WATCHED, 14% of the content watched are new content

# COMMAND ----------

def get_churn_bin_sdv(df_in, grpby, nbins = 100):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id']+ grpby +['is_cancel']).sum().reset_index()
    # df['title_viewed_bin_bucket'] = pd.qcut(df['monthly_title_viewed'], np.linspace(0,1,nbins), duplicates='drop') 
    nbins = int(df.monthly_title_viewed.max())
    df['title_viewed_bin_bucket'] = pd.cut(df['monthly_title_viewed'], 
                                    np.linspace(0,nbins,2*nbins+1))   
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['title_viewed_bin_bucket'] +grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    
    df_bin['user_dist'] = df_bin['user_id']/df_bin['user_id'].sum()
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin_bucket'].apply(lambda x: (x.left+x.right)/2)
    df_bin['title_viewed_bin'] = df_bin['title_viewed_bin'].astype('float')
    df_bin['sdv'] = np.sqrt(df_bin['churn'] * (1- df_bin['churn'])/df_bin['user_id'])
    df_bin['error_low'] = df_bin['churn'] - 3*df_bin['sdv']
    df_bin['error_high'] = df_bin['churn'] + 3*df_bin['sdv']
    return(df_bin)

# COMMAND ----------

def churn_plot_new_library(df_60, m, exclusive = False):
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    ########## add seg total ####################
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])
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


    fig = px.scatter(title=m)
    fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = 'segment total'))
    # fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=14, color='red', line=dict(color='black', width=2)), showlegend=False)
    # fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, name = 'Segment Total Raw Data Points', mode='markers')



    ##### ADD BY Category ######
    for i in ['monthly_new_titles_viewed', 'monthly_library_titles_viewed']:
        
        df_seg_amw= df_60.copy()
        df_seg_amw['monthly_title_viewed'] = df_seg_amw[i]

        if exclusive == True:
            print( 'Mutually Exclusive')
            if i == 'library':
                df_seg_amw = df_seg_amw[df_seg_amw['user_id'].isin(df_library.user_id)]
            else:
                df_seg_amw = df_seg_amw[~df_seg_amw['user_id'].isin(df_library.user_id)]


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
    
        if i == 'monthly_new_titles_viewed': 
            i = 'new'
            color = 'pink'
        else:
            color = 'red'
        fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = i+' content', line_color = color))
        fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, mode='markers', name = i+'_raw', line_color = color)
        # fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=10, color='red', line=dict(color='black', width=1)), showlegend=False)
        # fig.add_scatter(x=df_60_s.title_viewed_bin, y=exponential_decay(df_60_s.title_viewed_bin, a_fit, b_fit, c_fit), 
        #                 showlegend=False, error_y = dict(type = 'data', array = df_60_s.sdv, visible = True),
        #                 name = i+' error bars', mode='markers', marker = dict(color = color))


    fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0.01,15]),
    # yaxis=dict(range=[0.00,0.08])
    ) 
    fig.show()


# COMMAND ----------

# df_test = df_60.groupby(by=['user_id'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
# df_test = df_test[df_test['monthly_title_viewed'] == 1]
# df_1 = df_60[df_60['user_id'].isin(df_test.user_id)]
# df_1 = df_60.groupby(['user_id', 'old_new'])[['monthly_hours_viewed']].sum()/df_60.groupby(['user_id'])[['monthly_hours_viewed']].sum()
# df_1 = df_1.reset_index()
# df_library = df_1[(df_1['old_new']=='library') & (df_1['monthly_hours_viewed']>=0.85)] #80% of the population

# COMMAND ----------

m = 'tenure 2+'
df_60_2_plus = df_60[(df_60['sub_month'] > 1) & (df_60['titles_viewed'] > 0)]
churn_plot_new_library(df_60_2_plus, m, exclusive = False)


# COMMAND ----------

df_60['monthly_hours_viewed'] = df_60['monthly_hours_viewed'].astype('float')
df_60['monthly_title_viewed'] = df_60['monthly_title_viewed'].astype('float')

# COMMAND ----------


def churn_plot_new_library_ouputtable(df_60, m, exclusive = False):

    # ########## add seg total ####################
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
    df_60_s = get_equal_churn_bin(df_60_t, [])
    df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
    output_df['total'] = df_60_s


    ##### ADD BY Category ######
    for i in ['monthly_new_titles_viewed', 'monthly_library_titles_viewed']:
        
        df_seg_amw= df_60.copy()
        df_seg_amw['monthly_title_viewed'] = df_seg_amw[i]
        df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
        df_60_s = get_equal_churn_bin(df_60_t, [])
        df_60_s['title_viewed_bin'] = df_60_s['title_viewed_bin'].astype(float)
        output_df[i] = df_60_s

# COMMAND ----------

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

# COMMAND ----------

m = 'tenure 2+'
output_df = {}
df_60_test = df_60[df_60['sub_month']>1]
churn_plot_new_library_ouputtable(df_60_test,m, exclusive = False)

# COMMAND ----------

output_df['monthly_new_titles_viewed'][['title_viewed_bin_bucket', 'title_viewed_bin', 'churn', 'user_dist', 'user_id', 'is_cancel',]]

# COMMAND ----------

output_df['monthly_new_titles_viewed'][['title_viewed_bin_bucket', 'title_viewed_bin', 'churn', 'user_dist', 'user_id', 'is_cancel',]]

# COMMAND ----------

# MAGIC %md
# MAGIC # Invol VS Vol

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vol Cohort

# COMMAND ----------

df_60_vol = df_60.copy()
df_60_vol.loc[(df_60_vol['is_voluntary'] == 1) & (df_60_vol['is_cancel']==1), 'vol_churn'] = 1
df_60_vol['vol_churn'] = df_60_vol['vol_churn'].fillna(0)
df_60_vol['is_cancel'] = df_60_vol['vol_churn']

# COMMAND ----------

groupby_col = 'old_new'
m = 'tenure 2+'
churn_plot_new_library(df_60_vol,m, groupby_col, exclusive = False)

# COMMAND ----------

churn_plot_new_library(df_60_vol,m, groupby_col, exclusive = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invol Cohort

# COMMAND ----------

def churn_plot_new_library_with_raw(df_60, m, groupby_col=[], exclusive = False):
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    # add seg total
    df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])
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
    fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = 'segment total'))
    # fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=14, color='red', line=dict(color='black', width=2)), showlegend=False)
    fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, name = 'Segment Total Raw Data Points', mode='markers')



    ##### ADD BY Category ######
    for i in df_60[groupby_col].unique():
        
        df_seg_amw= df_60[(df_60[groupby_col] == i)]

        if exclusive == True:
            print( 'Mutually Exclusive')
            if i == 'library':
                df_seg_amw = df_seg_amw[df_seg_amw['user_id'].isin(df_library.user_id)]
            else:
                df_seg_amw = df_seg_amw[~df_seg_amw['user_id'].isin(df_library.user_id)]


        df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_title_viewed', 'monthly_hours_viewed']].sum().reset_index()
        df_60_s = get_churn_bin_sdv(df_60_t, [])
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
    
        if i == 'current': 
            i = 'new'
            color = 'pink'
        else:
            color = 'red'
        fig.add_trace(go.Scatter(x=x_fit, y=y_fit, mode='lines', showlegend=True, name = i+' content', line_color = color))
        # fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=False)
        fig.add_scatter(x=np.array(med_x), y=np.array(y_med), mode='markers', marker=dict(size=10, color='red', line=dict(color='black', width=1)), showlegend=False)
        fig.add_scatter(x=df_60_s.title_viewed_bin, y=df_60_s.churn, showlegend=True, name = i + ' Raw Data Points', mode='markers')


    fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0.25,15]),
    ) 
    fig.show()


# COMMAND ----------

df_60_invol = df_60.copy()
df_60_invol.loc[(df_60_invol['is_voluntary'] == 0) & (df_60_invol['is_cancel']==1), 'invol_churn'] = 1
df_60_invol['invol_churn'] = df_60_invol['invol_churn'].fillna(0)
df_60_invol['is_cancel'] = df_60_invol['invol_churn']

# COMMAND ----------

groupby_col = 'old_new'
m = 'tenure 2+'
churn_plot_new_library_with_raw(df_60_invol,m, groupby_col, exclusive = False)

# COMMAND ----------

churn_plot_new_library_with_raw(df_60_invol,m, groupby_col, exclusive = True)

# COMMAND ----------


