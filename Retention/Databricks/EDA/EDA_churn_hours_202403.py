# Databricks notebook source
# !pip install snowflake-connector-python
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

# from utils import *

# COMMAND ----------

from scipy.stats import percentileofscore


def get_df_test(df_test):
    df_test['tenure_months'] = df_test['sub_month']
    df_test['monthly_hours_viewed'] = np.where(df_test['tenure_months']>1, df_test['hours_viewed']/2, df_test['hours_viewed'])
    user_total = df_test.groupby(['user_id'])['monthly_hours_viewed'].transform('sum')
    df_test['frc'] = df_test['monthly_hours_viewed'] / user_total
    
    df_test['program_type'] = np.where((df_test.program_type=='original') & (df_test.old_new=='library'), 'acquired', df_test.program_type)
    df_test = df_test[df_test.tenure_months>2]
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


def get_churn_bin(df_in, grpby):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    nbins = 100
    df['monthly_hours_viewed'] = df['monthly_hours_viewed'].astype(float)
    df['hours_viewed_bin'] = pd.qcut(df['monthly_hours_viewed'], np.linspace(0,1,nbins), duplicates='drop')
    df['hours_viewed_bin'] = df['hours_viewed_bin'].apply(lambda x: (x.left+x.right)/2)
    df['hours_viewed_bin'] = df['hours_viewed_bin'].astype('float')
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['hours_viewed_bin']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    return(df_bin)

# COMMAND ----------

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
                  width=500, height=400)
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
            text='y= {:.2f} * e^({:.2f} * hours_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit),  # the text to display
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
    print('y= {:.2f} * e^({:.2f} * hours_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
    print('y= {:.3f} * e^({:.2f} * hours_viewed)'.format(a_fit*b_fit,b_fit))
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
        xaxis=dict(range=[0,50]),
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
    df_i = df_i[df_i.is_cancel>=20]
#         display(df_i.tail(5))

    x_var = df_i.hours_viewed_bin
    y_data = df_i.churn
    p0 = [0.5, -0.1, 0.01] 
    param_bounds = ([0, -0.8, 0.01], [np.inf, -0.1, np.inf])

    x_fit, params = fit_exponential(x_var, y_data, p0, param_bounds)
    a_fit, b_fit, c_fit = params
    y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)
    
    if x_med==0:
        fig = get_simple_plot_multiple(df_i, 'hours_viewed_bin', 'churn', x_fit, y_fit, params, f'{title}')
    else:
        y_med = exponential_decay(x_med, a_fit, b_fit, c_fit)
        y_med_slope = exponential_decay_slope(x_med, a_fit, b_fit)
        print(x_med)
        print('average churn: ' + str('{:.3f}'.format(y_med)))
        print('slope: ' + str('{:.3f}'.format(y_med_slope*100))+'%')
        fig = get_simple_plot_multiple_dot(df_i, 'hours_viewed_bin', 'churn', x_fit, y_fit, params, x_med, np.array(y_med), f'{title}')
    # display(df_i.head())
    param_dic[title] = params
    return fig, params



def get_simple_plot_dot(df_plt, x, y, x_fit, y_fit, params, x_med, y_med, title=''):
    if title=='':
        
        title = f'{y} vs {x}'
       
    a_fit, b_fit, c_fit = params
    print('y= {:.2f} * e^({:.2f} * hours_viewed) + {:.2f}'.format(a_fit, b_fit, c_fit))
    print('y= {:.3f} * e^({:.2f} * hours_viewed)'.format(a_fit*b_fit,b_fit))
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
        xaxis=dict(range=[0,50]),
        xaxis_title= "hours_viewed_bin",
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

    x_var = df_i.hours_viewed_bin
    x_fit = np.linspace(0, x_var.max(), 100)   
    a_fit, b_fit, c_fit = params
    y_fit = exponential_decay_slope(x_fit, a_fit, b_fit)
    
    y_med = exponential_decay_slope(x_med, a_fit, b_fit)
    print(x_med)
    print(y_med)
    fig = get_simple_plot_dot(df_i, 'hours_viewed_bin', 'churn', x_fit, y_fit, params, x_med, np.array(y_med), f'{title}')
    display(df_i.head())
    param_dic[title] = params
    return fig


# COMMAND ----------

df_60_00 = pd.read_parquet(file_path+'churn_user_stream60d_segmented_20240101.parquet')
df_60_00=get_df_test(df_60_00)

# COMMAND ----------

df_60_00 = pd.read_parquet(file_path+'churn_user_stream60d_segmented_20240103.parquet')
df_60_00=get_df_test(df_60_00)
df_60_00.head()

# COMMAND ----------

df_list = get_df_60_h([df_60_00]) #, df_60_0, df_60_1, df_60_2])
df_60 = pd.concat(df_list)
display(df_60.head())

# COMMAND ----------

import plotly.express as px

# COMMAND ----------

param_dict = {}
med_dict = {}

# COMMAND ----------

seg_name = 'gen_pop'
df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, [])

## Get median 
med_x= df_60_t.monthly_hours_viewed.median()

fig, params = get_churn_plot_simple(df_60_s, seg_name, param_dict, np.array(med_x))
med_dict[seg_name] = med_x

# COMMAND ----------

# MAGIC %md
# MAGIC # BY Segment

# COMMAND ----------

segment_info = spark.sql('''
SELECT a.entertainment_segment_lifetime, count(distinct a.user_id) as user_count
FROM  bolt_growthml_int.gold.max_content_preference_v3_segment_assignments_360_landing_table a
join bolt_cus_dev.bronze.user_retain_churn_list_test_wbd_max b
ON a.user_id = b.user_id 
where cycle_expire_date between '2024-01-01' and '2024-01-31'
group by all
order by user_count desc        
                  ''').toPandas()

# COMMAND ----------

segment_info.head(8)o

# COMMAND ----------

for seg_name in segment_info['entertainment_segment_lifetime'].unique():
    df_seg_amw= df_60[df_60['entertainment_segment_lifetime'] == seg_name]
    df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed']].sum().reset_index()
    df_60_s = get_churn_bin(df_60_t, [])

    med_x= df_60_t.monthly_hours_viewed.median()
    fig, params = get_churn_plot_simple(df_60_s, seg_name, param_dict, np.array(med_x))
    # slope = get_churn_slope_plot_simple(df_60_s, , params, np.array(med_x))
    med_dict[seg_name] = med_x
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Action Movies Watchers

# COMMAND ----------

df_seg_amw= df_60[df_60['entertainment_segment_lifetime'] == 'Action Movies Watchers']
df_60_t = df_seg_amw.groupby(by=['user_id','is_cancel','sub_month'])[['monthly_hours_viewed']].sum().reset_index()
df_60_s = get_churn_bin(df_60_t, [])

med_x= df_60_t.monthly_hours_viewed.median()
fig, params = get_churn_plot_simple(df_60_s, 'Action Movies Watchers', param_dic, np.array(med_x))
slope = get_churn_slope_plot_simple(df_60_s, 'Action Movies Watchers slope', params, np.array(med_x))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution of the audience 

# COMMAND ----------

segment_info_top

# COMMAND ----------

segment_info_top = segment_info.head(6)

# COMMAND ----------

def get_churn_equal_bin(df_in, grpby):
    df = df_in[df_in.monthly_hours_viewed<=60]
    df = df.groupby(by=['user_id','sub_month']+ grpby +['is_cancel']).sum().reset_index()
    bins_range = list(range(0, 61, 5))
    df['monthly_hours_viewed'] = df['monthly_hours_viewed'].astype(float)
    df['hours_viewed_bin'] = pd.cut(df['monthly_hours_viewed'], bins_range, include_lowest=True)
    df['hours_viewed_bin'] = df['hours_viewed_bin'].apply(lambda x: (str(int(x.left))+'-'+str(int(x.right))))
    # df['hours_viewed_bin'] = df['hours_viewed_bin'].astype('float')
    df['churn'] = 1*df['is_cancel']  
    
    df_bin = df.groupby(['hours_viewed_bin']+grpby).agg({'churn':'mean', 'user_id':'count',
                                                         'is_cancel':'sum','monthly_hours_viewed':'sum'}).reset_index()
    return(df_bin)

# COMMAND ----------

df_60_t = df_60.groupby(by=['user_id','is_cancel','sub_month', 'entertainment_segment_lifetime'])[['monthly_hours_viewed']].sum().reset_index()
df_60_t = df_60_t[df_60_t['entertainment_segment_lifetime'].isin(segment_info_top.entertainment_segment_lifetime.unique())]
df_60_s = get_churn_equal_bin(df_60_t, ['entertainment_segment_lifetime'])

user_total = df_60_s.groupby(['entertainment_segment_lifetime'])['user_id'].transform('sum')
df_60_s['Composition'] = df_60_s['user_id']/user_total

# COMMAND ----------

df_60_s.head()

# COMMAND ----------

fig = px.bar(df_60_s, x="hours_viewed_bin", y="Composition",
             color='entertainment_segment_lifetime', barmode='group',
             height=400)
fig.layout.yaxis.tickformat = ',.0%'
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Segments by Segments

# COMMAND ----------

param_dict 
med_dict 

# COMMAND ----------

param_dict

# COMMAND ----------

import plotly.graph_objects as go

# COMMAND ----------

fig = go.Figure()
i = 0
for seg_name in param_dict.keys():
    color_discrete_sequence=["red", "green", "blue", "goldenrod", "magenta", "orange", "purple"]
    color_cur = color_discrete_sequence[i]
    if seg_name in segment_info_top.entertainment_segment_lifetime.unique():
        a_fit, b_fit, c_fit = param_dict[seg_name]
        x_med = med_dict[seg_name]
        x_fit = np.linspace(0, 60, 100)   

        y_med = exponential_decay(x_med, a_fit, b_fit, c_fit)
        y_fit = exponential_decay(x_fit, a_fit, b_fit, c_fit)

        # fig.add_scatter( 
        #         x=x_fit, 
        #         y=y_fit, name = seg_name, color_discrete_sequence = color_cur)
        fig.add_trace(
            go.Scatter(
                mode='lines',
                x=x_fit,
                y=y_fit,
                marker=dict(
                    color=color_cur,
                    size=20,
                    line=dict(
                        color=color_cur,
                        width=2
                    )
                ),
                showlegend=True,
                name = seg_name
            )
        )   
        
        fig.add_scatter( 
                x=np.array(x_med), 
                y=np.array(y_med),
                    mode='markers',
                marker=dict(size=14, color=color_cur, line=dict(color='black', width=2),
                ),
                showlegend=False
                )
        i = i+1

fig.update_layout(
    template='simple_white',
    # showlegend=True,
    xaxis=dict(range=[0,50]),
    
) 
fig.show()



# COMMAND ----------


