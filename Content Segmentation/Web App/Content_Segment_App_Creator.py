import streamlit as st
import pandas as pd
import numpy as np
from PIL import Image


### SETTING STARTING FORMAT

st.set_page_config(layout="wide")
st.title("Content Groups")

# SELECT NUMBER OF CLUSTERS
num_of_cluster = st.sidebar.slider('Select number of content clusters', 0, 16, 7)

# READ CLUSTERING RESULTS
result = pd.read_csv('clustering_results.csv')
result = result[result['number_of_cluster'] == num_of_cluster]
# result.drop(['index'], axis = 1, inplace = True)
num_comps = 15

# read/write images
st.header('Clustering Results')
result = result.sort_values(by = ['acquiring_power'], ascending = False)\
               .groupby('cluster').head(num_comps)\
               .sort_values(by = ['cluster'])
for c in result.cluster.unique():
    image_df = result[result['cluster'] == c].sort_values(by = ['acquiring_power'], ascending = False).reset_index()
    cols = st.columns(num_comps+1)
    for i, x in enumerate(cols):
        if i >= num_comps:
            x.write('Cluster ' + str(c))
        else:
            imdb_url = image_df.loc[i]['imdb_url']    
            title_name = image_df.loc[i]['title_name']
            x.image(imdb_url, use_column_width = True, caption = title_name)
            
# Sankey Diagram
st.header('Genre Assignments')
original_genre = pd.read_csv('original_genre.csv')
assignments_genre = result.merge(original_genre, on = ['title_id'])
assignments_genre = assignments_genre.groupby(['genre', 'cluster'])['title_id'].count().reset_index()
label = list(assignments_genre.genre.unique()) + list(assignments_genre.cluster.unique())

label_dict = {}
for i in range(len(assignments_genre.genre.unique())):
    label_dict[label[i]] = i
    
assignments_genre = assignments_genre.replace({'genre':label_dict})
number_of_genre = len(assignments_genre.genre.unique())
assignments_genre['cluster'] = assignments_genre['cluster'] + number_of_genre

source = list(assignments_genre.genre.values)
target = list(assignments_genre.cluster.values)
value = list(assignments_genre.title_id.values)

import plotly.graph_objects as go

link = dict(source = source, target = target, value = value)
node = dict(label = label, pad=50, thickness=5)
data = go.Sankey(link = link, node=node)
# plot
fig = go.Figure(data)
st.plotly_chart(fig, use_container_width=True)

# Features to come:
st.header('--------------------------------------------------------------')
st.header('FEATURES IN DEVELOPMENT:')
st.subheader('1. Any Number of cluster can be selected')
st.subheader('2. Using NLP and LDA to self Generated Keywords from Loglines/Tags for each cluster')
st.subheader('3. Demographic Analysis for each cluster')
st.subheader('4. Cluster Churns and Signup analysis')