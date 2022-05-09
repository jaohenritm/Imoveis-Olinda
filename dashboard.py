import pandas as pd
import requests
import folium
import matplotlib.pyplot as plt
import descartes
import geopandas as gpd
import sqlite3
import streamlit as st
import plotly.express as px

from shapely.geometry import Point, Polygon
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim

st.title('Imóveis na cidade de Olinda')



# read data
@st.cache(allow_output_mutation=True)
def get_data(path):
    data = pd.read_csv(path)

    return data

data = get_data('/home/joaohenritm/repos/Olinda-Imoveis/dataset.csv')

st.dataframe(data)

data = data.loc[data['preco'] != 'Sob consulta']

# map
fig = px.scatter_mapbox(data,
                  lat='lat',
                  lon='lon',
                  color_continuous_scale=px.colors.cyclical.IceFire,
                  size_max=15,
                  zoom=10)

fig.update_layout(mapbox_style='open-street-map')
fig.update_layout(height=600, margin={'r':0, 'l':0, 'b':0, 't':0})
st.plotly_chart(fig)