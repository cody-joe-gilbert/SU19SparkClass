# -*- coding: utf-8 -*-
"""
CSCI-GA.3033-001: Summer 2019 Team Project
Cody Gilbert, Fang Han, Jeremy Lao

Code to produce a choropleth of US Counties with a color given by some other
set of values

@author: Cody Gilbert
"""
import plotly.figure_factory as ff
import pandas as pd
import plotly as py
data = pd.read_json('hmdaGEOIDCount.json', lines=True)

noBins = 20
bins = range(50,
             max(data['count'].tolist()),
             int((max(data['count'].tolist()) -
                 min(data['count'].tolist()))/noBins))



fig = ff.create_choropleth(fips=data['CYKEY'].tolist(),
                           values=data['count'].tolist(),
                           binning_endpoints=[x for x in bins],
                           county_outline={'color': 'rgb(255,255,255)', 'width': 0.5},
                           round_legend_values=True,
                           legend_title='Loans by County',
                           title='US HMDA Data')
py.offline.plot(fig)

data.hist('count', bins=50)