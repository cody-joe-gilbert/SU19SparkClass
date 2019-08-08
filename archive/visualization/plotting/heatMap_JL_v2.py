import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.offline as offline
from plotly.graph_objs import *
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

df = pd.read_csv('../data/denial_overall_jl.csv')
### colorscale:    
scl = [[0.0, '#ffffff'],[0.05, '#b4a8ce'],[0.10, '#8573a9'],
               [0.15, '#7159a3'],[0.20, '#5732a1'],[0.30, '#2c0579']] # reds

### create empty list for slider:    
slider = []

#### populate the data object
for year in df.year.unique():
     
  # select the year (and remove DC for now)
  yearly_denial_rate = df[(df['year']== year)]
                
  for col in yearly_denial_rate.columns:  
      yearly_denial_rate[col] = yearly_denial_rate[col].astype(str)

  data_one_year = dict(
          type='choropleth',
          locations = yearly_denial_rate['state'],
          z=yearly_denial_rate['denRate'].astype(float),
          locationmode='USA-states',
          colorscale = scl
          )

  slider.append(data_one_year)  # add the dictionary to the list of dictionaries for the slideri

steps = []
for i in range(len(slider)):
  step = dict(method='restyle',
          args=['visible', [False] * len(slider)],
          label='year {}'.format(i + 2007)) # label to be displayed for each step (year)
            
  step['args'][1][i] = True
  steps.append(step)

  ##  create the 'sliders' object from the 'steps' 

sliders = [dict(active=0, pad={"t": 1}, steps=steps)]


# Set up the layout (including slider option)
layout = dict(geo=dict(scope='usa',projection={'type': 'albers usa'}),sliders=sliders, template="plotly_dark")

# Create the figure object:
fig = dict(data=slider, layout=layout) 

# to plot in the notebook
plotly.offline.iplot(fig)

# to plot in a separete browser window
offline.plot(fig, auto_open=True, image_filename="heatMap_slider" ,image_width=2000, image_height=1000, filename='heatMap_slider.html', validate=True)

fig.show()
