# Make sure plotly is upgraded to v4!!!
import plotly
import plotly.offline as offline
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

########################## PROCESS PLOT DATA ###########################
########################################################################
# PROCESS GENDER DATA
df = pd.read_csv('../data/denial_sex.csv')
years = df.year.unique()
years.sort()

female = []
male = []
not_applicable = []

for year in years:
    yearData = df[df['year'] == year]
    f = yearData[yearData['sex'] == 2]
    m = yearData[yearData['sex'] == 1]
    n = yearData[yearData['sex'] == 4]
    female.append(f['denial'].values[0].astype('float'))   
    male.append(m['denial'].values[0].astype('float'))  
    not_applicable.append(n['denial'].values[0].astype('float'))  

# PROCESS RACE DATA
race = pd.read_csv('../data/denial_race.csv')

AmericanIndians_AlaskaNative = []
Asian = []
African_American = []
PacificIslander = []
White = []

for year in years:
    yearData = race[race['year'] == year]
    indian = yearData[yearData['race'] == 1]
    asian = yearData[yearData['race'] == 2]
    african = yearData[yearData['race'] == 3]
    p = yearData[yearData['race'] == 4]
    w = yearData[yearData['race'] == 5]
    AmericanIndians_AlaskaNative.append(indian['denial'].values[0].astype('float'))   
    Asian.append(asian['denial'].values[0].astype('float'))  
    African_American.append(african['denial'].values[0].astype('float'))  
    PacificIslander.append(p['denial'].values[0].astype('float'))  
    White.append(w['denial'].values[0].astype('float'))  
    
    
# PROCESS ETHNICITY DATA
eth = pd.read_csv('../data/denial_eth.csv')
hispanic = []
noHis = []
notApp = []
for year in years:
    yearData = eth[eth['year'] == year]
    h = yearData[yearData['eth'] == 1]
    n = yearData[yearData['eth'] == 2]
    na = yearData[yearData['eth'] == 4]
    hispanic.append(h['denial'].values[0].astype('float'))   
    noHis.append(n['denial'].values[0].astype('float'))  
    notApp.append(na['denial'].values[0].astype('float'))  

    
# PROCESS INCOME DATA
income = pd.read_csv('../data/denial_income.csv')
per10 = []
per20 = []
per30 = []
per40 = []
per50 = []
per60 = []
per70 = []
per80 = []
per90 = []
top10 = []
for year in years:
    yearData = income[income['year'] == year]
    p10 = yearData[yearData['percentile'] == 10]
    p20 = yearData[yearData['percentile'] == 20]
    p30 = yearData[yearData['percentile'] == 30]
    p40 = yearData[yearData['percentile'] == 40]
    p50 = yearData[yearData['percentile'] == 50]
    p60 = yearData[yearData['percentile'] == 60]
    p70 = yearData[yearData['percentile'] == 70]
    p80 = yearData[yearData['percentile'] == 80]
    p90 = yearData[yearData['percentile'] == 90]
    top = yearData[yearData['percentile'] == 100]
    per10.append(p10['denial'].values[0].astype('float'))
    per20.append(p20['denial'].values[0].astype('float'))
    per30.append(p30['denial'].values[0].astype('float'))
    per40.append(p40['denial'].values[0].astype('float'))
    per50.append(p50['denial'].values[0].astype('float'))
    per60.append(p60['denial'].values[0].astype('float'))
    per70.append(p70['denial'].values[0].astype('float'))
    per80.append(p80['denial'].values[0].astype('float'))
    per90.append(p90['denial'].values[0].astype('float'))
    top10.append(top['denial'].values[0].astype('float'))
    
    
############################ SET UP SUBPLOTS ###########################
########################################################################
fig = make_subplots(
    rows=3, cols=2,
    column_widths=[0.5, 0.5], # corresponding to each row!
    row_heights=[0.25, 0.30, 0.45], # corresponding to each column!
    specs=[[{"type": "scatter", "rowspan": 2}, {"type": "scatter", "rowspan": 2}],
           [            None                 ,       None         ],
           [{"type": "scatter"}, {"type": "scatter"}]],
    subplot_titles=("Denial Rate Per Race", "Denial Rate Per Income Percentile", 
                    "Denial Rate Per Ethnicity", "Denial Rate Per Gender")
)

##############################  SUBPLOTS  ##############################
########################################################################
# PLOTTING GENDER
fig.add_trace(go.Scatter(x=years, y=female,
                    mode='lines+text',
                    name='Female',
                    #showlegend=True, # plotly only offers one legend for all subplots. this is useless...unfortunately
                    text=["", "", "", "", "female"],
                    textposition="top center"), 
                    row=3, col=2)
fig.add_trace(go.Scatter(x=years, y=male,
                    mode='lines+markers+text',
                    text=["", "", "", "", "male"],
                    textposition="top center",
                    name='Male'), row=3, col=2)
fig.add_trace(go.Scatter(x=years, y=not_applicable,
                    mode='markers+text',
                    text=["", "", "", "", "not applicable"],
                    textposition="top center",
                    name='Not Applicable'), row=3, col=2)

## PLOTTING RACE
fig.add_trace(go.Scatter(x=years, y=AmericanIndians_AlaskaNative,
                    mode='lines+text',
                    name='American Indians or Alaska Native',
                    text=["","Indians"],
                    textposition="top center",    
                    showlegend=True), 
                    row=1, col=1)
fig.add_trace(go.Scatter(x=years, y=Asian,
                    mode='lines+text',
                    name='Asian', 
                    text=["Asians"],
                    textposition="bottom center"), row=1, col=1)
fig.add_trace(go.Scatter(x=years, y=African_American,
                    mode='lines+text', 
                    name='Black or African American',
                    text=["Black"],
                    textposition="bottom center"), row=1, col=1)
fig.add_trace(go.Scatter(x=years, y=PacificIslander,
                    mode='lines+text', 
                    name='Native Hawaiian or Other Pacific Islander',
                    text=["","Pacific Islander"],
                    textposition="top center"), row=1, col=1)
fig.add_trace(go.Scatter(x=years, y=White,
                    mode='lines+text', 
                    name='White',
                    text=["","White"],
                    textposition="top center"), row=1, col=1)

# PLOTTING ETHNICITY
fig.add_trace(go.Scatter(x=years, y=hispanic,
                    mode='lines+markers+text',
                    name='Hispanic or Latino',
                    text=["", "Hispanic or Latino"],
                    textposition="top center",
                    showlegend=True), 
                    row=3, col=1)
fig.add_trace(go.Scatter(x=years, y=noHis,
                    mode='lines+markers+text',
                    text=["", "", "Not Hispanic or Latino"],
                    textposition="bottom center",
                    name='Not Hispanic or Latino'), row=3, col=1)
fig.add_trace(go.Scatter(x=years, y=notApp,
                    mode='markers+text', 
                    name='Not Applicable',
                    text=["", "Not Applicable"],
                    textposition="top center"), row=3, col=1)

## PLOTTING INCOME
fig.add_trace(go.Scatter(x=years, y=per10,
                    mode='lines+text',
                    name='10th percentile',
                    text=["10th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per20,
                    mode='lines+text',
                    name='20th percentile',
                    text=["20th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per30,
                    mode='lines+text',
                    name='30th percentile',
                    text=["30th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per40,
                    mode='lines+text',
                    name='40th percentile',
                    text=["40th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per50,
                    mode='lines+text',
                    name='50th percentile',
                    text=["50th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per60,
                    mode='lines+text',
                    name='60th percentile',
                    text=["60th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per70,
                    mode='lines+text',
                    name='70th percentile',
                    text=["70th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per80,
                    mode='lines+text',
                    name='80th percentile',
                    text=["80th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=per90,
                    mode='lines+text',
                    name='90th percentile',
                    text=["90th"],
                    textposition="middle center"), 
                    row=1, col=2)
fig.add_trace(go.Scatter(x=years, y=top10,
                    mode='lines+text',
                    name='top 10 percent',
                    text=["", "top 10%"],
                    textposition="middle center"), 
                    row=1, col=2)


########################### ARRANGE LAYOUT #############################
########################################################################
# OVERALL LAYOUT Set theme, margin, and annotation in layout
fig.update_xaxes(dtick=1)

# Update xaxis properties
fig.update_xaxes(title_text="Year", row=1, col=1)
fig.update_xaxes(title_text="Year", row=1, col=2)
fig.update_xaxes(title_text="Year", row=3, col=1)
fig.update_xaxes(title_text="Year", row=3, col=2)

# Update yaxis properties
fig.update_yaxes(title_text="Denial Rate", row=1, col=1)
fig.update_yaxes(title_text="Denial Rate", row=1, col=2)
fig.update_yaxes(title_text="Denial Rate", row=3, col=1)
fig.update_yaxes(title_text="Denial Rate", row=3, col=2)

fig.update_layout(
    template="plotly_dark",
    title_text="Looking At Key Parameters"
)


plotly.offline.iplot(fig)

# to plot in a separete browser window
offline.plot(fig, auto_open=True, image_filename="denialPlots" ,image_width=2000, image_height=2000, 
              filename='denialPlots', validate=True)