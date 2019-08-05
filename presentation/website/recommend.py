"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fan Han, Jeremy Lao

Class Usage: 
from recommend import plotTopLenders
plotter = plotTopLenders()
plotter.plot(n) # n is the number of top lenders to be shown

Note: 
The orca executable is required to export figures as static images, as performed in plot().
To install:  $conda install -c plotly plotly-orca


@author: Fang Han
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as offline


class plotTopLenders(): 
    def __init__(self, df = None):
        self.tmp_jsonPath = "file:///Users/fanghan/Desktop/BDAD_summer19/SU19SparkClass/presentation/website/testProbabilities.json"
        if df is None:
            self.df = pd.read_json(self.tmp_jsonPath)   
        else: 
            self.df = df
        
    def getLenders(self, top = 3): # static method 
        """
        First transform the dataframe:
        Split the probability column which is an array into two columns, 
        'tag_1' for approve, 'tag_2' for deny, 
        then sort by approve in descending order
        """
        tmp = self.df['probability'].apply(pd.Series)                                                                                                                                                               
        probs = tmp['values'].apply(pd.Series)
        probs = probs.rename(columns = lambda x : 'tag_' + str(x))  
        self.df = pd.concat([self.df[:], probs[:]], axis=1).sort_values(by='tag_1', ascending=False)  

        lenders = self.df["Respondent Name (Panel)"].unique()
        self.topN = lenders[:top]
        #return self.topN
        
    def getYearlyRates(self, top = 3):
        """
        Create a dictionary that maps lender names to their probability each year
        """
        #topN = self.getLenders(top)
        years = range(2007, 2018)
        self.years = []
        self.lendersByYear = dict.fromkeys(self.topN) 
        self.lendersPeak = dict.fromkeys(self.topN) # get the peak historic probability of each lender
        for name in self.topN:
            lenderData = self.df[self.df["Respondent Name (Panel)"] == name]
            accum = []
            for i in years:
                year = lenderData[lenderData['as_of_year'] == i]
                accum.append(year['tag_1'].values[0].astype('float'))    
                self.years.append(i)
            self.lendersByYear[name] = accum
            self.lendersPeak[name] = max(accum)
        #return lendersByYear

    def plot(self, top = 3):
        self.getLenders(top)
        self.getYearlyRates(top)
        # turn lendersPeak into a dataframe                                                                        
        tab_df = pd.DataFrame.from_dict({'Lender': list(self.lendersPeak.keys()), 
                                        'Highest Historic Probability Of Approval':list(self.lendersPeak.values())}) 
        fig = go.Figure()
        ############################ SET UP SUBPLOTS ###########################
        ########################################################################
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            specs=[[{"type": "table"}],
                [{"type": "scatter"}]]
        )
        
        ############################ PLOT TABLE ################################
        ########################################################################
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["Lender", "Highest <br>Historic Probability<br> Of Approval"],
                    font=dict(size=10),
                    align="center"
                ),
                cells=dict(
                    values=[tab_df[k].tolist() for k in tab_df.columns[0:]],
                    align = "center")
                ),
                row=1, col=1
        )
        
        ############################ SCATTER PLOT ##############################
        ########################################################################        
        for lender in self.lendersByYear.keys():
            fig.add_trace(
                go.Scatter(
                    x = self.years,
                    y = self.lendersByYear[lender],
                    mode="lines+markers+text",
                    name = lender,
                    text=["", "", lender],
                    textposition="middle center", 
                ),
                row = 2, col = 1
            )
             
        fig.update_layout(
            template="plotly_dark",
            height=925,
            showlegend=True,
            title_text="Top Lenders We'd Recommend To You: ",
        )
        
        offline.iplot(fig)
        # to plot in a separete browser window
        offline.plot(fig, auto_open=True, image_filename="lenderRecommendations" ,image_width=2000, image_height=2000, 
              filename='lenderRecommendations', validate=True)
        #fig.write_image("file:///Users/fanghan/Desktop/BDAD_summer19/SU19SparkClass/presentation/website/templates", format='jpg')
