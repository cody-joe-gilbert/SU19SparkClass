import pandas as pd

class plotTopLenders(): 
    def __init__(self, df = None):
        self.tmp_jsonPath = "file:///Users/fanghan/Desktop/BDAD_summer19/SU19SparkClass/presentation/website/testProbabilities.json"
        if df is None:
            self.df = pd.read_json(self.tmp_jsonPath)   
        else: 
            self.df = df
        
    def plot(self): # static method 
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

