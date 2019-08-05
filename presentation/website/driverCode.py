"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fan Han, Jeremy Lao

This code performs the modeling steps for input data from the Flask entry.py
code. When the runModel class is instantiated, a pyspark sql context is created
for later use. This may take a few seconds to intialize.

function runPrediction(self, form) does the main grunt work of the modeling
process. See its documentation below

The primary output of this code is the self.predData Pandas dataframe. It has
the schema
predData(Respondent Name (Panel):String Lender Name,
         as_of_year:String predicted year,
         probability:List[float] where List[0] is prob. of denial,
                                       List[1] is prob of approval)

CURRENT DRAFT: UNFINISHED

@author: Cody Gilbert
"""
import pandas as pd
import sys
#sys.path.insert(0, r'C:\spark\spark-2.4.3-bin-hadoop2.7\python')
#sys.path.insert(0, r:'/Users/fanghan/anaconda3/bin/python')

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidatorModel, ParamGridBuilder
from forms import RegistrationForm


class runModel():
    def __init__(self):
        self.sc = SparkSession.builder.master("local[*]").getOrCreate()
        #self.lenderFile = r"C:\Users\Cody Gilbert\Desktop\SparkClass\SU19SparkClass\presentation\website\modelingMatrix.csv"
        self.lenderFile = "file:///Users/fanghan/Desktop/BDAD_summer19/SU19SparkClass/presentation/website/modelingMatrix.csv "
        self.modelFolder = "file:///Users/fanghan/Desktop/BDAD_summer19/SU19SparkClass/presentation/website/lenderModel"

    def runPrediction(self, form):
        '''
        Takes in the form object from the web UI, sets up the required fields
        for the model object, and sets the dataframe of predicted values
        ~Requirements~
        NaiveBayesPipeline: An MLLib model that can take in a Spark DataFrame,
            perform all pipeline transformations, and output the results
            of the model on a given input Spark DataFrame. Must be located
            in the folder self.modelFolder
        modelingMatrix.csv: a CSV file with the following columns:
            "respondent_id"
            "agency_code"
            "Respondent Name (Panel)"
            "as_of_year"
        ~Input~
        form: a RegistrationForm(FlaskForm) objected created within forms.py
            that contains the user-supplied information
        ~Output~
        self.predictionDF: *pandas* dataframe that can be used in output
        visualization
        '''
        # Load in the pre-fit model
        self.model = PipelineModel.load(self.modelFolder)


        # Load in lender-Year matrix
        self.inputDF = pd.read_csv(self.lenderFile)
        self.inputDF.columns = ["respondent_id",
                                   "agency_code",
                                   "Respondent Name (Panel)",
                                   "as_of_year"]
        self.inputDF["applicant_sex"] = form.gender.data#[1]
        self.inputDF["state_code"] = form.state.data#[0]
        self.inputDF["loan_amount_000s"] = form.loanAmnt.data
        self.inputDF["applicant_income_000s"] = form.income.data
        self.inputDF["applicant_race_1"] = form.race.data#[1]
        self.inputDF["applicant_ethnicity"] = form.ethnicity.data#[1]

        # Create the Spark dataframe from the user input
        self.modeledDF = self.sc.createDataFrame(self.inputDF)

        # Transform and model the data with the model
        self.prediction = self.model.transform(self.modeledDF)

        # Pull out the requested lenders, years, and proabaility of approval
        self.selectedData = self.prediction.select("Respondent Name (Panel)",
                                                   "as_of_year", "probability")
        self.selectedData
        # Convert to a pandas dataframe and save it to the class
        self.predData = self.selectedData.toPandas()

    def __exit__(self):
        self.sc.stop()




