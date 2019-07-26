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
the schema predData(lender:String, year:int, probability:float) where
lender is column of pre-selected lenders, year is the year at which the
model processes the data, and probability is the final probability of loan
approval.


CURRENT DRAFT: UNFINISHED

@author: Cody Gilbert
"""
import pandas as pd
import sys
import os
sys.path.insert(0, r'C:\spark\spark-2.4.3-bin-hadoop2.7\python')

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidatorModel, ParamGridBuilder
from forms import RegistrationForm


class runModel():
    def __init__(self):
        self.sc = SparkSession.builder.master("local[*]").getOrCreate()
        self.MDFolder = os.path.join(os.getcwd(), "modelData")
        self.modelFolder = os.path.join(self.MDFolder, "HMDAModel")
        self.lenderFile = os.path.join(self.MDFolder, "lenders.csv")
        print(self.MDFolder)
        print(self.modelFolder)
        print(self.lenderFile)

    def runPrediction(self, form):
        '''
        Takes in the form object from the web UI, sets up the required fields
        for the model object, and sets the dataframe of predicted values
        ~Requirements~
        logModelPipeline: An MLLib model that can take in a Spark DataFrame,
            perform all pipeline transformations, and output the results
            of the model on a given input Spark DataFrame. Must be located
            in the folder self.modelFolder
        lenders.csv: a CSV file with the following columns:
            "lender" containing the lender name
            "parent_id" containing the parent_id that is joined to data in the
                        HMDA dataset
            "year" if 'YRS' is a set of all years contained within the HMDA
                dataset, then this column is the cartesian product of
                'YRS' to 'lender'
                e.g. lender1, year1
                     lender1, year2
                     lender2, year1
                     ...
        ~Input~
        form: a RegistrationForm(FlaskForm) objected created within forms.py
            that contains the user-supplied information
        ~Output~
        self.predictionDF: *pandas* dataframe that can be used in output
        visualization
        '''
        # Load in the pre-fit model
        model = CrossValidatorModel.load(self.modelFolder)

        # Load in lender-Year matrix
        inputDF = pd.read_csv(self.lenderFile)
        inputDF["applicant_sex_name"] = form.gender.data[1]
        inputDF["state_abbr"] = form.state.data[0]
        inputDF["loan_amount_000s"] = form.loan.data
        inputDF["applicant_income_000s"] = form.income.data
        inputDF["applicant_race_name_1"] = form.race.data[1]
        inputDF["applicant_ethnicity_name"] = form.ethnicity.data[1]

        # Create the Spark dataframe from the user input
        modeledDF = self.sc.createDataFrame(inputDF)

        # Transform and model the data with the model
        prediction = model.transform(modeledDF)

        # Pull out the requested lenders, years, and proabaility of approval
        selectedData = prediction.select("lender", "year", "probability")

        # Convert to a pandas dataframe and save it to the class
        self.predData = selectedData.toPandas()

    def __exit__(self):
        self.sc.stop()




