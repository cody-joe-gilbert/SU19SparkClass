"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fan Han, Jeremy Lao

The code forms the overall flow of the HMDA Loan prediction recomendation and
analysis tool.

CURRENT DRAFT: UNFINISHED

@author: Cody Gilbert
"""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

class runSpark():
    def __init__(self):
    # create Pyspark session
    # Running local as speed is more important and file size is small
    self.sc = SparkSession.master("local[*]").getOrCreate()

#    # Define folder paths
#    self.modelFolder = "hdfs:///user/cjg507/sparkproject/HMDAModel"
#    self.lenderFile = "lenders.csv"
#    self.optionsFile = "userOptions.json"
#
#    # Load in the pre-fit model
#    model = CrossValidator.load(modelFolder)
#
#    # Load in lender-Year matrix
#    lenders = pd.read_csv(lenderFile)
#
#    # Load in the available user fields
#    options = pd.read_csv(optionsFile)
#
#    # Main UI Loop:
#    ### UNFINISHED: Plotly UI will be used later; this is a draft of the flow only
#
#    # 1. Ask user for an input of State, Race, Gender, Ethnicity, Loan Amount,
#    #   income amount
#
#    incomeAmnt = input("Enter your income : ")
#    loanAmnt = input("Enter your loan amount : ")
#    state = input("Enter your state : ")
#    race = input("Enter your Race : ")
#    ethnicity = input("Enter your ethnicity : ")
#    gender = input("Enter your gender : ")
#
#    # 2. Add values to dataframe
#    lenders["applicant_sex_name"] = gender
#    lenders["state_abbr"] = state
#    lenders["loan_amount_000s"] = loanAmnt
#    lenders["applicant_income_000s"] = incomeAmnt
#    lenders["applicant_race_name_1"] = race
#    lenders["applicant_ethnicity_name"] = ethnicity
#
#    # 3. Create the Spark dataframe from the user input
#    modeledDF = spark.createDataFrame(lenders)
#
#    # 4. Transform and model the data with the model
#    prediction = model.transform(modeledDF)
#
#    # 5. Return the list of predicted probabilities by lender and
#    #   year
#    selected = prediction.select("lender", "year", "probability")
#    for row in selected.collect():
#        print(row)





