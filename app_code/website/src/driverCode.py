"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fang Han, Jeremy Lao

This code performs the modeling steps for input data from the Flask entry.py
code. When the runModel class is instantiated, a pyspark sql context is created
for later use. This may take a few seconds to intialize, however a new thread is used to
launch the pySpark context while the user navigates through the input pages.

function runPrediction(self, form) does the main grunt work of the modeling
process. See its documentation below

The primary output of this code is the self.predData Pandas dataframe. It has
the schema
predData(Respondent Name (Panel):String Lender Name,
         as_of_year:String predicted year,
         probability:List[float] where List[0] is prob. of denial,
                                       List[1] is prob of approval)



@author: Cody Gilbert
"""
import pandas as pd
import logging
import logging.config
import sys
import threading
import time
#sys.path.insert(0, r'C:\spark\spark-2.4.3-bin-hadoop2.7\python')
sys.path.insert(0, r'/Users/fanghan/anaconda3/bin/python')

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidatorModel, ParamGridBuilder
from src.forms import RegistrationForm


class runModel():
    def __init__(self):
        logging.config.fileConfig('src/log/logging.conf')
        self.logger = logging.getLogger('entry.driverCode')
        self.logger.info('creating instance of runModel')
        self.sc = None  # set to None to catch in case of speedy launching
        self.model = None  # set to none in case of model launch error
        # Cody's path
        #self.lenderFile = r"C:\Users\Cody Gilbert\Desktop\SparkClass\SU19SparkClass\presentation\website\modelingMatrix.csv"
        #self.modelFolder = r"C:\spark\modelData\lenderModel"
        # Fang's path
        self.lenderFile = "file:///Users/fanghan/Desktop/SU19SparkClass/app_code/website/src/model/modelingMatrix.csv "
        self.modelFolder = "file:///Users/fanghan/Desktop/SU19SparkClass/app_code/website/src/model/lenderModel"
        
        self.scThread = threading.Thread(target=self.startSession)
        self.scThread.start()
        self.logger.info('Spark Context thread created with ID: %s' % self.scThread.ident)
    
    def startSession(self):
        '''
        This method launches the Spark context, loads in pre-trained model,
        and loads the modeling template.
        This has  been included as a standalone method to allow dispatch
        via multithreading. Creating the Spark Context and loading take
        some time, therefore including it in a separate thread during startup
        will "hide" the loading from the user.
        '''
        # Load in lender-Year matrix into a pandas dataframe
        self.logger.info('loading model matrix from %s' % self.lenderFile)
        self.inputDF = pd.read_csv(self.lenderFile)
        
        # Load each of the form inputs as modeling columns
        self.inputDF.columns = ["respondent_id",
                                   "agency_code",
                                   "Respondent Name (Panel)",
                                   "as_of_year"]
        
        # Load in the spark context
        self.logger.info('Creating local[*] SparkContext')
        self.sc = SparkSession.builder.master("local[*]").getOrCreate()
        
        # Load in the pre-fit model
        self.logger.info('loading model from %s' % self.modelFolder)
        try:
            self.model = PipelineModel.load(self.modelFolder)
        except:
            # log an error in case of failure,
            self.logger.exception('Error in loading model!')
            raise Exception('Model load error')
        self.logger.info('model loaded')

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

        gender = form.gender.data
        self.logger.info('Setting model DF' +
                         ' applicant_sex ' +
                         ' to %s:%s ' % (repr(gender),
                                         repr(type(gender))))
        self.inputDF["applicant_sex"] = gender
        state = form.state.data
        self.logger.info('Setting model DF' +
                         ' state_code ' +
                         ' to %s:%s ' % (repr(state),
                                         repr(type(state))))
        self.inputDF["state_code"] = state
        loanAmnt = form.loanAmnt.data / 1000
        self.logger.info('Setting model DF' +
                         ' loan_amount_000s ' +
                         ' to %s:%s ' % (repr(loanAmnt),
                                         repr(type(loanAmnt))))
        self.inputDF["loan_amount_000s"] = loanAmnt
        income = form.income.data / 1000
        self.logger.info('Setting model DF' +
                         ' applicant_income_000s ' +
                         ' to %s:%s ' % (repr(income),
                                         repr(type(income))))
        self.inputDF["applicant_income_000s"] = income
        race = form.race.data 
        self.logger.info('Setting model DF' +
                         ' applicant_race_1 ' +
                         ' to %s:%s ' % (repr(race),
                                         repr(type(race))))
        self.inputDF["applicant_race_1"] = race
        ethnicity = form.ethnicity.data
        self.logger.info('Setting model DF' +
                         ' applicant_ethnicity ' +
                         ' to %s:%s ' % (repr(ethnicity),
                                         repr(type(ethnicity))))
        self.inputDF["applicant_ethnicity"] = ethnicity
        
        # Check for startup thread status
        if self.sc is None or self.model is None:
            # In case form submitted before sc given time to start
            self.logger.info('SparkContext or model not created. Waiting 20 secs')
            time.sleep(20)
        if self.model is None:
            self.logger.exception('Model not created. See above log of separate thread')
            raise Exception('Model load error')
        
        # Create the Spark dataframe from the user input
        self.logger.info('creating the PySpark RDD of Pandas dataframe ')
        self.modeledDF = self.sc.createDataFrame(self.inputDF)

        # Transform and model the data with the model
        self.logger.info('transforming RDD to create predictions')
        self.prediction = self.model.transform(self.modeledDF)

        # Pull out the requested lenders, years, and proabaility of approval
        self.selectedData = self.prediction.select("Respondent Name (Panel)",
                                                   "as_of_year", "probability")
        
        # Convert to a pandas dataframe and save it to the class
        self.logger.info('converting RDD to pandas (Spark Action)')
        self.predData = self.selectedData.toPandas()
        self.logger.debug('Predicted Data Head: \n' +
                          '%s' % self.predData.loc[0:5,:].to_string())
        
        self.logger.info('prediction completed!')
        return(self.predData)
    def __exit__(self):
        self.logger.info('stopping Spark Context')
        self.sc.stop()




