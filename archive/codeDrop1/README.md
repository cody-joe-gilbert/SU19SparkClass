# Code Drop 1

## Included Files

* 'dataPrep.scala': Spark Scala codes used to filter HMDA data, join institution data, and prepare the final input dataframe to the modeling software
* 'dataModeling.scala': Spark Scala code that uses MLLib to fit the input data to a binary classification model, perform model analysis, and save the fitted model for use by the UI tool
* 'userInterface': Folder contains the Python code which includes the use of Flask and Pyspark to create a UI tool that take in user demographic data and the fitted model, and produces the predicted loan approval probabilities over time and lender. 

