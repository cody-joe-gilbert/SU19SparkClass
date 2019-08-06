# Data Modeling
### Author: C.J. Gilbert

This folder contains the tools used to generate the [Apache Spark MLLib Naive Bayes]( https://spark.apache.org/docs/2.2.0/mllib-naive-bayes.html ) model to predict lender approval probabilities. This primarily uses the Scala script `dataModeling.scala` to perform the following
tasks:

1. Join the institution (lender identification) data to the HMDA data, clean and bucket related
actions and create the final feature and label dataframe.

2. Create modeling matrices used by the webapp utility for prediction,

3. Create a Naive Bayes model pipeline, fit the feature dataframe to the model, and save it for later use in the webapp.

## Software Used

* Scala Ver. 2.11.8
* Apache Spark Ver. 2.2.0
	* Including heavy use of the [MLLib Library]( https://spark.apache.org/docs/2.2.0/ml-guide.html )
* Apache Hadoop 2.6.0 with Yarn



## Hardware Used

* [NYU HPC Dumbo Cluster]( https://wikis.nyu.edu/display/NYUHPC/Clusters+-+Dumbo ) for big data processing

## Included files

### Scripts
* `runscript.sh` Script used to run Scala code on NYU Dumbo cluster
* `dataModeling.scala` Scala code for modeling lender approval probabilities with HMDA data

### HDFS Data

* `/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv` HMDA data. See paper and HMDA section for details.
* `/user/fh643/InstitutionData/data/panel_07-09` Institution (lender identification) data for years 2007-2009
* `/user/fh643/InstitutionData/data/panel_10-17` Institution (lender identification) data for years 2010-2017
* `/user/cjg507/sparkproject/modelingMatrix.csv` Webapp modeling template
* `/user/cjg507/lenderModel` final MLLib NaiveBayes model

## Process Description

### Creating Model Feature and Label Input
After the HMDA and institution data had been cleaned and profiled, the next task was to create a model that constructs the probability of lender approval for various lenders over time given the applicant demographics. To construct such a model, the input features must include each of the features on which an approval prediction will be made, and the corresponding binary label of approved or denied. These features included the following based on their locations in the given datasets:

* Year of Record: `HMDA.as_of_year`
* Applicant Demographic Features
	* Income: `HMDA.applicant_income_000s`
	* Loan Amount: `HMDA.loan_amount_000s`
	* Race: `HMDA.applicant_race_1`
	* Ethnicity: `HMDA.applicant_ethnicity`
	* Gender: `HMDA.applicant_sex`
	* State: `HMDA.state_code`
* Lender Identifiers
	* Respondent ID: `InstitutionData.Respondent ID` & `HMDA.respondent_id` 
	* Agency Code: `InstitutionData.Agency Code` & `HMDA.agency_code` 
	* Institution Name: `InstitutionData.Respondent State (Panel)`
* Action Taken: `HMDA.action_taken`

This meant that a single Spark dataframe had to be constructed with the above features for each record to be used to fit the model.

The binary label of "Approved" or "Denied" could be derived from the `HMDA.action_taken` column. This column contained several classifications, most of which could be easily interpreted as either "Approved" or "Denied". Classifications that were ambiguous or indicated missing data were dropped. The following table provides the binary mapping/filtering:

| HMDA Classification | Mapping | No. Records |
| -------------- | ----------- | --------- |
| "Application denied by financial institution"      | Denied   | 32269700 |
| "Preapproval request denied by financial institution"      | Denied   | 1578696 |
| "Loan originated"      | Approved   | 89180013 |
| "Application approved but not accepted"      | Approved   | 8314173 |
| "Preapproval request approved but not accepted"      | Approved   | 823436 |
| "Loan purchased by the institution"      | Approved   | 32350741 |
| "File closed for incompleteness"      | Dropped   | 5741376 |
| "Application withdrawn by applicant"      | Dropped   | 17204311 |

The features used to predict the binary lnputs and their associated probabilities were located entirely within the HMDA dataset, however the Lender Name used to determine the institutions to be modeled and to present to the user within the webapp was defined only within the Institution dataset. As determined previously in the institution data profiling section, the institution can be uniquely defined by Respondent ID, Agency Code, and State Code.  

The state codes were not present in the given HMDA data, however they are defined within its associated documentation. A dataframe relating the state codes to state abbrevations was manually created and joined to the HMDA data to create a new state code column.

The institution dataset was then joined to the HMDA data to create the final feature and label dataframe.

### Creating Webapp Template Matrix

The web application used to model approval probabilities will ask the user for all of applicant demographic features listed above, but will otherwise generate the predicted years and lenders. To create a probability for each combination of demographic, year, and lender, the webapp will create a dataframe that is the cartesian product (demographics X set of years considered X set of lenders). Although there will only be one set of demographics and 11 years, there are several thousand unique lenders within the HMDA dataset. Predicting a cartesian product with the entire set of lenders will be infesible for an interactive application, therefore the number of lenders considered was reduced to the top 20 lenders with the largest number of records in the HMDA dataset. 

The top 20 lenders were extracted and a template dataframe containing the cartesian product of the (set of years X set of top 20 lenders) was pre-calculated in Spark and saved as the CSV file `/user/cjg507/sparkproject/modelingMatrix.csv`. To produce a modeling dataframe, the webapp will only have to create a column for each demographic feature and copy the same value for each row within the template year X lender dataframe. 

### Creating the Modeling Pipeline and Naive Bayes Model

The web application will need to quickly calculate approval probabilities to be considered interactive. To speed model prediction, the model will be fit offline, saved to a local machine, and loaded during webapp execution. As examined in the HMDA data profiling, the Naive Bayes model produced the highest (albeit remarkably poor) AUC accuracy, and thus will be the final model used in the webapp.

The feature dataframe will require several transformations to allow for modeling with the Spark MLLib NaiveBayes model. These transformations will have to be performed during fitting and prediction, therefore a standardized method of "funneling" data from a readable text dataframe to the model called a Pipeline will be used. The Pipeline will consist of 4 major stage categories:

* String Indexing
* One-Hot Encoding
* Assembling
* Final Modeling/Fitting

The string indexing step is powered by the MLLib StringIndexer class that maps input strings to integer categories. These categories will flag the downstream model to treat the given feature as categorical.
One-hot encoding is provided by the MLLib OneHotEncoder class that maps a set of integers to a sparse set of binary vectors. This is a requirement of the Bernoulli NaiveBayes model.
Assembling is provided by the MLLib Assembler class that joins the columns of sparse vectors produced by the one-hot encoders to a single sparse vector of features. This features vector is the final input to the NaiveBayes model.
The final modeling is provided by the MLLib NaiveBayes model, which fits the model and/or creates a binary probability of approval, and a prediction label.

The full bucketed joined HMDA-Institution dataset was fit using the above pipeline, and the resulting trained model was saved to `/user/cjg507/lenderModel`.






