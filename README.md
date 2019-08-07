TODO:

*Remove the list item as you do it so we can track what has been completed*

* Fang: Add institution data steps to each of the given folders as applicable
* Fang: When app is finished, move the final copy of the website folder into the /app_code folder
* Jeremy: Ensure all code is moved into the applicable folders and a desciption is included in the README
* Fang: Ensure all code is moved into the applicable folders and a desciption is included in theREADME
* Fang: Include the applicable writeup to the Flask app in the /app_code section. I've included some of the steps already from the website folder README and I've added files I could think of.
* ~~Fang: Ensure all HDFS data is correctly listed~~
* Fang: Add screenshots of app working to /screenshots
* Jeremy: Add testing code to the /test_code folder (optional)
* Fang: Add testing code to the /test_code folder (optional)


#CSCI-GA.3033-001: Summer 2019 Team Project
Authors: Cody Gilbert, Fang Han, Jeremy Lao

Repository for the team project in the Summer 2019 Spark Class.

This repository contains all the files used to create and execute the HMDA Data Exploration web application. This project is broken down into separate sections by subfolder. See each subfolder for details of execution, methodology, and application enviroments.


## Table of Contents

* [Software Used](#SoftwareUsed)
* [Hardware Used](#HardwareUsed)
* [Folders](#Folders)
	* [/data_ingest](#dataingest)
	* [/etl_code](#etlcode)
	* [/profiling_code](#profilingcode)
	* [/app_code](#appcode)
	* [/screenshots](#screenshots)
	* [/test_code](#testcode)

## Software Used <a name="SoftwareUsed"></a>

* Scala Ver. 2.11.8
	* Python Ver. 3.7
	* [Plotly]( https://plot.ly/ ) Ver. 3.10.0
* Apache Spark Ver. 2.2.0 (Big Data Analysis)
	* [MLLib Library]( https://spark.apache.org/docs/2.2.0/ml-guide.html )
	* [GeoSpark]( https://datasystemslab.github.io/GeoSpark/ ) Ver. 1.2.0
	* geospark-viz_2.3 Ver. 1.2.0
	* geospark-sql_2.3 Ver. 1.2.0
* Apache Hadoop 2.6.0 with Yarn
* Python Ver. 3.6+
	* [Flask]( https://palletsprojects.com/p/flask/ ) Ver. 1.0.2
	* [flask_sqlalchemy]( https://flask-sqlalchemy.palletsprojects.com/en/2.x/ ) Ver. 2.4.0
	* [Plotly]( https://plot.ly/ ) Ver. 3.10.0
	* Pandas Ver. 0.24.2
* Apache Spark Ver. 2.4.3 (Local Machine version for Webapp execution)

## Hardware Used <a name="HardwareUsed"></a>

* [NYU HPC Dumbo Cluster]( https://wikis.nyu.edu/display/NYUHPC/Clusters+-+Dumbo ) for big data processing
* Personal Windows 10 Laptop
* Personal Mac OSX Laptop

## Folders <a name="Folders"></a>

The following folders are included as required:

* `data_ingest`
* `etl_code`
* `profiling_code`
* `app_code`
* `screenshots`
* `test_code`

### /data\_ingest <a name="dataingest"></a>
This folder contains the scripts used to ingest each dataset.

#### Included Files

* `get-zip-bash` folder containing tools to fetch and zip HMDA data
* `unzip-bash` folder containing tools to unzip and store HMDA codes data

#### HDFS Data

* `/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv` HMDA codes data.
* `/user/jjl359/project/data/HMDA_2007_to_2017.csv` HMDA labels data
* `/user/fh643/InstitutionData/InstitutionData/data/panel_07-09/_*.csv` HMDA nationwide institution panel data from 2007 to 2009
* `/user/fh643/InstitutionData/InstitutionData/data/panel_10-17/_*.csv` HMDA nationwide institution panel data from 2010 to 2017
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.*` US Census State Shapefile data
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.tl_2017_us_state.shp.xml` US Census State Shapefile data metadata, schema, column names and explanations
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_county.*` US Census County Shapefile data
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.tl_2017_us_county.shp.xml` US Census State Shapefile data metadata, schema, column names and explanations

#### Use bash scripts for HMDA zip Data Ingest

The bash scripts will Get/download Home Mortgage Disclosure Act data zip files from the CFPB's website.

There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```shell
chmod +x *.bash
./<script_name> netID
```

Example:

```shell
./get_hmda_code_zip_files.bash jjl359
```

This should download the zip and subsequently unzip the LAR files into your scratch workspace. 

#### Use Bash Script to Unzip Data Files into Scratch

The scripts are a pipeline that are meant to be executed in the following order: 

1.  Unzip the files into the target folder 
2.  Concatenate the files together 

There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```shell
chmod +x *.bash
./<script_name> netID
```

Example:

```shell
./unzip_data.bash jjl359
./unzip_files_hmda_codes.bash jjl359
```

This should download the zip and subsequently unzip the LAR files into your scratch workspace.

In order to put together all 11 files together, use the `concatenate` bash scripts in the folder where the files live: 

```shell
./concatenate.bash
./concatenate_hmda_codes.bash
```

This will ensure all the files are combined into one set, with only one set of headers. 

 
Once the files are ready in scratch, then they can be put into hdfs by: 

```shell
hdfs dfs -put <filename> /user/<username>/<target-folder>
```

#### US Census Geography Data Acquisition

The county and state geometries were contained in shapefiles created by the US Census Bureau and downloaded directly from the US government open data source Data.gov at the following links:

* [2017 County Data]( https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-county-and-equivalent-national-shapefile )
* [2017 State Data]( https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-state-and-equivalent-national )

These data files were downloaded as `tl_2017_us_county.zip` and `tl_2017_us_state.zip` respectively. The files were then passed to the Dumbo cluster via SCP, and unzipped to the directories `/home/cjg507/geometries/states` and `/home/cjg507/geometries/counties`. The files were then uploaded to Dumbo HDFS with the command

```shell
hdfs dfs -put /home/cjg507/geometries sparkproject/
```

where the shapefile data is now contained in the following directories:

* **State:** `/user/cjg507/sparkproject/geometries/states`
* **County:** `/user/cjg507/sparkproject/geometries/counties`

### /etl\_code <a name="etlcode"></a>
This folder contains the code for data cleaning and pre-processing.

#### Script Files

* `src` folder containing the scripts for HMDA etl
* `pom.xml` Maven dependencies file for HMDA etl scripts in `src`
* `runscript.sh` Script used to run Scala etl code for US Census Geography data
* `translateShapefiles.scala` Scala etl code for translating US Census Shapefiles to JSON files


#### HDFS Data

* `/user/cjg507/sparkproject/geometries/states/stateGeom.json` JSON-formatted state geometry data with the following schema:

| Column Name  | Data Type | Description |
| ----------- | ----------- | ----------- |
| STFP      | Int       |  **Primary key** US Census State Federal Information Processing Standard (FIPS) code |
| STUSPS   | String        | US Postal Service State Abbreviation|
| STNAME   | String        | State Name |
| STLAT   | Float        | State Midpoint Latitude |
| STLON   | Float        | State Midpoint Longitude |
| STGEOM   | String: WKT  | State boundary line as well-known text (WKT) formatted string. Includes long strings (> 1000 chars) and commas. |

* `/user/cjg507/sparkproject/geometries/county/countyGeom.json` JSON-formatted county geometry data with the following schema:

| Column Name  | Data Type | Description |
| ----------- | ----------- | ----------- |
| CYKEY      | Int       |  **Primary key** Concatenation of the US Census State and County FIPS codes |
| STFP      | Int       |  US Census State Federal Information Processing Standard (FIPS) code |
| CYFP      | Int       |  US Census County Federal Information Processing Standard (FIPS) code  |
| CYNAME   | String        | Full County Name |
| CYFSTAT   | String        | County Functional Status - Preserved as potential identifier of special county classifications|
| CYLAT   | Float        | County Midpoint Latitude |
| CYLON   | Float        | County Midpoint Longitude |
| CYGEOM   | String: WKT  | County boundary line as well-known text (WKT) formatted string. Includes long strings (> 1000 chars) and commas. |


#### HMDA Data: Execute the Scala-Spark File Using Maven to ETL the Raw Data into a Usable CSV Files

Compile the data with this command, using the `pom.xml` in the folder: 

```shell
/opt/maven/bin/mvn package
nohup spark2-submit --class DataPrep --master yarn target/scala-0.0.1-SNAPSHOT.jar  &
```

or deploy to the cluster:

```shell
spark2-submit --class DataPrep --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar
```

|Column of Interest|  Description	  |  Used or Not Used in Analysis	| Range of Values	| Data Type|
|------------------|----------------|-------------------------------|-----------------|----------|
|as_of_year	       |  The year that the data represents	  |      Used in the analysis	  2007 to 2017	 String or Integer
|respondent_id	    |  9 to 12 digit identifier of the lending institution (ID RSSD or Tax ID)	|  Used in analysis	 1-9 digit ID RSSD or 9 digit tax identifier (xx-xxxxxxx)	| String or Integer|
|agency_code	      |  The agency that regulates the lender (for example the Fed or FDIC)	 | not used	 one to nine representing different agencies	 |string or integer (string if NA is submitted)|
|loan_type	        |   Whether the loan is conforming / not conforming / jumbo	| Will be used	 |there four distinct values (Conventional / FH / VA / FSA) however in some cases NA is submitted	| string |
|property_type	    |   1-4 family home / multifamily / manufactured	| used	 |there are only three types of property types but NA is sometimes submitted	| string or integer |
|loan_purpose	    |    purchase / home improvement / refinancing	 |not used 	 |there are only three choices but NA if nothing is indicated on the application|	 string or integer| 
|owner_occupancy	  |   owner occupied unit or buy to rend unit	 |not used	 |there are three options (Owner occupied / not owner occupied / not applicable)	| string or integer|
|loan_amount_000s	|    the amount of the loan being applied for	| used	 |the range is from 1000 dollars to 99 million dollars and there are some NA or blank values. However it appears that the loan amount is normally distributed between 1000 dollars to 500000 dollars	| integer unless NA|
|preapproval	      |   whether the loan application was pre-approved	 |not used	 |three options	 |integer or string|
|action_taken	    |     whether the loan application was originated or denied	  |used	 |there are 8 distinct actions that can be taken (such as loan originated or application denied)	 |string or integer|
|state_code	      |    the state where the property is located	 |used	 |string or integer of the abbreviations or FIPS number of the state (Federal Information Processing Standard)	 |string or integer|
|county_code	      |   the county where the loan is located	 |used	 |the name of the county or a three digit FIPS number	| string or integer |
|applicant_sex	| gender	 |used 	 |male or female are the options|	 string or integer |
|applicant_income_000s	 |income of the applicant	 |used 	 |the range of incomes are 1000 to 99 million and the average income over the 11 year period is 3.3 million / however the weighted average income by frequency of applications per income listed is 98 thousand	| string or integer |
|rate_spread	| the amount of interest charged above the conventional or market rate	 |not used	| 	 |




#### US Census Geography Shapefile Conversion

Shapefile data are distributed over several different files, and require specialized tools to convert to Spark-readable text. The open-source Spark plugin [GeoSpark]( https://datasystemslab.github.io/GeoSpark/ ) contains a Scala method 'ShapefileReader', among other tools, that were used to translate the data into Spark DataFrames. This process was executed with `runscript.sh`.

The Scala script `translateShapefiles.scala` was used to convert the shapefiles into the following JSON files:

* **State:** `/user/cjg507/sparkproject/geometries/states/stateGeom.json`
* **County:** `/user/cjg507/sparkproject/geometries/county/countyGeom.json`

The JSON files could more easily be used clean and profile the data in the following steps.

The `cleanMapData.scala` script imports the geometry JSON files, drops unnecessary columns, and joins the county and state data on the `STFP` column that contains the State FIPS number, a unique key for each US state, that is present in both geometry tables. The number of counties present in each US state was calculated and saved within `/user/cjg507/sparkproject/geometries/numStates.csv` for analysis of county distribution over the data and to check for join errors.  The number of records for each US Geographical region were calculated and saved to `/user/cjg507/sparkproject/geometries/numRegions.csv` for analysis of global county distribution. 

### /profiling\_code  <a name="profilingcode"></a>
This folder contains the scripts and tools used to profile the input datasets.

#### Script Files

* `src` folder containing the scripts for HMDA profiling
* `pom.xml` Maven dependencies file for HMDA profiling scripts in `src`
* `cleanMapData.scala` Scala code for cleaning and profiling map data
* `plotCounties.py` Python code for plotting HMDA records by US Census geographical location

#### Data Files

* `HMDACounties.html` choropleth of the number of HMDA records by county

#### HMDA Data: Execute the Scala-Spark File Using Maven to Profile the Raw Data and Calculate Denial Rates by Various Feature Mixtures

Compile the data with this command, using the `pom.xml` in the folder: 

```shell
/opt/maven/bin/mvn package
nohup spark2-submit --class DataProfiler --master yarn target/scala-0.0.1-SNAPSHOT.jar
nohup spark2-submit --class CalculateAverageDenialRate --master yarn target/scala-0.0.1-SNAPSHOT.jar  &
```

or deploy to the cluster:

```shell
spark2-submit --class DataProfiler --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar

spark2-submit --class CalculateAverageDenialRate --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar
```

Three Levels of Denial Rate Analysis: 

   1.  Overall Denial Rate by State: ```hdfs dfs -ls /user/jjl359/project/denial-rate-analysis/denial_overall```
   2.  Denial Rate by Race-Ethnicity pair: ```hdfs dfs -ls /user/jjl359/project/denial-rate-analysis/high_level```
   3.  Denial Rate by Year, State, Race-Ethnicity Pair: ```hdfs dfs -ls /user/jjl359/project/denial-rate-analysis/low_level```
   
   |YEAR|State|Race|Ethnicity|Denial Rate|
   |----|-----|----|---------|-----------|
   |2007|AR   |White|Hispanic| x%|
   |... |..   |..  | .. | ..|
   
   
   ![Data Profiling Output](https://github.com/cody-joe-gilbert/SU19SparkClass/blob/master/screenshots/data-profiling-output.PNG)
   

#### US Census Geography Data Profiling

To better understand the geographical distribution of the HMDA data, the combined state-county table was inner-joined to the HMDA data set by state and county name. The number of HMDA records per state and county were calculated and the results saved to `/user/cjg507/sparkproject/geometries/hmdaCountyJoinedCount.json`. This dataset was transferred to a local machine and the `plotCounties.py` script was executed on the data to generate the choropleth saved to `HMDACounties.html`. The results show that the overwhelming majority of counties have few records within the HMDA data set, and that analysis should be made at the state level rather than the county level to make sufficiently well-informed conclusions.

To validate the HMDA to state and county join, an anti-join was created and output was saved to `/user/cjg507/sparkproject/withoutJoins.json`. The anti-joined table was profiled by a breakdown of county and state, the results of which were saved in `/user/cjg507/sparkproject/missingBreakdown.csv`. The results show that approximately 2.5% of the data was not joined, due to either special characters within the county names or the state and county names being missing altogether. This amount of data was considered minor in the context of the greater data set and was dropped from further analysis.

### /app\_code <a name="appcode"></a>

#### Included Files

* `src` folder containing the scripts for HMDA initial model assesment
* `pom.xml` Maven dependencies file for HMDA initial model assesment scripts in `src`
* `runscript.sh` Script used to run modeling Scala code on NYU Dumbo cluster
* `dataModeling.scala` Scala code for modeling lender approval probabilities with HMDA data
* `website` folder contains the Flask web application files and associated Python files used to create the US lender exploration web application. The majority of the included files, folders, and Python scripts were automatically generated by Flask during app creation. This listing will focus on specific customized files and directories.
	* `entry.py` Flask entry point Python script. Used to bootstrap the Flask application and define initial setup.
	* `driverCode.py` Python and Pyspark script that provides the `runModel` class for lender probability modeling
	* `forms.py` Defines the UI form used to collect applicant demographic data
	* `formsEntries.py` Defines the user-provided demographic options that link human-readable options to HMDA codes for modeling
	* `lenderModel` precalculated Naive Bayes Spark MLLib Pipeline model folder.
	* `modelingMatrix.csv` the modeling template created by the modeling code.
	* `logging.conf` configuration file for webapp logging
	* `flaskLog.txt` debug/information log for the Flask webapp. Used for debugging and analysis.

#### HDFS Data

* `/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv` HMDA data used for modeling
* `/user/fh643/InstitutionData/data/panel_07-09` Institution (lender identification) data for years 2007-2009
* `/user/fh643/InstitutionData/data/panel_10-17` Institution (lender identification) data for years 2010-2017
* `/user/cjg507/sparkproject/modelingMatrix.csv` Webapp modeling template
* `/user/cjg507/lenderModel` final MLLib NaiveBayes model


#### Initial Model Evaluation
The code will execute machine learning model evaluation code. It evaluates the AUC (area under curve) for Naive Bayes, SVM, and Logistic Regression.

This segment of the application uses the following folder for its data: ```hdfs dfs -ls /user/jjl359/project/df_for_logistic_regression```

Running spark-submit:

```/opt/maven/bin/mvn package```

```spark2-submit --class ModelEval --deploy-mode cluster --executor-memory 50G --total-executor-cores 9182 target/scala-0.0.1-SNAPSHOT.jar > output.txt```



#### Creating Model Feature and Label Input
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

#### Creating Webapp Template Matrix

The web application used to model approval probabilities will ask the user for all of applicant demographic features listed above, but will otherwise generate the predicted years and lenders. To create a probability for each combination of demographic, year, and lender, the webapp will create a dataframe that is the cartesian product (demographics X set of years considered X set of lenders). Although there will only be one set of demographics and 11 years, there are several thousand unique lenders within the HMDA dataset. Predicting a cartesian product with the entire set of lenders will be infesible for an interactive application, therefore the number of lenders considered was reduced to the top 20 lenders with the largest number of records in the HMDA dataset. 

The top 20 lenders were extracted and a template dataframe containing the cartesian product of the (set of years X set of top 20 lenders) was pre-calculated in Spark and saved as the CSV file `/user/cjg507/sparkproject/modelingMatrix.csv`. To produce a modeling dataframe, the webapp will only have to create a column for each demographic feature and copy the same value for each row within the template year X lender dataframe. 

#### Creating the Modeling Pipeline and Naive Bayes Model

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

#### Flask Web Application




#### Flask Installation

1. Ensure Flask & flask_sqlalchemy is installed

```shell
pip install -U Flask
pip install flask_sqlalchemy
```

2. Install a local version of [Spark]( https://spark.apache.org/downloads.html ) verify PySpark is reachable from your python PATH. This can be verified by

```python
import pyspark
```

#### Flask Execution
To execute the flask web application run **entry.py** with
```shell
python entry.py
```
### /screenshots <a name="screenshots"></a>


### /test\_code <a name="testcode"></a>

Code in this folder was used to prototype four machine learning models on the entire data set using Scala-Spark REPL.
