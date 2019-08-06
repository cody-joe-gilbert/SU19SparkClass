# CSCI-GA.3033-001: Summer 2019 Team Project
### Cody Gilbert, Fang Han, Jeremy Lao

Repository for the team project in the Summer 2019 Spark Class.

This repository contains all the files used to create and execute the HMDA Data Exploration web application. This project is broken down into separate sections by subfolder. See each subfolder for details of execution, methodology, and application enviroments.


## Subfolders

* `app_code`
* `profiling_code`
* `etl_code`
* `data_ingest`
* `screenshots`
* `test_code`

### /data_ingest

#### Use bash scripts for HMDA zip Data Ingest

> The bash scripts will Get/download Home Mortgage Disclosure Act data zip files from the CFPB's website.

> There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

> How to execute the following scripts:

```chmod +x *.bash```

```./<script_name> netID```

> Example:

````./get_hmda_code_zip_files.bash jjl359````

> This should download the zip and subsequently unzip the LAR files into your scratch workspace. 

#### Use Bash Script to Unzip Data Files into Scratch

The scripts are a pipeline that are meant to be executed in the following order: 

  1.  Unzip the files into the target folder 
  2.  Concatenate the files together 

There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```chmod +x *.bash```

```./<script_name> netID```

Example:

````./unzip_data.bash jjl359````
````./unzip_files_hmda_codes.bash jjl359````

This should download the zip and subsequently unzip the LAR files into your scratch workspace.

In order to put together all 11 files together, use the ````concatenate```` bash scripts in the folder where the files live: 

````./concatenate.bash````
````./concatenate_hmda_codes.bash````

This will ensure all the files are combined into one set, with only one set of headers. 

 
Once the files are ready in scratch, then they can be put into hdfs by: 

````hdfs dfs -put <filename> /user/<username>/<target-folder>````


### /etl_code

#### Execute the Scala-Spark File Using Maven to ETL the Raw Data into a Usable CSV Files

Compile the data with this command, using the ````pom.xml```` in the folder: 

````/opt/maven/bin/mvn package````

````nohup spark2-submit --class DataPrep --master yarn target/scala-0.0.1-SNAPSHOT.jar  &````

or deploy to the cluster:

````spark2-submit --class DataPrep --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar````

### /profiling_code

#### Execute the Scala-Spark File Using Maven to Profile the Raw Data and Calculate Denial Rates by Various Feature Mixtures

Compile the data with this command, using the ````pom.xml```` in the folder: 

````/opt/maven/bin/mvn package````

````nohup spark2-submit --class DataProfiler --master yarn target/scala-0.0.1-SNAPSHOT.jar````

````nohup spark2-submit --class CalculateAverageDenialRate --master yarn target/scala-0.0.1-SNAPSHOT.jar  &````

or deploy to the cluster:

````spark2-submit --class DataProfiler --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar````

````spark2-submit --class CalculateAverageDenialRate --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar````

### /app_code

The code will execute machine learning model evaluation code. It evaluates the AUC (area under curve) for Naive Bayes, SVM, and Logistic Regression.

Running spark-submit:

/opt/maven/bin/mvn package

spark2-submit --class ModelEval --deploy-mode cluster --executor-memory 50G --total-executor-cores 9182 target/scala-0.0.1-SNAPSHOT.jar > output.txt

* `website` contains the Flask application used to host the HMDA Data Exploration application and all associated UI tools
* `mapping` contains the tools and methods used in processing the US Census Geography Data
* `modeling` contains the Scala scripts and modeling method for the underlying model of the Lender Recommendation Tool
* `InstitutionData` contains the Scala scripts and data used to profile the Nationwide Institution Data
* `HMDA` contains the Scala scripts and data used to profile the Home Mortgage Disclosure Act (HMDA) dataset
* `docs` contains the papers and presentations created by this project. 
