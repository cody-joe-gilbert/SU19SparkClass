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

#### HMDA Data Ingest

> The bash scripts will Get/download Home Mortgage Disclosure Act data zip files from the CFPB's website.

> There are two sets of download, unzip, and concatenate scripts.  This is because there are two ways of representing the data set.  The representation with larger amounts of memory has extensive string representations of the data.  The representation that takes less memory is an integer representation of the text options. 

How to execute the following scripts:

```chmod +x *.bash```

```./<script_name> netID```

Example:

````./get_hmda_code_zip_files.bash jjl359````

This should download the zip and subsequently unzip the LAR files into your scratch workspace. 


* `website` contains the Flask application used to host the HMDA Data Exploration application and all associated UI tools
* `mapping` contains the tools and methods used in processing the US Census Geography Data
* `modeling` contains the Scala scripts and modeling method for the underlying model of the Lender Recommendation Tool
* `InstitutionData` contains the Scala scripts and data used to profile the Nationwide Institution Data
* `HMDA` contains the Scala scripts and data used to profile the Home Mortgage Disclosure Act (HMDA) dataset
* `docs` contains the papers and presentations created by this project. 
