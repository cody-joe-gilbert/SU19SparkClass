# Geographic Mapping
### Author: C.J. Gilbert

This folder contains the tools and methods to download US Census county and state mapping data that is used to geographically partition HMDA loan data for analysis. After processing and profiler, it was ultimately decided that the geographic information added no additional value to the final application and no further manipulation was performed on the data.

## Software Used

* Scala Ver. 2.11.8
* Python Ver. 3.7
	* [Plotly]( https://plot.ly/ ) Ver. 3.10.0
* Apache Spark Ver. 2.2.0
	* [GeoSpark]( https://datasystemslab.github.io/GeoSpark/ ) Ver. 1.2.0
	* geospark-viz_2.3 Ver. 1.2.0
	* geospark-sql_2.3 Ver. 1.2.0
* Apache Hadoop 2.6.0 with Yarn

## Hardware Used

* [NYU HPC Dumbo Cluster]( https://wikis.nyu.edu/display/NYUHPC/Clusters+-+Dumbo ) for big data processing
* Personal Windows 10 Laptop for visualizations

## Included files

### Data Files
* `HMDACounties.html` choropleth of the number of HMDA records by county

### Scripts
* `runscript.sh` Script used to run Scala code on NYU Dumbo cluster
* `translateShapefiles.scala` Scala code for translating US Census Shapefiles to JSON files
* `cleanMapData.scala` Scala code for cleaning and profiling map data
* `plotCounties.py` Python code for plotting HMDA records by geographical location


### HDFS Data

* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.*` US Census State Shapefile data
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.tl_2017_us_state.shp.xml` US Census State Shapefile data metadata, schema, column names and explanations
* `/user/cjg507/sparkproject/geometries/states/stateGeom.json` JSON-formated State Census data
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_county.*` US Census County Shapefile data
* `/user/cjg507/sparkproject/geometries/states/tl_2017_us_state.tl_2017_us_county.shp.xml` US Census State Shapefile data metadata, schema, column names and explanations
* `/user/cjg507/sparkproject/geometries/county/countyGeom.json` JSON-formated County Census data
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


## Process Description

## Data Acquisition 
The county and state geometries were contained in shapefiles created by the US Census Bureau and downloaded directly from the US government open data source Data.gov at the following links:

* [2017 County Data]( https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-county-and-equivalent-national-shapefile )
* [2017 State Data]( https://catalog.data.gov/dataset/tiger-line-shapefile-2017-nation-u-s-current-state-and-equivalent-national )

These data files were downloaded as `tl_2017_us_county.zip` and `tl_2017_us_state.zip` respectively. The files were then passed to the Dumbo cluster via SCP, and unzipped to the directories `/home/cjg507/geometries/states` and `/home/cjg507/geometries/counties`. The files were then uploaded to Dumbo HDFS with the command
```
$ hdfs dfs -put /home/cjg507/geometries sparkproject/
```
where the shapefile data is now contained in the following directories:

* **State:** `/user/cjg507/sparkproject/geometries/states`
* **County:** `/user/cjg507/sparkproject/geometries/counties`


## Data Conversion and Cleaning 
Shapefile data are distributed over several different files, and require specialized tools to convert to Spark-readable text. The open-source Spark plugin [GeoSpark]( https://datasystemslab.github.io/GeoSpark/ ) contains a Scala method 'ShapefileReader', among other tools, that were used to translate the data into Spark DataFrames. This process was executed with `runscript.sh`.

The Scala script `translateShapefiles.scala` was used to convert the shapefiles into the following JSON files:

* **State:** `/user/cjg507/sparkproject/geometries/states/stateGeom.json`
* **County:** `/user/cjg507/sparkproject/geometries/county/countyGeom.json`

The JSON files could more easily be used clean and profile the data in the following steps.

The `cleanMapData.scala` script imports the geometry JSON files, drops unnecessary columns, and joins the county and state data on the `STFP` column that contains the State FIPS number, a unique key for each US state, that is present in both geometry tables. The number of counties present in each US state was calculated and saved within `/user/cjg507/sparkproject/geometries/numStates.csv` for analysis of county distribution over the data and to check for join errors.  The number of records for each US Geographical region were calculated and saved to `/user/cjg507/sparkproject/geometries/numRegions.csv` for analysis of global county distribution. 

To better understand the geographical distribution of the HMDA data, the combined state-county table was inner-joined to the HMDA data set by state and county name. The number of HMDA records per state and county were calculated and the results saved to `/user/cjg507/sparkproject/geometries/hmdaCountyJoinedCount.json`. This dataset was transferred to a local machine and the `plotCounties.py` script was executed on the data to generate the choropleth saved to `HMDACounties.html`. The results show that the overwhelming majority of counties have few records within the HMDA data set, and that analysis should be made at the state level rather than the county level to make sufficiently well-informed conclusions.

To validate the HMDA to state and county join, an anti-join was created and output was saved to `/user/cjg507/sparkproject/withoutJoins.json`. The anti-joined table was profiled by a breakdown of county and state, the results of which were saved in `/user/cjg507/sparkproject/missingBreakdown.csv`. The results show that approximately 2.5% of the data was not joined, due to either special characters within the county names or the state and county names being missing altogether. This amount of data was considered minor in the context of the greater data set and was dropped from further analysis.


