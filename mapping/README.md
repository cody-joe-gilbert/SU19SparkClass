# Geographic Mapping
This folder contains the tools use to geographically map the loan data by state and county.

## Folders
* `profilingCleaning` contains the scripts, data, and writeup for acquiring, cleaning, profiling, and generating the input US Census geography data.


## HDFS Data
* **/user/cjg507/sparkproject/geometries/states/stateGeom.json** JSON-formatted state geometry data with the following schema:

| Column Name  | Data Type | Description |
| ----------- | ----------- | ----------- |
| STFP      | Int       |  **Primary key** US Census State Federal Information Processing Standard (FIPS) code |
| STUSPS   | String        | US Postal Service State Abbreviation|
| STNAME   | String        | State Name |
| STLAT   | Float        | State Midpoint Latitude |
| STLON   | Float        | State Midpoint Longitude |
| STGEOM   | String: WKT  | State boundary line as well-known text (WKT) formatted string. Includes long strings (> 1000 chars) and commas. |

* **/user/cjg507/sparkproject/geometries/county/countyGeom.json** JSON-formatted county geometry data with the following schema:

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