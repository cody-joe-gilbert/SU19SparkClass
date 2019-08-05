/*
CSCI-GA.3033-001: Summer 2019 Team Project
Cody Gilbert, Fang Han, Jeremy Lao

US Census Geography file cleaning and processing scripts.
Uses GeoSpark to read in the downloaded Shapefiles and convert
to more Spark-manageable JSON files.


@author: Cody Gilbert
*/
import org.apache.spark.sql.functions.col
import spark.implicits._

// cleaning the .json files (dropping unnecessary rows)
val stateShapefile="/user/cjg507/sparkproject/geometries/states"
val dataForAnalysis = spark.master("yarn").read.format("json").option("header", "true").option("inferSchema", "true").load(stateShapefile + "/stateGeom.json")
val stateCleaned = dataForAnalysis.select($"REGION".alias("STREGION"),
 $"STATEFP".alias("STFP"),
 $"STUSPS",
 $"NAME".alias("STNAME"),
 $"INTPTLAT".alias("STLAT"),
 $"INTPTLON".alias("STLON"), 
 $"geometry".alias("STGEOM"))
stateCleaned.show()

val countyShapefile="/user/cjg507/sparkproject/geometries/county"
val dataForAnalysis = spark.read.format("json").option("header", "true").option("inferSchema", "true").load(countyShapefile + "/countyGeom.json")
val countyCleaned = dataForAnalysis.select( $"STATEFP".alias("STFP"),
 $"COUNTYFP".alias("CYFP"),
 $"GEOID".alias("CYKEY"),
 $"NAMELSAD".alias("CYNAME"),
 $"FUNCSTAT".alias("CYFSTAT"),
 $"INTPTLAT".alias("CYLAT"),
 $"INTPTLON".alias("CYLON"),
 $"geometry".alias("CYGEOM"))
countyCleaned.show()

// Join state and counties into a single table
val combined = countyCleaned.join(stateCleaned,
stateCleaned.col("STFP") === countyCleaned.col("STFP"),
"inner").drop(stateCleaned.col("STFP"))

// Profile Map data
val numStates = combined.groupBy("STNAME").count().write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/geometries/numStates.csv")
val numRegions = combined.groupBy("STREGION").count().write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/geometries/numRegions.csv")

// read in the HMDA data set to get regional profiles
val path = "/user/jjl359/project/data/HMDA_2007_to_2017.csv"
val bigFileSchema = Array("sequence_number",
"state_name",
"state_abbr",
"county_name")
val hmda = spark.read.format("csv").
option("header", "true").
option("inferSchema", "true").
load(path).
select(bigFileSchema(0), bigFileSchema.drop(1):_*).persist()

// get number of HMDA entries by county and state
val hmdainnerjoined = hmda.join(combined,
hmda.col("county_name") <=> combined.col("CYNAME") && hmda.col("state_abbr") <=> combined.col("STUSPS"),
"inner")
hmdainnerjoined.write.mode("overwrite").format("json").save("/user/cjg507/sparkproject/geometries/hmdaCountyJoinedCount.json")

// check if any hmda entries were not matched
val hmdaresidual = hmda.join(combined,
hmda.col("county_name") <=> combined.col("CYNAME") && hmda.col("state_abbr") <=> combined.col("STUSPS"),
"leftanti").persist()

// write out the missing (unjoined) data for further use
hmdaresidual.write.mode("overwrite").format("json").save("/user/cjg507/sparkproject/withoutJoins.json")

// Write out a summary of the missing (unjoined) data
hmdaresidual.groupBy("state_name", "county_name").count().write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/missingBreakdown.csv")






