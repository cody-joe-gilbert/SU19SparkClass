/**
* CSCI-GA.3033-001: Big Data Application Development
* Team Project Code
* Cody Gilbert, Fang Han, Jeremy Lao
* The code extracts columns from the institution data join them with HMDA data  
* @author: Fang Han
* REQUIREMENT: spark-shell 2.4.0+ 
*/

//*********** Read in csv files ***********
val before10path = "/user/fh643/InstitutionData/data/panel_07-09"
val after10path = "/user/fh643/InstitutionData/data/panel_10-17"
val csv10_17 = spark.read.option("header", "true").
                option("inferSchema", "true").
                csv(after10path)
val csv07_09 = spark.read.option("header", "true").
                option("inferSchema", "true").
                csv(before10path)

//*********** Data profiling ***********
//csv07_09.printSchema
//csv10_17.printSchema

//csv10_17.groupBy("Respondent ID").count().orderBy($"count".desc).show()
//csv07_09.groupBy("Respondent Identification Number").count().orderBy($"count".desc).show()

//val select07_09 = csv07_09.select("Respondent Identification Number", "Parent Identification Number", "Respondent Name")
//val select10_17 = csv10_17.select("Respondent ID", "Parent Respondent ID", "Respondent Name (Panel)")

//*********** Save intermediate data ***********
//select10_17.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/selected/10_17")
//select07_09.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/selected/07_09")

//*********** Merge Schemas ***********
//val tmp = spark.read.option("header", "true").option("mergeSchema", "true").option("nullValue", "[^0-9-]*").csv("/user/fh643/InstitutionData/data/selected")

//*********** Rename the Columns *********** 
//val names = Seq("RespondentID", "ParentID", "RespondentName")
//val df = tmp.toDF(names: _*)

//df.count
//df.select("RespondentID").distinct.count
//df.select("ParentID").distinct.count
//df.select("RespondentName").distinct.count
//df.groupBy("RespondentID").count().orderBy($"count".desc).show()
/*
+------------+-----+                                                            
|RespondentID|count|
+------------+-----+
|  0000009788|   12|
|  0000007748|   12|
|  0000024540|   12|
|  0000022769|   12|
|  0000008145|   11|
|  0000020448|   10|
+------------+-----+
*/


// filter valid parentIDs
//val validParents = df.filter($"ParentID" rlike "[0-9]+-?[0-9]+")
//df.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/merged")

// lots of duplicates
//val distinct = df.distinct
//distinct.orderBy($"RespondentID".asc).write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/distinct")

//*********** Join Institution Data On Three Keys  ***********


//*********** Read In HMDA Data ***********
val hmdaPath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"

// join 1 -- csv10_17 joined with hmda in its entirety 
val hmda = spark.read.option("header", "true").option("inferSchema", "true").csv(hmdaPath)
val bigJoin = hmda.join(csv10_17, $"respondent_id" === $"Respondent ID")
bigJoin.repartition(1).write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/joinedData/bigJoin")

// join 2 -- selected columns from 2007-2017 joined with hmda
val smallJoin = hmda.join(df, $"respondent_id" === $"RespondentID")
smallJoin.repartition(1).write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/joinedData/smallJoin")