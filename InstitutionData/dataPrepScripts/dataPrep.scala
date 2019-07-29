// make sure spark-shell 2.4.0+ is loaded.

val csv10_17 = spark.read.option("header", "true").
                option("inferSchema", "true").
                csv("/user/fh643/InstitutionData/data/panel_10-17")

val csv07_09 = spark.read.option("header", "true").
                option("inferSchema", "true").
                csv("/user/fh643/InstitutionData/data/panel_07-09")

csv07_09.printSchema
csv10_17.printSchema

csv10_17.groupBy("Respondent ID").count().orderBy($"count".desc).show()
csv07_09.groupBy("Respondent Identification Number").count().orderBy($"count".desc).show()

val select07_09 = csv07_09.select("Respondent Identification Number", "Parent Identification Number", "Respondent Name")
val select10_17 = csv10_17.select("Respondent ID", "Parent Respondent ID", "Respondent Name (Panel)")

select10_17.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/selected/10_17")
select07_09.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/selected/07_09")


// new dataframe for selected columns
val tmp = spark.read.option("header", "true").
            option("mergeSchema", "true").
            option("nullValue", "[^0-9-]*").
            csv("/user/fh643/InstitutionData/data/selected")

// rename the columns
val names = Seq("RespondentID", "ParentID", "RespondentName")
val df = tmp.toDF(names: _*)

df.count
df.select("RespondentID").distinct.count
df.select("ParentID").distinct.count
df.select("RespondentName").distinct.count

df.groupBy("RespondentID").count().orderBy($"count".desc).show()
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
|  0000005636|    9|
|  0000001427|    9|
|  0000008854|    9|
|  0000000644|    9|
|  0000003218|    9|
|  0000018710|    8|
|  0000016401|    8|
|  0000016402|    8|
|  0000000340|    8|
|  0000022141|    8|
|  0000010375|    8|
|  0000005885|    8|
|  0000016629|    8|
|  0000034153|    8|
+------------+-----+

*/


// filter valid parentIDs
val validParents = df.filter($"ParentID" rlike "[0-9]+-?[0-9]+")

df.write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/merged")

// lots of duplicates
val distinct = df.distinct
distinct.orderBy($"RespondentID".asc).write.mode("overwrite").format("csv").save("/user/fh643/InstitutionData/data/distinct")