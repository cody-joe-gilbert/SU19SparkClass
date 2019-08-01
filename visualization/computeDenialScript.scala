/**
* CSCI-GA.3033-001: Big Data Application Development
* Team Project Code
* Cody Gilbert, Fang Han, Jeremy Lao
* The code prepares data for the multi-year heatmap  
* @author: Fang Han
* REQUIREMENT: spark-shell 2.4.0+ 
*/

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import spark.implicits._

//************************** STAGE ONE **************************
val hmdaPath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
val hmda = spark.read.option("header", "true").option("inferSchema", "true").csv(hmdaPath)
val year_approve = hmda.select("as_of_year", "state_code", "action_taken")
val names = Seq("as_of_year", "state_code", "action_taken")

val rdd = year_approve.rdd
val cleaned = rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    // FILTER USEFUL ACTION RECORDS [1, 2, 8] approve, [3, 7] deny 
                    filter(x => x(2) == "1" ||   
                                x(2) == "2" ||
                                x(2) == "8" ||
                                x(2) == "3" ||
                                x(2) == "7").
                    // MAP STATE CODES TO LETTER ABBREV.
                    map(x => Row(
                       x(0),  
                       x(1),
                       x(2).
                         replace("1", "accept").replace("2", "accept").replace("8", "accept").
                         replace("3", "deny").replace("7", "deny"))).
                    map({case Row(v1: String, v2: String, v3: String) => (v1, v2, v3)}).
                    toDF(names: _*)

val writePath = "/user/fh643/VisualPrep/heatmap"
cleaned.write.mode("overwrite").format("csv").save(writePath)


//************************** STAGE TWO **************************
val readPath = "/user/fh643/VisualPrep/heatmap"
val countPath = "/user/fh643/VisualPrep/counts"
val data = spark.read.option("header", "true").option("inferSchema", "true").csv(readPath)

val counts = cleaned.groupBy("as_of_year", "state_code", "action_taken").count
counts.write.mode("overwrite").format("csv").save(countPath)

//val counts = spark.read.option("header", "true").option("inferSchema", "true").csv(countPath)
val header = Seq("Year", "state", "action", "count")
// map state code to letter abbre. note that not all codes are well-formed strings. some are single digit nums. 
def isNumeric(str:String): Boolean = str.matches("\\d+")
val parsed = counts.rdd.
              map(x => x.toString.drop(1).stripSuffix("]").split(",")).  // drop the left bracket 
              filter(x => !x.contains("null") && !x.contains("NA")).
              map(x => Row(x(0), 
                       f"${x(1).toInt}%02d".   // reformat the state string
                         replace("01","AL").replace("02","AK").replace("04","AZ").replace("05","AR").
                         replace("06","CA").replace("08","CO").replace("09","CT").replace("10","DE").
                         replace("12","FL").replace("13","GA").replace("15","HI").replace("16","ID").
                         replace("17","IL").replace("18","IN").replace("19","IA").replace("20","KS").
                         replace("21","KY").replace("22","LA").replace("23","ME").replace("24","MD").
                         replace("25","MD").replace("26","MI").replace("27","MN").replace("28","MS").
                         replace("29","MO").replace("30","MT").replace("31","NE").replace("32","NV").
                         replace("33","NH").replace("34","NJ").replace("35","NM").replace("36","NY").
                         replace("37","NC").replace("38","ND").replace("39","OH").replace("40","OK").
                         replace("41","OR").replace("42","PA").replace("44","RI").replace("45","SC").
                         replace("46","SD").replace("47","TN").replace("48","TX").replace("49","UT").
                         replace("50","VT").replace("51","VA").replace("53","WA").replace("55","WI").
                         replace("56","WY").replace("60","AS").replace("66","GU").replace("69","MP").
                         replace("72","PR").replace("78","VI"),
                       x(2), 
                       x(3).toLong)).
              filter(x => !isNumeric(x(1).toString)).          
              map({case Row(v1: String, v2: String, v3: String, v4: Long) => (v1, v2, v3, v4)}).
              toDF(header: _*)

//************************** STAGE THREE -- COMPUTE DENIAL RATE **************************
val tmp = parsed.groupBy("Year", "state").agg(sum("count"))
val joined = tmp.join(parsed, tmp("Year") <=> parsed("Year") &&
                    tmp("state") <=> parsed("state"),  "full_outer").toDF("Year", "State", "Sum", "y", "s", "a", "Count").filter($"action" !== "accept")

val select = joined.select("Year", "State", "Sum", "Count")
val out = select.rdd.
              map(x => x.toString.drop(1).stripSuffix("]").split(",")).  // drop the left bracket 
              map(x => Row(x(0), x(1), x(2), (x(3).toDouble) / (x(2).toDouble))).
                       map({case Row(v1: String, v2: String, v3: String, v4: Double) => (v1, v2, v3, v4)}).
                       toDF("Year", "State", "Sum", "DenialRate").
              select("Year", "State", "DenialRate").
              filter($"DenialRate" !== 1.0)

// Check how many states/year are left out
out.groupBy("State").count.orderBy($"count".asc).show
out.groupBy("Year").count.orderBy($"count".asc).show

val outPath = "/user/fh643/VisualPrep/denialCount"
out.write.mode("overwrite").format("csv").save(outPath)