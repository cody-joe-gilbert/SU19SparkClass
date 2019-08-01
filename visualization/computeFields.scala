import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import spark.implicits._

val sexPath = "/user/fh643/VisualPrep/sexData"
val racePath = "/user/fh643/VisualPrep/raceData"
val ethPath = "/user/fh643/VisualPrep/ethData"
val incomePath = "/user/fh643/VisualPrep/incomeData"
val sexOut = "/user/fh643/VisualPrep/sexDenials"
val raceOut = "/user/fh643/VisualPrep/raceDenials"
val ethOut = "/user/fh643/VisualPrep/ethDenials"
val incomeOut = "/user/fh643/VisualPrep/incomeDenials"

val sex = spark.read.option("header", "true").option("inferSchema", "true").csv(sexPath)
val race = spark.read.option("header", "true").option("inferSchema", "true").csv(racePath)
val eth = spark.read.option("header", "true").option("inferSchema", "true").csv(ethPath)
val income = spark.read.option("header", "true").option("inferSchema", "true").csv(incomePath)

/*************************  Compute Denial Rate Per Sex Group *************************/
val sex_count = sex.groupBy("year", "sex", "action").count.orderBy($"year".asc)
val tmp = sex_count.groupBy("year", "sex").agg(sum($"count"))
val joined = sex_count.join(tmp,
			sex_count("year") <=>tmp("year") && 
			sex_count("sex") <=> tmp("sex")).
		       filter($"action" === "deny").
		       toDF("year", "sex", "action", "count", "y", "s", "sum").
		       select("year", "sex", "count", "sum")

// ---- USER DEFINED FUNCTION TO TAKE DIVISION
val myUDF = udf((num: Long, denom: Long) => (num.toDouble/denom.toDouble))
val sex_out = joined.withColumn("denial", myUDF(col("count"), col("sum"))).select("year", "sex", "denial")
sex_out.coalesce(1).write.
    mode("overwrite").
    option("header","true").
    format("csv").
    save(sexOut)


/*************************  Compute Denial Rate Per Race Group *************************/
val race_count = race.groupBy("year", "race", "action").count.orderBy($"year".asc)
val tmp = race_count.groupBy("year", "race").agg(sum($"count"))
val joined = race_count.filter($"action" === "deny").
			join(tmp,
                        race_count("year") <=> tmp("year") && 
                        race_count("race") <=> tmp("race")).
                      // filter($"action" === "deny").
                       toDF("year", "race", "action", "count", "y", "r", "sum").
                       select("year", "race", "count", "sum")

val race_out = joined.withColumn("denial", myUDF(col("count"), col("sum"))).select("year", "race", "denial")
race_out.coalesce(1).write.
    mode("overwrite").
    option("header","true").
    format("csv").
    save(raceOut)

/*************************  Compute Denial Rate Per Ethnicity Group *************************/
val eth_count = eth.groupBy("year", "eth", "action").count.orderBy($"year".asc)
val tmp = eth_count.groupBy("year", "eth").agg(sum($"count"))
val joined = eth_count.filter($"action" === "deny").
			join(tmp,
                        eth_count("year") <=> tmp("year") &&    // BEWARE: "===" DOESN'T WORK 
                        eth_count("eth") <=> tmp("eth")).
                       toDF("year", "eth", "action", "count", "y", "r", "sum").
                       select("year", "eth", "count", "sum")

val eth_out = joined.withColumn("denial", myUDF(col("count"), col("sum"))).select("year", "eth", "denial")
eth_out.coalesce(1).write.
    mode("overwrite").
    option("header","true").
    format("csv").
    save(ethOut)

/*************************  Compute Denial Rate Per Income Quantile *************************/
// transform income to Int
val numerical = income.rdd.
              map(x => x.toString.drop(1).stripSuffix("]").  // drop the left/right bracket
	      split(",")).
	      filter(x => x(1)!= "null").
	      map(x => Row(x(0).toInt, x(1).toInt, x(2))).
              map({case Row(v1: Int, v2: Int, v3: String) => (v1, v2, v3)}).
              toDF("year", "income", "action")

//numerical.describe("income").show 
//import org.apache.spark.sql.DataFrameStatFunctions
/*
+-------+------------------+                                                    
|summary|            income|
+-------+------------------+
|  count|         123734350|
|   mean|105.19944659668072|
| stddev| 258.0252207984417|
|    min|                 1|
|    max|            610715|
+-------+------------------+
*/

numerical.stat.approxQuantile("income", Array(0.20, 0.40, 0.60, 0.80, 0.90), 0.1)
//Array[Double] = Array(58.0, 77.0, 128.0, 186.0, 610715.0)

numerical.stat.approxQuantile("income", Array(0.05, 0.1, 0.15, 0.20, 0.25, 0.3,0.35, 0.40, 0.45, 0.5, 0.55, 0.60,0.65, 0.7, 0.75,0.80,0.85, 0.90, 0.95, 1.0), 0.1)
//Array(29.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 67.0, 72.0, 77.0, 85.0, 92.0, 100.0, 112.0, 122.0, 138.0, 161.0, 196.0, 300.0, 610715.0)

numerical.stat.approxQuantile("income", Array(0.05, 0.1, 0.15, 0.20, 0.25, 0.3,0.35, 0.40, 0.45, 0.5, 0.55, 0.60,0.65, 0.7, 0.75,0.80,0.85, 0.90, 0.95, 1.0), 0.001)
//Array[Double] = Array(26.0, 33.0, 38.0, 43.0, 48.0, 54.0, 59.0, 64.0, 70.0, 76.0, 83.0, 90.0, 98.0, 108.0, 120.0, 134.0, 154.0, 186.0, 257.0, 610715.0)







