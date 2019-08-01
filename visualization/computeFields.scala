import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import spark.implicits._

val sexPath = "/user/fh643/VisualPrep/sexData"
val racePath = "/user/fh643/VisualPrep/raceData"
val ethPath = "/user/fh643/VisualPrep/ethData"
val incomePath = "/user/fh643/VisualPrep/incomeData"

val sex = spark.read.option("header", "true").option("inferSchema", "true").csv(sexPath)
val race = spark.read.option("header", "true").option("inferSchema", "true").csv(racePath)
val eth = spark.read.option("header", "true").option("inferSchema", "true").csv(ethPath)
val income = spark.read.option("header", "true").option("inferSchema", "true").csv(incomePath)