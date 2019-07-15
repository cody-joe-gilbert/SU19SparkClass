import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.Random
import sqlContext.implicits._

val data = sc.textFile("project/data/top_1000.csv")
//val header = data.first
//val data_filtered = data.filter(_(0) != header(0))
val data_split = data_filtered.map(line => line.split(",").mkString(","))

val data_tuple = data_split.map(line => {
   val lines = line.split(',')
  (lines(6), lines(8), lines(9), lines(10), lines(12), lines(13), lines(14), lines(18), lines(19), lines(26), lines(35), lines(37), lines(42), lines(43), lines(44), lines(45), lines(46))
  })
val output = data_tuple.map(values => values.toString).
   map(s=>s.substring(1,s.length-1))
   output.take(10).foreach(println)
 
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val dataSet = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("project/data/top_1000.csv")

output.saveAsTextFile("project/clean-data/data_for_analysis.csv")
