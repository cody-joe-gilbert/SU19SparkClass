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


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val dataSet = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("project/data/top_1000.csv")

val dataForAnalysis = dataSet.select("loan_amount_000s","applicant_income_000s","state_name","state_abbr","respondent_id","purchaser_type_name","property_type_name","loan_type_name","loan_purpose_name","county_name","as_of_year","applicant_sex_name","applicant_race_name_1","applicant_ethnicity_name","agency_name","agency_abbr","action_taken_name")

dataSet.saveAsTextFile("project/clean-data/")

