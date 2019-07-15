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

val dataSet = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("project/data/hmda_2013.csv")

val dataForAnalysis = dataSet.select("loan_amount_000s","applicant_income_000s","state_name","state_abbr","respondent_id","purchaser_type_name","property_type_name","loan_type_name","lien_status_name","loan_purpose_name","county_name","as_of_year","applicant_sex_name","applicant_race_name_1","applicant_ethnicity_name","agency_name","agency_abbr","action_taken_name")

//Maximum-Minimum for Loan amount
println("===========================")
println("Loan Amount - Maximum,Minimum")
dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(0))), min(dataForAnalysis(dataForAnalysis.columns(0)))).show

println("===========================")
println("Applicant Income - Maximum,Minimum")
dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(1))), min(dataForAnalysis(dataForAnalysis.columns(1)))).show
val mrAppIncome = dataForAnalysis.groupBy("applicant_income_000s").count()
mrAppIncome.show()

println("===========================")
println("Distinct Actions Taken")
val distinctActionsTaken = dataForAnalysis.select(dataForAnalysis("action_taken_name")).distinct
distinctActionsTaken.collect().foreach(println)
val mrDistinctActionsTaken = dataForAnalysis.groupBy("action_taken_name").count()
mrDistinctActionsTaken.show()

println("===========================")
println("Purchaser Type Name")
val distinctPurchaserType = dataForAnalysis.select(dataForAnalysis("purchaser_type_name")).distinct
distinctPurchaserType.collect().foreach(println)
val mrDistinctPurchaserType = dataForAnalysis.groupBy("purchaser_type_name").count()
mrDistinctPurchaserType.show()

println("===========================")
println("Property Type Name")
val distinctPropertyType = dataForAnalysis.select(dataForAnalysis("property_type_name")).distinct
distinctPropertyType.collect().foreach(println)


println("===========================")
println("Loan Type Name")
val distinctLoanType = dataForAnalysis.select(dataForAnalysis("loan_type_name")).distinct
distinctLoanType.collect().foreach(println)


println("===========================")
println("Loan Purpose Name")
val distinctLoanPurpose = dataForAnalysis.select(dataForAnalysis("loan_purpose_name")).distinct
distinctLoanPurpose.collect().foreach(println)

println("===========================")
println("Applicant Race")
val applicant_race_name = dataForAnalysis.select(dataForAnalysis("applicant_race_name_1")).distinct
applicant_race_name.collect().foreach(println)


println("===========================")
println("Applicant Ethnicity")
val applicant_ethnicity_name = dataForAnalysis.select(dataForAnalysis("applicant_ethnicity_name")).distinct
applicant_ethnicity_name.collect().foreach(println)

dataSet.saveAsTextFile("project/clean-data/")

