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
import org.apache.spark.sql.SparkSession


def dataProfiling(spark : SparkSession, hdfsPath : String) = {
    //val spark = SparkSession.builder().appName("DataProfiling").getOrCreate()
    val dataForAnalysis = spark.read.format("csv").
                  option("header", "true").  
                  option("inferSchema", "true").
                  load(hdfsPath).
                  select("loan_amount_000s",
                         "applicant_income_000s",
                         "state_name","state_abbr",
                         "respondent_id",
                         "purchaser_type_name",
                         "property_type_name",
                         "loan_type_name",
                         "lien_status_name",
                         "loan_purpose_name",
                         "county_name",
                         "as_of_year",
                         "applicant_sex_name",
                         "applicant_race_name_1",
                         "applicant_ethnicity_name",
                         "agency_name","agency_abbr",
                         "action_taken_name")
  
    //val dataSet = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("project/data/HMDA_DATA_2007_2017.csv")
    println("===========================")
    println("Loan Amount - Maximum,Minimum")
    dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(0))), min(dataForAnalysis(dataForAnalysis.columns(0)))).show
    val mrLoanAmount = dataForAnalysis.groupBy("loan_amount_000s").count()
    mrLoanAmount.coalesce(1).write.mode("overwrite").format("csv").save("loan-amount-dist")

    //val dataForAnalysis = dataSet.select("loan_amount_000s","applicant_income_000s","state_name","state_abbr","respondent_id","purchaser_type_name","property_type_name","loan_type_name","lien_status_name","loan_purpose_name","county_name","as_of_year","applicant_sex_name","applicant_race_name_1","applicant_ethnicity_name","agency_name","agency_abbr","action_taken_name")

    //Maximum-Minimum for Loan amount
    println("===========================")
    println("Loan Amount - Maximum,Minimum")
    dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(0))), min(dataForAnalysis(dataForAnalysis.columns(0)))).show
    val mrLoanAmount = dataForAnalysis.groupBy("loan_amount_000s").count()
    mrLoanAmount.coalesce(1).write.mode("overwrite").format("csv").save("loan-amount-dist")

    println("===========================")
    println("Applicant Income - Maximum,Minimum")
    dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(1))), min(dataForAnalysis(dataForAnalysis.columns(1)))).show
    val mrAppIncome = dataForAnalysis.groupBy("applicant_income_000s").count()
    mrAppIncome.coalesce(1).write.mode("overwrite").format("csv").save("app-income-dist")

    println("===========================")
    println("Distinct Actions Taken")
    val distinctActionsTaken = dataForAnalysis.select(dataForAnalysis("action_taken_name")).distinct
    distinctActionsTaken.collect().foreach(println)
    val mrDistinctActionsTaken = dataForAnalysis.groupBy("action_taken_name").count()
    mrDistinctActionsTaken.coalesce(1).write.mode("overwrite").format("csv").save("distinct-actions")


    println("===========================")
    println("Purchaser Type Name")
    val distinctPurchaserType = dataForAnalysis.select(dataForAnalysis("purchaser_type_name")).distinct
    distinctPurchaserType.collect().foreach(println)
    val mrDistinctPurchaserType = dataForAnalysis.groupBy("purchaser_type_name").count()
    mrDistinctPurchaserType.coalesce(1).write.mode("overwrite").format("csv").save("distinct-purchaser-type")


    println("===========================")
    println("Property Type Name")
    val distinctPropertyType = dataForAnalysis.select(dataForAnalysis("property_type_name")).distinct
    distinctPropertyType.collect().foreach(println)
    val property_type_name = dataForAnalysis.groupBy("property_type_name").count()
    property_type_name.coalesce(1).write.mode("overwrite").format("csv").save("property-type-name")


    println("===========================")
    println("Loan Type Name")
    val distinctLoanType = dataForAnalysis.select(dataForAnalysis("loan_type_name")).distinct
    distinctLoanType.collect().foreach(println)
    val loan_type_name = dataForAnalysis.groupBy("loan_type_name").count()
    loan_type_name.coalesce(1).write.mode("overwrite").format("csv").save("loan-type-name")


    println("===========================")
    println("Loan Purpose Name")
    val distinctLoanPurpose = dataForAnalysis.select(dataForAnalysis("loan_purpose_name")).distinct
    distinctLoanPurpose.collect().foreach(println)
    val loan_purpose_name = dataForAnalysis.groupBy("loan_purpose_name").count()
    loan_purpose_name.coalesce(1).write.mode("overwrite").format("csv").save("loan_purpose_name")


    println("===========================")
    println("Applicant Race")
    val applicant_race_name = dataForAnalysis.select(dataForAnalysis("applicant_race_name_1")).distinct
    applicant_race_name.collect().foreach(println)
    val applicant_race_name_1 = dataForAnalysis.groupBy("applicant_race_name_1").count()
    applicant_race_name_1.coalesce(1).write.mode("overwrite").format("csv").save("applicant_race_name_1")


    println("===========================")
    println("Applicant Ethnicity")
    val applicant_ethnicity_name = dataForAnalysis.select(dataForAnalysis("applicant_ethnicity_name")).distinct
    applicant_ethnicity_name.collect().foreach(println)
    val applicant_ethnicity_name = dataForAnalysis.groupBy("applicant_ethnicity_name").count()
    applicant_ethnicity_name.coalesce(1).write.mode("overwrite").format("csv").save("applicant_ethnicity_name")

    println("===========================")
    println("Applicant Gender")
    //val applicant_sex_name = dataForAnalysis.select(dataForAnalysis("applicant_sex_name")).distinct
    //applicant_sex_name.collect().foreach(println)
    val applicant_sex_name = dataForAnalysis.groupBy("applicant_sex_name").count()
    applicant_sex_name.coalesce(1).write.mode("overwrite").format("csv").save("applicant_sex_name")


    println("===========================")
    println("Applicant Lender")
    //val applicant_sex_name = dataForAnalysis.select(dataForAnalysis("applicant_sex_name")).distinct
    //applicant_sex_name.collect().foreach(println)
    val respondent_id = dataForAnalysis.groupBy("as_of_year","respondent_id").count()
    respondent_id.coalesce(1).write.mode("overwrite").format("csv").save("respondent_id")

    println("===========================")
    println("County")
    //val applicant_sex_name = dataForAnalysis.select(dataForAnalysis("applicant_sex_name")).distinct
    //applicant_sex_name.collect().foreach(println)
    val county_name = dataForAnalysis.groupBy("as_of_year","state_abbr","county_name").count()
    county_name.coalesce(1).write.mode("overwrite").format("csv").save("state_county_occurrence")

}


val spark = SparkSession.builder().appName("DataProfiling").getOrCreate()
val path = "project/data/HMDA_2007_to_2017.csv"
val smallFilePath = "project/data/top_1000.csv"
dataProfiling(spark,smallFilePath)

//dataSet.saveAsTextFile("project/clean-data/")

