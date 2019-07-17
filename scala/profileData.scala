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


val spark = SparkSession.builder().master("yarn").appName("DataProfiling").getOrCreate()
val path = "/user/jjl359/project/data/HMDA_2007_to_2017.csv"  // CJG: Added full path so everyone can access
val smallFilePath = "/user/jjl359/project/data/top_1000.csv" // CJG: Added full path so everyone can access
val outputFolder = "/user/cjg507/sparkproject/"
val smallFileSchema: Array[String] = Array("loan_amount_000s",
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
val bigFileSchema: Array[String] = Array("tract_to_msamd_income",
  "rate_spread",
  "population",
  "minority_population",
  "number_of_owner_occupied_units",
  "number_of_1_to_4_family_units",
  "loan_amount_000s",
  "hud_median_family_income",
  "applicant_income_000s",
  "state_name",
  "state_abbr",
  "sequence_number",
  "respondent_id",
  "purchaser_type_name",
  "property_type_name",
  "preapproval_name",
  "owner_occupancy_name",
  "msamd_name",
  "loan_type_name",
  "loan_purpose_name",
  "lien_status_name",
  "hoepa_status_name",
  "edit_status_name",
  "denial_reason_name_3",
  "denial_reason_name_2",
  "denial_reason_name_1",
  "county_name",
  "co_applicant_sex_name",
  "co_applicant_race_name_5",
  "co_applicant_race_name_4",
  "co_applicant_race_name_3",
  "co_applicant_race_name_2",
  "co_applicant_race_name_1",
  "co_applicant_ethnicity_name",
  "census_tract_number",
  "as_of_year",
  "application_date_indicator",
  "applicant_sex_name",
  "applicant_race_name_5",
  "applicant_race_name_4",
  "applicant_race_name_3",
  "applicant_race_name_2",
  "applicant_race_name_1",
  "applicant_ethnicity_name",
  "agency_name",
  "agency_abbr",
  "action_taken_name")


def dataProfiling(spark : SparkSession,
				  hdfsPath : String,
				  verbose: Boolean = true): Unit = {
  val smallDataFlag: Boolean = hdfsPath.split('/').last == "top_1000.csv"
  val dataForAnalysis = if(smallDataFlag) {
	// read in the following if the smaller dataset is used
	spark.read.format("csv").
	  option("header", "true").
	  option("inferSchema", "true").
	  load(hdfsPath).
	  select(smallFileSchema(0), smallFileSchema.drop(1):_*)
  } else {
	// read in the following if the larger dataset is used
	spark.read.format("csv").
	  option("header", "true").
	  option("inferSchema", "true").
	  load(hdfsPath).
	  select(bigFileSchema(0), bigFileSchema.drop(1):_*)
  }
  val schema: Array[String] =
	if(smallDataFlag) {
	  smallFileSchema
	} else {
	  bigFileSchema
	}


  def runMinMax(title: String, group: String*): Unit = {
	val index = schema.indexOf(group(0))
	if(verbose) {
		println("Calculating " + title)
		dataForAnalysis.agg(max(dataForAnalysis(dataForAnalysis.columns(index))),
							min(dataForAnalysis(dataForAnalysis.columns(index)))).show
	}
	
	val minmaxCount = dataForAnalysis.groupBy().count()
	minmaxCount.coalesce(1).write.mode("overwrite").format("csv").save(outputFolder + title)
  }
  def runDistinct(title: String, group: String*): Unit = {
	if(verbose) {
		println("Calculating " + title)
	}
	  val mrDistincts = if(group.length > 1) {
							dataForAnalysis.groupBy(group(0), group.drop(1):_*).count()
						} else {
						val inp = group(0)
							dataForAnalysis.groupBy(inp).count()
						}
	mrDistincts.coalesce(1).write.mode("overwrite").format("csv").save(outputFolder + title)
  }
  
  // The following rows will execute regardless of the large or small dataset
  runMinMax(title="LoanAmount-MinMax", group=Array("loan_amount_000s"):_*)
  runMinMax(title="ApplicantIncome-MinMax", group=Array("applicant_income_000s"):_*)
  runDistinct(title="DistinctActionsTaken", group=Array("action_taken_name"):_*)
  runDistinct(title="DistinctPurchaserTypeName", group=Array("purchaser_type_name"):_*)
  runDistinct(title="DistinctPropertyTypeName", group=Array("property_type_name"):_*)
  runDistinct(title="DistinctLoanTypeName", group=Array("loan_type_name"):_*)
  runDistinct(title="DistinctLoanPurposeName", group=Array("loan_purpose_name"):_*)
  runDistinct(title="DistinctApplicantRace", group=Array("applicant_race_name_1"):_*)
  runDistinct(title="DistinctApplicantEthnicity", group=Array("applicant_ethnicity_name"):_*)
  runDistinct(title="DistinctApplicantGender", group=Array("applicant_sex_name"):_*)
  runDistinct(title="DistinctApplicantLender", group=Array("respondent_id"):_*)
  runDistinct(title="DistinctCountyName", group=Array("as_of_year","state_abbr","county_name"):_*)

}

dataProfiling(spark,path)
