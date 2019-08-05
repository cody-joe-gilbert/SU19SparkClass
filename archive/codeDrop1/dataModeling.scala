import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder


val path = "/user/jjl359/project/data/HMDA_2007_to_2017.csv"
val bigFileSchema: Array[String] = Array(
// "tract_to_msamd_income",
//  "rate_spread",
//  "population",
//  "minority_population",
//  "number_of_owner_occupied_units",
//  "number_of_1_to_4_family_units",
  "loan_amount_000s",
//  "hud_median_family_income",
  "applicant_income_000s",
//  "state_name",
  "state_abbr",
//  "sequence_number",
//  "respondent_id",
//  "purchaser_type_name",
//  "property_type_name",
//  "preapproval_name",
//  "owner_occupancy_name",
//  "msamd_name",
//  "loan_type_name",
//  "loan_purpose_name",
//  "lien_status_name",
//  "hoepa_status_name",
//  "edit_status_name",
//  "denial_reason_name_3",
//  "denial_reason_name_2",
//  "denial_reason_name_1",
//  "county_name",
//  "co_applicant_sex_name",
//  "co_applicant_race_name_5",
//  "co_applicant_race_name_4",
//  "co_applicant_race_name_3",
//  "co_applicant_race_name_2",
//  "co_applicant_race_name_1",
//  "co_applicant_ethnicity_name",
//  "census_tract_number",
  "as_of_year",
//  "application_date_indicator",
  "applicant_sex_name",
//  "applicant_race_name_5",
//  "applicant_race_name_4",
//  "applicant_race_name_3",
//  "applicant_race_name_2",
  "applicant_race_name_1",
  "applicant_ethnicity_name",
//  "agency_name",
//  "agency_abbr",
  "action_taken_name")
val hmda = spark.read.format("csv").
option("header", "true").
option("inferSchema", "true").
load(path).
select(bigFileSchema(0), bigFileSchema.drop(1):_*).persist()


// filter out values
val hmdaFiltered = hmda.filter(
col("applicant_sex_name").notEqual("Information not provided by applicant in mail, Internet, or telephone application") &&
col("applicant_sex_name").notEqual("Not applicable") &&
col("applicant_sex_name").isNotNull  &&
col("state_abbr").notEqual("") &&
col("state_abbr").notEqual("VI") &&
col("state_abbr").isNotNull &&
col("applicant_ethnicity_name").notEqual("Information not provided by applicant in mail, Internet, or telephone application") &&
col("applicant_ethnicity_name").notEqual("Not applicable") &&
col("applicant_ethnicity_name").isNotNull &&
col("applicant_race_name_1").notEqual("Information not provided by applicant in mail, Internet, or telephone application") &&
col("applicant_race_name_1").isNotNull &&
col("applicant_income_000s").isNotNull &&
col("loan_amount_000s").isNotNull
)



// Bucket the actions by overall value
val actionBuckets = Seq(
("Application denied by financial institution", "Denied"),
("Loan originated", "Approved"),
("Application approved but not accepted", "Approved"),
("Preapproval request approved but not accepted", "Approved"),
("Preapproval request denied by financial institution", "Denied"),
("Loan purchased by the institution", "Approved")
 )
 
val actionBucketsDF = actionBuckets.toDF("action_taken_name", "action_bucket")

// join the HMDA data with the bucket definition to get only binary classifications
val hmdaBucketed = hmdaFiltered.join(actionBucketsDF, Seq("action_taken_name", "action_taken_name"),
"inner")


// Here I take a very small sample for testing purposes
val hmdaBucketedSample = hmdaBucketed.sample(false, 0.002).persist()


// split up the training and testing data
val randomSplit = hmdaBucketedSample.randomSplit(Array(0.8, 0.2), 42)
val trainingSet = randomSplit(0)
val testSet = randomSplit(1)

// df.write.partitionBy("agency_name").saveAsTable("agencyTables")  //splits data up by group

// hmdaBucketed.groupBy("action_bucket").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_action_taken_name.csv")
// hmdaBucketed.groupBy("applicant_race_name_1").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_applicant_race_name_1.csv")
// hmdaBucketed.groupBy("applicant_ethnicity_name").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_applicant_ethnicity_name.csv")
// hmdaBucketed.groupBy("state_abbr").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_state_abbr.csv")
// hmdaBucketed.groupBy("applicant_sex_name").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_applicant_sex_name.csv")
// hmdaBucketed.groupBy("applicant_income_000s").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_applicant_income_000s.csv")
// hmdaBucketed.groupBy("loan_amount_000s").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_loan_amount_000s.csv")
// hmdaBucketed.groupBy("hud_median_family_income").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_hud_median_family_income.csv")
// hmdaBucketed.groupBy("minority_population").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_minority_population.csv")
// hmdaBucketed.groupBy("population").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_population.csv")
// hmdaBucketed.groupBy("tract_to_msamd_income").count.write.mode("overwrite").format("csv").save("/user/cjg507/sparkproject/bucket_tract_to_msamd_income.csv")

// index the buckets to a binary index
val indexer = new StringIndexer().
setInputCol("action_bucket").
setOutputCol("label")
 
// convert strings to classifiers
val indexer1 = new StringIndexer().
setInputCol("applicant_race_name_1").
setOutputCol("applicant_race_index")
val encoder1 = new OneHotEncoder().
setInputCol("applicant_race_index").
setOutputCol("applicant_race_vec")
val indexer2 = new StringIndexer().
setInputCol("applicant_ethnicity_name").
setOutputCol("applicant_ethnicity_index")
val encoder2 = new OneHotEncoder().
setInputCol("applicant_ethnicity_index").
setOutputCol("applicant_ethnicity_vec")
val indexer3 = new StringIndexer().
setInputCol("state_abbr").
setOutputCol("state_index")
val encoder3 = new OneHotEncoder().
setInputCol("state_index").
setOutputCol("state_vec")
val indexer4 = new StringIndexer().
setInputCol("applicant_sex_name").
setOutputCol("applicant_sex_index")
val encoder4 = new OneHotEncoder().
setInputCol("applicant_sex_index").
setOutputCol("applicant_sex_vec")


// Assemble the features into a features vector
val assembler = new VectorAssembler().
setInputCols(Array(
"applicant_race_vec",
"applicant_ethnicity_vec",
"state_vec",
"applicant_sex_vec",
"as_of_year",
"applicant_income_000s",
"loan_amount_000s"
)).
setOutputCol("features")


// Set the log regression
val lr = new LogisticRegression().
setMaxIter(10)

// Setting up a modeling pipeline
val pipeline = new Pipeline().
setStages(Array(
indexer,
indexer1,
indexer2,
indexer3,
indexer4,
encoder1,
encoder2,
encoder3,
encoder4,
assembler,
lr))

// Setting up tuning grid
val paramGrid = new ParamGridBuilder().
addGrid(lr.regParam, Array(0.1, 0.01)).
addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).
build()

// Setting up the cross-validation fitter
val cv = new CrossValidator().
setEstimator(pipeline).
setEvaluator(new BinaryClassificationEvaluator).
setEstimatorParamMaps(paramGrid).
setNumFolds(4)  // Use 3+ in practice


// finally call the model for fitting
val cvModel = cv.fit(trainingSet)
cvModel.save("testModel")


