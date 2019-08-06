/*
CSCI-GA.3033-001: Summer 2019 Team Project
Cody Gilbert, Fang Han, Jeremy Lao

This modeling script does the following tasks:

1. Joins the institution data to the HMDA codes data, cleans and buckets related
actions into approved and denied to create the final feature and label dataframe
for modeling.

2. Creates modeling matrices used by the webapp utility for predicting
user input

3. Creates a Naive Bayes model pipeline, fits the model, and saves it for later
use in the webapp.

@author: Cody Gilbert
*/

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
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.storage.StorageLevel
import sys.process._

/*
The following section sets a number of fixed HDFS path variables, schemas,
and feature dataframes for later use.
*/

// setup file paths
val HMDACodePath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
val post10dataPath = "/user/fh643/InstitutionData/data/panel_10-17"
val pre10dataPath = "/user/fh643/InstitutionData/data/panel_07-09"

// Schema of the HMDA codes file
val CodeFileSchema: Array[String] = Array(
  "loan_amount_000s",
  "applicant_income_000s",
  "state_code",
  "as_of_year",
  "applicant_sex",
  "applicant_race_1",
  "applicant_ethnicity",
  "action_taken",
  "agency_code",
  "respondent_id")

// Schema of the expanded HMDA file
val HMDAFileSchema: Array[String] = Array(
  "loan_amount_000s",
  "applicant_income_000s",
  "state_code",
  "as_of_year",
  "applicant_sex",
  "applicant_race_1",
  "applicant_ethnicity",
  "action_taken",
  "agency_code",
  "respondent_id")

// Set actions buckets by overall value
val actionBuckets = Seq(
(3, "Application denied by financial institution", 0),
(1, "Loan originated", 1),
(2, "Application approved but not accepted", 1),
(8, "Preapproval request approved but not accepted", 1),
(7, "Preapproval request denied by financial institution", 0),
(6, "Loan purchased by the institution", 1)
).
toDF("action_taken", "action_taken_name", "label")

// Set Action codes by state
val stateCodes = Seq(
("AL","01"),
("AK","02"),
("AZ","04"),
("AR","05"),
("CA","06"),
("CO","08"),
("CT","09"),
("DE","10"),
("FL","12"),
("GA","13"),
("HI","15"),
("ID","16"),
("IL","17"),
("IN","18"),
("IA","19"),
("KS","20"),
("KY","21"),
("LA","22"),
("ME","23"),
("MD","24"),
("MD","25"),
("MI","26"),
("MN","27"),
("MS","28"),
("MO","29"),
("MT","30"),
("NE","31"),
("NV","32"),
("NH","33"),
("NJ","34"),
("NM","35"),
("NY","36"),
("NC","37"),
("ND","38"),
("OH","39"),
("OK","40"),
("OR","41"),
("PA","42"),
("RI","44"),
("SC","45"),
("SD","46"),
("TN","47"),
("TX","48"),
("UT","49"),
("VT","50"),
("VA","51"),
("WA","53"),
("WI","55"),
("WY","56"),
("AS","60"),
("GU","66"),
("MP","69"),
("PR","72"),
("VI","78")).
toDF("state_abbr", "state_code")

// Ethnicity Codes
val totalEths = Seq(
("1", "Hispanic or Latino"),
("2", "Not Hispanic or Latino")).
toDF("applicant_ethnicity", "applicant_ethnicity_name")

// Gender Codes
val totalSexes = Seq(
("1", "Male"),
("2", "Female")).
toDF("applicant_sex", "applicant_sex_name")

// Race Codes 
val totalRaces = Seq(
("1", "American Indian or Alaska Native"),
("2", "Asian"),
("3", "Black or African American"),
("4", "Native Hawaiian or Other Pacific Islander"),
("5", "White")).
toDF("applicant_race_1", "applicant_race_1_name")

/*
This section reads in the instituion data and unites it into a single dataframe
*/

// Pull in the instituion data in two parts; pre-2010 and post-2010
val post10data = spark.read.format("csv").
option("header", "true").
option("inferSchema", "true").
load(post10dataPath).
select("Respondent ID", "Agency Code", "Respondent State (Panel)", "Respondent Name (Panel)").
withColumn(
"Respondent State (Panel)",
trim(col("Respondent State (Panel)"))
)
val pre10data = spark.read.format("csv").
option("header", "true").
option("inferSchema", "true").
load(pre10dataPath).
select("Respondent Identification Number", "Agency Code", "Respondent State","Respondent Name").
withColumn(
"Respondent State",
trim(col("Respondent State"))
)
// combine all the institution data
val allInstitutions = post10data.unionAll(pre10data)

/*
This section reads in and cleans the HMDA data, joins it to the fixed state code data and instituion data, and buckets it by action via a join to the actionBuckets DF defined above 
*/

// read in the HMDA data
val hmdaCodes = spark.read.
option("header", "true").
option("inferSchema", "true").
csv(HMDACodePath).
select(HMDAFileSchema(0), HMDAFileSchema.slice(1,HMDAFileSchema.length):_*)


// filter out null values and create correct types
val hmdaFiltered = hmdaCodes.na.drop.
withColumn("loan_amount_000s", 'loan_amount_000s.cast("Double")).
withColumn("applicant_income_000s", 'applicant_income_000s.cast("Double"))

// join the HMDA data state codes
val hmdaStates = hmdaFiltered.join(stateCodes,
Seq("state_code"),
"inner")

// now join with the institution info
val hmdaInstitutions = hmdaStates.join(allInstitutions,
hmdaStates("respondent_id") <=> allInstitutions("Respondent ID") &&
hmdaStates("agency_code") <=> allInstitutions("Agency Code") &&
hmdaStates("state_abbr") <=> allInstitutions("Respondent State (Panel)"),
"inner").drop(
"Respondent ID",
"Agency Code",
"Respondent State (Panel)").
na.drop

// Bucket similar actions 
val hmdaInstitutionsBucketed = hmdaInstitutions.join(actionBuckets,
Seq("action_taken"),
"inner").persist(StorageLevel.MEMORY_AND_DISK_SER)

/*
This section reads in the lenders from the HMDA data, and takes the top 20 records.
It then creates the skeleton of the webapp modeling framework by creating the cartesian
product of the top 20 lenders and the number of year present in the data
*/

// Create a list of lenders by record counts
val lenders = hmdaInstitutionsBucketed.groupBy(
"respondent_id",
"agency_code",
"Respondent Name (Panel)").count

// get the top lenders for modeling
val topLenders = lenders.
orderBy(desc("count")).
limit(20).persist(StorageLevel.MEMORY_AND_DISK_SER)

// get the distinct years in the dataset
val yearsIncluded = hmdaInstitutionsBucketed.select("as_of_year").distinct

// Create the modeling matrix used in the web app
val modelingMatrix = topLenders.crossJoin(yearsIncluded).
drop("count")

// Save the modeling matrix for webapp use
modelingMatrix.
write.mode("overwrite").
format("csv").
save("sparkproject/modelingMatrix")


/*
This section creates the final Naive Bayes modeling pipeline
used for applicant approval prediction. The data is first split into 
training and test sets (80% and 20% respectively) using random sampling.
The features are translated into numerical indices, one-hot-encoded into
categorical vectors, and assembled into a single column of feature vectors.

After the pipeline is created, the final training model is fit for assessment and
the final use model is trained for use in the webapp.
*/

// split up the training and testing data
val randomSplit = hmdaInstitutionsBucketed.randomSplit(Array(0.8, 0.2), 42)
val trainingSet = randomSplit(0)
val testSet = randomSplit(1)

// convert strings to classifiers
val indexer1 = new StringIndexer().
setInputCol("state_code").
setOutputCol("state_code_index")
val encoder1 = new OneHotEncoder().
setInputCol("state_code_index").
setOutputCol("state_code_vec")

val indexer2 = new StringIndexer().
setInputCol("as_of_year").
setOutputCol("as_of_year_index")
val encoder2 = new OneHotEncoder().
setInputCol("as_of_year_index").
setOutputCol("as_of_year_vec")

val indexer3 = new StringIndexer().
setInputCol("applicant_sex").
setOutputCol("applicant_sex_index")
val encoder3 = new OneHotEncoder().
setInputCol("applicant_sex_index").
setOutputCol("applicant_sex_vec")

val indexer4 = new StringIndexer().
setInputCol("applicant_race_1").
setOutputCol("applicant_race_1_index")
val encoder4 = new OneHotEncoder().
setInputCol("applicant_race_1_index").
setOutputCol("applicant_race_1_vec")

val indexer5 = new StringIndexer().
setInputCol("applicant_ethnicity").
setOutputCol("applicant_ethnicity_index")
val encoder5 = new OneHotEncoder().
setInputCol("applicant_ethnicity_index").
setOutputCol("applicant_ethnicity_vec")

val indexer6 = new StringIndexer().
setInputCol("agency_code").
setOutputCol("agency_code_index")
val encoder6 = new OneHotEncoder().
setInputCol("agency_code_index").
setOutputCol("agency_code_vec")

val indexer7 = new StringIndexer().
setInputCol("respondent_id").
setOutputCol("respondent_id_index")
val encoder7 = new OneHotEncoder().
setInputCol("respondent_id_index").
setOutputCol("respondent_id_vec")

// Assemble the features into a features vector
val assembler = new VectorAssembler().
setInputCols(Array(
"loan_amount_000s",
"applicant_income_000s",
"state_code_vec",
"as_of_year_vec",
"applicant_sex_vec",
"applicant_race_1_vec",
"applicant_ethnicity_vec",
"agency_code_vec",
"respondent_id_vec")).
setOutputCol("features")

// Set the Naive Bayes Model
val nb = new NaiveBayes()

// Setting up a modeling pipeline
val pipeline = new Pipeline().
setStages(Array(
indexer1,
indexer2,
indexer3,
indexer4,
indexer5,
indexer6,
indexer7,
encoder1,
encoder2,
encoder3,
encoder4,
encoder5,
encoder6,
encoder7,
assembler,
nb))

// call the model for fitting the sample model and save
val nbModel = pipeline.fit(trainingSet)
nbModel.write.overwrite.save("testModel")

// finally call the model for fitting the sample model and save
val nbFinalModel = pipeline.fit(hmdaInstitutionsBucketed)
nbFinalModel.write.overwrite.save("lenderModel")


