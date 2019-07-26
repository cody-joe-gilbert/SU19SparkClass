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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}



val filePath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
val smallFilePath = "/user/jjl359/project/data/HMDA_codes_5000_lines.csv"
val rawData = sc.textFile(filePath)
val firstLine = rawData.first() 
val data = rawData.filter(row => row != firstLine)


                
                         
//Application Outcome, State, Race, Ethnicity, Occurrences 
val pass_one = data.map(x => x.split(",")).
             filter( x => x(9).toString == "1" || 
                          x(9).toString == "2" ||
                          x(9).toString == "3").
             filter(x =>  x(16).toString == "1" ||
                          x(16).toString == "2" ||
                          x(16).toString == "3" ||
                          x(16).toString == "4" ||
                          x(16).toString == "5").
             filter(x =>  x(14).toString == "1" ||
                          x(14).toString == "2").
             filter(x =>  x(26).toString == "1" ||
                          x(26).toString == "2").
             filter(x =>  !x(7).contains("NA") &&
                          x(7).replaceAll("\\s", "")  != "").
             filter(x =>  !x(28).contains("NA") &&
                          x(28).replaceAll("\\s", "")  != "").
             map(c => c(1).toString +","+                 //respondent_id
                      c(7).toDouble +","+    //loan_amount_000s
                      c(28).toDouble +","+   //applicant_income_000s
                      c(11).toString +","+   //state code
                      c(0).toString + "," +  //year
                      c(26).toString +"," +  //applicant sex
                      c(16).toString +"," +  //applicant race
                      c(14).toString +","+   //applicant ethnicity
                      c(9).toString.replace("2","1")  //action 
                      ).persist
     
val pass_two = pass_one.
               map(x => x.split(",")).
                 map(x => 
                   x(0).toString + "," + 
                   scala.util.Try(x(1).toDouble).getOrElse(-1.0) + "," + 
                   scala.util.Try(x(2).toDouble).getOrElse(-1.0) + "," +
                   x(3).toString.
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
                     replace("50","VT").replace("51","VA").replace("WA","53").replace("55","WI").
                     replace("56","WY").replace("60","AS").replace("66","GU").replace("69","MP").
                     replace("72","PR").replace("78","VI") +","+
                   x(4) +","+
                   x(5).toString.
                     replace("1","Male").
                     replace("2","Female") +","+
                   x(6).toString.
                     replace("1","American Indian or Alaska Native").
                     replace("2","Asian").
                     replace("3","Black or African American").
                     replace("4","Native Hawaiian or Other Pacific Islander").
                     replace("5","White").
                     replace("6","Information not provided by applicant in mail Internet or telephone application").
                     replace("7","Not applicable").
                     replace("8","No co-applicant")+","+
                   x(7).toString.
                     replace("1","Hispanic or Latino").
                     replace("2","Not Hispanic or Latino").
                     replace("3","Information not provided by applicant in mail Internet or telephone application").
                     replace("4","Not applicable").
                     replace("5","No co-applicant")+","+
                   x(8).toString.
                     replace("1","Approved"). 
                     replace("2","Approved").
                     replace("3","Denied").
                     replace("4","Application withdrawn by applicant").
                     replace("5","File closed for incompleteness").
                     replace("6","Loan purchased by the institution").
                     replace("7","Preapproval request denied by financial institution").
                     replace("8","Preapproval request approved but not accepted").
                   mkString("")                     
                 ).persist



val header = "respondent_id" +","+" loan_amount_000s" +","+ "applicant_income_000s"+","+"state_vec"+","+"as_of_year"+","+"applicant_sex_vec"+","+"applicant_race_vec"+","+"applicant_ethnicity_vec"+","+"action";

val df = pass_two.map(row => row.split(",")).map{ case Array(respondent_id, loan_amount_000s, applicant_income_000s, state_vec, as_of_year, applicant_sex_vec,applicant_race_vec,applicant_ethnicity_vec,action) => (respondent_id, loan_amount_000s.toDouble, applicant_income_000s.toDouble, state_vec, as_of_year, applicant_sex_vec,applicant_race_vec,applicant_ethnicity_vec,action)}.toDF(header.split(","):_*)                     
         

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


