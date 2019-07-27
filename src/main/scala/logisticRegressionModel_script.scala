import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import spark.sqlContext.implicits._
import spark.sqlContext._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature._


val rdd = sc.textFile("project/df_for_logistic_regression/part*")
val data = rdd.
             map(line => line.split(',')).
             map(columns => columns(4).toInt + "," +
                            columns(1).toDouble + "," +
                            columns(2).toDouble + "," +
                            columns(6).toString + "-" + columns(7).toString +","+
                            columns(5).toString +","+
                            columns(8).replace("Approved","1").replace("Denied","0").toInt)

val header = "year" +","+"loan_amount_000s" +","+ "applicant_income_000s"+","+"race"+","+"gender"  +","+"action"

val dataDF = data.map(row => row.split(",")).
                  map{ case Array(year,loan_amount_000s, applicant_income_000s,race, gender, action) => (year.toInt,loan_amount_000s.toDouble, applicant_income_000s.toDouble, race.toString,gender, action.toInt)}.
                  toDF(header.split(","):_*).
                  where(($"loan_amount_000s" < 500)).
                  where(($"applicant_income_000s" < 100)).
                  where(($"loan_amount_000s" > 50)).
                  where(($"applicant_income_000s" > 25)).
                  where(($"year" > 2015)).
                  persist


                  
val indexer = new StringIndexer().setInputCol("race").setOutputCol("raceIndex")
val test = indexer.fit(dataDF)
val encoded = test.transform(dataDF)

val indexer_2 = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex")
val test_2 = indexer_2.fit(encoded)
val encoded_2 = test_2.transform(encoded)


val encoder = new OneHotEncoderEstimator().setInputCols(Array("raceIndex","genderIndex")).setOutputCols(Array("race_vec","gender_vec"))

val test_3 = encoder.fit(encoded_2)

val encoded_3 = test_3.transform(encoded_2)

val featureCols = Array("loan_amount_000s", "applicant_income_000s", "race_vec","gender_vec")

//set the input and output column names**
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")



//return a dataframe with all of the  feature columns in  a vector column**
val df = assembler.transform(encoded_3).persist

// the transform method produced a new column: features.**
df.show


val labelIndexer = new StringIndexer().setInputCol("action").setOutputCol("label")
val df2 = labelIndexer.fit(df).transform(df)


val splitSeed = 5043
val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))




// create the classifier,  set parameters for training**
val lr = new LogisticRegression().setMaxIter(100000).setElasticNetParam(1)
//  use logistic regression to train (fit) the model with the training data**
val model = lr.fit(trainingData)    

// Print the coefficients and intercept for logistic regression**
println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")


// run the  model on test features to get predictions**
val predictions = model.transform(testData)
//As you can see, the previous model transform produced a new columns: rawPrediction, probablity and prediction.**
predictions.show


val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
// Evaluates predictions and returns a scalar metric areaUnderROC(larger is better).**
val accuracy = evaluator.evaluate(predictions)
