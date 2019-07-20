import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf


object DataProfiler {

def mapReduceFunc(dataForAnalysis : RDD[String], colNum : Integer) : RDD[String] = {
  val firstLine = dataForAnalysis.first() 
  val data = dataForAnalysis.filter(row => row != firstLine)
  val keyAmt = data.map(_.split(",")).
               map(c => (c(colNum),1)).
               reduceByKey((x,y) => x+y)
  val mrAmt = keyAmt.
              map(x => x._1.stripPrefix("\"").stripSuffix("\"") + "," + x._2)
  mrAmt
}

def dataProfilingHMDACode(hdfsPath : String, outputPath : String) = {
    
    val conf = new SparkConf().setAppName("DataProfiling").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = new SparkContext(conf)
    
    val dataForAnalysis = sc.textFile(hdfsPath)
    
    //Maximum-Minimum for Loan amount
    println("===========================")
    println("Loan Amount")
    val header: RDD[String]= sc.parallelize(List("loan_amount,frequency"))
    val reducedLoanAmtData = mapReduceFunc(dataForAnalysis, 7).
                         map( x => x.split(",").
                         map(_.replace("NA","0").replace("","0")).
                         map(_.trim.toDouble).
                         mkString(",")).
                         repartition(1)
    header.union(reducedLoanAmtData).saveAsTextFile(outputPath+"/loan-amount-dist")
    
    //Applicant Income
    println("===========================")
    println("Applicant Income")
    val header_2: RDD[String]= sc.parallelize(List("applicant_income,frequency"))     
    val reducedIncData = mapReduceFunc(dataForAnalysis, 28).
                         map( x => x.split(",").
                         map(_.replace("NA","0").replace("","0").replace("000 0 0","0")).
                         map(_.trim.toDouble).
                         mkString(",")).
                         repartition(1)
    header_2.union(reducedIncData).saveAsTextFile(outputPath+"/app-income-dist")
    
    //Distinct Actions Taken
    println("===========================")
    println("Distinct Actions Taken")
    val header_3: RDD[String]= sc.parallelize(List("distinct_actions,frequency"))
    val reducedActionData = mapReduceFunc(dataForAnalysis, 9).
                         map( x => x.split(",").
                         map(_.replace("NA","NA").replace("","NA")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.replace("1","Loan originated"). 
                             replace("2","Application approved but not accepted").
                             replace("3","Application denied by financial institution").
                             replace("4","Application withdrawn by applicant").
                             replace("5","File closed for incompleteness").
                             replace("6","Loan purchased by the institution").
                             replace("7","Preapproval request denied by financial institution").
                             replace("8","Preapproval request approved but not accepted")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_3.union(reducedActionData).saveAsTextFile(outputPath+"/distinct-actions")
    
    println("===========================")
    println("Purchaser Type Name")
    val header_4: RDD[String]= sc.parallelize(List("purchaser_type,frequency"))
    val reducedPTypeData = mapReduceFunc(dataForAnalysis, 29).
                         map( x => x.split(",").
                         map(_.replace("NA","NA").replace("","NA")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("0","Loan was not originated or was not sold in calendar year covered by register"). 
                             replace("1","Fannie Mae (FNMA)").
                             replace("2","Ginnie Mae (GNMA)").
                             replace("3","Freddie Mac (FHLMC)").
                             replace("4","Farmer Mac (FAMC) ").
                             replace("5","Private securitization").
                             replace("6","Commercial bank savings bank or savings association").
                             replace("7","Life insurance company credit union mortgage bank or finance company").
                             replace("8","Affiliate institution").
                             replace("9","Other type of purchaser")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_4.union(reducedPTypeData).saveAsTextFile(outputPath+"/distinct-purchaser-type")

    println("===========================")
    println("Property Type Name")
    val header_5: RDD[String]= sc.parallelize(List("property_type,frequency"))
    val reducedPropType = mapReduceFunc(dataForAnalysis, 4).
                         map( x => x.split(",").
                         map(_.replace("NA","NA").replace("","NA")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("1","One to four-family (other than manufactured housing)").
                             replace("2","Manufactured housing").
                             replace("3","Multifamily")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_5.union(reducedPropType).saveAsTextFile(outputPath+"/property-type-name")

    println("===========================")
    println("Loan Type Name")
    val header_6: RDD[String]= sc.parallelize(List("loan_type,frequency"))
    val reducedLoanType = mapReduceFunc(dataForAnalysis, 5).
                         map( x => x.split(",").
                         map(_.replace("NA","NA").replace("","NA")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("1","Conventional (any loan other than FHA, VA, FSA, or RHS loans)").
                             replace("2","FHA-insured (Federal Housing Administration)").
                             replace("3","VA-guaranteed (Veterans Administration)").
                             replace("4","FSA/RHS (Farm Service Agency or Rural Housing Service)")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_6.union(reducedLoanType).saveAsTextFile(outputPath+"/loan-type-name")

    
    println("===========================")
    println("Applicant Race")
    val header_7: RDD[String]= sc.parallelize(List("applicant_race,frequency"))
    val appRace = mapReduceFunc(dataForAnalysis, 16).
                         map( x => x.split(",").
                         map(_.replace("NA","7").replace("","7")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("1","American Indian or Alaska Native").
                             replace("2","Asian").
                             replace("3","Black or African American").
                             replace("4","Native Hawaiian or Other Pacific Islander").
                             replace("5","White").
                             replace("6","Information not provided by applicant in mail Internet or telephone application").
                             replace("7","Not applicable").
                             replace("8","No co-applicant")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_7.union(appRace).saveAsTextFile(outputPath+"/applicant_race_name")

    println("===========================")
    println("Applicant Ethnicity")
    val header_8: RDD[String]= sc.parallelize(List("applicant_ethnicity,frequency"))
    val appEth = mapReduceFunc(dataForAnalysis, 14).
                         map( x => x.split(",").
                         map(_.replace("NA","4")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("1","Hispanic or Latino").
                             replace("2","Not Hispanic or Latino").
                             replace("3","Information not provided by applicant in mail Internet or telephone application").
                             replace("4","Not applicable").
                             replace("5","No co-applicant")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_8.union(appEth).saveAsTextFile(outputPath+"/applicant_ethnicity")

    println("===========================")
    println("Applicant Gender")
    val header_9: RDD[String]= sc.parallelize(List("applicant_gender,frequency"))
    val appGen = mapReduceFunc(dataForAnalysis, 26).
                         map( x => x.split(",").
                         map(_.replace("NA","0")).
                         mkString(",")).
                         repartition(1).
                         map(x => x.split(",")).map(x => x(0).toString.
                             replace("1","Male").
                             replace("2","Female").
                             replace("3","Information not provided by applicant in mail Internet or telephone application").
                             replace("4","Not applicable").
                             replace("5","No co-applicant")+","+x(1).toString.mkString("")).
                             repartition(1)
                             
    header_9.union(appGen).saveAsTextFile(outputPath+"/applicant_gender")

    println("===========================")
    println("Lender")
    val header_10: RDD[String]= sc.parallelize(List("lender,frequency"))
    val lender = mapReduceFunc(dataForAnalysis, 1).
                         map( x => x.split(",").
                         mkString(",")).
                         repartition(1)
                         
    header_10.union(lender).saveAsTextFile(outputPath+"/lender")
    
    
    println("===========================")
    println("County")
    val header_11: RDD[String]= sc.parallelize(List("county,frequency"))
    val county = mapReduceFunc(dataForAnalysis, 12).
                         map( x => x.split(",").
                         map(_.replace("NA","0").replace("","0")).
                         map(_.trim.toInt).
                         mkString(",")).
                         repartition(1)
                         
    header_11.union(county).saveAsTextFile(outputPath+"/county")
    
    
    println("===========================")
    println("State")
    val header_12: RDD[String]= sc.parallelize(List("state,frequency"))
    val state = mapReduceFunc(dataForAnalysis, 11).
                        // map( x => x.split(",").
                        // map(_.replace("NA","0").replace("","0")).
                        // map(_.trim.toInt).
                        // mkString(",")).
                         repartition(1)
                         
    header_12.union(state).saveAsTextFile(outputPath+"/state")

    println("===========================")
    println("Year")
    val header_13: RDD[String]= sc.parallelize(List("year,frequency"))
    val year = mapReduceFunc(dataForAnalysis, 0).
                        map( x => x.split(",").
                        map(_.replace("NA","0").replace("","0")).
                        map(_.trim.toInt).
                        mkString(",")).
                        repartition(1)
                         
    header_13.union(year).saveAsTextFile(outputPath+"/year")

}



def dataProfiling(spark : SparkSession, hdfsPath : String, outputPath : String) = {
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

                         
    //Maximum-Minimum for Loan amount
    println("===========================")
    println("Loan Amount - Maximum,Minimum")
    val mrLoanAmount = dataForAnalysis.groupBy("loan_amount_000s").count()
    mrLoanAmount.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/loan-amount-dist")

    println("===========================")
    println("Applicant Income - Maximum,Minimum")
    val mrAppIncome = dataForAnalysis.groupBy("applicant_income_000s").count()
    mrAppIncome.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/app-income-dist")

    println("===========================")
    println("Distinct Actions Taken")
    val mrDistinctActionsTaken = dataForAnalysis.groupBy("action_taken_name").count()
    mrDistinctActionsTaken.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/distinct-actions")


    println("===========================")
    println("Purchaser Type Name")
    val mrDistinctPurchaserType = dataForAnalysis.groupBy("purchaser_type_name").count()
    mrDistinctPurchaserType.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/distinct-purchaser-type")


    println("===========================")
    println("Property Type Name")
    val property_type_name = dataForAnalysis.groupBy("property_type_name").count()
    property_type_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/property-type-name")


    println("===========================")
    println("Loan Type Name")
    val loan_type_name = dataForAnalysis.groupBy("loan_type_name").count()
    loan_type_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/loan-type-name")


    println("===========================")
    println("Loan Purpose Name")
    val loan_purpose_name = dataForAnalysis.groupBy("loan_purpose_name").count()
    loan_purpose_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/loan_purpose_name")


    println("===========================")
    println("Applicant Race")
    val applicant_race_name_1 = dataForAnalysis.groupBy("applicant_race_name_1").count()
    applicant_race_name_1.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/applicant_race_name_1")


    println("===========================")
    println("Applicant Ethnicity")
    val applicant_ethnicity_name = dataForAnalysis.groupBy("applicant_ethnicity_name").count()
    applicant_ethnicity_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/applicant_ethnicity_name")

    println("===========================")
    println("Applicant Gender")
    val applicant_sex_name = dataForAnalysis.groupBy("applicant_sex_name").count()
    applicant_sex_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/applicant_sex_name")


    println("===========================")
    println("Applicant Lender")
    val respondent_id = dataForAnalysis.groupBy("as_of_year","respondent_id").count()
    respondent_id.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/respondent_id")

    println("===========================")
    println("County")
    val county_name = dataForAnalysis.groupBy("as_of_year","state_abbr","county_name").count()
    county_name.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/state_county_occurrence")

}


def dataFilteringRDD(hdfsPath : String, outputPath : String) = {

    val conf = new SparkConf().setAppName("DataProfiling").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = new SparkContext(conf)
    
    val dataForAnalysis = sc.textFile(hdfsPath)
    
    val firstLine = dataForAnalysis.first() 
    val data = dataForAnalysis.filter(row => row != firstLine)
    val keyAmt = data.map(_.split(",")).map(c => (c(9).toString +","+c(28).toString,1)).
               reduceByKey((x,y) => x+y)
    val mrAmt = keyAmt.map(x => x._1.stripPrefix("\"").stripSuffix("\"") + "," + x._2).
                       map(x => x.split(",")).map(x => x(0).toString.stripPrefix("\"").stripSuffix("\"").replace("1","Loan originated"). 
                         replace("2","Application approved but not accepted").
                         replace("3","Application denied by financial institution").
                         replace("4","Application withdrawn by applicant").
                         replace("5","File closed for incompleteness").
                         replace("6","Loan purchased by the institution").
                         replace("7","Preapproval request denied by financial institution").
                         replace("8","Preapproval request approved but not accepted")+","+x(1).toString.stripPrefix("\"").stripSuffix("\"")+","+x(2).toString.mkString(""))
     
    mrAmt.repartition(1).saveAsTextFile(outputPath+"/action-taken")
    sc.stop()
}


def dataFiltering(spark : SparkSession, hdfsPath : String, outputPath : String) = {

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
    
    //Filtering the data frame for analysis
      
      val filteredDataForAnalysis = dataForAnalysis.
                          filter(col("applicant_ethnicity_name").like("%Hispanic_or_Latino")).
                          filter(col("applicant_race_name_1").equalTo("White") || col("applicant_race_name_1").equalTo("Asian")  || col("applicant_race_name_1").equalTo("Black_or_African American")  || col("applicant_race_name_1").equalTo("Native_Hawaiian_or_Other_Pacific_Islander") || col("applicant_race_name_1").equalTo("American_Indian_or_Alaska_Native")).
                          filter(col("action_taken_name").equalTo("Application denied by financial institution") || col("action_taken_name").equalTo("Loan originated") || col("action_taken_name").equalTo("Application approved but not accepted"))
    
    filteredDataForAnalysis.coalesce(1).write.mode("overwrite").format("csv").save(outputPath+"/filtered-data")

}

  def main(args: Array[String]) {
  
    //val spark = SparkSession.builder().appName("DataProfiling").getOrCreate()
    

    
    val path_hmda_codes = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
    val smallFilePath_hmda_codes = "/user/jjl359/project/data/HMDA_codes_5000_lines.csv"
    val outputPath_hmda_codes = "/user/jjl359/project/profiling-hmda-codes-v2"
    val outputPathAnalysis_hmda_codes = "/user/jjl359/project/profiling-hmda-codes-analysis-v2"
    
    val path = "/user/jjl359/project/data/HMDA_2007_to_2017.csv"
    val smallFilePath = "/user/jjl359/project/data/top_1000.csv"
    //val outputPath = args(0)
    
    
    dataProfilingHMDACode(path_hmda_codes,outputPath_hmda_codes)
    //dataFilteringRDD(path_hmda_codes,outputPathAnalysis_hmda_codes)
    //dataProfiling(spark,path,outputPath)
    //dataFiltering(spark,path,outputPath)
  }
}
