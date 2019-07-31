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
import scala.util.{Try, Success, Failure}


object DataPrep {


  def main(args: Array[String]) {
  
    //val spark = SparkSession.builder().appName("DataProfiling").getOrCreate()
    

    val conf = new SparkConf().setAppName("DataProfiling")
    val sc = new SparkContext(conf)

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
                              x(7)  != null).
                 filter(x =>  !x(28).contains("NA") &&
                              x(28)  != null).
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
                         replace("33","NH").replace("NJ","34").replace("35","NM").replace("36","NY").
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
                         replace("8","Preapproval request approved but not accepted") 
                     ).persist



    val header = "respondent_id" +","+" loan_amount_000s" +","+ "applicant_income_000s"+","+"state_vec"+","+"as_of_year"+","+"applicant_sex_vec"+","+"applicant_race_vec"+","+"applicant_ethnicity_vec"+","+"action";
    val sqlContext= SQLContext.getOrCreate(mrAmt.sparkContext)
    import sqlContext._
    import sqlContext.implicits._
    
    val df = pass_two.map(row => row.split(",")).map{ case Array(respondent_id, loan_amount_000s, applicant_income_000s, state_vec, as_of_year, applicant_sex_vec,applicant_race_vec,applicant_ethnicity_vec,action) => (respondent_id, loan_amount_000s.toDouble, applicant_income_000s.toDouble, state_vec, as_of_year, applicant_sex_vec,applicant_race_vec,applicant_ethnicity_vec,action)}.toDF(header.split(","):_*)                     
    
    df.write.mode("overwrite").format("csv").save(/user/jjl359/df_for_logistic_regression")

}}
