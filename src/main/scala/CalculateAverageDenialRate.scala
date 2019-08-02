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


object CalculateAverageDenialRate {


def calculateAverage(data : RDD[String], sc : SparkContext, hdfsPath : String, outputPath : String) = {


                        
                         
//Application Outcome, State, Race, Ethnicity, Occurrences 
val keyAmt = data.map(x => x.split(",")).
             filter( x => x(9).toString != "6" && 
                          x(9).toString != "5" &&
                          x(9).toString != "4" &&
                          x(9).toString != "7" &&
                          x(9).toString != "8").
             filter(x =>  x(16).toString == "1" ||
                          x(16).toString == "2" ||
                          x(16).toString == "3" ||
                          x(16).toString == "4" ||
                          x(16).toString == "5").
             filter(x =>  x(14).toString == "1" ||
                          x(14).toString == "2"). 
             map(c => (c(0).toString + "," + c(9).toString.replace("2","1") +","+ c(11) +","+ c(16) +"," + c(14),1)).  
             reduceByKey((x,y) => x+y)
             
val mrAmt = keyAmt.map(x => x._1.stripPrefix("\"").stripSuffix("\"") + "," + x._2).
                       map(x => x.split(",")).
                       map(x => 
                         x(0).toString.stripPrefix("\"").stripSuffix("\"") + "," + 
                         x(1).toString.stripPrefix("\"").stripSuffix("\"").
                           replace("1","Approved"). 
                           replace("2","Approved").
                           replace("3","Denied").
                           replace("4","Application withdrawn by applicant").
                           replace("5","File closed for incompleteness").
                           replace("6","Loan purchased by the institution").
                           replace("7","Preapproval request denied by financial institution").
                           replace("8","Preapproval request approved but not accepted")+","+
                         x(2).toString.stripPrefix("\"").stripSuffix("\"").
                           replace("01","AL").replace("02","AK").replace("04","AZ").replace("05","AR").
                           replace("06","CA").replace("08","CO").replace("09","CT").replace("10","DE").
                           replace("11","DC").replace("12","FL").replace("13","GA").replace("15","HI").
                           replace("16","ID").replace("17","IL").replace("18","IN").replace("19","IA").
                           replace("20","KS").replace("21","KY").replace("22","LA").replace("23","ME").
                           replace("24","MD").replace("25","MA").replace("26","MI").replace("27","MN").
                           replace("28","MS").replace("29","MO").replace("30","MT").replace("31","NE").
                           replace("32","NV").replace("33","NH").replace("NJ","34").replace("35","NM").
                           replace("36","NY").replace("37","NC").replace("38","ND").replace("39","OH").
                           replace("40","OK").replace("41","OR").replace("42","PA").replace("44","RI").
                           replace("45","SC").replace("46","SD").replace("47","TN").replace("48","TX").
                           replace("49","UT").replace("50","VT").replace("51","VA").replace("53","WA").
                           replace("55","WI").replace("54","WV").replace("56","WY").replace("60","AS").
                           replace("66","GU").replace("69","MP").replace("72","PR").replace("78","VI") +","+
                         x(3).toString.stripPrefix("\"").stripSuffix("\"").
                           replace("1","American Indian or Alaska Native").
                           replace("2","Asian").
                           replace("3","Black or African American").
                           replace("4","Native Hawaiian or Other Pacific Islander").
                           replace("5","White").
                           replace("6","Information not provided by applicant in mail Internet or telephone application").
                           replace("7","Not applicable").
                           replace("8","No co-applicant")+","+
                         x(4).toString.stripPrefix("\"").stripSuffix("\"").
                           replace("1","Hispanic or Latino").
                           replace("2","Not Hispanic or Latino").
                           replace("3","Information not provided by applicant in mail Internet or telephone application").
                           replace("4","Not applicable").
                           replace("5","No co-applicant")+","+
                         x(5).toString.mkString(""))
     
//val myDF = mrAmt.toDF("year","outcome","state","race","ethnicity","count")

//val header: RDD[String]= sc.parallelize(List("year,outcome,state,race,ethnicity,count"))

//val finalRDD = header.union(mrAmt)




val sqlContext= SQLContext.getOrCreate(mrAmt.sparkContext)
import sqlContext._
import sqlContext.implicits._

val header = "year"+","+"outcome"+","+"state"+","+"race"+","+"ethnicity"+","+"count";

val df = mrAmt.
          map(row => row.split(",")).
          map{ case Array(year, outcome, state, race, ethnicity, count) => (year, outcome, state, race, ethnicity, count.toInt)}.
          toDF(header.split(","):_*)                     
                    
//val df2 = df.groupBy("year","state","race","ethnicity").agg(sum("count").alias("sum")).withColumn("fraction", col("sum") /  sum("sum").over())


val df_rich = df.
                groupBy("year","state","race","ethnicity").
                agg(sum(when($"outcome"==="Approved",$"count")).
                    as("approved"),sum(when($"outcome"==="Denied",$"count")).
                    as("Denied"),sum($"count").
                    as("total")).withColumn("denRate",col("Denied").
                                            divide(col("total")))

val df_high = df.
               groupBy("race","ethnicity").
               agg(sum(when($"outcome"==="Approved",$"count")).
                   as("approved"),sum(when($"outcome"==="Denied",$"count")).
                   as("Denied"),sum($"count").
                   as("total")).withColumn("denRate",col("Denied").
                                           divide(col("total")))

val overall = df.
               groupBy("year","state").
               agg(sum(when($"outcome"==="Approved",$"count")).
                   as("approved"),sum(when($"outcome"==="Denied",$"count")).
                   as("Denied"),sum($"count").
                   as("total")).withColumn("denRate",col("Denied").
                                           divide(col("total")))

df_rich.repartition(1).write.mode("overwrite").format("csv").save(outputPath+"/low_level")

df_high.repartition(1).write.mode("overwrite").format("csv").save(outputPath+"/high_level")

overall.repartition(1).write.mode("overwrite").format("csv").save(outputPath+"/denial_overall")

}

  def main(args: Array[String]) {    
  
    val conf = new SparkConf().setAppName("CalculateAverageDenialRate")
    val sc = new SparkContext(conf)
    
    val path_hmda_codes = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
    val rawData = sc.textFile(path_hmda_codes)
    val firstLine = rawData.first() 
    val data = rawData.filter(row => row != firstLine)
    
    
    val outputPathAnalysis_hmda_codes = "/user/jjl359/project/denial-rate-analysis"
    
    calculateAverage(data,sc,path_hmda_codes,outputPathAnalysis_hmda_codes)

    sc.stop()

  }
}

