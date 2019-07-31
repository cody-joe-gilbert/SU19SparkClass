/**
* CSCI-GA.3033-001: Big Data Application Development
* Team Project Code
* Cody Gilbert, Fang Han, Jeremy Lao
* The code extracts columns from the institution data join them with HMDA data  
* @author: Fang Han
* REQUIREMENT: spark-shell 2.4.0+ 
*/

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import spark.implicits._

object JoinData {

    def main(args: Array[String]): Unit = {
        val names = Seq("RespondentID", "AgencyCode", "RespondentState", "RespondentName")
        val savePath = "/user/fh643/InstitutionData/AllYearsJoined"
        val numericalSavePath = "/user/fh643/InstitutionData/AllYearsJoined/numericalState"
        val hmdaPath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
        val hmdaJoinPath = "/user/fh643/InstitutionData/HmdaJoined"
        
        //*************** Read All CSV Files Into DataFrames ***************
        val csv17 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2017_panel.csv", true, names)
        val csv16 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2016_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))
        val csv15 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2015_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))
        val csv14 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2014_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))
        val csv13 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2013_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))
        val csv12 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2012_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))
        val csv11 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2011_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))     
        val csv10 = readCSV("/user/fh643/InstitutionData/data/panel_10-17/hmda_2010_panel.csv", true, names).
                    withColumn("RespondentState",expr("substring(RespondentState, 1, 2)"))     
        val csv09 = readCSV("/user/fh643/InstitutionData/data/panel_07-09/hmda_2009_panel.csv", false, names)
        val csv08 = readCSV("/user/fh643/InstitutionData/data/panel_07-09/hmda_2008_panel.csv", false, names)
        val csv07 = readCSV("/user/fh643/InstitutionData/data/panel_07-09/hmda_2007_panel.csv", false, names)

        //*************** Pair-wise Join All csv ***************
        val frames = Array(csv17, csv16, csv15, csv14, csv13, csv12, csv11, csv10, csv09, csv08, csv07)
        val joined = chainedJoins(frames)

        //*************** Save Joined Institution Data ***************
        joined.write.mode("overwrite").format("csv").save(savePath)

        //*************** Convert State to Numerical Value (Consistent with HMDA) and Save ***************
        val numJoined = convertFields(joined, names)
        numJoined.write.mode("overwrite").format("csv").save(numericalSavePath)

        //*************** Read In HMDA Data, Join with Instituions, Save ***************
        val hmda = spark.read.option("header", "true").option("inferSchema", "true").csv(hmdaPath)
        val hmdaJoined = hmda.join(numJoined, hmda("respondent_id") <=> numJoined("RespondentID") &&
                            hmda("agency_code") <=> numJoined("AgencyCode") &&
                            hmda("state_code") <=> numJoined("RespondentState"))
        hmdaJoined.write.mode("overwrite").format("csv").save(hmdaJoinPath)
    }

    def readCSV(path: String, after10: Boolean, names: Seq[String]): DataFrame = {
        if(after10)
            spark.read.option("header", "true").
                    option("inferSchema", "true").
                    csv(path).
                    select("Respondent ID", "Agency Code", "Respondent State (Panel)", "Respondent Name (Panel)").
                    toDF(names: _*)
        else 
            spark.read.option("header", "true").
                    option("inferSchema", "true").
                    csv(path).
                    select("Respondent Identification Number", "Agency Code", "Respondent State", "Respondent Name").
                    toDF(names: _*)
    }

    def fullOuterJoin(df1: DataFrame, df2: DataFrame): DataFrame = {
        return df1.join(df2, df2("RespondentID") <=> df1("RespondentID") &&
                    df2("AgencyCode") <=> df1("AgencyCode") &&
                    df2("RespondentState") <=> df1("RespondentState"), "full_outer")
    }

    def joinDF(df1: DataFrame, df2: DataFrame): DataFrame = {
        val names = Seq("RespondentID", "AgencyCode", "RespondentState", "RespondentName")
        val df = fullOuterJoin(df1, df2)
        // convert to rdd 
        val rdd = df.rdd
        
        def getNonNullRec(x: String): Row = {
            val l = x.substring(1, x.length - 1).split(",")
            if(l(0).contains("null"))
                Row(l(4), l(5), l(6), l(7))
            else 
                Row(l(0), l(1), l(2), l(3))
        }

        val rows = rdd.map(x => x.toString).map(getNonNullRec(_)).
                    map({case Row(v1: String, v2: String, v3: String, v4: String) => (v1, v2, v3, v4)})

        return rows.toDF(names: _*)
    }

    def chainedJoins(fs: Array[DataFrame]): DataFrame = {
        var df = joinDF(fs(0), fs(1))
        for (i <- 2 to fs.length - 1)
            df = joinDF(df, fs(i))
        return df
    }

    /**
    * Convert Respondent State in the institution file into numerical values, AgencyCode into Int
    */
    def convertFields(df: DataFrame, names: Seq[String]): DataFrame = {
        val rdd = df.rdd
        val rows = rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    map(x => Row(x(0), 
                                x(1),
                                (x(2).
                                    replace("AL","01").replace("AK","02").replace("AZ","04").replace("AR","05").
                                    replace("CA","06").replace("CO","08").replace("CT","09").replace("DE","10").
                                    replace("FL","12").replace("GA","13").replace("HI","15").replace("ID","16").
                                    replace("IL","17").replace("IN","18").replace("IA","19").replace("KS","20").
                                    replace("KY","21").replace("LA","22").replace("ME","23").replace("MD","24").
                                    replace("MD","25").replace("MI","26").replace("MN","27").replace("MS","28").
                                    replace("MO","29").replace("MT","30").replace("NE","31").replace("NV","32").
                                    replace("NH","33").replace("NJ","34").replace("NM","35").replace("NY","36").
                                    replace("NC","37").replace("ND","38").replace("OH","39").replace("OK","40").
                                    replace("OR","41").replace("PA","42").replace("RI","44").replace("SC","45").
                                    replace("SD","46").replace("TN","47").replace("TX","48").replace("UT","49").
                                    replace("VT","50").replace("VA","51").replace("WA","53").replace("WI","55").
                                    replace("WY","56").replace("AS","60").replace("GU","66").replace("MP","69").
                                    replace("PR","72").replace("VI","78")),
                                x(3))).
                    map({case Row(v1: String, v2: String, v3: String, v4: String) => (v1, v2, v3, v4)})

        return rows.toDF(names: _*)
    }
}