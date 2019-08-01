import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import spark.implicits._

//************************** STAGE ONE **************************
val hmdaPath = "/user/jjl359/project/data/HMDA_2007_to_2017_codes.csv"
val hmda = spark.read.option("header", "true").option("inferSchema", "true").csv(hmdaPath)

/** PARSE FIELDS INDIVITUALLY TO PRESERVE DATA AS WE'LL BE FILTERING OUT LARGE NUMBERS OF IT CONCERNING EACH FIELD*/
//----------------- parse sex --------------------------
val sex = hmda.select("as_of_year", "applicant_sex", "action_taken")

val rdd = sex.rdd
val sex_parsed = rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    // FILTER USEFUL ACTION RECORDS [1, 2, 8] approve, [3, 7] deny
                    filter(x => x(2) == "1" ||
                                x(2) == "2" ||
                                x(2) == "8" ||
                                x(2) == "3" ||
                                x(2) == "7").
		    filter(x => x(1) == "1" || //male
				x(1) == "2" || //female
				x(1) == "4").  //not applicable
                    // MAP ACTIONS TO DENIAL/ACCEPTANCE
                    map(x => Row(
                       x(0),
                       x(1),
                       x(2).
                         replace("1", "accept").replace("2", "accept").replace("8", "accept").
                         replace("3", "deny").replace("7", "deny"))).
                    map({case Row(v1: String, v2: String, v3: String) => (v1, v2, v3)}).
                    toDF("year", "sex", "action")
val sexPath = "/user/fh643/VisualPrep/sexData"
sex_parsed.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(sexPath)

//----------------- parse race --------------------------
val race = hmda.select("as_of_year", "applicant_race_1", "action_taken")
val race_parsed = race.rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    // FILTER USEFUL ACTION RECORDS [1, 2, 8] approve, [3, 7] deny
                    filter(x => x(2) == "1" ||
                                x(2) == "2" ||
                                x(2) == "8" ||
                                x(2) == "3" ||
                                x(2) == "7").
                    filter(x => x(1) == "1" || // American Indian or Alaska Native
                                x(1) == "2" || // Asian
                                x(1) == "3" || // Black
				x(1) == "4" || // Pacific Islander
				x(1) == "5" || // White
				x(1) == "7"). // not applicable 
                    // MAP ACTIONS TO DENIAL/ACCEPTANCE
                    map(x => Row(
                       x(0),
                       x(1),
                       x(2).
                         replace("1", "accept").replace("2", "accept").replace("8", "accept").
                         replace("3", "deny").replace("7", "deny"))).
                    map({case Row(v1: String, v2: String, v3: String) => (v1, v2, v3)}).
                    toDF("year", "race", "action")
val racePath = "/user/fh643/VisualPrep/raceData"
race_parsed.coalesce(1).write.mode("overwrite").option("header","true").format("csv").save(racePath)

//----------------- parse ethnicity --------------------------
val eth = hmda.select("as_of_year", "applicant_ethnicity", "action_taken")
val eth_parsed = eth.rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    // FILTER USEFUL ACTION RECORDS [1, 2, 8] approve, [3, 7] deny
                    filter(x => x(2) == "1" ||
                                x(2) == "2" ||
                                x(2) == "8" ||
                                x(2) == "3" ||
                                x(2) == "7").
                    filter(x => x(1) == "1" || // Hispanic or latino
                                x(1) == "2" || // Not hispanic or latino
                                x(1) == "4"). // not applicable
                    // MAP ACTIONS TO DENIAL/ACCEPTANCE
                    map(x => Row(
                       x(0),
                       x(1),
                       x(2).
                         replace("1", "accept").replace("2", "accept").replace("8", "accept").
                         replace("3", "deny").replace("7", "deny"))).
                    map({case Row(v1: String, v2: String, v3: String) => (v1, v2, v3)}).
                    toDF("year", "eth", "action")
val ethPath = "/user/fh643/VisualPrep/ethData"
eth_parsed.write.mode("overwrite").option("header","true").format("csv").save(ethPath)

//----------------- parse income --------------------------
val income = hmda.select("as_of_year", "applicant_income_000s", "action_taken")
val income_parsed = income.rdd.map(x => x.toString).
                    map(x => x.substring(1, x.length - 1).split(",")).
                    // FILTER USEFUL ACTION RECORDS [1, 2, 8] approve, [3, 7] deny
                    filter(x => x(2) == "1" ||
                                x(2) == "2" ||
                                x(2) == "8" ||
                                x(2) == "3" ||
                                x(2) == "7").
                    // MAP ACTIONS TO DENIAL/ACCEPTANCE
                    map(x => Row(
                       x(0),
                       x(1),
                       x(2).
                         replace("1", "accept").replace("2", "accept").replace("8", "accept").
                         replace("3", "deny").replace("7", "deny"))).
                    map({case Row(v1: String, v2: String, v3: String) => (v1, v2, v3)}).
                    toDF("year", "income", "action")
val incomePath = "/user/fh643/VisualPrep/incomeData"
income_parsed.write.mode("overwrite").option("header","true").format("csv").save(incomePath)



