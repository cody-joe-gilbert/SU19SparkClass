val data = sc.textFile("project/data/top_1000.csv")
val header = data.first
val data_filtered = data.filter(_(0) != header(0))
val data_split = data_filtered.map(line => line.split(",").mkString(","))

val data_tuple = data_split.map(line => {
   val lines = line.split(',')
  (lines(6), lines(8), lines(9), lines(10), lines(12), lines(13), lines(14), lines(18), lines(19), lines(26), lines(35), lines(37), lines(42), lines(43), lines(44), lines(45), lines(46))
  })
val output = data_tuple.map(values => values.toString).
   map(s=>s.substring(1,s.length-1))
   output.take(10).foreach(println)
  
 output.saveAsTextFile("project/clean-data/data_for_analysis.csv")
