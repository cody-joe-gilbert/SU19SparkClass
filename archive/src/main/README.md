Execute the code on spark submit using the attached pom.xml file for dependencies. 

Running spark-submit:

/opt/maven/bin/mvn package

nohup spark2-submit --class DataProfiler --master yarn target/scala-0.0.1-SNAPSHOT.jar  &


The code will execute data profiling instructions.  The .scala file includes code for processing the data using RDDs and SparkSQL dataframes.  

Processing RDDs is more efficient and runs much faster than data frames, and I included both in this file as there were very few data profiling operations where it was easier to use a dataframe despite the performance speed. 

