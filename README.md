# SU19SparkClass
Repository for the team project in the Summer 2019 Spark Class.

In order to use dataFrames type the following lines into the Dumbo shell (load modules and spark shell with databricks package): 

````module load java/1.8.0_72 ````

````module load spark/2.2.0 ````

````spark-shell --packages com.databricks:spark-csv_2.11:1.2.0 --master local -deprecation````



**Running** ````spark-submit````, provide a pathname to a folder for the output (arg[0]):

    /opt/maven/bin/mvn package
    
    nohup spark-submit --class DataProfiler --master yarn target/scala-0.0.1-SNAPSHOT.jar  /user/<network id>/<output_folder_name>  &
