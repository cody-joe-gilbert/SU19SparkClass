# Execute the Scala-Spark File Using Maven to Profile the Raw Data and Calculate Denial Rates by Various Feature Mixtures

Compile the data with this command, using the ````pom.xml```` in the folder: 

````/opt/maven/bin/mvn package````

````nohup spark2-submit --class DataProfiler --master yarn target/scala-0.0.1-SNAPSHOT.jar````

````nohup spark2-submit --class CalculateAverageDenialRate --master yarn target/scala-0.0.1-SNAPSHOT.jar  &````

or deploy to the cluster:

````spark2-submit --class DataProfiler --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar````

````spark2-submit --class CalculateAverageDenialRate --deploy-mode cluster --executor-memory 100G --total-executor-cores 2048 target/scala-0.0.1-SNAPSHOT.jar````
