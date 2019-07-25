#!/usr/bin/bash
# Spark Processing Script
# Cody Gilbert


# Execution Parameters
OBJECT=ProfileData
NETID=cjg507
OUTPUT=/scratch/${NETID}/output
ITER=3

# Build/compile/Create package using maven
#echo "Building .jar file"
#/opt/maven/bin/mvn package &> mavenlog.txt

# Execute spark
echo "Executing Scala Code"
spark-submit --class $OBJECT --master yarn &> sparklog.txt
