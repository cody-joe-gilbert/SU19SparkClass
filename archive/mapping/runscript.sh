#!/usr/bin/bash
# Spark Processing Script
# Cody Gilbert

# Execution Parameters
INPUT=/scratch/cjg507/spark/profiling/translateShapefiles.scala
PACKAGES=org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0,org.datasyslab:geospark-viz_2.3:1.2.0
MASTER=yarn

# Spark Execution
spark-shell --master $MASTER --packages $PACKAGES -i $INPUT


# Execution Parameters
INPUT=/scratch/cjg507/spark/profiling/cleanMapData.scala
MASTER=yarn

# Spark Execution
spark-shell --master $MASTER  -i $INPUT