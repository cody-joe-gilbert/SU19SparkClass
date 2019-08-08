#!/usr/bin/bash
# Spark Processing Script
# Cody Gilbert

# Execution Parameters
INPUT=/scratch/cjg507/spark/modeling/dataModeling.scala
MASTER=yarn

# Spark Execution
spark-shell --master $MASTER -i $INPUT