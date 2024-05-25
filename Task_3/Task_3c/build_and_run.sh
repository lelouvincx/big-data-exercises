#!/bin/bash -e

args="${@:-2 3}"

# Compile and package the source code
mvn -q package
# Run with Spark
spark-submit target/task3c-1.0-SNAPSHOT-uber.jar $args
# Remove build outputs
mvn -q clean
