#!/bin/bash -e

args="${@:-car truck}"

# Compile and package the source code
mvn -q package
# Run with Spark
spark-submit target/task3d-1.0-SNAPSHOT-uber.jar $args
# Remove build outputs
mvn -q clean
