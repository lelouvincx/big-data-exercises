#!/bin/bash -e

# Compile and package the source code
mvn -q package
# Run with Spark
spark-submit target/taskbonus1-1.0-SNAPSHOT-uber.jar
# Remove build outputs
mvn -q clean
