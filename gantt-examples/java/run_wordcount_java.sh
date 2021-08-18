#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
JOB_NAME=fail-wordcount-java
TEMP_LOCATION="gs://$PROJECT_ID/dataflow/temp"
WRONG_OUTPUT="gs://${PROJECT_ID}_nonexisting/dataflow/output"

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner
    --project=${PROJECT_ID}
    --region=us-central1
    --tempLocation=$TEMP_LOCATION
    --jobName=$JOB_NAME
    --numWorkers=4 \
    --autoscalingAlgorithm=NONE \
    --inputFile=gs://dataflow-samples/shakespeare/* \
    --output=$WRONG_OUTPUT
EOF
)

mvn -Pdataflow-runner clean compile exec:java \
  -Dexec.mainClass=org.apache.beam.examples.WordCount \
  -Dexec.args="$ARGS"
