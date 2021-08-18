#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
JOB_NAME=stuck-java
TEMP_LOCATION="gs://$PROJECT_ID/dataflow/temp"
INPUTS=1000  # Will generate 1000 * 1M elements to write into GCS
OUTPUT_PREFIX="gs://$PROJECT_ID/dataflow/output/$JOB_NAME/output-"
SHARDS=1

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner
    --project=${PROJECT_ID}
    --region=us-central1
    --tempLocation=$TEMP_LOCATION
    --jobName=$JOB_NAME
    --maxNumWorkers=20
    --inputs=$INPUTS
    --shards=$SHARDS
    --outputPrefix=$OUTPUT_PREFIX
EOF
)

mvn -Pdataflow-runner clean compile exec:java \
  -Dexec.mainClass=baeminbo.StuckPipeline \
  -Dexec.args="$ARGS"
