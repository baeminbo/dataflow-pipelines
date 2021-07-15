#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
REGION=us-central1

MAIN_CLASS=baeminbo.DoubleRunSideinputPipeline

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner
    --project=$PROJECT_ID
    --region=$REGION
    --experiments=upload_graph
EOF
)

mvn -Pdataflow-runner -Djava.util.logging.config.file=logging.properties\
  compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS" \



#mvn  compile exec:java \
#  -Dexec.mainClass=$MAIN_CLASS \
#  -Dexec.args="--runner=DirectRunner"