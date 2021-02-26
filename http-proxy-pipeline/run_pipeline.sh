#!/bin/bash

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"

MAIN_CLASS=HttpProxyPipeline

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner
    --project=$PROJECT_ID
    --region=$REGION
EOF
)

mvn -Pdataflow-runner compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS"
