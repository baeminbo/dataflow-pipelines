#!/bin/bash
trap exit INT

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"

MAIN_CLASS="baeminbo.Log4j2CoreLoggingPipeline"
REGION="us-central1"

mvn clean compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    "


