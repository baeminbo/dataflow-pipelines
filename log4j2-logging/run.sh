#!/bin/bash

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
MAIN_CLASS="baeminbo.Log4j2LoggingPipeline"
REGION="us-central1"
POM=pom.xml

mvn -f $POM clean compile exec:java \
  -Pdataflow-runner \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    "
