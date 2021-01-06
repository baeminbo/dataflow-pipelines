#!/bin/bash

JAR=target/urn-notfound-1.0.0-assembly.jar
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
MAIN_CLASS=byop.UrnNotFoundPipeline

echo "PROJECT_ID: $PROJECT_ID"
echo "REGION: $REGION"
echo "MAIN_CLASS: $MAIN_CLASS"

mvn -Pdataflow-runner clean package

java -cp $JAR $MAIN_CLASS --runner=DataflowRunner --project=$PROJECT_ID --region=$REGION
