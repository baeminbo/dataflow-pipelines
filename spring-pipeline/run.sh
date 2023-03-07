#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS=baeminbo.SpringPipeline
UBER_JAR=target/spring-pipeline-bundled-1.0.0.jar

#mvn clean compile exec:java -Dexec.mainClass="$MAIN_CLASS" \
#  -Dexec.args="\
#    --runner=Dataflow \
#    --project=$PROJECT \
#    --region=$REGION \
#    --numberOfWorkerHarnessThreads=10 \
#    --autowireConfiguration=baeminbo.config.InfoPrinterConfig \
#  "

mvn clean package
java -cp "$UBER_JAR" "$MAIN_CLASS" \
    --runner=Dataflow \
    --project=$PROJECT \
    --region=$REGION \
    --numberOfWorkerHarnessThreads=10 \
    --autowireConfiguration=baeminbo.config.WarnPrinterConfig
#--autowireConfiguration=baeminbo.config.InfoPrinterConfig