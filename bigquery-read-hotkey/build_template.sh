#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
STAGING_LOCATION="gs://$PROJECT/dataflow/staging"
TEMPLATE_LOCATION="gs://$PROJECT/dataflow/template/bigquery-read-hotkey"
MAIN_CLASS="baeminbo.BigQueryReadHotkeyMain"
UBER_JAR="target/bigquery-read-hotkey-bundled-1.0.0.jar"

mvn clean package

java -cp "$UBER_JAR" "$MAIN_CLASS" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --maxNumWorkers=100 \
  --stagingLocation=$STAGING_LOCATION \
  --templateLocation=$TEMPLATE_LOCATION \

#mvn compile exec:java \
#  -Dexec.mainClass=$MAIN_CLASS \
#  -Dexec.args=" \
#  --runner=DataflowRunner \
#  --project=$PROJECT \
#  --region=$REGION \
#  --maxNumWorkers=100 \
#  --stagingLocation=$STAGING_LOCATION \
#  --templateLocation=$TEMPLATE_LOCATION \
#  "
#
