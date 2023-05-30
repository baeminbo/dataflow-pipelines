#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
OUTPUT="gs://$PROJECT/dataflow/output/bigquery-read-hotkey.out"
MAIN_CLASS="baeminbo.BigQueryReadHotkeyMain"

mvn compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args=" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --maxNumWorkers=100 \
  --output=$OUTPUT \
  "
