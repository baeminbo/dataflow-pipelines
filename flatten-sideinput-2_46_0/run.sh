#!/bin/bash

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS=baeminbo.WriteBigQueryPipeline

DATASET="dataset1"
TABLE="dftest"
WRITE_METHOD="STREAMING_INSERTS"
#WRITE_METHOD="STORAGE_WRITE_API"

if ! bq show "$PROJECT:$DATASET.$TABLE" >/dev/null 2>&1
then
  echo "Creating table."
  bq mk -p "$PROJECT" -t "$DATASET.$TABLE" k:string,v:integer
fi

mvn clean compile exec:java -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION  \
  --streaming \
  --enableStreamingEngine \
  --table=$PROJECT:$DATASET.$TABLE \
  --writeMethod=$WRITE_METHOD \
  "

