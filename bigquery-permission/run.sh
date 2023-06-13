#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.BigQueryPermissionPipelineMain"
# Set the query with your BigQuery table. e.g. 'SELECT * FROM `project`.`dataset`.`table`'.
# QUERY='SELECT * FROM `project`.`dataset`.`table`'

mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --query=\"$QUERY\" \
  "