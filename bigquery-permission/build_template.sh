#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
STAGING_LOCATION="gs://$PROJECT/dataflow/staging"
TEMPLATE_LOCATION="gs://$PROJECT/dataflow/template/bigquery-permission"
MAIN_CLASS="baeminbo.BigQueryPermissionPipelineMain"
UBER_JAR="target/bigquery-permission-bundled-1.0.0.jar"
# Set the default query with your BigQuery table. e.g. 'SELECT * FROM `project`.`dataset`.`table`'.
# DEFAULT_QUERY='SELECT * FROM `project`.`dataset`.`table`'

mvn clean package

java -cp "$UBER_JAR" "$MAIN_CLASS" \
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --stagingLocation=$STAGING_LOCATION \
    --templateLocation=$TEMPLATE_LOCATION \
    --query="$DEFAULT_QUERY"
