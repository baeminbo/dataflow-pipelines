#!/bin/bash

trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
TEMPLATE_LOCATION="gs://$PROJECT/dataflow/template/bigquery-permission"
TEMP_LOCATION="gs://$PROJECT/dataflow/temp"
JOB_NAME="bigquery-permission"

gcloud dataflow jobs run "$JOB_NAME" \
  --region="$REGION" \
  --gcs-location="$TEMPLATE_LOCATION" \
  --staging-location="$TEMP_LOCATION"
