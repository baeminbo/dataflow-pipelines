#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
TEMPLATE_LOCATION="gs://$PROJECT/dataflow/template/bigquery-read-hotkey"
TEMP_LOCATION="gs://$PROJECT/dataflow/temp"
OUTPUT="gs://$PROJECT/dataflow/output"
JOB_NAME="bigquery-read-hotkey-run"

gcloud dataflow jobs run "$JOB_NAME" \
  --region="$REGION" \
  --gcs-location="$TEMPLATE_LOCATION" \
  --staging-location="$TEMP_LOCATION" \
  --parameters=output="$OUTPUT"