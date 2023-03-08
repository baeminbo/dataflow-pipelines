#!/bin/bash

PROJECT=$(gcloud config get-value project)
REGION=us-central1
FLEX_TEMPLATE_FILE_GCS_PATH="gs://$PROJECT/dataflow/flex/java-pipeline-python-transform"
JOB_NAME=java-pipeline-python-transform-from-flex

gcloud dataflow flex-template run "$JOB_NAME"\
  --project="$PROJECT" \
  --region="$REGION" \
  --template-file-gcs-location="$FLEX_TEMPLATE_FILE_GCS_PATH" \
  --parameters=printerName=FlexPrinter