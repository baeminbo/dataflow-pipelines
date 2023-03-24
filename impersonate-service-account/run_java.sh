#!/bin/bash

set -eu
trap exit INT

MAIN=baeminbo.PipelineMain
PROJECT=$(gcloud config get-value project)
REGION=us-central1
TEMP_LOCATION="gs://$PROJECT/temp"
# For Dataflow job creation, the service account must have permission for
# Dataflow job creation and GCS object reading/write in the GCS bucket.
IMPERSONATE_SERVICE_ACCOUNT="sandbox@$PROJECT.iam.gserviceaccount.com"

mvn clean compile exec:java -Dexec.mainClass="$MAIN" \
  -Dexec.args=" \
  --runner=Dataflow \
  --project=$PROJECT \
  --region=$REGION \
  --tempLocation=$TEMP_LOCATION \
  --impersonateServiceAccount=$IMPERSONATE_SERVICE_ACCOUNT \
  "