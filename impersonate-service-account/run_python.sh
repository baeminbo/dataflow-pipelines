#!/bin/bash

set -eu
trap exit INT

ENV=env
BEAM_VERSION=2.46.0
PIPELINE=pipeline_main
PROJECT=$(gcloud config get-value project)
REGION=us-central1
TEMP_LOCATION="gs://$PROJECT/temp"
# For Dataflow job creation, the service account must have permission for
# Dataflow job creation and GCS object reading/write in the GCS bucket.
IMPERSONATE_SERVICE_ACCOUNT="sandbox@$PROJECT.iam.gserviceaccount.com"

if [ ! -d "$ENV" ]
then
  echo "## Creating virtual environment."
  python -m venv "$ENV"
  source "$ENV/bin/activate"
  pip install --upgrade pip
  pip install apache-beam[gcp]=="$BEAM_VERSION"
else
  source "$ENV/bin/activate"
fi

echo "## Run pipeline."
python -m "$PIPELINE" \
  --runner=Dataflow \
  --project="$PROJECT" \
  --region="$REGION" \
  --temp_location="$TEMP_LOCATION" \
  --impersonate_service_account="$IMPERSONATE_SERVICE_ACCOUNT"