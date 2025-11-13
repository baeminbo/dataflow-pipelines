#!/bin/bash

PROJECT="$(gcloud config get-value project)"
BUCKET_NAME="$PROJECT"
REPOSITORY="dataflow"
REGION="us-central1"
PIPELINE_FILE=flex_pipeline.py
TEMPLATE_NAME=py-flex-inspect

ENV="env"  # virtual environment directory
BEAM_VERSION=2.69.0

if [ ! -d  "$ENV" ]; then
  echo "Creating virtual environment in directory: $ENV"
  python -m venv "$ENV"
  source "$ENV/bin/activate"
  pip install --upgrade pip
  pip install "apache-beam[gcp]==${BEAM_VERSION}"
else
  source "$ENV/bin/activate"
fi

echo "Build Flex template ..."
gcloud dataflow flex-template build "gs://${BUCKET_NAME}/${TEMPLATE_NAME}.json" \
 --image-gcr-path "${REGION}-docker.pkg.dev/${PROJECT}/${REPOSITORY}/flex/${TEMPLATE_NAME}:latest" \
 --sdk-language "PYTHON" \
 --flex-template-base-image "PYTHON3" \
 --py-path "." \
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=${PIPELINE_FILE}"

 echo "Run Flex template job ..."
 gcloud dataflow flex-template run "${TEMPLATE_NAME}-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "gs://${BUCKET_NAME}/${TEMPLATE_NAME}.json" \
  --region "$REGION"





