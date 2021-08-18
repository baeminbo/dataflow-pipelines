#!/bin/bash

python -m venv env || exit 1
source env/bin/activate
pip install --upgrade pip
pip install -r requirements-build.txt

PROJECT_ID=$(gcloud config get-value project)
JOB_NAME=fail-wordcount-py-v2
TEMP_LOCATION="gs://$PROJECT_ID/dataflow/temp"
WRONG_OUTPUT="gs://${PROJECT_ID}_nonexisting/dataflow/output"

python -m apache_beam.examples.wordcount \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --job_name=$JOB_NAME \
  --temp_location=$TEMP_LOCATION \
  --num_workers=4 \
  --autoscaling_algorithm=NONE \
  --experiments=use_runner_v2 \
  --input=gs://dataflow-samples/shakespeare/* \
  --output=$WRONG_OUTPUT


