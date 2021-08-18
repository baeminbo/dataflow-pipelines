#!/bin/bash

python -m venv env || exit 1
source env/bin/activate
pip install --upgrade pip
pip install -r requirements-build.txt

PROJECT_ID=$(gcloud config get-value project)
JOB_NAME=stuck-py-legacy
TEMP_LOCATION="gs://$PROJECT_ID/dataflow/temp"
INPUTS=1000  # Will generate 1000 * 1M elements to write into GCS
OUTPUT_PREFIX="gs://$PROJECT_ID/dataflow/output/$JOB_NAME/output-"
SHARDS=1

python stuck_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --job_name=$JOB_NAME \
  --temp_location=$TEMP_LOCATION \
  --max_num_workers=20 \
  --experiments=disable_runner_v2 \
  --inputs=$INPUTS \
  --output_prefix=$OUTPUT_PREFIX \
  --shards=$SHARDS

