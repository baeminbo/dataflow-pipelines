#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)

# Install beam if necessary
# pip install apache-beam[gcp]==2.30.0

python mysql_pipeline.py --runner=DataflowRunner \
  --project="$PROJECT_ID" \
  --region=us-central1 \
  --temp_location="gs://$PROJECT_ID/dataflow/temp" \
  --setup_file=./setup.py \
  --experiments=use_runner_v2