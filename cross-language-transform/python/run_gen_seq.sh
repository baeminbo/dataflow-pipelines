#!/bin/bash
cd $(dirname $0) || exit

# Virtualenv directory
ENV="env"
virtualenv --python=python3 $ENV
source $ENV/bin/activate

pip install apache-beam[gcp]==2.31.0

# Run Dataflow job
PROJECT_ID=$(gcloud config get-value project)
PIPELINE=pipelines.generate_sequence_external_pipeline

python -m $PIPELINE \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --job_name=gen-seq-external \
  --temp_location="gs://$PROJECT_ID/dataflow/temp" \
  --setup_file=./setup.py \
  --experiments=use_runner_v2 \
  --streaming

