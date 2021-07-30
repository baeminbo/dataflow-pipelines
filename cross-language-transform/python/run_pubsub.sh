#!/bin/bash

cd $(dirname $0) || exit

# Virtualenv directory
ENV="env"
virtualenv --python=python3 $ENV
source $ENV/bin/activate

TOPIC=${1:?Error! Missing Pub/Sub topic argument}
PROJECT_ID=$(gcloud config get-value project)

PIPELINE=pipelines.pubsub_external_pipeline
EXPANSION_SERVICE=../java/target/xlang-transforms-bundled-1.0.0.jar

python -m $PIPELINE \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --job_name=pubsub-external \
  --temp_location="gs://$PROJECT_ID/dataflow/temp" \
  --setup_file=./setup.py \
  --experiments=use_runner_v2 \
  --streaming \
  --expansion_service=$EXPANSION_SERVICE \
  --topic=$TOPIC


