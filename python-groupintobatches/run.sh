#!/bin/bash
trap exit INT
set -eu

BASE=$(dirname $0)
ENV="$BASE/env"
PROJECT=$(gcloud config get-value project)
REGION=us-central1
PIPELINE=simple_pipeline

if [ ! -d "$ENV" ]; then
  echo "Creating new environment."
  python -m venv $ENV
  source $ENV/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
else
  echo "Using existing environment."
  source $ENV/bin/activate
fi



echo "Run streaming pipeline."
python -m "$PIPELINE" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --streaming \
  --save_main_session \
  --job_name=python-groupintobatches-v2-streaming


echo "Run streaming pipeline with pickle_library=dill."
python -m "$PIPELINE" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --streaming \
  --save_main_session \
  --job_name=python-groupintobatches-v2-streaming-dill \
  --pickle_library=dill

echo "Run batch pipeline."
python -m "$PIPELINE" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --save_main_session \
  --job_name=python-groupintobatches-v2-batch


echo "Run batch pipeline with pickle_library=dill."
python -m "$PIPELINE" \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --save_main_session \
  --job_name=python-groupintobatches-v2-batch-dill \
  --pickle_library=dill
