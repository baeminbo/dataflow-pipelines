#!/bin/bash
set -eu
trap exit INT

BASEDIR=$(dirname $0)
ENVDIR="$BASEDIR/env"
if [ ! -d "$ENVDIR" ]; then
  echo "Creating new environment."
  python -m venv $ENVDIR
  source $ENVDIR/bin/activate
  pip install --upgrade pip
  pip install -r local_requirements.txt
else
  echo "Using existing environment."
  source $ENVDIR/bin/activate
fi

PROJECT=$(gcloud config get-value project)
REGION=us-central1

echo "Run pipeline with 'iter' type sideinput."
python -m pipeline_main \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --worker_machine_type=n1-standard-1 \
  --save_main_session \
  --side_type=iter \
  --job_name=py-sideinput-iter

echo "Run pipeline with 'iter' type sideinput with state_cache_size=10."
python -m pipeline_main \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --worker_machine_type=n1-standard-1 \
  --save_main_session \
  --experiments=state_cache_size=10 \
  --side_type=iter \
  --job_name=py-sideinput-iter-cache

echo "Run pipeline with 'list' type sideinput."
python -m pipeline_main \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --worker_machine_type=n1-standard-1 \
  --save_main_session \
  --side_type=list \
  --job_name=py-sideinput-list
