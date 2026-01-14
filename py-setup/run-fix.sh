#!/bin/bash
set eu
trap exit INT

BEAM_VERSION="2.69.0"

BASEDIR=$(dirname $0)
ENVDIR="$BASEDIR/env"
if [ ! -d "$ENVDIR" ]; then
  echo "Creating new environment."
  python -m venv $ENVDIR
  source $ENVDIR/bin/activate
  pip install --upgrade pip
  pip install apache-beam[gcp]==$BEAM_VERSION
else
  echo "Using existing environment."
  source $ENVDIR/bin/activate
fi


PROJECT=$(gcloud config get-value project)
REGION="us-central1"


python -m pipeline \
  --runner=Dataflow \
  --project=$PROJECT \
  --region=$REGION \
  --save_main_session \
  --setup_file=./setup.py \
  --no_use_public_ips \
  --requirements_file=requirements.txt