#!/bin/bash

PIPELINE=${1?No pipeline file. Specifiy flatten_unzipped_pipeline.py or nested_sideinput_pipeline.py}
echo "Pipeline file: $PIPELINE"

BASE_DIR=$(cd $(dirname $0); pwd -P)
VIRTUALENV_DIR=$BASE_DIR/env
BEAM_VERSION=2.30.0

# Build virtual environment
echo "Virtualenv directory: $VIRTUALENV_DIR"
virtualenv --python=python3 $VIRTUALENV_DIR
source $VIRTUALENV_DIR/bin/activate
pip install apache-beam[gcp]==$BEAM_VERSION

# Reproduce the issue
PROJECT_ID=$(gcloud config get-value project)
TEMP_LOCATION=gs://$PROJECT_ID/dataflow/temp

python $PIPELINE \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1  \
  --temp_location=$TEMP_LOCATION \
  --streaming \
  --experiments=use_runner_v2

# Workaround
#python $PIPELINE \
#  --runner=DataflowRunner \
#  --project=$PROJECT_ID \
#  --region=us-central1  \
#  --temp_location=$TEMP_LOCATION \
#  --streaming \
#  --experiments=disable_runner_v2 \
#  --experiments=disable_streaming_engine \
#  --experiments=allow_non_updatable_job

