#!/bin/bash
PROJECT=$(gcloud config get-value project)

REGION=us-central1
PIPELINE=translate_pipeline.py
MACHINE_TYPE=n1-standard-1

REQUIREMENTS_FILE=requirements.txt
SETUP_FILE=./setup.py
DOWNLOAD_DIR=downloads

VIRTUALENV_DIR=env

COMMON_ARGS=$(
tr '\n' ' ' <<EOF
  --runner=DataflowRunner
  --project=$PROJECT
  --region=$REGION
  --machine_type=$MACHINE_TYPE
  --experiments=use_runner_v2
EOF
)

function _init {
  python -m venv $VIRTUALENV_DIR
  source $VIRTUALENV_DIR/bin/activate

  pip install -U -q pip
  # "gcp" extension is required to create default Dataflow gcs bucket
  pip install -q apache-beam[gcp]==2.31.0
}

function run_dataflow_runner {
  python $PIPELINE $COMMON_ARGS
}

function run_direct_runner {
  python $PIPELINE --project=$PROJECT
}

function run_dataflow_runner_with_requirements {
  python $PIPELINE $COMMON_ARGS --requirements_file=$REQUIREMENTS_FILE
}

function run_dataflow_runner_with_setup {
  python $PIPELINE $COMMON_ARGS --setup_file=$SETUP_FILE
}

function run_dataflow_runner_with_extra_package {
  rm -f $DOWNLOAD_DIR/*
  pip download -r $REQUIREMENTS_FILE --dest=$DOWNLOAD_DIR --no-deps --platform=manylinux1_x86_64
  EXTRA_PACKAGES_ARGS=$(find $DOWNLOAD_DIR -type f | sed 's/^/--extra_packages=/')
  python $PIPELINE $COMMON_ARGS $EXTRA_PACKAGES_ARGS
}

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 (default | requirements | setup | extra_package | direct)"
  exit 1
fi

_init

case $1 in
  default)
    run_dataflow_runner
    ;;
  requirements)
    run_dataflow_runner_with_requirements
    ;;
  setup)
    run_dataflow_runner_with_setup
    ;;
  extra_package)
    run_dataflow_runner_with_extra_package
    ;;
  direct)
    run_direct_runner
    ;;
  *)
    echo "Unexpected argument: $1"
    ;;
esac
