#!/bin/bash

PROJECT=$(gcloud config get-value project)
PIPELINE=biggbk.py

# NOTE: Job has no progress if --number_of_worker_harness_threads=1
python $PIPELINE --runner=DataflowRunner \
  --project=$PROJECT \
  --region=us-central1 \
  --streaming \
  --worker_machine_type custom-1-102400-ext \
  --experiments=use_runner_v2 \
  --experiments=no_use_multiple_sdk_containers \
  --max_num_workers=1 \
  --number_of_worker_harness_threads=2
