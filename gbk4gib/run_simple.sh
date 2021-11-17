#!/bin/bash
PROJECT=$(gcloud config get-value project)

PIPELINE=simple_gib_pipeline.py
#PIPELINE=simple_gbk_pipeline.py
#PIPELINE=simple_custom_gib_pipeline.py

python $PIPELINE \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=us-central1 \
  --streaming \
  --num_workers=100 \
  --max_num_workers=100 \
  --worker_machine_type=n1-highmem-2
