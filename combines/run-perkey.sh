#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)

mvn -Pdataflow-runner clean compile exec:java  \
  -Dexec.mainClass=baeminbo.PerKeyCombinePipeline \
  -Dexec.args=" \
    --runner=DataflowRunner\
    --project=${PROJECT_ID} \
    --region=us-central1 \
    --jobName=per-key-combine-java \
    --streaming \
    --autoscalingAlgorithm=THROUGHPUT_BASED \
    --maxNumWorkers=10
  "