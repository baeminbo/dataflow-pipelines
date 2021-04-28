#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)

mvn -Pdataflow-runner clean compile exec:java  \
  -Dexec.mainClass=baeminbo.GroupValuesPipeline \
  -Dexec.args=" \
    --runner=DataflowRunner\
    --project=${PROJECT_ID} \
    --region=us-central1 \
    --jobName=group-values-combine-java \
    --streaming \
    --autoscalingAlgorithm=THROUGHPUT_BASED \
    --maxNumWorkers=10
  "