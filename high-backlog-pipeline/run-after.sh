#!/bin/bash

PROJECT_ID="${PROJECT_ID:?Variable not set}"
SUBSCRIPTION="${SUBSCRIPTION:?Variable not set}"

mvn -Pdataflow-runner clean compile exec:java  \
  -Dexec.mainClass=baeminbo.HighBacklogPipeline \
  -Dexec.args=" \
    --runner=DataflowRunner\
    --project=${PROJECT_ID} \
    --region=us-central1 \
    --jobName=high-backlog-after-sleep \
    --streaming \
    --enableStreamingEngine \
    --subscription=$SUBSCRIPTION \
    --afterSleepMillis=10000
  "