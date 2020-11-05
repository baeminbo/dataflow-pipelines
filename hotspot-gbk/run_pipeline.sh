#!/bin/bash

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n1-standard-1}"
NUM_WORKERS="${NUM_WORKERS:-10}"
MAX_NUM_WORKERS="${MAX_NUM_WORKERS:-$NUM_WORKERS}"
USE_DATAFLOW_SHUFFLE="${USE_DATAFLOW_SHUFFLE:-false}" # true or false
MAIN_CLASS=baeminbo.HotspotGbkPipeline

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner 
    --project=$PROJECT_ID
    --region=$REGION
    --workerMachineType=$WORKER_MACHINE_TYPE
    --numWorkers=$NUM_WORKERS
    --maxNumWorkers=$MAX_NUM_WORKERS
    --workerLogLevelOverrides='{"org.apache.beam":"TRACE","org.apache.beam.runners.dataflow.worker.repackaged":"INFO"}'
    --startElement=0
    --endElement=1000000000
    --shardCount=1
EOF
)
if $USE_DATAFLOW_SHUFFLE; then ARGS+=" --experiments=shuffle_mode=service"; fi

mvn -Pdataflow-runner compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS"
