#!/bin/bash

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n1-standard-1}"
NUM_WORKERS="${NUM_WORKERS:-2}"
MAX_NUM_WORKERS="${MAX_NUM_WORKERS:-$NUM_WORKERS}"
AUTOSCALING_ALGORITHM="${AUTOSCALING_ALGORITHM:-NONE}" # NONE or THROUGHPUT_BASED
STREAMING="${STREAMING:-false}"
SOURCE_MODE="${SOURCE_MODE:-BOUNDED}" # BOUNDED or UNBOUNDED
PRINT_STEP_MODE="${PRINT_STEP_MODE:-GBK}" # GBK or STATEFUL

MAIN_CLASS=baeminbo.SimpleGbkPipeline

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner 
    --project=$PROJECT_ID
    --region=$REGION
    --workerMachineType=$WORKER_MACHINE_TYPE
    --numWorkers=$NUM_WORKERS
    --maxNumWorkers=$MAX_NUM_WORKERS
    --autoscalingAlgorithm=$AUTOSCALING_ALGORITHM
    --workerLogLevelOverrides='{"org.apache.beam":"TRACE","org.apache.beam.runners.dataflow.worker.repackaged":"INFO"}'
    --streaming=$STREAMING
    --sourceMode=$SOURCE_MODE
    --printStepMode=$PRINT_STEP_MODE
EOF
)

mvn -Pdataflow-runner compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS"
