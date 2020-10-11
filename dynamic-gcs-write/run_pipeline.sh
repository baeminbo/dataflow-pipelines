#!/bin/bash

MAIN_CLASS=baeminbo.DynamicGcsWritePipeline

OUTPUT_LOCATION=${OUTPUT_LOCATION:?}

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner 
    --project=${PROJECT_ID:-$(gcloud config get-value project)}
    --region=${REGION:-us-central1}
    ${WORKER_MACHINE_TYPE+--workerMachineType=$WORKER_MACHINE_TYPE}
    --workerLogLevelOverrides='{"org.apache.beam":"TRACE","org.apache.beam.runners.dataflow.worker.repackaged":"INFO"}'
    --experiments=shuffle_mode=service

    --outputLocation=$OUTPUT_LOCATION
    ${SHARD_COUNT+--shardCount=$SHARD_COUNT}
    ${DESTINATION_COUNT+--destinationCoud=$DESTINATION_COUNT}
EOF
)

mvn -Pdataflow-runner compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS"
