#!/bin/bash

MAIN_CLASS=baeminbo.DynamicGcsWritePipeline

STAGING_LOCATION=${STAGING_LOCATION:?}
TEMPLATE_LOCATION=${TEMPLATE_LOCATION:?}
TEMPLATE_METADATA_LOCATION="${TEMPLATE_LOCATION}_metadata"

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner 
    --project=${PROJECT_ID:-$(gcloud config get-value project)}
    --region=${REGION:-us-central1}
    ${WORKER_MACHINE_TYPE+--workerMachineType=$WORKER_MACHINE_TYPE}
    --workerLogLevelOverrides='{"org.apache.beam":"TRACE","org.apache.beam.runners.dataflow.worker.repackaged":"INFO"}'
    --experiments=shuffle_mode=service

    --stagingLocation=$STAGING_LOCATION
    --templateLocation=$TEMPLATE_LOCATION
EOF
)

mvn -Pdataflow-runner compile exec:java \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="$ARGS"

gsutil cp src/main/resources/template_metadata.json $TEMPLATE_METADATA_LOCATION
