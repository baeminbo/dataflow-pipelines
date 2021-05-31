#!/bin/bash

PROJECT_ID="$(gcloud config get-value project)"

ARGS=$(
  tr '\n' ' ' <<EOF
    --runner=DataflowRunner
    --project=${PROJECT_ID}
    --region=us-central1
    --jobName=jdbc-write-2-25-0
    --enableStreamingEngine
EOF
)

mvn -Pdataflow-runner clean compile exec:java \
  -Dexec.mainClass=baeminbo.JdbcWritePipeline \
  -Dexec.args="$ARGS"
