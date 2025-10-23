#!/bin/bash
trap exit INT
set eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.TlsPipeline"

mvn clean compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --streaming \
    --enableStreamingEngine \
    --experiments=disable_runner_v2 \
    --jobName=tls-2-50-0 \
  "
