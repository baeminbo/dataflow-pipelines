#!/bin/bash
trap exit INT
set eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.BigQuerySetFPipeline"

mvn clean compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --streaming \
    --experiments=disable_runner_v2 \
  "
