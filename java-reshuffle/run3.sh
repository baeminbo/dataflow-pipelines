#!/bin/bash
trap exit INT
set eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.StatefulPipeline"

mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --maxNumWorkers=200 \
    --experiments=use_runner_v2 \
    --jobName=stateful-v2 \
  "

mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --maxNumWorkers=200 \
    --experiments=disable_runner_v2 \
    --jobName=stateful-v1 \
  "