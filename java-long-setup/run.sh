#!/bin/bash
trap exit INT TERM
set eu

PROJECT=$(gcloud config get-value project)
MAIN_CLASS=baeminbo.LongSetupPipeline
REGION=us-central1

echo "Creating Runner v2 job."
mvn compile exec:java -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="\
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --jobName=batch-long-setup-v2 \
  --experiments=use_runner_v2 \
  "

echo "Creating Legacy job."
mvn compile exec:java -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="\
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --jobName=batch-long-setup-legacy \
  --experiments=disable_runner_v2 \
  --workerLogLevelOverrides='{\
    \"org.apache.beam.runners.dataflow.worker\":\"DEBUG\", \
    \"org.apache.beam.runners.dataflow.worker.repackaged\":\"INFO\" \
  }' \
  "
