#!/bin/bash
trap exit INT
set eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"

for i in $(seq 1 10)
do
  MAIN_CLASS="baeminbo.FlattenUnzippedSideInputPipeline"
  echo "Run job $MAIN_CLASS [$i]"
  mvn compile exec:java -Dexec.mainClass=$MAIN_CLASS \
    -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --streaming \
    --experiments=allow_non_updatable_job \
    "
done


for i in $(seq 1 10)
do
  MAIN_CLASS="baeminbo.LeftOnlyFlattenUnzippedSideInputPipeline"
  echo "Run job $MAIN_CLASS [$i]"
  mvn compile exec:java -Dexec.mainClass=$MAIN_CLASS \
    -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --streaming \
    --experiments=allow_non_updatable_job \
    "
done
