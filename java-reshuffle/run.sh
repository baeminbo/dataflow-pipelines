#!/bin/bash
trap exit INT
set eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.ReshuffleBatchPipeline"

#for count in 1000000 2000000 5000000 10000000
for count in 1000000
do
#  for size in 1000 500 200 100 50 20 10
  for size in 1000
  do
  mvn compile exec:java \
    -Dexec.mainClass="$MAIN_CLASS" \
    -Dexec.args="\
      --runner=DataflowRunner \
      --project=$PROJECT \
      --region=$REGION \
      --maxNumWorkers=1 \
      --experiments=disable_runner_v2 \
      --numberOfWorkerHarnessThreads=48 \
      --elementCount=${count} \
      --elementSize=${size} \
      --latencyMs=3000 \
      --jobName=reshuffle-v1-${count}-${size} \
    "
  done
done

# --experiments=use_runner_v2 \

