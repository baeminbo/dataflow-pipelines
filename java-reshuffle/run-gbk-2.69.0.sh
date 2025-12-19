#!/bin/bash
trap exit INT
set -eu

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
MAIN_CLASS="baeminbo.GBKBatchPipeline"
POM=pom-2.69.0.xml

echo "Project is: $PROJECT"

count=1000000
size=1000

mvn clean compile -f $POM

mvn exec:java -f $POM \
    -Dexec.mainClass="$MAIN_CLASS" \
    -Dexec.args="\
      --runner=DataflowRunner \
      --project=$PROJECT \
      --region=$REGION \
      --maxNumWorkers=1 \
      --experiments=use_runner_v2,large_iterables_page_size_bytes=2097152,enable_fnapi_large_iterables_v2 \
      --numberOfWorkerHarnessThreads=48 \
      --elementCount=${count} \
      --elementSize=${size} \
      --latencyMs=3000 \
      --jobName=gbk-2p69p0-v2-${count}-${size} \
    "

mvn exec:java -f $POM \
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
      --jobName=gbk-2p69p0-v1-${count}-${size} \
    "