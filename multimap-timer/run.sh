#!/bin/bash
set -eu
trap exit INT

PROJECT=$(gcloud config get project)
REGION="us-central1"
MAIN_CLASS=baeminbo.MultimapTimerPipeline

JOB_NAME="multiple-timer"
mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --jobName=$JOB_NAME \
    --enableStreamingEngine \
    --experiments=disable_runner_v2 \
    --workerCacheMb=0 \
  "

DEBUG_JOB_NAME="multiple-timer-debug"
DATAFLOW_WORKER_JAR="beam-runners-google-cloud-dataflow-java-legacy-worker-2.59.0.jar"
mvn compile exec:java \
  -Dexec.mainClass="$MAIN_CLASS" \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT \
    --region=$REGION \
    --jobName=$DEBUG_JOB_NAME \
    --enableStreamingEngine \
    --experiments=disable_runner_v2 \
    --workerCacheMb=0 \
    --dataflowWorkerJar=$DATAFLOW_WORKER_JAR \
  "


