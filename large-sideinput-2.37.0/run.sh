#!/bin/bash

MAIN_CLASS=baeminbo.LargeSideInputPipeline
PROJECT=$(gcloud config get-value project)
REGION=us-central1

# Use "n1-standard-32" (32 CPUs) to increase concurrency reading side input.
mvn -Pdataflow-runner clean compile exec:java -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --workerMachineType=n1-standard-32"
