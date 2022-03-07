#!/bin/bash
MAIN_CLASS=baeminbo.ZstdOutputPipeline
PROJECT=$(gcloud config get-value project)
REGION=us-central1

mvn -Pdataflow-runner clean compile exec:java -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner --project=$PROJECT --region=$REGION"