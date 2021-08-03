#!/bin/bash

cd $(dirname $0) || exit

# Virtualenv directory
ENV="env"
virtualenv --python=python3 $ENV
source $ENV/bin/activate

BOOTSTRAP_SERVERS=${1:?Error! Missing Kafka bootstrap_servers "<hostname1>:<port1>,<hostname2>:<port2>,<hostname3>:<port3>"}
TOPIC=${2:?Error! Missing Kafka topic argument}

PROJECT_ID=$(gcloud config get-value project)

PIPELINE=pipelines.kafka_external_pipeline
EXPANSION_SERVICE=../java/target/xlang-transforms-bundled-1.0.0.jar

python -m $PIPELINE \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=us-central1 \
  --job_name=kafka-external \
  --temp_location="gs://$PROJECT_ID/dataflow/temp" \
  --setup_file=./setup.py \
  --experiments=use_runner_v2 \
  --streaming \
  --expansion_service=$EXPANSION_SERVICE \
  --bootstrap_servers=$BOOTSTRAP_SERVERS \
  --topic=$TOPIC


