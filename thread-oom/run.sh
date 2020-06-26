#!/bin/bash
FILENAME=$(basename $0)
BYOP=byop11

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
TEMP_BUCKET="${TEMP_BUCKET:-gs://$PROJECT_ID-$BYOP}"
SOURCE_TOPIC="${SOURCE_TOPIC:-$BYOP-source-topic}"
SOURCE_SUBSCRIPTION="${SOURCE_SUBSCRIPTION:-$BYOP-source-subscription}"
SINK_TOPIC="${SINK_TOPIC:-$BYOP-sink-topic}"
JOB_NAME="${USER}-byop-thread-oom"
WORKER_MACHINE_TYPE="${WORKER_MACHINE:-'n1-highmem-8'}"
REGION="us-central1"

RunSetup() {
  if ! gsutil ls -b "$TEMP_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Creating GCS temp bucket..."
    gsutil mb "$TEMP_BUCKET"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create bucket."
      exit 1
    fi
  fi

  if ! gcloud pubsub topics describe "$SOURCE_TOPIC" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Creating Pubsub Source Topic..."
    gcloud pubsub topics create "$SOURCE_TOPIC" --project="$PROJECT_ID"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create source topic."
      exit 1
    fi
  fi

  if ! gcloud pubsub subscriptions describe "$SOURCE_SUBSCRIPTION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Creating Pubsub Source Subscription..."
    gcloud pubsub subscriptions create "$SOURCE_SUBSCRIPTION" --topic="$SOURCE_TOPIC" --project="$PROJECT_ID"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create source subscription."
      exit 1
    fi
  fi

  if ! gcloud pubsub topics describe "$SINK_TOPIC" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Creating Pubsub Sink Topic..."
    gcloud pubsub topics create "$SINK_TOPIC" --project="$PROJECT_ID"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create sink topic."
      exit 1
    fi
  fi
}

RunCleanup() {
  JOB_ID=$(gcloud dataflow jobs list --filter="name=$JOB_NAME" --project="$PROJECT_ID" --region=$REGION --status=active --format='value(id)')
  if [ -n "$JOB_ID" ]; then
    echo "$FILENAME: Cancelling Dataflow job..."
    gcloud dataflow jobs cancel "$JOB_ID" --region="$REGION" --project="$PROJECT_ID"
  fi
  if gcloud pubsub topics describe "$SINK_TOPIC" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting Pusbub Sink Topic..."
    gcloud pubsub topics delete "$SINK_TOPIC" --project="$PROJECT_ID"
  fi

  if gcloud pubsub subscriptions describe "$SOURCE_SUBSCRIPTION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting Pusbub Source Subscription..."
    gcloud pubsub subscriptions delete "$SOURCE_SUBSCRIPTION" --project="$PROJECT_ID"
  fi

  if gcloud pubsub topics describe "$SOURCE_TOPIC" --project=$PROJECT_ID >/dev/null 2>&1; then
    echo "$FILENAME: Deleting Pubsub Source Topic..."
    gcloud pubsub topics delete "$SOURCE_TOPIC" --project="$PROJECT_ID"
  fi

  if gsutil ls -b "$TEMP_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting objecst and GCS temp bucket..."
    gsutil rm -r "$TEMP_BUCKET"
  fi
}

RunPipelne() {
  TEMP_LOCATION="$TEMP_BUCKET/dataflow/temp"
  STAGING_LOCATION="$TEMP_BUCKET/dataflow/staging"

  SOURCE_SUBSCRIPTION_PATH="projects/$PROJECT_ID/subscriptions/$SOURCE_SUBSCRIPTION"
  SINK_TOPIC_PATH="projects/$PROJECT_ID/topics/$SINK_TOPIC"

  mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.ThreadOOMPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --region=$REGION \
    --jobName=$JOB_NAME \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --tempLocation=$TEMP_LOCATION \
    --stagingLocation=$STAGING_LOCATION \
    --sourceSubscription=$SOURCE_SUBSCRIPTION_PATH \
    --sinkTopic=$SINK_TOPIC_PATH \
    "
}

RunPublish() {
  for k in $(seq 0 1023); do
    message=$(seq 0 8191 | awk '{printf("%02x:%04d ", '"$k"' % 256, $1)}')
    gcloud pubsub topics publish "$SOURCE_TOPIC" --message="$message"
  done
}

main() {
  COMMAND=$1
  case "$COMMAND" in
  setup)
    RunSetup
    ;;
  cleanup)
    RunCleanup
    ;;
  pipeline)
    RunPipelne
    ;;
  publish)
    RunPublish
    ;;
  "")
    echo 1>&2 "Error. Please specify command."
    exit 1
    ;;
  *)
    echo 1>&2 "Error. Unexpected command: '$COMMAND'"
    exit 1
    ;;
  esac
}

main "$@"
