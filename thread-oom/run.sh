#!/bin/bash
trap "exit" INT

FILENAME=$(basename $0)
BYOP=byop10

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

RunPipeline() {
  TEMP_LOCATION="$TEMP_BUCKET/dataflow/temp"
  STAGING_LOCATION="$TEMP_BUCKET/dataflow/staging"

  SOURCE_SUBSCRIPTION_PATH="projects/$PROJECT_ID/subscriptions/$SOURCE_SUBSCRIPTION"
  SINK_TOPIC_PATH="projects/$PROJECT_ID/topics/$SINK_TOPIC"

  mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.ThreadOOMPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --jobName=$JOB_NAME \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --tempLocation=$TEMP_LOCATION \
    --stagingLocation=$STAGING_LOCATION \
    --sourceSubscription=$SOURCE_SUBSCRIPTION_PATH \
    --sinkTopic=$SINK_TOPIC_PATH \
    "
}

GetRunningJobId() {
  JOB_ID=$(gcloud dataflow jobs list --filter="name=$JOB_NAME" --project="$PROJECT_ID" --region=$REGION --status=active --format='value(id)')
  echo "$JOB_ID"
}

RunCancel() {
  JOB_ID=$(GetRunningJobId)
  if [ -n "$JOB_ID" ]; then
    echo "$FILENAME: Cancelling Dataflow job..."
    gcloud dataflow jobs cancel "$JOB_ID" --region="$REGION" --project="$PROJECT_ID"

    for i in $(seq 0 16); do
      if [ -n "$(GetRunningJobId)" ]; then
        SLEEP_TIME=$(bc <<<"x=2 ^ $i")
        if (($(bc <<<"$SLEEP_TIME > 10.0"))); then
          SLEEP_TIME="10.0"
        fi
        echo "$FILENAME: Waiting for job completion. Will sleep $SLEEP_TIME second(s)"
        sleep "$SLEEP_TIME"
      else
        echo "$FILENAME: Job was cancelled."
        return 0
      fi
    done
    echo "$FILENAME: Failed to wait for job cancellation due to timeout."
    return 1
  else
    echo "$FILENAME: No Dataflow job running with $JOB_NAME."
  fi
}

RunPublish() {
  for k in $(seq 0 1023); do
    message=$(seq 0 8191 | awk '{printf("%02x:%04d ", '"$k"' % 256, $1)}')
    gcloud pubsub topics publish "$SOURCE_TOPIC" --message="$message"
  done
}

RunCleanup() {
  RunCancel

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
    echo "$FILENAME: Deleting objects and GCS temp bucket..."
    gsutil -m -q rm -r "$TEMP_BUCKET"
  fi
}

PrintHelp() {
  echo "Usage: $FILENAME (setup|pipeline|publish|cleanup|help)
  setup     Prepare GCS temp bucket and Pubsub Topics/Subscriptions.
  pipeline  Create Dataflow job.
  cancel    Cancel Dataflow job.
  publish   Publish messages to Pubsub until stopped. Type 'ctrl-c' to stop.
            The job may start to process the messages.
  cleanup   Stop Dataflow job and delete all resources.
  help      Print this help message."
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
    RunPipeline
    ;;
  cancel)
    RunCancel
    ;;
  publish)
    RunPublish
    ;;
  help)
    PrintHelp
    ;;
  "")
    echo 1>&2 "Error. Please specify command."
    PrintHelp
    exit 1
    ;;
  *)
    echo 1>&2 "Error. Unexpected command: '$COMMAND'"
    PrintHelp
    exit 1
    ;;
  esac
}

main "$@"
