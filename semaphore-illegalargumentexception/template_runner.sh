#!/bin/bash

LDAP="${LDAP:-${USER}}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="${REGION:-us-central1}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE:-n1-highmem-8}"
EXTENSION="${EXTENSION}" # "beam_2.22.0" or "streaming_engine" is acceptable
if [[ ! -z "$EXTENSION" ]]; then
  if [[ "$EXTENSION" != "beam_2.22.0" ]] && [[ "$EXTENSION" != "streaming_engine" ]]; then
    echo "$FILENAME: Fatal. EXTENSION must be either 'beam_2.22.0' or 'streaming_engine' if set, "
    exit 1
  fi
fi

FILENAME=$(basename "$0")
BYOP="byop11"
JOB_BUCKET="gs://${PROJECT_ID}-${BYOP}"
TEMPLATE_LOCATION="gs://dataflow-mastery/templates/byop-11${EXTENSION:+-$EXTENSION}"
STAGING_LOCATION="$JOB_BUCKET/dataflow/temp"
TOPIC_ID="projects/pubsub-public-data/topics/taxirides-realtime"
SUBSCRIPTION_ID="${LDAP}-${BYOP}-subscription"
SUBSCRIPTION_NAME="projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_ID"
JOB_NAME="${LDAP}-${BYOP}"

RunSetup() {
  if ! gsutil ls -b "$JOB_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Creating GCS bucket..."

    if ! gsutil mb "$JOB_BUCKET"; then
      echo "$FILENAME: Fatal. Failed to create bucket."
      exit 1
    fi
  fi

  if ! gcloud pubsub subscriptions describe "$SUBSCRIPTION_ID" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Creating Pubsub Source Subscription..."
    if ! gcloud pubsub subscriptions create "$SUBSCRIPTION_ID" --topic=$TOPIC_ID --project="$PROJECT_ID"; then
      echo "$FILENAME: Fatal. Failed to create source subscription."
      exit 1
    fi
  fi
}

RunTemplate() {
  gcloud dataflow jobs run "$JOB_NAME" \
    --gcs-location="$TEMPLATE_LOCATION" \
    --staging-location="$STAGING_LOCATION" \
    --worker-machine-type="$WORKER_MACHINE_TYPE" \
    --region="$REGION" \
    --parameters=subscription="$SUBSCRIPTION_NAME"
}

GetRunningJobId() {
  job_id=$(gcloud dataflow jobs list --filter="name=$JOB_NAME" --project="$PROJECT_ID" --region=$REGION --status=active --format='value(id)')
  echo "$job_id"
}

RunCancel() {
  job_id=$(GetRunningJobId)
  if [ -n "$job_id" ]; then
    echo "$FILENAME: Cancelling Dataflow job..."
    gcloud dataflow jobs cancel "$job_id" --region="$REGION" --project="$PROJECT_ID"

    for i in $(seq 0 16); do
      if [ -n "$(GetRunningJobId)" ]; then
        SLEEP_TIME="10.0"
        echo "$FILENAME: Waiting for job completion. Will sleep $SLEEP_TIME second(s)"
        sleep "$SLEEP_TIME"
      else
        echo "$FILENAME: Job was cancelled."
        return 0
      fi
    done
    echo "$FILENAME: Failed to wait for job cancellation due to timeout."
    return 1
  fi
}

RunCleanup() {
  RunCancel

  if gcloud pubsub subscriptions describe "$SUBSCRIPTION_ID" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting Pusbub Source Subscription..."
    gcloud pubsub subscriptions delete "$SUBSCRIPTION_ID" --project="$PROJECT_ID"
  fi

  if gsutil ls -b "$JOB_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting objects and GCS temp bucket..."
    gsutil -m -q rm -r "$JOB_BUCKET"
  fi
}

PrintHelp() {
  echo "Usage: $FILENAME (setup|pipeline|publish|cleanup|help)
  setup     Prepare GCS temp bucket and Pubsub Topics/Subscriptions.
  template  Create Dataflow job from template.
  cancel    Cancel Dataflow job.
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
  template)
    RunTemplate
    ;;
  cancel)
    RunCancel
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
