#!/bin/bash

FILENAME=$(basename $0)
BYOP="byop11"

LDAP="$USER"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"

GCS_BUCKET="${GCS_BUCKET:-gs://$PROJECT_ID-$BYOP}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE:-n1-highmem-8}"
SUBSCRIPTION_JOB_ID="${SUBSCRIPTION_JOB_ID:-$BYOP-subscription}"
JOB_NAME="${LDAP}-${BYOP}"
REGION="us-central1"
ENABLE_STREAMING_ENGINE="${ENABLE_STREAMING_ENGINE:-false}"
BEAM_VERSION="${BEAM_VERSION}" # Referred in pom.xml

RunSetup() {
  if ! gsutil ls -b "$GCS_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Creating GCS temp bucket..."
    gsutil mb "$GCS_BUCKET"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create bucket."
      exit 1
    fi
  fi

  if ! gcloud pubsub subscriptions describe "$SUBSCRIPTION_JOB_ID" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Creating Pubsub Source Subscription..."
    gcloud pubsub subscriptions create "$SUBSCRIPTION_JOB_ID" --topic=projects/pubsub-public-data/topics/taxirides-realtime --project="$PROJECT_ID"
    if [ $? -ne 0 ]; then
      echo "$FILENAME: Fatal. Failed to create source subscription."
      exit 1
    fi
  fi
}

RunPipeline() {
  TEMP_LOCATION="$GCS_BUCKET/dataflow/temp"
  STAGING_LOCATION="$GCS_BUCKET/dataflow/staging"

  SUBSCRIPTION_NAME="projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_JOB_ID"

  mvn compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.SemaphoreIllegalArgumentExceptionPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --jobName=$JOB_NAME \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --tempLocation=$TEMP_LOCATION \
    --stagingLocation=$STAGING_LOCATION \
    --subscription=$SUBSCRIPTION_NAME \
    --enableStreamingEngine=$ENABLE_STREAMING_ENGINE \
    "
}

RunCancel() {
  GetRunningJobId() { # echo empty string if not running
    JOB_ID=$(gcloud dataflow jobs list --filter="name=$JOB_NAME" --project="$PROJECT_ID" --region=$REGION --status=active --format='value(id)')
    echo "$JOB_ID"
  }

  JOB_ID=$(GetRunningJobId)
  if [ -n "$JOB_ID" ]; then
    echo "$FILENAME: Cancelling Dataflow job..."
    gcloud dataflow jobs cancel "$JOB_ID" --region="$REGION" --project="$PROJECT_ID"

    for _ in $(seq 0 16); do
      if [ -n "$(GetRunningJobId)" ]; then
        sleep_time="10.0"
        echo "$FILENAME: Waiting for job completion. Will sleep $sleep_time second(s)"
        sleep "$sleep_time"
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

RunCleanup() {
  RunCancel

  if gcloud pubsub subscriptions describe "$SUBSCRIPTION_JOB_ID" --project="$PROJECT_ID" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting Pusbub Source Subscription..."
    gcloud pubsub subscriptions delete "$SUBSCRIPTION_JOB_ID" --project="$PROJECT_ID"
  fi

  if gsutil ls -b "$GCS_BUCKET" >/dev/null 2>&1; then
    echo "$FILENAME: Deleting objects and GCS temp bucket..."
    gsutil -m -q rm -r "$GCS_BUCKET"
  fi
}

# Used to upload templates. Hide from users.
_UploadTemplates() {
  staging_location="gs://dataflow-mastery/staging"

  template_location="gs://dataflow-mastery/templates/byop-11"
  echo "$FILENAME: Upload default template (2.19.0 without Streaming Engine) to $template_location"
  mvn clean compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.SemaphoreIllegalArgumentExceptionPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --stagingLocation=$staging_location \
    --templateLocation=$template_location \
    "

  template_location="gs://dataflow-mastery/templates/byop-11-beam_2.22.0"
  echo "$FILENAME: Upload 2.22.0 template (2.22.0 without Streaming Engine) to $template_location"
  BEAM_VERSION=2.22.0 mvn clean compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.SemaphoreIllegalArgumentExceptionPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --stagingLocation=$staging_location \
    --templateLocation=$template_location \
    "

  template_location="gs://dataflow-mastery/templates/byop-11-streaming_engine"
  echo "$FILENAME: Upload Streaming Engine template (2.19.0 with Streaming Engine) to $template_location"
  mvn clean compile exec:java \
    -Pdataflow-runner \
    -Dexec.mainClass=baeminbo.SemaphoreIllegalArgumentExceptionPipeline \
    -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --workerMachineType=$WORKER_MACHINE_TYPE \
    --stagingLocation=$staging_location \
    --templateLocation=$template_location \
    --enableStreamingEngine \
    "

  template_runner=$(dirname "$0")/template_runner.sh
  echo "$FILENAME: Upload template runner to gs://dataflow-mastery/data/byop-11/template_runner.sh"
  gsutil cp "$template_runner" gs://dataflow-mastery/data/byop-11/template_runner.sh
}

PrintHelp() {
  echo "Usage: $FILENAME (setup|pipeline|publish|cleanup|help)
  setup     Prepare GCS temp bucket and Pubsub Topics/Subscriptions.
  pipeline  Create Dataflow job.
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
  pipeline)
    RunPipeline
    ;;
  cancel)
    RunCancel
    ;;
  _upload_templates)
    _UploadTemplates
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
