#!/bin/bash
SECRET_NAME="" # Set your secret name in GCP Secret Manager
SECRET_VERSION="latest" # Set secret version.
SERVICE_ACCOUNT="" # Set Dataflow worker service account which can access the secret

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
REGION="us-central1"
SECRET_VERSION_NAME="projects/$PROJECT_ID/secrets/${SECRET_NAME:?SECRET_NAME empyt or not set}/versions/$SECRET_VERSION"

MAIN_CLASS="baeminbo.SecretManagerPipeline"
echo mvn -Pdataflow-runner clean compile exec:java -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --secretVersionName=$SECRET_VERSION_NAME \
    ${SERVICE_ACCOUNT:+--serviceAccount=$SERVICE_ACCOUNT} \
    "
