#!/bin/bash

# Please run 'gcloud auth login' and 'gcloud auth application-default login'
# to authenticate your account for the following commands.

PROJECT_ID=$(gcloud config get-value project)

INSTANCE_ID=bt-test1
DISPLAY_NAME=$INSTANCE_ID
CLUSTER_ID=${INSTANCE_ID}-cluster1
CLUSTER_ZONE=us-central1-a
TABLE_ID=table1
COLUMN_FAMILY=cf1
APP_PROFILE_ID=dfapp

STAGING_LOCATION=gs://$PROJECT_ID/dataflow/staging
TEMPLATE_LOCATION=gs://$PROJECT_ID/datafow/templates/bigtable-write
JOB_NAME=bigtable-write

echo "**** Create instance: $INSTANCE_ID ****"
cbt -project=$PROJECT_ID createinstance $INSTANCE_ID $DISPLAY_NAME $CLUSTER_ID  $CLUSTER_ZONE 1 SSD

echo "**** Create table: $TABLE_ID with column family: $COLUMN_FAMILY ****"
cbt -project=$PROJECT_ID -instance=$INSTANCE_ID createtable  $TABLE_ID families=$COLUMN_FAMILY

echo "**** Create app profile: $APP_PROFILE_ID"
cbt -instance=$INSTANCE_ID createappprofile $INSTANCE_ID $APP_PROFILE_ID "Test profile for Dataflow jobs" route-any

echo "**** Create Dataflow template ****"
mvn -Pdataflow-runner clean compile exec:java -Dexec.mainClass=baeminbo.BigtableWritePipeline \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=us-central1 \
    --stagingLocation=$STAGING_LOCATION \
    --templateLocation=$TEMPLATE_LOCATION"

echo "**** Run Dataflow template job ****"
gcloud dataflow jobs run $JOB_NAME \
  --gcs-location=$TEMPLATE_LOCATION \
  --region=us-central1 \
  --parameters=bigtableProject=$PROJECT_ID,bigtableInstance=$INSTANCE_ID,bigtableTable=$TABLE_ID,bigtableColumnFamily=$COLUMN_FAMILY,bigtableAppProfileId=$APP_PROFILE_ID




