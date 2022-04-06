#!/bin/bash
trap exit INT

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"

MAIN_CLASS="baeminbo.Log4j2LoggingPipeline"
REGION="us-central1"

BEAM_VERSION=2.36.0 # Or, try "2.35.0" for older beam versions with old log4j-to-slf4j dependency (2.5)
LOG4J_VERSION=2.17.1 # Or, try "2.5" that works for beam 2.35.0 or older

JOB_NAME_PREFIX=log4jtoslf4j-test
JOB_NAME_BEAM_VERSION=$(echo "beam-$BEAM_VERSION" | tr \. -) # "beam-2.36.0" -> "beam-2-36-0"
JOB_NAME_LOG4J_VERSION=$(echo "log4j-$LOG4J_VERSION" | tr \. -) # "log4j-2.17.1" -> "log4j-2-17-1"

mvn clean compile exec:java \
  -Dbeam.version=$BEAM_VERSION \
  -Dlog4j.version=$LOG4J_VERSION \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --jobName=$JOB_NAME_PREFIX-$JOB_NAME_BEAM_VERSION-$JOB_NAME_LOG4J_VERSION-batch \
    "

mvn clean compile exec:java \
  -Dbeam.version=$BEAM_VERSION \
  -Dlog4j.version=$LOG4J_VERSION \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --streaming \
    --jobName=$JOB_NAME_PREFIX-$JOB_NAME_BEAM_VERSION-$JOB_NAME_LOG4J_VERSION-streaming \
    "

# Runner v2 needs log4j-to-slf4j in pipeline packages.
# See profile "add-log4j-to-slf4j" in pom.xml
mvn clean compile exec:java \
  -Padd-log4j-to-slf4j \
  -Dbeam.version=$BEAM_VERSION \
  -Dlog4j.version=$LOG4J_VERSION \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --experiments=use_runner_v2 \
    --jobName=$JOB_NAME_PREFIX-$JOB_NAME_BEAM_VERSION-$JOB_NAME_LOG4J_VERSION-runner-v2-batch \
    "

# Runner v2 needs log4j-to-slf4j in pipeline packages.
# See profile "add-log4j-to-slf4j" in pom.xml
mvn clean compile exec:java \
  -Padd-log4j-to-slf4j \
  -Dbeam.version=$BEAM_VERSION \
  -Dlog4j.version=$LOG4J_VERSION \
  -Dexec.mainClass=$MAIN_CLASS \
  -Dexec.args="--runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --experiments=use_runner_v2 \
    --streaming \
    --jobName=$JOB_NAME_PREFIX-$JOB_NAME_BEAM_VERSION-$JOB_NAME_LOG4J_VERSION-runner-v2-streaming \
    "


