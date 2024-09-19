#!/bin/bash
set -eu
trap exit INT

PROJECT=$(gcloud config get project)
REGION="us-central1"
MAIN_CLASS=baeminbo.LateDataJob

mvn compile

for USEGBK in "true" "false"
do
  for DOFN in PLAIN TIMER STATE STATE_TIMER
  do
    # Dataflow not allow "_" in the job name.
    JOB_NAME_PART=$(echo "GBK-$USEGBK-DOFN-$DOFN" | tr '_' '-')
    mvn exec:java \
      -Dexec.mainClass="$MAIN_CLASS" \
      -Dexec.args="\
        --runner=DataflowRunner \
        --project=$PROJECT \
        --region=$REGION \
        --enableStreamingEngine \
        --experiments=disable_runner_v2 \
        --jobName=$JOB_NAME_PART-v1 \
        --useGBK=$USEGBK \
        --doFnClass=$DOFN \
      "
  done
done
