#!/bin/bash
trap exit int
set -eu

BASEDIR="$(cd $(dirname $0); pwd)"
PROJECT=$(gcloud config get-value project)
REGION=us-central1
GCB_SOURCE=build.tar.gz
FLEX_TEMPLATE_IMAGE_URL="$REGION-docker.pkg.dev/$PROJECT/dataflow/flex/java-pipeline-python-transform"
FLEX_TEMPLATE_FILE_GCS_PATH="gs://$PROJECT/dataflow/flex/java-pipeline-python-transform"

cd "$BASEDIR"

pushd python
echo "## Build python package"
python setup.py sdist
popd

pushd java
echo "## Compile java pipeline."
mvn clean compile package
popd

echo "## Package Cloud Build source."
tar -czf "$GCB_SOURCE" Dockerfile \
  -C "$BASEDIR/java/target" java-pipeline-python-transform-bundled-1.0.0.jar \
  -C "$BASEDIR/python/dist" mytransforms-1.0.0.tar.gz

echo "## Run Cloud Build for flex template image."
gcloud builds submit --project="$PROJECT" --region="$REGION" \
  --tag="$FLEX_TEMPLATE_IMAGE_URL" "$GCB_SOURCE"

echo "## Create Dataflow Flex template."
gcloud dataflow flex-template build "$FLEX_TEMPLATE_FILE_GCS_PATH" \
  --project="$PROJECT" \
  --image="$FLEX_TEMPLATE_IMAGE_URL" \
  --sdk-language=JAVA