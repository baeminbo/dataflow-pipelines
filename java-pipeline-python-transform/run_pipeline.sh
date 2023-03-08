#!/bin/bash

trap exit INT
set -eu
cd $(dirname $0)


MAIN_CLASS=baeminbo.MyJavaPipeline
PROJECT="$(gcloud config get-value project)"
REGION="us-central1"
EXTRA_PACKAGES=../python/dist/mytranforms-1.0.0.tar.gz

pushd python

echo "Build python package"

python setup.py sdist

popd

pushd python

echo "Compile java pipeline."
mvn clean compile package

java -cp target/java-pipeline-python-transform-bundled-1.0.0.jar \
  $MAIN_CLASS \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --pythonExtraPackages="$EXTRA_PACKAGES"

popd
