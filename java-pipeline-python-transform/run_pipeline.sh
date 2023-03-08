#!/bin/bash

trap exit INT
set -eu

BASE_DIR="$(cd $(dirname $0); pwd)"
MAIN_CLASS=baeminbo.MyJavaPipeline
PROJECT="$(gcloud config get-value project)"
REGION="us-central1"
EXTRA_PACKAGES="$BASE_DIR/python/dist/mytransforms-1.0.0.tar.gz"

cd "$BASE_DIR"

pushd python
echo "Build python package"
python setup.py sdist
popd

pushd java
echo "Compile java pipeline."
mvn clean compile package
echo "Run java pipeline."
java -cp target/java-pipeline-python-transform-bundled-1.0.0.jar \
  $MAIN_CLASS \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --region=$REGION \
  --pythonExtraPackages="$EXTRA_PACKAGES" \
  --printerName=LogPrinter
popd
