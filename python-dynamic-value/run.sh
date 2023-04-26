#!/bin/bash

PROJECT=$(gcloud config get-value project)
REGION="us-central1"
ENV="env"

if [ ! -d "$ENV" ]; then
  echo "Creating virtual environment."
  python -m venv "$ENV"
  source "$ENV/bin/activate"
  pip install --upgrade pip
  pip install apache-beam[gcp]==2.46.0
fi

python -m pipeline_main \
  --runner=DataflowRunner \
  --project="$PROJECT" \
  --region="$REGION" \
  --today="2023-04-25"