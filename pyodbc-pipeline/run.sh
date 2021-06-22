#!/bin/bash

virtualenv --python=python3.6 env

source env/bin/activate

pip install apache-beam[gcp]==2.23.0

python pyodbc_pipeline.py \
 --runner=DataflowRunner \
 --project=baeminbo-2021 \
 --region=us-central1 \
 --temp_location=gs://baeminbo-2021/dataflow/temp \
 --setup_file=./setup.py