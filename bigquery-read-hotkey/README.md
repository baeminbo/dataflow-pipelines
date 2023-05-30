BigQuery Read Hotkey Pipeline
====

This is an exercise pipeline for troubleshooting training of Dataflow job. The
pipeline reads a BigQuery table and writes to GCS, which takes an unexpected
long time to finish. What is the root cause and how to fix it?

* [run_job.sh](run_job.sh): Create a Dataflow job
* [build_template.sh](build_template.sh) and [run_template.sh](run_template.sh):
  Scripts to create a Classic Dataflow template and execute the template. 

