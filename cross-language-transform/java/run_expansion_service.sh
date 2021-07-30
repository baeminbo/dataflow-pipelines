#!/bin/bash

cd $(dirname $0) || exit

MAIN_CLASS=org.apache.beam.sdk.expansion.service.ExpansionService
PORT=8765

mvn exec:java -Dexec.mainClass=$MAIN_CLASS -Dexec.args="$PORT"