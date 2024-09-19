#!/bin/bash

MAIN_CLASS=baeminbo.LateDataJob

mvn compile

for USEGBK in "true" "false"
do
  for DOFN in PLAIN TIMER STATE STATE_TIMER
  do
    LOG_FILE="GBK-$USEGBK-DOFN-$DOFN.out"
    echo "Running GBK: $USEGBK, DFN: $DOFN => $LOG_FILE"
    mvn exec:java \
      -Dexec.mainClass="$MAIN_CLASS" \
      -Dexec.args="\
        --runner=DirectRunner \
        --useGBK=$USEGBK \
        --doFnClass=$DOFN \
      " > "$LOG_FILE" 2>&1 &
  done
done
