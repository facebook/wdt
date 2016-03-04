#! /bin/bash

source `dirname "$0"`/common_functions.sh
setBinaries

BASEDIR=/tmp/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d $BASEDIR/XXXXXX`
echo "Testing in $DIR"

$WDT_RECEIVER -directory $DIR -enable_download_resumption \
   -abort_after_seconds 2 2>&1 | tee $DIR/server1.log &
pidofreceiver1=$!

# sleep so that the first receiver acquires the file lock
sleep 1

# start another receiver for the same directory
$WDT_RECEIVER -directory $DIR -enable_download_resumption \
  2>&1 | tee $DIR/server2.log
STATUS=$?

wait $pidofreceiver1

if [ $STATUS -ne 25 ]; then
  echo "Second receiver should have exited with TRANSFER_LOG_ACQUIRE_ERROR \
, but exited with $STATUS. Logs in $DIR"
  exit 1
fi
echo "Test passed, removing directory $DIR"
rm -rf $DIR
exit 0
