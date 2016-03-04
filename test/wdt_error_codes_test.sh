#! /bin/bash

source `dirname "$0"`/common_functions.sh
setBinaries

transfer() {
  $WDT_BIN -skip_writes > $DIR/server${TEST_COUNT}.log 2>&1 &
  pidofreceiver=$!
  $WDT_BIN -destination=$HOSTNAME -directory=$SRC_DIR \
    2>&1 | tee $DIR/client${TEST_COUNT}.log
  SENDER_STATUS=$?
  wait $pidofreceiver
  RECEIVER_STATUS=$?
  if [ $SENDER_STATUS -ne $1 ]; then
    printServerLog
    echo "Expected sender status to be $1, but got $SENDER_STATUS"
    wdtExit 1
  fi
  if [ $RECEIVER_STATUS -ne $2 ]; then
    printServerLog
    echo "Expected receiver status to be $2, but got $RECEIVER_STATUS"
    wdtExit 1
  fi
  TEST_COUNT=$((TEST_COUNT + 1))
}

STARTING_PORT=22356
WDTBIN_OPTS="-ipv6 -start_port=$STARTING_PORT -read_timeout_millis=100 \
-write_timeout_millis=100 -connect_timeout_millis=100 -transfer_id=wdt \
-max_accept_retries=10"
extendWdtOptions
WDT_BIN="$WDT_BINARY $WDTBIN_OPTS"

BASEDIR=/tmp/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d $BASEDIR/XXXXXX`
echo "Testing in $DIR"

TEST_COUNT=0

echo "Testing directory permission error"
SRC_DIR=$DIR/inaccessible_dir
mkdir $SRC_DIR
chmod 000 $SRC_DIR
transfer 7 7  # BYTE_SOURCE_READ_ERROR

echo "Testing file permission error"
SRC_DIR=$DIR/src1
mkdir $SRC_DIR
touch $SRC_DIR/inaccessible_file
chmod 000 $SRC_DIR/inaccessible_file
transfer 7 7  # BYTE_SOURCE_READ_ERROR

echo "Testing port block and file permission issue"
blockDportByRejecting $STARTING_PORT
transfer 7 7 # read error more "interesting" than transient network error
undoLastIpTableChange

echo "Test passed, removing $DIR"
rm -rf $DIR
wdtExit 0
