#! /bin/bash

source `dirname "$0"`/common_functions.sh

setBinaries
setDirectory
generateRandomFiles $SRC_DIR 16384

WDTBIN_OPTS="-start_port 0 -fork -abort_after_seconds 3"
extendWdtOptions

TEST_COUNT=0
# Test stdin pipe:

( $WDT_RECEIVER $WDTBIN_OPTS -directory $DIR/dst${TEST_COUNT} \
    2> "$DIR/server${TEST_COUNT}.log" ; cd $SRC_DIR; ls -1) \
 | $WDT_SENDER $WDTBIN_OPTS -directory $SRC_DIR -manifest - - \
    2>&1 | tee "$DIR/client${TEST_COUNT}.log"

checkLastCmdStatus
verifyTransferAndCleanup

TEST_COUNT=1

( $WDT_RECEIVER $WDTBIN_OPTS -directory $DIR/dst${TEST_COUNT} \
    2> "$DIR/server${TEST_COUNT}.log" ; cd $SRC_DIR ) \
 | $WDT_SENDER $WDTBIN_OPTS -directory $SRC_DIR -manifest - - \
    2>&1 | tee "$DIR/client${TEST_COUNT}.log"

checkLastCmdStatus

if [ $(ls -1 $DIR/dst${TEST_COUNT} | wc -l) -ne 0 ] ; then
  echo "This shouldnt have transferred any files (empty fileInfo)"
  wdtExit 1
fi

echo "Test passed, deleting directory $DIR"
rm -rf "$DIR"
wdtExit 0
