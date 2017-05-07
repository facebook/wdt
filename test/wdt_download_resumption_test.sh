#! /bin/bash

source `dirname "$0"`/common_functions.sh

startNewTransfer() {
  if [ "$1" == "add" ] && [ $DELETE_EXTRA_FILES == "true" ]; then
    echo "Adding extra files for test ${TEST_COUNT}"
    fallocate -l 20M $SRC_DIR/extra_file${TEST_COUNT}_1
    fallocate -l 80M $SRC_DIR/extra_file${TEST_COUNT}_2
  fi
  if [ "$1" == "remove" ] && [ $DELETE_EXTRA_FILES == "true" ]; then
    echo "Removing extra files for test ${TEST_COUNT}"
    rm $SRC_DIR/extra_file${TEST_COUNT}_1 $SRC_DIR/extra_file${TEST_COUNT}_2
  fi
  $WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} -start_port=$STARTING_PORT \
  -transfer_id=$RECEIVER_ID -protocol_version=$RECEIVER_PROTOCOL_VERSION \
  -recovery_id=$RECOVERY_ID -disable_preallocation=$DISABLE_PREALLOCATION \
  >> $DIR/server${TEST_COUNT}.log 2>&1 &
  pidofreceiver=$!
  $WDTBIN_CLIENT -directory $SRC_DIR -destination $HOSTNAME \
  -start_port=$STARTING_PORT -block_size_mbytes=$BLOCK_SIZE_MBYTES \
  -transfer_id=$SENDER_ID -protocol_version=$SENDER_PROTOCOL_VERSION \
  -two_phases=$TWO_PHASES \
  |& tee -a $DIR/client${TEST_COUNT}.log &
  pidofsender=$!
}

usage="
The possible options to this script are
-s sender protocol version
-r receiver protocol version
-p start port
-d turns on file deletion
-c combination of options to run. Valid values are 1, 2, 3 and 4.
   1. pre-allocation and block-mode enabled, resumption done using transfer log
   2. pre-allocation disabled, block-mode enabled, resumption done using
      directory tree. This effectively disables resumption.
   3. pre-allocation and block-mode disabled, resumption done using directory
      tree
   4. pre-allocation enabled and block-mode disabled, resumption done using
      transfer log
"

#protocol versions, used to check version verification
#version 0 represents default version
SENDER_PROTOCOL_VERSION=0
RECEIVER_PROTOCOL_VERSION=0

DISABLE_PREALLOCATION=false
BLOCK_SIZE_MBYTES=10
RESUME_USING_DIR_TREE=false

RECOVERY_ID="foo"

STARTING_PORT=25000

DELETE_EXTRA_FILES=false

while getopts ":c:s:p:r:dh" opt; do
  case $opt in
    s) SENDER_PROTOCOL_VERSION="$OPTARG"
    ;;
    r) RECEIVER_PROTOCOL_VERSION="$OPTARG"
    ;;
    p) STARTING_PORT="$OPTARG"
    ;;
    d) DELETE_EXTRA_FILES=true
    ;;
    c)
      case $OPTARG in
        1) echo "pre-allocation and block-mode enabled, resumption using \
transfer log"
        # no need to change anything, settings are initialized for this case
        ;;
        2) echo "pre-allocation disabled, block-mode enabled, resumption done \
using directory tree, resumption is effectively disabled"
           DISABLE_PREALLOCATION=true
           RESUME_USING_DIR_TREE=true
        ;;
        3) echo "pre-allocation and block-mode disabled, resumption done \
using directory tree"
           BLOCK_SIZE_MBYTES=0
           DISABLE_PREALLOCATION=true
           RESUME_USING_DIR_TREE=true
        ;;
        4) echo "pre-allocation enabled, block-mode disabled, resumption done \
using transfer log"
        BLOCK_SIZE_MBYTES=0
        ;;
        *) echo "Invalid combination, valid values are 1, 2, 3 and 4"
           wdtExit 1
        ;;
      esac
    ;;
    h) echo "$usage"
        wdtExit
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

setBinaries

if [ -z "$TWO_PHASES" ]; then
  TWO_PHASES=false
fi

echo "sender protocol version $SENDER_PROTOCOL_VERSION, receiver protocol \
version $RECEIVER_PROTOCOL_VERSION"

if [ $RECEIVER_PROTOCOL_VERSION -ne 0 ] && [ $RECEIVER_PROTOCOL_VERSION -lt 23 \
] && [ -z $NO_ENCRYPT ]; then
  echo "Can not support encryption for receiver with version \
$RECEIVER_PROTOCOL_VERSION"
  # exit with 0 to make the test pass
  exit 0
fi

threads=4
ERROR_COUNT=5

#sender and receiver id, used to check transfer-id verification
SENDER_ID="123456"
RECEIVER_ID="123456"

WDTBIN_OPTS="-ipv6 -num_ports=$threads -full_reporting \
-max_mbytes_per_sec=25 -run_as_daemon=false \
-full_reporting -read_timeout_millis=500 -write_timeout_millis=500 \
-enable_download_resumption -treat_fewer_port_as_error \
-resume_using_dir_tree=$RESUME_USING_DIR_TREE -enable_perf_stat_collection \
-connect_timeout_millis 100 -delete_extra_files=$DELETE_EXTRA_FILES \
-exit_on_bad_flags=false"
extendWdtOptions
WDTBIN_CLIENT="$WDT_SENDER $WDTBIN_OPTS -buffer_size=2097152"
WDTBIN_SERVER="$WDT_RECEIVER $WDTBIN_OPTS -avg_mbytes_per_sec=80 "

BASEDIR=/dev/shm/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d $BASEDIR/XXXXXX`
echo "Testing in $DIR"

#pkill -x wdt

SRC_DIR=$DIR/src

generateRandomFiles "$SRC_DIR" 4 # 4 Mb for smallest file

TEST_COUNT=0

echo "Testing that connection failure results in failed transfer"
# first create a deep directory structure
# this is done so that directory thread gets aborted before discovering any
# file
CURDIR=`pwd`
cd $DIR
for ((i = 0; i < 100; i++))
do
  mkdir d
  cd d
done
touch file
cd $CURDIR
# start the sender without starting receiver and set connect retries to 1
$WDT_SENDER -directory $DIR/d -destination $HOSTNAME -max_retries 1 \
-start_port $STARTING_PORT -num_ports $threads
checkLastCmdStatusExpectingFailure
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test(1)"
startNewTransfer add
sleep 5
killCurrentTransfer
# rm a file to create an invalid log entry
rm -f $DIR/dst${TEST_COUNT}/file0
startNewTransfer remove
sleep 5
killCurrentTransfer
startNewTransfer
waitForTransferEnd
verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test(2)"
startNewTransfer add
sleep 5
killCurrentTransfer
# change the file size in the receiver side
fallocate -l 70M $DIR/dst${TEST_COUNT}/file0
startNewTransfer remove
sleep 5
killCurrentTransfer
startNewTransfer
waitForTransferEnd
verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test with log truncation"
startNewTransfer
sleep 5
killCurrentTransfer
LOG_FILE=$DIR/dst${TEST_COUNT}/.wdt.log
LOG_SIZE=`stat --printf="%s" $LOG_FILE`
NEW_LOG_SIZE=$(( LOG_SIZE / 2 ))
truncate -s $NEW_LOG_SIZE $LOG_FILE
startNewTransfer
waitForTransferEnd
verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test with log corruption"
startNewTransfer
sleep 5
killCurrentTransfer
LOG_FILE=$DIR/dst${TEST_COUNT}/.wdt.log
# write some garbage at the end of the log
echo "abc" >> $LOG_FILE
startNewTransfer
sleep 2
killCurrentTransfer
startNewTransfer
waitForTransferEnd

# check whether we have .wdt.log.bug present or not
stat "${LOG_FILE}.bug"
if [ $? -ne 0 ]; then
  printServerLog
  echo "Transfer log corrupted, but not detected by wdt, failing test"
  wdtExit 1
fi
rm "${LOG_FILE}.bug"

verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

changePreallocationSettings() {
  if [ $DISABLE_PREALLOCATION == "false" ]; then
    DISABLE_PREALLOCATION="true"
  else
    DISABLE_PREALLOCATION="false"
  fi
}

if [ $RESUME_USING_DIR_TREE == "false" ]; then
  echo "Download resumption with pre-allocation settings changed in the middle"
  startNewTransfer
  sleep 5
  killCurrentTransfer
  # change pre-allocation settings
  changePreallocationSettings
  startNewTransfer
  sleep 5
  killCurrentTransfer
  # change it back
  changePreallocationSettings
  startNewTransfer
  waitForTransferEnd
  verifyTransferAndCleanup
  TEST_COUNT=$((TEST_COUNT + 1))
fi

echo "Download resumption with network error test"
startNewTransfer add
sleep 10
killCurrentTransfer
startNewTransfer remove
simulateNetworkGlitchesByDropping
waitForTransferEnd
verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

if [ $BLOCK_SIZE_MBYTES -gt 0 ]; then
  # try to change the block size and also disable block mode
  NEW_BLOCK_SIZES=(8 0)
else
  # try to enable block mode
  NEW_BLOCK_SIZES=(8)
fi

for NEW_BLOCK_SIZE in ${NEW_BLOCK_SIZES[*]}
do
  echo "Download resumption with network error and block size change test(1)"
  startNewTransfer
  simulateNetworkGlitchesByDropping
  killCurrentTransfer
  # change the block size for next transfer
  PRE_BLOCK_SIZE=$BLOCK_SIZE_MBYTES
  echo "Changing block size to $NEW_BLOCK_SIZE"
  BLOCK_SIZE_MBYTES=$NEW_BLOCK_SIZE
  startNewTransfer
  waitForTransferEnd
  verifyTransferAndCleanup
  echo "Resetting block size to $PRE_BLOCK_SIZE"
  BLOCK_SIZE_MBYTES=$PRE_BLOCK_SIZE
  TEST_COUNT=$((TEST_COUNT + 1))

  echo "Download resumption with network error and block size change test(2)"
  startNewTransfer
  simulateNetworkGlitchesByDropping
  killCurrentTransfer
  # change the block size for next transfer
  PRE_BLOCK_SIZE=$BLOCK_SIZE_MBYTES
  echo "Changing block size to $NEW_BLOCK_SIZE"
  BLOCK_SIZE_MBYTES=$NEW_BLOCK_SIZE
  startNewTransfer
  sleep 5
  killCurrentTransfer
  echo "Resetting block size to $PRE_BLOCK_SIZE"
  BLOCK_SIZE_MBYTES=$PRE_BLOCK_SIZE
  startNewTransfer
  waitForTransferEnd
  verifyTransferAndCleanup
  TEST_COUNT=$((TEST_COUNT + 1))
done

echo "Download resumption test for append-only file"
# truncate file0
cp $DIR/src/file0 $DIR/file0.bak
truncate -s 10M $DIR/src/file0
startNewTransfer
sleep 5
killCurrentTransfer
# restore file0
mv $DIR/file0.bak $DIR/src/file0
startNewTransfer
sleep 5
killCurrentTransfer
startNewTransfer
waitForTransferEnd
verifyTransferAndCleanup
TEST_COUNT=$((TEST_COUNT + 1))

if [ $RESUME_USING_DIR_TREE == "true" ] && [ $BLOCK_SIZE_MBYTES -eq 0 ]; then
  # combination 3
  echo "Test with download resumption disabled and block mode enabled \
for the sender in the middle"
  startNewTransfer
  sleep 2
  killCurrentTransfer
  PREV_WDT_CLIENT=$WDTBIN_CLIENT
  BLOCK_SIZE_MBYTES=1
  WDTBIN_CLIENT="$WDTBIN_CLIENT -enable_download_resumption=false"
  startNewTransfer
  simulateNetworkGlitchesByDropping
  killCurrentTransfer
  WDTBIN_CLIENT=$PREV_WDT_CLIENT
  BLOCK_SIZE_MBYTES=0
  startNewTransfer
  waitForTransferEnd
  verifyTransferAndCleanup
  TEST_COUNT=$((TEST_COUNT + 1))
fi

# abort set-up
ABORT_AFTER_SECONDS=5
ABORT_CHECK_INTERVAL_MILLIS=100
ABORT_AFTER_MILLIS=$((ABORT_AFTER_SECONDS * 1000))
EXPECTED_TRANSFER_DURATION_MILLIS=$((ABORT_AFTER_MILLIS + \
ABORT_CHECK_INTERVAL_MILLIS))
# add 800ms overhead. We need this because we can not control timeouts for disk
# writes
EXPECTED_TRANSFER_DURATION_MILLIS=$((EXPECTED_TRANSFER_DURATION_MILLIS + 800))

echo "Abort timing test(1) - Sender side abort"
WDTBIN_CLIENT_OLD=$WDTBIN_CLIENT
WDTBIN_CLIENT+=" -abort_check_interval_millis=$ABORT_CHECK_INTERVAL_MILLIS \
-abort_after_seconds=$ABORT_AFTER_SECONDS"
START_TIME_MILLIS=`date +%s%3N`
startNewTransfer
wait $pidofsender
END_TIME_MILLIS=`date +%s%3N`
wait $pidofreceiver
DURATION=$((END_TIME_MILLIS - START_TIME_MILLIS))
echo "Abort timing test, transfer duration ${DURATION} ms, expected duration \
${EXPECTED_TRANSFER_DURATION_MILLIS} ms."
if (( $DURATION > $EXPECTED_TRANSFER_DURATION_MILLIS \
  || $DURATION < $ABORT_AFTER_MILLIS )); then
  echo "Abort timing test failed, exiting"
  printServerLog
  wdtExit 1
fi
WDTBIN_CLIENT=$WDTBIN_CLIENT_OLD
removeDestination
TEST_COUNT=$((TEST_COUNT + 1))

echo "Abort timing test(2) - Receiver side abort"
WDTBIN_SERVER_OLD=$WDTBIN_SERVER
WDTBIN_SERVER+=" -abort_check_interval_millis=$ABORT_CHECK_INTERVAL_MILLIS \
-abort_after_seconds=$ABORT_AFTER_SECONDS"
# Block a port to the beginning
blockDportByDropping "$STARTING_PORT"
START_TIME_MILLIS=`date +%s%3N`
startNewTransfer
wait $pidofreceiver
END_TIME_MILLIS=`date +%s%3N`
wait $pidofsender
DURATION=$((END_TIME_MILLIS - START_TIME_MILLIS))
echo "Abort timing test, transfer duration ${DURATION} ms, expected duration \
${EXPECTED_TRANSFER_DURATION_MILLIS} ms."
undoLastIpTableChange
if (( $DURATION > $EXPECTED_TRANSFER_DURATION_MILLIS \
  || $DURATION < $ABORT_AFTER_MILLIS )); then
  echo "Abort timing test failed, exiting"
  printServerLog
  wdtExit 1
fi
WDTBIN_SERVER=$WDTBIN_SERVER_OLD
removeDestination
TEST_COUNT=$((TEST_COUNT + 1))

echo "Transfer-id mismatch test"
SENDER_ID_OLD=$SENDER_ID
SENDER_ID="abcdef"
startNewTransfer
waitForTransferEndExpectingFailure
SENDER_ID=$SENDER_ID_OLD
removeDestination
TEST_COUNT=$((TEST_COUNT + 1))

# create another src directory full of files with the same name and size as the
# actual src directory
mkdir -p $DIR/src1
cd $DIR/src1
for ((i = 0; i < 4; i++))
do
  fallocate -l 16M sample${i}
done
for ((i = 0; i < 16; i++))
do
  fallocate -l 64M file${i}
done
cd -

startAndKillIpv4Transfer() {
  PREV_WDT_CLIENT=$WDTBIN_CLIENT
  PREV_WDT_SERVER=$WDTBIN_SERVER
  WDTBIN_CLIENT+=" -ipv6=false -ipv4"
  WDTBIN_SERVER+=" -ipv6=false -ipv4"
  startNewTransfer
  sleep 5
  killCurrentTransfer
  # change it back to IPV6
  WDTBIN_CLIENT=$PREV_WDT_CLIENT
  WDTBIN_SERVER=$PREV_WDT_SERVER
}

echo "Test hostname mismatch(1)"
# start first transfer
startAndKillIpv4Transfer
# change src directory
SRC_DIR=$DIR/src1
startNewTransfer
waitForTransferEnd
SRC_DIR=$DIR/src
verifyTransferAndCleanup true
TEST_COUNT=$((TEST_COUNT + 1))

echo "Test hostname mismatch(2)"
# start first transfer
startAndKillIpv4Transfer
# change src directory
SRC_DIR=$DIR/src1
startNewTransfer
sleep 2
killCurrentTransfer
startNewTransfer
waitForTransferEnd
SRC_DIR=$DIR/src
verifyTransferAndCleanup true
TEST_COUNT=$((TEST_COUNT + 1))

echo "Test recovery-id mismatch"
startNewTransfer
sleep 5
killCurrentTransfer
# change recovery-id and src directory
RECOVERY_ID="bar"
SRC_DIR=$DIR/src1
startNewTransfer
sleep 2
killCurrentTransfer
startNewTransfer
waitForTransferEnd
SRC_DIR=$DIR/src
verifyTransferAndCleanup true
TEST_COUNT=$((TEST_COUNT + 1))

echo "Good run, deleting logs in $DIR"
rm -rf "$DIR"

wdtExit 0
