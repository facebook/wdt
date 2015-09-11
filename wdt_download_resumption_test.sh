#! /bin/bash

set -o pipefail

restoreIptable() {
  if [ -e "$DIR/ip6table" ]; then
    # try to restore only if iptable was modified
    sudo ip6tables-restore < $DIR/ip6table
  fi
}

printServerLog() {
  echo "Server log($DIR/server${TEST_COUNT}.log):"
  cat $DIR/server${TEST_COUNT}.log
}

checkLastCmdStatus() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -ne 0 ] ; then
    restoreIptable
    echo "exiting abnormally with status $LAST_STATUS - aborting/failing test"
    printServerLog
    exit $LAST_STATUS
  fi
}

checkLastCmdStatusExpectingFailure() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -eq 0 ] ; then
    restoreIptable
    echo "expecting wdt failure, but transfer was successful, failing test"
    printServerLog
    exit 1
  fi
}

startNewTransfer() {
  $WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} -start_port=$STARTING_PORT \
  -transfer_id=$RECEIVER_ID -protocol_version=$RECEIVER_PROTOCOL_VERSION \
  >> $DIR/server${TEST_COUNT}.log 2>&1 &
  pidofreceiver=$!
  $WDTBIN_CLIENT -directory $SRC_DIR -destination $HOSTNAME \
  -start_port=$STARTING_PORT -block_size_mbytes=$BLOCK_SIZE_MBYTES \
  -transfer_id=$SENDER_ID -protocol_version=$SENDER_PROTOCOL_VERSION \
  |& tee -a $DIR/client${TEST_COUNT}.log &
  pidofsender=$!
}

waitForTransferEnd() {
  wait $pidofreceiver
  checkLastCmdStatus
  wait $pidofsender
  checkLastCmdStatus
}

waitForTransferEndWithoutCheckingStatus() {
  wait $pidofreceiver
  wait $pidofsender
}

waitForTransferEndExpectingFailure() {
  wait $pidofreceiver
  checkLastCmdStatusExpectingFailure
  wait $pidofsender
  checkLastCmdStatusExpectingFailure
}

killCurrentTransfer() {
  kill -9 $pidofsender
  kill -9 $pidofreceiver
  wait $pidofsender
  wait $pidofreceiver
}

usage="
The possible options to this script are
-s sender protocol version
-r receiver protocol version
-c combination of options to run. Valid values are 1, 2 and 3.
   1. pre-allocation and block-mode enabled, resumption done using transfer log
   2. pre-allocation disabled, block-mode enabled, resumption done using
      directory tree. This effectively disables resumption.
   3. pre-allocation and block-mode disabled, resumption done using directory
      tree
"

#protocol versions, used to check version verification
#version 0 represents default version
SENDER_PROTOCOL_VERSION=0
RECEIVER_PROTOCOL_VERSION=0

DISABLE_PREALLOCATION=false
BLOCK_SIZE_MBYTES=10
RESUME_USING_DIR_TREE=false

if [ "$1" == "-h" ]; then
  echo "$usage"
  exit 0
fi
while getopts ":c:s:r:h:" opt; do
  case $opt in
    s) SENDER_PROTOCOL_VERSION="$OPTARG"
    ;;
    r) RECEIVER_PROTOCOL_VERSION="$OPTARG"
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
        *) echo "Invalid combination, valid values are 1, 2 and 3"
           exit 1
        ;;
      esac
    ;;
    h) echo "$usage"
       exit
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

echo "sender protocol version $SENDER_PROTOCOL_VERSION, receiver protocol \
version $RECEIVER_PROTOCOL_VERSION"

threads=4
STARTING_PORT=22500
ERROR_COUNT=10

#sender and receiver id, used to check transfer-id verification
SENDER_ID="123456"
RECEIVER_ID="123456"

WDTBIN_OPTS="-ipv6 -num_ports=$threads -full_reporting \
-avg_mbytes_per_sec=40 -max_mbytes_per_sec=50 -run_as_daemon=false \
-full_reporting -read_timeout_millis=500 -write_timeout_millis=500 \
-enable_download_resumption -keep_transfer_log=false \
-treat_fewer_port_as_error -disable_preallocation=$DISABLE_PREALLOCATION \
-resume_using_dir_tree=$RESUME_USING_DIR_TREE"
WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"
WDTBIN_CLIENT="$WDTBIN -recovery_id=abcdef"
WDTBIN_SERVER=$WDTBIN

BASEDIR=/tmp/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d $BASEDIR/XXXXXX`
echo "Testing in $DIR"

#pkill -x wdt

mkdir -p $DIR/src
cd $DIR/src
for ((i = 0; i < 4; i++))
do
  dd if=/dev/urandom of=sample${i} bs=16M count=1
done
# we will generate 1G of random data. 16 files each of size 64mb.
for ((i = 0; i < 16; i++))
do
  touch file${i}
  for ((j = 0; j < 4; j++))
  do
    sample=$((RANDOM % 4))
    cat sample${sample} >> file${i}
  done
done
cd -
SRC_DIR=$DIR/src
TEST_COUNT=0
# Tests for which there is no need to verify source and destination md5s
TESTS_SKIP_VERIFICATION=()

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
_bin/wdt/wdt -directory $DIR/d -destination $HOSTNAME -max_retries 1 \
-start_port $STARTING_PORT -num_ports $threads
checkLastCmdStatusExpectingFailure
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test(1)"
startNewTransfer
sleep 5
killCurrentTransfer
# rm a file to create an invalid log entry
rm -f $DIR/dst${TEST_COUNT}/file0
startNewTransfer
waitForTransferEnd
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test(2)"
startNewTransfer
sleep 5
killCurrentTransfer
# change the file size in the receiver side
fallocate -l 70M $DIR/dst${TEST_COUNT}/file0
startNewTransfer
sleep 5
killCurrentTransfer
startNewTransfer
waitForTransferEnd
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption with network error test(3)"
startNewTransfer
sleep 10
killCurrentTransfer
startNewTransfer
for ((i = 1; i <= ERROR_COUNT; i++))
do
  sleep 0.3 # sleep for 300ms
  port=$((STARTING_PORT + RANDOM % threads))
  echo "blocking $port"
  sudo ip6tables-save > $DIR/ip6table
  if [ $(($i % 2)) -eq 0 ]; then
    sudo ip6tables -A INPUT -p tcp --sport $port -j DROP
  else
    sudo ip6tables -A INPUT -p tcp --dport $port -j DROP
  fi
  sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep more
            # than that
  echo "unblocking $port"
  sudo ip6tables-restore < $DIR/ip6table
done
waitForTransferEnd
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption with network error test(4)"
startNewTransfer
for ((i = 1; i <= ERROR_COUNT; i++))
do
  sleep 0.3 # sleep for 300ms
  port=$((STARTING_PORT + RANDOM % threads))
  echo "blocking $port"
  sudo ip6tables-save > $DIR/ip6table
  if [ $(($i % 2)) -eq 0 ]; then
    sudo ip6tables -A INPUT -p tcp --sport $port -j DROP
  else
    sudo ip6tables -A INPUT -p tcp --dport $port -j DROP
  fi
  sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep more
            # than that
  echo "unblocking $port"
  sudo ip6tables-restore < $DIR/ip6table
done
killCurrentTransfer
# change the block size for next transfer
PRE_BLOCK_SIZE=$BLOCK_SIZE_MBYTES
BLOCK_SIZE_MBYTES=8
startNewTransfer
waitForTransferEnd
BLOCK_SIZE_MBYTES=$PRE_BLOCK_SIZE
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption test for append-only file(5)"
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
TEST_COUNT=$((TEST_COUNT + 1))

# abort set-up
ABORT_AFTER_SECONDS=5
ABORT_CHECK_INTERVAL_MILLIS=100
ABORT_AFTER_MILLIS=$((ABORT_AFTER_SECONDS * 1000))
EXPECTED_TRANSFER_DURATION_MILLIS=$((ABORT_AFTER_MILLIS + \
ABORT_CHECK_INTERVAL_MILLIS))
# add 500ms overhead. We need this because we can not control timeouts for disk
# writes
EXPECTED_TRANSFER_DURATION_MILLIS=$((EXPECTED_TRANSFER_DURATION_MILLIS + 500))

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
  exit 1
fi
WDTBIN_CLIENT=$WDTBIN_CLIENT_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
TEST_COUNT=$((TEST_COUNT + 1))

echo "Abort timing test(2) - Receiver side abort"
WDTBIN_SERVER_OLD=$WDTBIN_SERVER
WDTBIN_SERVER+=" -abort_check_interval_millis=$ABORT_CHECK_INTERVAL_MILLIS \
-abort_after_seconds=$ABORT_AFTER_SECONDS"
START_TIME_MILLIS=`date +%s%3N`
# Block a port to the beginning
sudo ip6tables-save > $DIR/ip6table
sudo ip6tables -A INPUT -p tcp --dport $STARTING_PORT -j DROP
startNewTransfer
wait $pidofreceiver
END_TIME_MILLIS=`date +%s%3N`
wait $pidofsender
DURATION=$((END_TIME_MILLIS - START_TIME_MILLIS))
echo "Abort timing test, transfer duration ${DURATION} ms, expected duration \
${EXPECTED_TRANSFER_DURATION_MILLIS} ms."
sudo ip6tables-restore < $DIR/ip6table
if (( $DURATION > $EXPECTED_TRANSFER_DURATION_MILLIS \
  || $DURATION < $ABORT_AFTER_MILLIS )); then
  echo "Abort timing test failed, exiting"
  printServerLog
  exit 1
fi
WDTBIN_SERVER=$WDTBIN_SERVER_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
TEST_COUNT=$((TEST_COUNT + 1))

echo "Transfer-id mismatch test"
SENDER_ID_OLD=$SENDER_ID
SENDER_ID="abcdef"
startNewTransfer
waitForTransferEndExpectingFailure
SENDER_ID=$SENDER_ID_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
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

# list of tests for which we should compare destination with $DIR/src1
USE_OTHER_SRC_DIR=()
echo "Test hostname mismatch"
# start first transfer
# change src directory and make the transfer IPV4
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
# change src directory and make the transfer IPV6
SRC_DIR=$DIR/src1
startNewTransfer
waitForTransferEnd
SRC_DIR=$DIR/src
USE_OTHER_SRC_DIR+=($TEST_COUNT)
TEST_COUNT=$((TEST_COUNT + 1))


STATUS=0
(cd $DIR/src ; ( find . -type f -print0 | xargs -0 md5sum | sort ) > ../src.md5s )
(cd $DIR/src1 ; ( find . -type f -print0 | xargs -0 md5sum | sort ) > ../src1.md5s )
for ((i = 0; i < TEST_COUNT; i++))
do
  if [[ ${TESTS_SKIP_VERIFICATION[*]} =~ $i ]]; then
    echo "Skipping verification for test $i"
    continue
  fi
  (cd $DIR/dst${i} ; ( find . -type f -print0 | xargs -0 md5sum | sort ) > \
  ../dst${i}.md5s )
  echo "Verifying correctness for test $((i + 1))"
  echo "Should be no diff"
  if [[ ${USE_OTHER_SRC_DIR[*]} =~ $i ]]; then
    SRC_MD5=src1.md5s
  else
    SRC_MD5=src.md5s
  fi
  (cd $DIR; diff -u $SRC_MD5 dst${i}.md5s)
  CUR_STATUS=$?
  if [ $CUR_STATUS -ne 0 ]; then
    cat $DIR/server${i}.log
  fi
  if [ $STATUS -eq 0 ] ; then
    STATUS=$CUR_STATUS
  fi
  # treating PROTOCOL_ERROR as errors
  cd $DIR; grep "PROTOCOL_ERROR" server${i}.log > /dev/null && STATUS=1
  cd $DIR; grep "PROTOCOL_ERROR" client${i}.log > /dev/null && STATUS=1
done

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
