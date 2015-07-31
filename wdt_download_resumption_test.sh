#! /bin/bash

set -o pipefail

checkLastCmdStatus() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -ne 0 ] ; then
    sudo iptables-restore < $DIR/iptable
    echo "exiting abnormally with status $LAST_STATUS - aborting/failing test"
    exit $LAST_STATUS
  fi
}

checkLastCmdStatusExpectingFailure() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -eq 0 ] ; then
    sudo iptables-restore < $DIR/iptable
    echo "expecting wdt failure, but transfer was successful, failing test"
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
  kill -9 $pidofreceiver
  kill -9 $pidofsender
}

usage="
The possible options to this script are
-s sender protocol version
-r receiver protocol version
"

#protocol versions, used to check version verification
#version 0 represents default version
SENDER_PROTOCOL_VERSION=0
RECEIVER_PROTOCOL_VERSION=0

if [ "$1" == "-h" ]; then
  echo "$usage"
  exit 0
fi
while getopts ":s:r:" opt; do
  case $opt in
    s) SENDER_PROTOCOL_VERSION="$OPTARG"
    ;;
    r) RECEIVER_PROTOCOL_VERSION="$OPTARG"
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

WDTBIN_OPTS="-ipv4 -num_ports=$threads \
-avg_mbytes_per_sec=40 -max_mbytes_per_sec=50 -run_as_daemon=false \
-full_reporting -read_timeout_millis=500 -write_timeout_millis=500 \
-enable_download_resumption -keep_transfer_log=false"
WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"
WDTBIN_CLIENT="$WDTBIN -recovery_id=abcdef"
WDTBIN_SERVER=$WDTBIN

BASEDIR=/dev/shm/tmpWDT

mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
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
BLOCK_SIZE_MBYTES=10
TEST_COUNT=0
# Tests for which there is no need to verify source and destination md5s
TESTS_SKIP_VERIFICATION=()

echo "Download resumption test(1)"
startNewTransfer
sleep 5
killCurrentTransfer
# It takes a while for the ports to be available, so using different port the
# second transfer
STARTING_PORT=$((STARTING_PORT + threads))
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
STARTING_PORT=$((STARTING_PORT + threads))
startNewTransfer
sleep 5
killCurrentTransfer
STARTING_PORT=$((STARTING_PORT + threads))
startNewTransfer
waitForTransferEnd
TEST_COUNT=$((TEST_COUNT + 1))

echo "Download resumption with network error test(3)"
startNewTransfer
sleep 10
killCurrentTransfer
STARTING_PORT=$((STARTING_PORT + threads))
startNewTransfer
for ((i = 1; i <= ERROR_COUNT; i++))
do
  sleep 0.3 # sleep for 300ms
  port=$((STARTING_PORT + RANDOM % threads))
  echo "blocking $port"
  sudo iptables-save > $DIR/iptable
  if [ $(($i % 2)) -eq 0 ]; then
    sudo iptables -A INPUT -p tcp --sport $port -j DROP
  else
    sudo iptables -A INPUT -p tcp --dport $port -j DROP
  fi
  sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep more
            # than that
  echo "unblocking $port"
  sudo iptables-restore < $DIR/iptable
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
  sudo iptables-save > $DIR/iptable
  if [ $(($i % 2)) -eq 0 ]; then
    sudo iptables -A INPUT -p tcp --sport $port -j DROP
  else
    sudo iptables -A INPUT -p tcp --dport $port -j DROP
  fi
  sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep more
            # than that
  echo "unblocking $port"
  sudo iptables-restore < $DIR/iptable
done
killCurrentTransfer
STARTING_PORT=$((STARTING_PORT + threads))
# change the block size for next transfer
BLOCK_SIZE_MBYTES=8
startNewTransfer
waitForTransferEnd
TEST_COUNT=$((TEST_COUNT + 1))
STARTING_PORT=$((STARTING_PORT + threads))

# abort set-up
ABORT_AFTER_SECONDS=2
ABORT_CHECK_INTERVAL_MILLIS=100
ABORT_AFTER_MILLIS=$((ABORT_AFTER_SECONDS * 1000))
EXPECTED_TRANSFER_DURATION_MILLIS=$((ABORT_AFTER_MILLIS + \
ABORT_CHECK_INTERVAL_MILLIS))
# add 50ms overhead
EXPECTED_TRANSFER_DURATION_MILLIS=$((EXPECTED_TRANSFER_DURATION_MILLIS + 50))

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
if [[ $DURATION -gt $EXPECTED_TRANSFER_DURATION_MILLIS ]]; then
  echo "Abort timing test failed, exiting"
  exit 1
fi
WDTBIN_CLIENT=$WDTBIN_CLIENT_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
STARTING_PORT=$((STARTING_PORT + threads))
TEST_COUNT=$((TEST_COUNT + 1))
STARTING_PORT=$((STARTING_PORT + threads))

echo "Abort timing test(2) - Receiver side abort"
WDTBIN_SERVER_OLD=$WDTBIN_SERVER
WDTBIN_SERVER+=" -abort_check_interval_millis=$ABORT_CHECK_INTERVAL_MILLIS \
-abort_after_seconds=$ABORT_AFTER_SECONDS"
START_TIME_MILLIS=`date +%s%3N`
startNewTransfer
wait $pidofreceiver
END_TIME_MILLIS=`date +%s%3N`
wait $pidofsender
DURATION=$((END_TIME_MILLIS - START_TIME_MILLIS))
echo "Abort timing test, transfer duration ${DURATION} ms, expected duration \
${EXPECTED_TRANSFER_DURATION_MILLIS} ms."
if [[ $DURATION -gt $EXPECTED_TRANSFER_DURATION_MILLIS ]]; then
  echo "Abort timing test failed, exiting"
  exit 1
fi
WDTBIN_SERVER=$WDTBIN_SERVER_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
STARTING_PORT=$((STARTING_PORT + threads))
TEST_COUNT=$((TEST_COUNT + 1))
STARTING_PORT=$((STARTING_PORT + threads))

echo "Transfer-id mismatch test"
SENDER_ID_OLD=$SENDER_ID
SENDER_ID="abcdef"
startNewTransfer
waitForTransferEndExpectingFailure
SENDER_ID=$SENDER_ID_OLD
TESTS_SKIP_VERIFICATION+=($TEST_COUNT)
STARTING_PORT=$((STARTING_PORT + threads))
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
startNewTransfer
sleep 5
killCurrentTransfer
# change src directory and make the trnafser IPV6
SRC_DIR=$DIR/src1
WDTBIN_CLIENT+=" -ipv4=false -ipv6"
WDTBIN_SERVER+=" -ipv4=false -ipv6"
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
  cat $DIR/server${i}.log
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
