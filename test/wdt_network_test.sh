#! /bin/bash

source `dirname "$0"`/common_functions.sh

startNewTransfer() {
  echo "Starting receiver:"
  echo "$WDTBIN_SERVER $EXTRA_SERVER_ARGS -directory $DIR/dst${TEST_COUNT}"
  $WDTBIN_SERVER $EXTRA_SERVER_ARGS -directory $DIR/dst${TEST_COUNT} > \
  $DIR/server${TEST_COUNT}.log 2>&1 &
  pidofreceiver=$!
  echo "Starting sender:"
  echo "$WDTBIN_CLIENT $EXTRA_CLIENT_ARGS"
  $WDTBIN_CLIENT $EXTRA_CLIENT_ARGS |& tee -a $DIR/client${TEST_COUNT}.log &
  pidofsender=$!
  unset EXTRA_SERVER_ARGS
  unset EXTRA_CLIENT_ARGS
}

processTransferFinish() {
  waitForTransferEnd
  verifyTransferAndCleanup
  undoLastIpTableChange
  TEST_COUNT=$((TEST_COUNT + 1))
}

usage="
The possible options to this script are
-s sender protocol version
-r receiver protocol version
-p start port
-x extraoptions
"

#protocol versions, used to check version negotiation
#version 0 represents default version
SENDER_PROTOCOL_VERSION=0
RECEIVER_PROTOCOL_VERSION=0
STARTING_PORT=24000

while getopts ":s:p:r:h" opt; do
  case $opt in
    s) SENDER_PROTOCOL_VERSION="$OPTARG"
    ;;
    r) RECEIVER_PROTOCOL_VERSION="$OPTARG"
    ;;
    p) STARTING_PORT="$OPTARG"
    ;;
    h) echo "$usage"
       exit 0
    ;;
    \?) echo "$usage"
       exit 1
    ;;
  esac
done

setBinaries

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
ERROR_COUNT=25
TEST_COUNT=0

BASEDIR=/dev/shm/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

WDTBIN_OPTS="-enable_perf_stat_collection -ipv6 -start_port=$STARTING_PORT \
-avg_mbytes_per_sec=15 -max_mbytes_per_sec=20 -run_as_daemon=false \
-full_reporting -read_timeout_millis=495 -write_timeout_millis=495 \
-progress_report_interval_millis=-1 -abort_check_interval_millis=100 \
-treat_fewer_port_as_error -exit_on_bad_flags=false -skip_fadvise \
-connect_timeout_millis 100 -transfer_id $$ -num_ports=$threads"
extendWdtOptions
WDTBIN_SERVER="$WDT_RECEIVER $WDTBIN_OPTS \
  -protocol_version=$RECEIVER_PROTOCOL_VERSION"
WDTBIN_CLIENT="$WDT_SENDER $WDTBIN_OPTS -destination $HOSTNAME \
  -protocol_version=$SENDER_PROTOCOL_VERSION -directory $DIR/src"
#pkill -x wdt

mkdir -p $DIR/src/dir1
cp -R folly $DIR/src/dir1
for ((i = 2; i <= 50; i++))
do
  mkdir $DIR/src/dir${i}
  cp -R $DIR/src/dir1 $DIR/src/dir${i}
done


# Testing with different start ports
echo "Testing with different start ports in sender and receiver"
EXTRA_CLIENT_ARGS="-start_port=$((STARTING_PORT + 1))"
startNewTransfer
processTransferFinish


# Testing with different less number of threads in sender
echo "Testing with less number of threads in client"
EXTRA_CLIENT_ARGS="-num_ports=$((threads - 1))"
startNewTransfer
processTransferFinish


echo "Testing with more number of threads in client"
EXTRA_CLIENT_ARGS="-num_ports=$((threads + 1))"
startNewTransfer
processTransferFinish


# Blocking sender port before transfer by
echo "Testing with port blocked before transfer(1)"
blockSportByDropping "$STARTING_PORT"
startNewTransfer
processTransferFinish


echo "Testing with port blocked before transfer(2)"
blockDportByDropping "$STARTING_PORT"
startNewTransfer
processTransferFinish


# Blocking a port in the middle of the transfer
echo "Testing by blocking a port in the middle of the transfer(1)"
startNewTransfer
sleep 5
echo "blocking $STARTING_PORT"
blockSportByDropping "$STARTING_PORT"
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer(2)"
startNewTransfer
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer and more \
client threads"
EXTRA_CLIENT_ARGS="-num_ports=$((threads + 1))"
startNewTransfer
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer and less \
client threads"
EXTRA_CLIENT_ARGS="-num_ports=$((threads - 1))"
startNewTransfer
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
processTransferFinish


# Simulating network glitches by rejecting packets
echo "Simulating network glitches by rejecting packets"
startNewTransfer
simulateNetworkGlitchesByRejecting
processTransferFinish


# Simulating network glitches by dropping packets
echo "Simulating network glitches by dropping packets"
startNewTransfer
simulateNetworkGlitchesByDropping
processTransferFinish

echo "Good run, deleting logs in $DIR"
rm -rf "$DIR"

wdtExit 0
