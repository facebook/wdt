#! /bin/bash

# without the following line, wdt piped to tee effectively has exit status of
# tee. source : http://petereisentraut.blogspot.com/2010/11/pipefail.html
set -o pipefail

source `dirname "$0"`/common_functions.sh

processTransferFinish() {
  # first check sender status
  checkLastCmdStatus
  wait $pidofreceiver
  # check receiver status
  checkLastCmdStatus
  verifyTransferAndCleanup
  undoLastIpTableChange
  TEST_COUNT=$((TEST_COUNT + 1))
}

usage="
The possible options to this script are
-s sender protocol version
-r receiver protocol version
-p start port
"

#protocol versions, used to check version verification
#version 0 represents default version
SENDER_PROTOCOL_VERSION=0
RECEIVER_PROTOCOL_VERSION=0
STARTING_PORT=24000

if [ "$1" == "-h" ]; then
  echo "$usage"
  wdtExit 0
fi
while getopts ":s:p:r:h:" opt; do
  case $opt in
    s) SENDER_PROTOCOL_VERSION="$OPTARG"
    ;;
    r) RECEIVER_PROTOCOL_VERSION="$OPTARG"
    ;;
    p) STARTING_PORT="$OPTARG"
    ;;
    h) echo "$usage"
       wdtExit
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

echo "sender protocol version $SENDER_PROTOCOL_VERSION, receiver protocol \
version $RECEIVER_PROTOCOL_VERSION"

threads=4
ERROR_COUNT=25
TEST_COUNT=0

WDTBIN_BASE="_bin/wdt/wdt --transfer_id $$"
WDTBIN_OPTS="-ipv6 -start_port=$STARTING_PORT \
-avg_mbytes_per_sec=60 -max_mbytes_per_sec=65 -run_as_daemon=false \
-full_reporting -read_timeout_millis=495 -write_timeout_millis=495 \
-progress_report_interval_millis=-1 -abort_check_interval_millis=100 \
-max_transfer_retries=5 -treat_fewer_port_as_error -connect_timeout_millis 100"
WDTBIN="$WDTBIN_BASE -num_ports=$threads $WDTBIN_OPTS"
WDTBIN_SERVER="$WDTBIN -protocol_version=$RECEIVER_PROTOCOL_VERSION"
WDTBIN_CLIENT="$WDTBIN -protocol_version=$SENDER_PROTOCOL_VERSION"
WDTBIN_MORE_THREADS="$WDTBIN_BASE -num_ports=$((threads + 1)) $WDTBIN_OPTS"
WDTBIN_LESS_THREADS="$WDTBIN_BASE -num_ports=$((threads - 1)) $WDTBIN_OPTS"
BASEDIR=/dev/shm/wdtTest_$USER

mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

#pkill -x wdt

mkdir -p $DIR/src/dir1
cp -R folly $DIR/src/dir1
for ((i = 2; i <= 200; i++))
do
  mkdir $DIR/src/dir${i}
  cp -R $DIR/src/dir1 $DIR/src/dir${i}
done



# Testing with different start ports
echo "Testing with different start ports in sender and receiver"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_BASE -ipv6 -num_ports=$threads \
-start_port=$((STARTING_PORT + 1)) \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
processTransferFinish


# Testing with different less number of threads in sender
echo "Testing with less number of threads in client"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_BASE -ipv6 -num_ports=$((threads - 1)) \
-start_port=$STARTING_PORT \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
processTransferFinish


echo "Testing with more number of threads in client"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_BASE -ipv6 -num_ports=$((threads + 1)) \
-start_port=$STARTING_PORT \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
processTransferFinish


# Blocking sender port before transfer by
echo "Testing with port blocked before transfer(1)"
blockSportByDropping "$STARTING_PORT"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log
processTransferFinish


echo "Testing with port blocked before transfer(2)"
blockDportByDropping "$STARTING_PORT"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log
processTransferFinish


# Blocking a port in the middle of the transfer
echo "Testing by blocking a port in the middle of the transfer(1)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
echo "blocking $STARTING_PORT"
blockSportByDropping "$STARTING_PORT"
wait $pidofsender
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer(2)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
wait $pidofsender
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer and more \
client threads"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_MORE_THREADS -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
wait $pidofsender
processTransferFinish


echo "Testing by blocking a port in the middle of the transfer and less \
client threads"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_LESS_THREADS -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
PORT_TO_BLOCK=$((STARTING_PORT + 1))
echo "blocking $PORT_TO_BLOCK"
blockDportByDropping "$PORT_TO_BLOCK"
wait $pidofsender
processTransferFinish


# Simulating network glitches by rejecting packets
echo "Simulating network glitches by rejecting packets"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
simulateNetworkGlitchesByRejecting
wait $pidofsender # wait for the sender to finish
processTransferFinish


# Simulating network glitches by dropping packets
echo "Simulating network glitches by dropping packets"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
simulateNetworkGlitchesByDropping
wait $pidofsender # wait for the sender to finish
processTransferFinish

echo "Good run, deleting logs in $DIR"
rm -rf "$DIR"

wdtExit 0
