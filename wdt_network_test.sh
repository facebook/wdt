#! /bin/bash

# without the following line, wdt piped to tee effectively has exit status of
# tee. source : http://petereisentraut.blogspot.com/2010/11/pipefail.html
set -o pipefail

checkLastCmdStatus() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -ne 0 ] ; then
    sudo iptables-restore < $DIR/iptable
    echo "exiting abnormally with status $LAST_STATUS - aborting/failing test"
    exit $LAST_STATUS
  fi
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
# starting port is different from default because e2e test and this test will be
# run in parallel during runtests.
STARTING_PORT=22400
ERROR_COUNT=25
TEST_COUNT=0

WDTBIN_OPTS="-ipv4 -ipv6=false -start_port=$STARTING_PORT \
-avg_mbytes_per_sec=60 -max_mbytes_per_sec=65 -run_as_daemon=false \
-full_reporting -read_timeout_millis=495 -write_timeout_millis=495 \
-progress_report_interval_millis=-1 -abort_check_interval_millis=100 \
-max_transfer_retries=5"
WDTBIN="_bin/wdt/wdt -num_ports=$threads $WDTBIN_OPTS"
WDTBIN_SERVER="$WDTBIN -protocol_version=$RECEIVER_PROTOCOL_VERSION"
WDTBIN_CLIENT="$WDTBIN -protocol_version=$SENDER_PROTOCOL_VERSION"
WDTBIN_MORE_THREADS="_bin/wdt/wdt -num_ports=$((threads + 1)) $WDTBIN_OPTS"
WDTBIN_LESS_THREADS="_bin/wdt/wdt -num_ports=$((threads - 1)) $WDTBIN_OPTS"
BASEDIR=/dev/shm/tmpWDT

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
_bin/wdt/wdt -ipv6=false -num_ports=$threads -start_port=$((STARTING_PORT + 1)) \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
TEST_COUNT=$((TEST_COUNT + 1))


# Testing with different less number of threads in sender
echo "Testing with less number of threads in client"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
_bin/wdt/wdt -ipv6=false -num_ports=$((threads - 1)) -start_port=$STARTING_PORT \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
TEST_COUNT=$((TEST_COUNT + 1))


echo "Testing with more number of threads in client"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
_bin/wdt/wdt -ipv6=false -num_ports=$((threads + 1)) -start_port=$STARTING_PORT \
-destination $HOSTNAME -directory $DIR/src -full_reporting \
|& tee -a $DIR/client${TEST_COUNT}.log
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
TEST_COUNT=$((TEST_COUNT + 1))


# Blocking sender port before transfer by
sudo iptables-save > $DIR/iptable
sudo iptables -A INPUT -p tcp --sport $STARTING_PORT -j DROP
echo "Testing with port blocked before transfer(1)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


sudo iptables-save > $DIR/iptable
sudo iptables -A INPUT -p tcp --dport $STARTING_PORT -j DROP
echo "Testing with port blocked before transfer(2)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


# Blocking a port in the middle of the transfer
echo "Testing by blocking a port in the middle of the transfer(1)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
sudo iptables-save > $DIR/iptable
echo "blocking $STARTING_PORT"
sudo iptables -A INPUT -p tcp --sport $STARTING_PORT -j DROP
wait $pidofsender 
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


echo "Testing by blocking a port in the middle of the transfer(2)"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
sudo iptables-save > $DIR/iptable
echo "blocking $STARTING_PORT"
sudo iptables -A INPUT -p tcp --dport $((STARTING_PORT + 1)) -j DROP
wait $pidofsender 
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


echo "Testing by blocking a port in the middle of the transfer and more \
client threads"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_MORE_THREADS -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
sudo iptables-save > $DIR/iptable
echo "blocking $STARTING_PORT"
sudo iptables -A INPUT -p tcp --dport $((STARTING_PORT + 1)) -j DROP
wait $pidofsender 
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


echo "Testing by blocking a port in the middle of the transfer and less \
client threads"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_LESS_THREADS -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
sleep 5
sudo iptables-save > $DIR/iptable
echo "blocking $STARTING_PORT"
sudo iptables -A INPUT -p tcp --dport $((STARTING_PORT + 1)) -j DROP
wait $pidofsender 
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
sudo iptables-restore < $DIR/iptable
TEST_COUNT=$((TEST_COUNT + 1))


# Simulating network glitches by rejecting packets
echo "Simulating network glitches by rejecting packets"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
for ((i = 1; i <= ERROR_COUNT; i++))
do
  sleep 0.3 # sleep for 300ms
  port=$((STARTING_PORT + RANDOM % threads))
  echo "blocking $port"
  sudo iptables-save > $DIR/iptable
  if [ $(($i % 2)) -eq 0 ]; then
    sudo iptables -A INPUT -p tcp --sport $port -j REJECT
  else
    sudo iptables -A INPUT -p tcp --dport $port -j REJECT
  fi
  sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep more
            # than that
  echo "unblocking $port"
  sudo iptables-restore < $DIR/iptable
done
wait $pidofsender # wait for the sender to finish
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
TEST_COUNT=$((TEST_COUNT + 1))


# Simulating network glitches by dropping packets
echo "Simulating network glitches by dropping packets"
$WDTBIN_SERVER -directory $DIR/dst${TEST_COUNT} > \
$DIR/server${TEST_COUNT}.log 2>&1 &
pidofreceiver=$!
$WDTBIN_CLIENT -directory $DIR/src -destination $HOSTNAME |& tee -a \
$DIR/client${TEST_COUNT}.log &
pidofsender=$!
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
wait $pidofsender # wait for the sender to finish
checkLastCmdStatus
wait $pidofreceiver
checkLastCmdStatus
TEST_COUNT=$((TEST_COUNT + 1))

STATUS=0

(cd $DIR/src ; ( find . -type f -print0 | xargs -0 md5sum | sort ) > ../src.md5s )
for ((i = 0; i < TEST_COUNT; i++))
do
  (cd $DIR/dst${i} ; ( find . -type f -print0 | xargs -0 md5sum | sort ) > \
  ../dst${i}.md5s )
  echo "Verifying correctness for test $((i + 1))"
  echo "Should be no diff"
  (cd $DIR; diff -u src.md5s dst${i}.md5s)
  CUR_STATUS=$?
  if [ $STATUS -eq 0 ] ; then
    STATUS=$CUR_STATUS
  fi
  # treating PROTOCOL_ERROR as errors
  cd $DIR; grep "PROTOCOL_ERROR" server${i}.log > /dev/null && STATUS=1
  cd $DIR; grep "PROTOCOL_ERROR" client${i}.log > /dev/null && STATUS=1
  cat $DIR/server${i}.log
done

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
