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

startNewTransfer() {
  $WDTBIN -directory $DIR/dst${TEST_COUNT} -start_port=$STARTING_PORT \
  >> $DIR/server${TEST_COUNT}.log 2>&1 &
  pidofreceiver=$!
  $WDTBIN -directory $DIR/src -destination $HOSTNAME \
  -start_port=$STARTING_PORT -block_size_mbytes=$BLOCK_SIZE_MBYTES \
  |& tee -a $DIR/client${TEST_COUNT}.log &
  pidofsender=$!
}

waitForTransferEnd() {
  wait $pidofreceiver
  checkLastCmdStatus
  wait $pidofsender
  checkLastCmdStatus
}

killCurrentTransfer() {
  kill -9 $pidofreceiver
  kill -9 $pidofsender
}

threads=4
STARTING_PORT=22500
ERROR_COUNT=10

WDTBIN_OPTS="-ipv4 -ipv6=false -start_port=$STARTING_PORT \
-avg_mbytes_per_sec=40 -max_mbytes_per_sec=50 -run_as_daemon=false \
-full_reporting -read_timeout_millis=500 -write_timeout_millis=500 \
-enable_download_resumption=true -keep_transfer_log=false \
-progress_report_interval_millis=-1"
WDTBIN="_bin/wdt/wdt -num_ports=$threads $WDTBIN_OPTS"

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
BLOCK_SIZE_MBYTES=10
TEST_COUNT=0

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
done

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
