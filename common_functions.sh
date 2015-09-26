IPTABLE_LOCK_FILE="/tmp/wdt.iptable.lock"

acquireIptableLock() {
  while true
  do
    lockfile -r 0 $IPTABLE_LOCK_FILE
    STATUS=$?
    if [ $STATUS -eq 0 ]; then
      break
    fi
    echo "Failed to get iptable lock $IPTABLE_LOCK_FILE"
  done
}

releaseIptableLock() {
  rm -f $IPTABLE_LOCK_FILE
}

blockSportByDropping() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --sport "$1" -j DROP"
  acquireIptableLock
  sudo ip6tables -A INPUT -p tcp --sport "$1" -j DROP
  releaseIptableLock
}

blockDportByDropping() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --dport "$1" -j DROP"
  acquireIptableLock
  sudo ip6tables -A INPUT -p tcp --dport "$1" -j DROP
  releaseIptableLock
}

blockSportByRejecting() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --sport "$1" -j REJECT"
  acquireIptableLock
  sudo ip6tables -A INPUT -p tcp --sport "$1" -j REJECT
  releaseIptableLock
}

blockDportByRejecting() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --dport "$1" -j REJECT"
  acquireIptableLock
  sudo ip6tables -A INPUT -p tcp --dport "$1" -j REJECT
  releaseIptableLock
}

undoLastIpTableChange() {
  if [ ! -z "$UNBLOCK_CMD" ]; then
    acquireIptableLock
    eval "$UNBLOCK_CMD"
    releaseIptableLock
    unset UNBLOCK_CMD
  fi
}

simulateNetworkGlitchesByRejecting() {
  for ((i = 1; i <= ERROR_COUNT; i++))
  do
    sleep 0.3 # sleep for 300ms
    port=$((STARTING_PORT + RANDOM % threads))
    echo "blocking $port"
    if [ $((i % 2)) -eq 0 ]; then
      blockSportByRejecting $port
      sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep
                # more than that
      echo "unblocking $port"
      undoLastIpTableChange
    else
      blockDportByRejecting $port
      sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep
                # more than that
      echo "unblocking $port"
      undoLastIpTableChange
    fi
  done
}

simulateNetworkGlitchesByDropping() {
  for ((i = 1; i <= ERROR_COUNT; i++))
  do
    sleep 0.3 # sleep for 300ms
    port=$((STARTING_PORT + RANDOM % threads))
    echo "blocking $port"
    if [ $((i % 2)) -eq 0 ]; then
      blockSportByDropping $port
      sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep
                # more than that
      echo "unblocking $port"
      undoLastIpTableChange
    else
      blockDportByDropping $port
      sleep 0.7 # sleep for 700ms, read/write timeout is 500ms, so must sleep
                # more than that
      echo "unblocking $port"
      undoLastIpTableChange
    fi
  done
}

printServerLog() {
  echo "Server log($DIR/server${TEST_COUNT}.log):"
  cat $DIR/server${TEST_COUNT}.log
}

wdtExit() {
  undoLastIpTableChange
  if [ $1 -ne 0 ] ; then
      echo "Failing test $0 : Test#${TEST_COUNT} - Logs in $DIR"
  fi
  exit $1
}

checkLastCmdStatus() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -ne 0 ] ; then
    echo "exiting abnormally with status $LAST_STATUS - aborting/failing test"
    printServerLog
    wdtExit $LAST_STATUS
  fi
}

checkLastCmdStatusExpectingFailure() {
  LAST_STATUS=$?
  if [ $LAST_STATUS -eq 0 ] ; then
    echo "expecting wdt failure, but transfer was successful, failing test"
    printServerLog
    wdtExit 1
  fi
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

generateRandomFiles() {
  if [ -z "$1" ]; then
    echo "generateRandomFile expects the directory to be passed as first \
      argument"
    wdtExit 1
  fi
  if [ -z "$2" ]; then
    echo "generateRandomFile expects base file size to be passed as second \
      argument"
    wdtExit 1
  fi
  mkdir -p $1
  cd $1
  for ((i = 0; i < 4; i++))
  do
    dd if=/dev/urandom of=sample${i} bs=$2 count=1
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
}

removeDestination() {
  echo "Removing destination directory $DIR/dst${TEST_COUNT}"
  rm -rf "$DIR/dst${TEST_COUNT}"
}

verifyTransferAndCleanup() {
  if [ -z "$MD5SUM" ]; then
    MD5SUM=$(which md5sum)
    STATUS=$?
    if [ $STATUS -ne 0 ] ; then
      MD5SUM=$(which md5)
    fi
    echo "Found md5sum as $MD5SUM"
  fi

  if [ ! -f "$DIR/src.md5s" ]; then
    (cd "$DIR/src" ; ( find . -type f -print0 | xargs -0 "$MD5SUM" | sort ) \
      > ../src.md5s )
  fi

  if [ -d "$DIR/src1" ] && [ ! -f "$DIR/src1.md5s" ]; then
    (cd "$DIR/src1" ; ( find . -type f -print0 | xargs -0 "$MD5SUM" | sort ) \
      > ../src1.md5s )
  fi

  STATUS=0
  (cd "$DIR/dst${TEST_COUNT}" ; ( find . -type f -print0 | xargs -0 "$MD5SUM" \
    | sort ) > "../dst${TEST_COUNT}.md5s" )
  echo "Verifying correctness for test $((TEST_COUNT + 1))"
  echo "Should be no diff"
  USE_OTHER_SRC=$1
  if [ "$USE_OTHER_SRC" == "true" ]; then
    SRC_MD5=src1.md5s
  else
    SRC_MD5=src.md5s
  fi
  (cd "$DIR"; diff -u "$SRC_MD5" "dst${TEST_COUNT}.md5s")
  STATUS=$?
  if [ $STATUS -ne 0 ]; then
    cat "$DIR/server${TEST_COUNT}.log"
  fi
  # treating PROTOCOL_ERROR as errors
  grep "PROTOCOL_ERROR" "$DIR/server${TEST_COUNT}.log" > /dev/null && STATUS=1
  grep "PROTOCOL_ERROR" "$DIR/client${TEST_COUNT}.log" > /dev/null && STATUS=1

  if [ $STATUS -eq 0 ] ; then
    echo "Test $TEST_COUNT succeeded"
    removeDestination
  else
    echo "Test $TEST_COUNT failed"
    wdtExit $STATUS
  fi
}

signalHandler() {
  echo "Caught signal, exiting..."
  wdtExit 1
}

trap signalHandler SIGINT
