#### TODO : make this work on  MacOS

IPTABLE_LOCK_FILE="/tmp/wdt.iptable.lock"

setTcOptions() {
  sudo tc qdisc add dev lo root netem delay 20ms 10ms duplicate 1% corrupt 0.1%
}

clearTcOptions() {
  sudo tc qdisc del dev lo root
}

runIptableCmd() {
  flock -x "$IPTABLE_LOCK_FILE" -c "$1"
}

blockSportByDropping() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --sport $1 -j DROP"
  CMD="sudo ip6tables -A INPUT -p tcp --sport $1 -j DROP"
  runIptableCmd "$CMD"
}

blockDportByDropping() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --dport $1 -j DROP"
  CMD="sudo ip6tables -A INPUT -p tcp --dport $1 -j DROP"
  runIptableCmd "$CMD"
}

blockSportByRejecting() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --sport $1 -j REJECT"
  CMD="sudo ip6tables -A INPUT -p tcp --sport $1 -j REJECT"
  runIptableCmd "$CMD"
}

blockDportByRejecting() {
  UNBLOCK_CMD="sudo ip6tables -D INPUT -p tcp --dport $1 -j REJECT"
  CMD="sudo ip6tables -A INPUT -p tcp --dport $1 -j REJECT"
  runIptableCmd "$CMD"
}

undoLastIpTableChange() {
  if [ ! -z "$UNBLOCK_CMD" ]; then
    CMD="eval $UNBLOCK_CMD"
    runIptableCmd "$CMD"
    unset UNBLOCK_CMD
  fi
}

setBinaries() {
  if [ -z "$WDT_BINARY" ]; then
    # Inside fb, building with buck, look there:
    WDT_BINARY="./buck-out/gen/wdt/wdt"
    if [ ! -x "$WDT_BINARY" ]; then
      # Open source / Cmake / old fbmake path:
      WDT_BINARY="./_bin/wdt/wdt"
    fi
  fi
  if [ -z "$WDT_SENDER" ]; then
    WDT_SENDER=$WDT_BINARY
  fi
  if [ -z "$WDT_RECEIVER" ]; then
    WDT_RECEIVER=$WDT_BINARY
  fi
  if [ -z "$WDT_GEN_FILES" ]; then
    WDT_GEN_FILES="./buck-out/gen/wdt/bench/wdt_gen_files"
    if [ ! -x "$WDT_GEN_FILES" ]; then
      WDT_GEN_FILES="./_bin/wdt/bench/wdt_gen_files"
    fi
  fi
  if [ -z "$WDT_GEN_BIGRAMS" ]; then
    WDT_GEN_BIGRAMS="$(dirname "$0")/../bench/book1.bigrams"
  fi
  echo "Wdt binary: $WDT_BINARY, Sender: $WDT_SENDER, Receiver: $WDT_RECEIVER"
  echo "wdt_gen_files: $WDT_GEN_FILES - bigram data $WDT_GEN_BIGRAMS"
}

setDirectory() {
  if [ -z "$BASEDIR" ]; then
      BASEDIR="/dev/shm/wdtTest_$USER"
  fi
  mkdir -p "$BASEDIR"
  DIR=$(mktemp -d "$BASEDIR/XXXXX")
  SRC_DIR="$DIR/src"
  echo "Testing in $DIR - src dir $SRC_DIR"
  mkdir "$SRC_DIR"
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
  wait "$pidofreceiver"
  checkLastCmdStatus
  wait "$pidofsender"
  checkLastCmdStatus
}

waitForTransferEndWithoutCheckingStatus() {
  wait "$pidofreceiver"
  wait "$pidofsender"
}

waitForTransferEndExpectingFailure() {
  wait "$pidofreceiver"
  checkLastCmdStatusExpectingFailure
  wait "$pidofsender"
  checkLastCmdStatusExpectingFailure
}

killCurrentTransfer() {
  kill -9 "$pidofsender"
  kill -9 "$pidofreceiver"
  wait "$pidofsender"
  wait "$pidofreceiver"
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
  mkdir -p "$1"
  fourx=$(echo "$2* 4" | bc -l)
  echo "Generating 4 files of $2 MBytes and 16 of $fourx Mbytes"
  for ((i = 0; i < 4; i++))
  do
    $WDT_GEN_FILES -stats_source="$WDT_GEN_BIGRAMS" \
        -directory="$1" -filename="sample${i}" -gen_size_mb="$2"
  done
  # we will generate 1G of random data. 16 files each of size 64mb.
  for ((i = 0; i < 16; i++))
  do
    $WDT_GEN_FILES -stats_source="$WDT_GEN_BIGRAMS" \
        -directory="$1" -filename="file${i}" -gen_size_mb="$fourx"
  done
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

  stat $DIR/dst${TEST_COUNT}/.wdt.log 2> /dev/null
  if [ $? -eq 0 ]; then
    # transfer log present, verify for correctness
    echo "Verifying transfer log in $DIR/dst${TEST_COUNT}"
    $WDT_RECEIVER -directory $DIR/dst${TEST_COUNT} -parse_transfer_log
    checkLastCmdStatus
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
    | sort | grep -v "\.wdt\.log\$") > "../dst${TEST_COUNT}.md5s" )
  echo "Verifying correctness for test $TEST_COUNT"
  echo "Should be no diff"
  USE_OTHER_SRC=$1
  if [ "$USE_OTHER_SRC" == "true" ]; then
    SRC_MD5=src1.md5s
  else
    SRC_MD5=src.md5s
  fi
  (cd "$DIR"; diff -u "$SRC_MD5" "dst${TEST_COUNT}.md5s")
  STATUS=$?
  # treating PROTOCOL_ERROR as errors
  grep "PROTOCOL_ERROR" "$DIR/server${TEST_COUNT}.log" > /dev/null
  if [ $? -eq 0 ]; then
    echo "server has PROTOCOL ERROR"
    STATUS=1
  fi
  grep "PROTOCOL_ERROR" "$DIR/client${TEST_COUNT}.log" > /dev/null
  if [ $? -eq 0 ]; then
    echo "client has PROTOCOL ERROR"
    STATUS=1
  fi

  if [ $STATUS -eq 0 ] ; then
    echo "Test $TEST_COUNT succeeded"
    removeDestination
  else
    printServerLog
    echo "Test $TEST_COUNT failed"
    wdtExit $STATUS
  fi
}

signalHandler() {
  echo "Caught signal, exiting..."
  wdtExit 1
}

extendWdtOptions() {
  WDTBIN_OPTS+=" $EXTRA_WDT_OPTIONS"
  WDTBIN_OPTS+=" $EXTRA_ENCRYPTION_CMD"
  if [ ! -z "$ENCRYPTION_TYPE" ]; then
    echo "encryption_type $ENCRYPTION_TYPE"
    WDTBIN_OPTS+=" -encryption_type=$ENCRYPTION_TYPE"
  fi
  if [ ! -z "$ENABLE_CHECKSUM" ]; then
    echo "enable_checksum $ENABLE_CHECKSUM"
    WDTBIN_OPTS+=" -enable_checksum=$ENABLE_CHECKSUM"
  fi
  WDTBIN_OPTS+=" -iv_change_interval_mb=32"
}

# without the following line, wdt piped to tee effectively has exit status of
# tee. source : http://petereisentraut.blogspot.com/2010/11/pipefail.html
set -o pipefail

trap signalHandler SIGINT SIGTERM
# Note this is not meant to be an example of a secure key, it's just for tests
# (thus the test_only in the option name) - the normal way is through the URL
# which generates a crypto. Add _secret for _secr in case of single digit pid.
TEST_ONLY_ENCRYPTION_KEY=$(echo not_a_key_$$_secret | fold -b -w 16 | head -n 1)
EXTRA_ENCRYPTION_CMD="-test_only_encryption_secret $TEST_ONLY_ENCRYPTION_KEY"
