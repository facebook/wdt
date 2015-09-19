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
    exit 1
  fi
  if [ -z "$2" ]; then
    echo "generateRandomFile expects base file size to be passed as second \
      argument"
    exit 1
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

verifyTransferAndCleanup() {
  MD5SUM=`which md5sum`
  STATUS=$?
  if [ $STATUS -ne 0 ] ; then
    MD5SUM=`which md5`
  fi
  echo "Found md5sum as $MD5SUM"

  STATUS=0
  (cd $DIR/src ; ( find . -type f -print0 | xargs -0 $MD5SUM | sort ) \
    > ../src.md5s )
  if [ -d "$DIR/src1" ]; then
    (cd $DIR/src1 ; ( find . -type f -print0 | xargs -0 $MD5SUM | sort ) \
      > ../src1.md5s )
  fi
  for ((i = 0; i < TEST_COUNT; i++))
  do
    if [[ ${TESTS_SKIP_VERIFICATION[*]} =~ $i ]]; then
      echo "Skipping verification for test $i"
      continue
    fi
    (cd $DIR/dst${i} ; ( find . -type f -print0 | xargs -0 $MD5SUM | sort ) > \
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
    # cp -r makes lib/locale not writeable somehow
    find $DIR -type d | xargs chmod 755
    rm -rf $DIR
  else
    echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
  fi
}
