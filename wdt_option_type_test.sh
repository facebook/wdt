#! /bin/bash

# Tests for option types can not be run in the same process because gflags
# can not be reset to default condition. This script runs all the tests one by
# one in separate processes

binaries=(_bin/wdt/option_type_test_long_flags \
  _bin/wdt/short_flags/option_type_test_short_flags)

for binary in "${binaries[@]}"
do
  for test_name in $($binary --gtest_list_tests | \
    tail -n +2 | sed 's/^ *//g')
  do
    echo "$binary --gtest_filter=*.$test_name"
    $binary --gtest_filter="*.$test_name"
    LAST_STATUS=$?
    if [ $LAST_STATUS -ne 0 ] ; then
      exit $LAST_STATUS
    fi
  done
done
exit 0
