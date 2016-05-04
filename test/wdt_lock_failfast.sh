#! /bin/bash

source `dirname "$0"`/common_functions.sh
setBinaries
BASEDIR=/tmp/wdtTest_$USER
setDirectory

# Simple error test for 2 receiver

WDTBIN_OPTS="-num_ports=1 -enable_download_resumption"
WDTBIN_OPTS+=" -directory $DIR/dst"
WDTBIN="$WDT_BINARY $WDTBIN_OPTS"

# Use -fork so this blocks until url is generated/receiver started
$WDTBIN -fork -abort_after_seconds=3 > "$DIR/url1"
# Second transfer
$WDTBIN -abort_after_seconds=5 > "$DIR/url2"

STATUS=$?

echo "2nd transfer status $STATUS - should be TRANSFER_LOG_ACQUIRE_ERROR 25"

ls -l $DIR/url*

URL2=$(cat "$DIR/url2")

if [ ! -z "$URL2" ] ; then
  echo "There shouldn't be a url in 2nd run - there was $URL2"
  STATUS=-1
fi

rm -rf "$DIR"


if [ $STATUS -eq 25 ] ; then
  echo "Good run!"
  exit 0
else
  echo "Bad run! $STATUS"
  exit 1
fi
