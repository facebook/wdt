#! /bin/bash

source `dirname "$0"`/common_functions.sh

setBinaries
setDirectory

# create a big file
dd if=/dev/zero of="$SRC_DIR/file" bs=536870912 count=1

WDTBIN_OPTS="-static_ports -num_ports=2 -transfer_id=wdt"
extendWdtOptions

TEST_COUNT=0
# start the server
$WDT_RECEIVER $WDTBIN_OPTS -skip_writes \
-connect_timeout_millis 100 \
-read_timeout_millis=200 > "$DIR/server${TEST_COUNT}.log" 2>&1 &
pidofreceiver=$!

# block 22356 for small duration so that file is transferred through 22357
blockDportByDropping 22356
# start client
$WDT_SENDER $WDTBIN_OPTS -destination localhost -directory "$DIR/src" \
-block_size_mbytes=-1 -avg_mbytes_per_sec=100 \
2>&1 | tee "$DIR/client${TEST_COUNT}.log" &
pidofsender=$!
sleep 0.1
undoLastIpTableChange
sleep 5
# block 22357 in the middle
blockDportByDropping 22357
waitForTransferEnd

echo "Test passed, deleting directory $DIR"
rm -rf "$DIR"
wdtExit 0
