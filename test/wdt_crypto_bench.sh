#! /bin/bash

# Modified version of wdt_max_send_test.sh to test crypto speed

echo "Run this sender performance test from fbcode/ directory"
echo "or give full path to wdt binary to use and staging directory"

set -o pipefail

if [ -z "$1" ]; then
  WDTBIN="_bin/wdt/wdt"
else
  WDTBIN="$1"
fi

if [ -z "$2" ]; then
    BASEDIR=/dev/shm/wdtTest_$USER
else
    BASEDIR=$2
fi

AWK=gawk

# TEST_COUNT is an environment variable. It is set up by the python benchmarking
# script.
if [ -z $TEST_COUNT ]; then
  TEST_COUNT=4
fi

# WDT_THROUGHPUT is an env var. Set it to overwrite the expected 16Gbyte/sec
if [ -z $WDT_THROUGHPUT ]; then
  WDT_THROUGHPUT=5000
fi


if [ -z "$HOSTNAME" ] ; then
    echo "HOSTNAME not set, will try with 'localhost'"
    HOSTNAME=localhost
else
    echo "Will self connect to HOSTNAME=$HOSTNAME"
fi

ENC_TYPE=aes128gcm
CRC=false

REMOTE=$HOSTNAME
SKIP_WRITES="true"

# Without throttling:
#WDTBIN_OPTS="-sleep_millis 1 -max_retries 3 -num_sockets 13"

#CPU normalization
NUM_CPU=`grep processor /proc/cpuinfo|wc -l`
# leave one cpu alone for the rest (or typically 2 because of /2)
NUM_THREADS=`echo $NUM_CPU/2|bc`
echo "Using $NUM_THREADS threads (on each sender, receiver) for $NUM_CPU CPUs"

# With, still gets almost same max (21G) with throttling set high enough
WDTBIN_OPTS="-sleep_millis 1 -max_retries 3 -num_ports $NUM_THREADS
-transfer_id=$$ -encryption_type=$ENC_TYPE --enable_checksum=$CRC
-exit_on_bad_flags=false"


WDTNAME=`basename $WDTBIN`
WDTCMD="$WDTBIN $WDTBIN_OPTS"

mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing $WDTBIN in $DIR"
if [ -z "$DIR" ]; then
  echo "Unable to create dir in $BASEDIR"
  exit 1
fi

pkill -x wdt
pkill -x wdt_fb
pkill -x $WDTNAME

mkdir $DIR/src

for size in 1k 64K 512K 1M 16M 256M 512M
do
    base=inp$size
    echo dd if=/dev/... of=$DIR/src/$base.1 bs=$size count=1
    dd if=/dev/zero of=$DIR/src/$base.1 bs=$size count=1
    for i in {2..32}
    do
        cp $DIR/src/$base.1 $DIR/src/$base.$i
    done
done
echo "Done with staging src test files"

#set -e

# wait for server to be up
#while [ `/bin/true | nc $REMOTE 22356; echo $?` -eq 1 ]
#do
#  echo "Server not up yet...`date`..."
#  sleep 0.5
#done
#echo "Server is up on $REMOTE 22356 - `date` - starting client run"

for ((i = 1; i <= TEST_COUNT; i++))
do
  echo "starting ${i}th run"
  TWO_PHASE_ARG=""
  # every other run will be two_phases
  [ $(($i % 2)) -eq 0 ] && TWO_PHASE_ARG="-two_phases"

  set -x
  time $WDTCMD -directory $DIR/dst$i -skip_writes=$SKIP_WRITES 2> \
    $DIR/server$i.log | time $WDTCMD -directory $DIR/src \
  $TWO_PHASE_ARG - |& tee $DIR/client$i.log
  set +x
  THROUGHPUT=`$AWK 'match($0, /.*Total sender throughput = ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "Server log:"
  cat $DIR/server$i.log
  echo "THROUGHPUT $THROUGHPUT"
  TRANSFER_TIME=`$AWK 'match($0, /.*Total sender time = ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "TRANSFER_TIME $TRANSFER_TIME"
done


MAXRATE=`$AWK 'BEGIN {max=0} match($0, /.*Total sender throughput = ([0-9.]+)/, res) {rate=res[1]; if (rate>max) max=rate} END {print int(max+.5)}' $DIR/client*.log`

ALLSPEEDS=`$AWK 'match($0, /.*Total sender throughput = ([0-9.]+)/, res) {printf("%s ", res[1])}' $DIR/client*.log`

echo "Deleting logs and staging in $DIR"
rm -rf $DIR

echo "Speed for all runs: $ALLSPEEDS"

# Normalize by CPU / number of threads
# (32 cores leaves 15 threads and can do ~20-22G, use 16 for margin of noise)
EXPECTED_SPEED=`echo $WDT_THROUGHPUT*$NUM_THREADS/16|bc`

echo "Best throughput for $WDTBIN"
if [ "$MAXRATE" -lt "$EXPECTED_SPEED" ]; then
    echo "Regression: $MAXRATE is too slow - before top"
    atop -l 1 1 | head -40
    echo "Regression: $MAXRATE is too slow for $NUM_CPU cpus ($EXPECTED_SPEED)"
    exit 1
else
    echo "Good rate $MAXRATE for $NUM_CPU cpus (threshold $EXPECTED_SPEED)"
    exit 0
fi

# all done
