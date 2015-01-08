#! /bin/bash

# This is to regress/test the max ~22000Mbytes/sec transfer rate

# modified from wdt_e2e_test.sh and fbonly/wdt_prof.sh

echo "Run this sender performance test from fbcode/ directory"
echo "or give full path to wdt binary to use and staging directory"

if [ -z "$1" ]; then
  WDTBIN="_bin/wdt/wdt"
else
  WDTBIN="$1"
fi

if [ -z "$2" ]; then
    BASEDIR=/dev/shm/tmpWDT$USER
else
    BASEDIR=$2
fi

AWK=gawk

# TEST_COUNT is an environment variable. It is set up by the python benchmarking
# script.
if [ -z $TEST_COUNT ]; then
  TEST_COUNT=2
fi

REMOTE=::1
SKIP_WRITES="true"

# Without throttling:
#WDTBIN_OPTS="-sleep_ms 1 -max_retries 3 -num_sockets 13"
# With, still gets almost same max (21G) with throttling set high enough
WDTBIN_OPTS="-sleep_ms 1 -max_retries 3 -num_sockets 13 --avg_mbytes_per_sec=26000 --max_mbytes_per_sec=26001"
CLIENT_PROFILE_FORMAT="%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata \
%Mmax)k\n%Iinputs+%Ooutputs (%Fmajor+%Rminor)pagefaults %Wswaps\nCLIENT_PROFILE %U \
%S %e"
SERVER_PROFILE_FORMAT="%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata \
%Mmax)k\n%Iinputs+%Ooutputs (%Fmajor+%Rminor)pagefaults %Wswaps\nSERVER_PROFILE %U \
%S %e"

WDTNAME=`basename $WDTBIN`
WDTCMD="$WDTBIN $WDTBIN_OPTS"

mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing $WDTBIN in $DIR"
if [ -z "$DIR" ]; then
  echo "Unable to create dir in $BASEDIR"
  exit 1
fi

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

/usr/bin/time -f "$SERVER_PROFILE_FORMAT" $WDTCMD -directory $DIR/dst -skip_writes=$SKIP_WRITES > \
$DIR/server.log 2>&1 &

# wait for server to be up
while [ `/bin/true | nc $REMOTE 22356; echo $?` -eq 1 ]
do
  echo "Server not up yet...`date`..."
  sleep 0.5
done
echo "Server is up on $REMOTE 22356 - `date` - starting client run"

for ((i = 1; i <= TEST_COUNT; i++))
do
  echo "starting ${i}th run"
  TWO_PHASE_ARG=""
  # every other run will be two_phases
  [ $(($i % 2)) -eq 0 ] && TWO_PHASE_ARG="-two_phases"

  /usr/bin/time -f "$CLIENT_PROFILE_FORMAT" $WDTCMD -directory $DIR/src \
  -destination $REMOTE $TWO_PHASE_ARG |& tee $DIR/client$i.log
  THROUGHPUT=`$AWK 'match($0, /.*Total sender throughput = ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "THROUGHPUT $THROUGHPUT"
  TRANSFER_TIME=`$AWK 'match($0, /.*Total sender time = ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "TRANSFER_TIME $TRANSFER_TIME"
done

echo -n e | nc $REMOTE 22356

echo "Server logs:"
cat $DIR/server.log

MAXRATE=`$AWK 'match($0, /.*Total sender throughput = ([0-9.]+)/, res) {rate=res[1]; if (rate>max) max=rate} END {print max}' $DIR/client?.log`

echo "Deleting logs and staging in $DIR"
rm -rf $DIR

echo "Rate for $WDTBIN"
echo $MAXRATE
# all done
