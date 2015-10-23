#! /bin/sh

# This script is mostly copied from wdt_e2e_test script. It has the same set up
# procedure. It is modified to support windtunnel integration.
# TODO: refactor this and e2e script to remove duplication of set up code

# TEST_COUNT is an environment variable. It is set up by the python benchmarking
# script.
if [ -z $TEST_COUNT ]; then
  TEST_COUNT=1
fi

WDTBIN_OPTS="-minloglevel=0 -num_ports=8 -enable_checksum=false"
if [ -z "$1" ]; then
  WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"
else
  WDTBIN="$1 $WDTBIN_OPTS"
fi
CLIENT_PROFILE_FORMAT="%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata \
%Mmax)k\n%Iinputs+%Ooutputs (%Fmajor+%Rminor)pagefaults %Wswaps\nCLIENT_PROFILE %U \
%S %e"
SERVER_PROFILE_FORMAT="%Uuser %Ssystem %Eelapsed %PCPU (%Xtext+%Ddata \
%Mmax)k\n%Iinputs+%Ooutputs (%Fmajor+%Rminor)pagefaults %Wswaps\nSERVER_PROFILE %U \
%S %e"

BASEDIR=/dev/shm/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

pkill -x wdt

mkdir $DIR/src

cp -R wdt folly $DIR/src
for size in 1k 64K 512K 1M 16M 256M 512M
do
    base=inp$size
    echo dd if=/dev/... of=$DIR/src/$base.1 bs=$size count=1
    dd if=/dev/zero of=$DIR/src/$base.1 bs=$size count=1
    for i in {2..8}
    do
        cp $DIR/src/$base.1 $DIR/src/$base.$i
    done
done
echo "done with setup"

for ((i = 1; i <= TEST_COUNT; i++))
do
  TWO_PHASE_ARG=""
  # every other run will be two_phases
  [ $(($i % 2)) -eq 0 ] && TWO_PHASE_ARG="-two_phases"
  /usr/bin/time -f "$SERVER_PROFILE_FORMAT" $WDTBIN -directory $DIR/dst$i > \
  $DIR/server$i.log 2>&1 &

  # wait for server to be up
  #while [ `/bin/true | nc $HOSTNAME 22356; echo $?` -eq 1 ]
  #do
  #  echo "Server not up yet...`date`..."
  #  sleep 0.5
  #done
  #echo "Server is up on $HOSTNAME 22356 - `date` - starting client run"

  /usr/bin/time -f "$CLIENT_PROFILE_FORMAT" $WDTBIN -directory $DIR/src \
  -destination $HOSTNAME $TWO_PHASE_ARG |& tee $DIR/client$i.log
  echo -n e | nc -4 $HOSTNAME 22356
  rm -rf $DIR/dst$i
  THROUGHPUT=`awk 'match($0, /.*Total sender throughput = ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "THROUGHPUT $THROUGHPUT"
  TRANSFER_TIME=`awk 'match($0, /.*Total sender time =  ([0-9.]+)/, res) \
  {print res[1]} END {}' $DIR/client$i.log`
  echo "TRANSFER_TIME $TRANSFER_TIME"
done

echo "Server logs:"
for ((i = 1; i <= TEST_COUNT; i++))
do
  cat $DIR/server$i.log
done

find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
echo "Deleting $DIR"
rm -rf $DIR
