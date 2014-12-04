#! /bin/sh

# This is to regress/test the max ~22000Mbytes/sec transfer rate

# modified from wdt_e2e_test.sh and fbonly/wdt_prof.sh

echo "Run this sender performance test from fbcode/ directory"

REMOTE=::1
SKIP_WRITES="true"

WDTBIN_OPTS="-sleep_ms 1 -max_retries 3 -num_sockets 13"
# Still gets almost same max (21G) with throttling set high enough
WDTBIN_OPTS="-sleep_ms 1 -max_retries 3 -num_sockets 13 --avg_mbytes_per_sec=26000 --max_mbytes_per_sec=26001"
WDTNAME="wdt"
WDTDIR="_bin/wdt"
WDTBIN="$WDTDIR/$WDTNAME"
WDTCMD="$WDTBIN $WDTBIN_OPTS"

BASEDIR=/dev/shm/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

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
echo "Done with staging src test files, starting server"

time $WDTCMD -directory $DIR/dst -skip_writes=$SKIP_WRITES > $DIR/server.log 2>&1 &

# wait for server to be up
while [ `/bin/true | nc $REMOTE 22356; echo $?` -eq 1 ]
do
 echo "Server not up yet...`date`..."
 sleep 0.5
done
echo "Server is up on $REMOTE 22356 - `date` - starting first client run"

time $WDTCMD -directory $DIR/src -destination $REMOTE |& tee $DIR/client1.log

echo "2nd run of client - stdout/err direct"
$WDTCMD -directory $DIR/src -destination $REMOTE

echo "3nd run of client with 2 phases"
time  $WDTCMD -directory $DIR/src -destination $REMOTE -two_phases |& tee $DIR/client3.log

echo "Making the server end gracefully"
echo -n "e" | nc $REMOTE 22356

echo "Server logs:"
cat $DIR/server.log

echo "Deleting logs and staging in $DIR"
rm -rf $DIR
# all done
