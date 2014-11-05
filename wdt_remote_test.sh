#! /bin/sh

#set -e
# modified from wdt_e2e_test.sh and fbonly/wdt_prof.sh

SRCHOST=$1
DSTHOST=$2
if [ -z $DSTHOST ] ; then
    echo "Usage: $0 srchost desthost"
    exit 1
fi

SKIP_WRITES="true"

echo "Run from ~fbcode (wdt's parent dir). Skip writes is $SKIP_WRITES"

#WDTBIN_OPTS="-minloglevel 2 -sleep_ms 10 -max_retries 5 -num_sockets 15"
WDTBIN_OPTS="-sleep_ms 1 -max_retries 3 -num_sockets 16 -ipv4=true"

BASEDIR=/dev/shm/tmpWDT


# Version with profiler:
#WDTNAME="wdt_prof"
#WDTDIR="_bin/wdt/fbonly"
# Non profiler version
WDTNAME="wdt"
WDTORIGDIR="_bin/wdt"
WDTDIR="$BASEDIR/_bin/wdt"
WDTBIN="$WDTDIR/$WDTNAME"
WDTCMD="$WDTBIN $WDTBIN_OPTS"

REMOTEUSER="root"
RSHDST="ssh -l $REMOTEUSER $DSTHOST"
RSHSRC="ssh -l $REMOTEUSER $SRCHOST"
SCP="scp"
NC="nc -4" # ipv4 only

#BASEDIR=/data/wdt/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

CMD="pkill -x $WDTNAME; mkdir -p $DIR/src ; mkdir -p $WDTDIR"
$RSHSRC $CMD
$RSHDST $CMD
$SCP $WDTORIGDIR/$WDTNAME $REMOTEUSER@$DSTHOST:$WDTBIN
$SCP $WDTORIGDIR/$WDTNAME $REMOTEUSER@$SRCHOST:$WDTBIN

#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec /usr/share $DIR/src
#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec $DIR/src
# TODO get a better test/load generation - remote execute the for below for ex
#$RSHSRC "cp -R /usr/bin /usr/share $DIR/src"

#$RSHSRC "cp -R /usr/bin /usr/lib /usr/lib64 $DIR/src"

$RSHSRC "dd if=/dev/zero of=$DIR/src/big.1 bs=256M count=1; for i in {2..64} ; do ln $DIR/src/big.1 $DIR/src/big.\$i; done; ls -lh $DIR/src"

echo 'done with setup'

echo "staging copy done on $SRCHOST, starting wdt on $DSTHOST"
# Can't have both client and server send to stdout in parallel or log lines
# get mangled/are missing - so we redirect the server one
$RSHDST "date; $WDTCMD -directory $DIR/dst -skip_writes=$SKIP_WRITES > $DIR/server.log 2>&1 &"

# wait for server to be up
while [ `$RSHSRC "/bin/true | $NC $DSTHOST 22356; echo $?"` -eq 1 ]
do
 echo "Server not up yet...`date`..."
 sleep 0.5
done
echo "Server is up on $DSTHOST 22356 - `date`"


# Only 1 socket (single threaded send/receive)
#$WDTCMD -num_sockets=1 -directory $DIR/src -destination ::1
# Normal

#time trickle -d 1000 -u 1000 $WDTCMD -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log
$RSHSRC "time $WDTCMD -directory $DIR/src -destination $DSTHOST |& tee $DIR/client.log"

echo "2nd run of client"
$RSHSRC "time $WDTCMD -directory $DIR/src -destination $DSTHOST |& tee $DIR/client2.log"

echo "3nd run of client with 2 phases"
$RSHSRC "time  $WDTCMD -directory $DIR/src -destination $DSTHOST -two_phases |& tee $DIR/client3.log"

echo "Skipping independant verification"
STATUS=0

echo "Making the server end gracefully"
$RSHSRC "echo -n e | $NC $DSTHOST 22356"

echo "Server logs:"
$RSHDST "cat $DIR/server.log"

echo "Deleting logs in $DIR"
$RSHSRC "find $DIR -type d | xargs chmod 755; rm -rf $DIR"
$RSHDST "pkill -x wdt; rm -rf $DIR"

echo "All done with testing from $SRCHOST to $DSTHOST"

exit $STATUS
