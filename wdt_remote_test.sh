#! /bin/sh

REMOTE=$1
if [ -z $REMOTE ] ; then
    echo "Usage: $0 desthost"
    exit 1
fi

echo "Remote Test - assumes wdt binary is in path - target is $REMOTE"

# Verbose:
#WDTBIN="wdt -minloglevel 0"
# Fastest:
BS=`expr 256 \* 1024`
WDTBIN_OPTS="-buffer_size=$BS -num_sockets=16 -minloglevel 2"
WDTBIN="wdt $WDTBIN_OPTS"

BASEDIR=/dev/shm/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

pkill -x wdt

mkdir $DIR/src
ssh $REMOTE "pkill -x wdt; mkdir -p $DIR/dst"


cp -R /usr/bin /usr/lib /usr/share $DIR/src

# Various smaller tests if the bigger one fails and logs are too hard to read:
#cp wdt/wdtlib.cpp wdt/wdtlib.h $DIR/src
#cp wdt/*.cpp $DIR/src
#cp /usr/bin/* $DIR/src
#cp wdt/wdtlib.cpp $DIR/src/a
#cp wdt/wdtlib.h  $DIR/src/b
#head -30 wdt/wdtlib.cpp >  $DIR/src/c
echo "staging copy done, starting wdt on $REMOTE"
# Can't have both client and server send to stdout in parallel or log lines
# get mangled/are missing - so we redirect the server one
ssh $REMOTE "date;$WDTBIN -directory $DIR/dst > $DIR/server.log 2>&1 &"

# wait for server to be up
while [ `/bin/true | nc $REMOTE 22356; echo $?` -eq 1 ]
do
 echo "Server not up yet...`date`..."
 sleep 0.5
done
echo "Server is up on $REMOTE 22356 - `date`"

# Only 1 socket (single threaded send/receive)
#$WDTBIN -num_sockets=1 -directory $DIR/src -destination ::1
# Normal
time $WDTBIN -directory $DIR/src -destination $REMOTE |& tee $DIR/client.log
# No need to wait for transfer to finish, client now exits when last byte is saved
echo "Checking for difference `date`"

(cd $DIR/src ; ( find . -type f | /bin/fgrep -v "/." | xargs md5sum | sort ) > ../src.md5s )
ssh $REMOTE "cd $DIR/dst ; find . -type f | xargs md5sum | sort" > $DIR/dst.md5s

echo "Should be no diff"
(cd $DIR; diff -u src.md5s dst.md5s)
STATUS=$?
#(cd $DIR; ls -lR src/ dst/ )

pkill -x wdt

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
  ssh $REMOTE "pkill -x wdt; rm -rf $DIR"
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR (and on $REMOTE)"
fi

exit $STATUS
