#! /bin/sh


echo "Run from ~/fbcode - or fbmake runtests"

# Verbose:
#WDTBIN="_bin/wdt/wdt -minloglevel 0"
# Fastest:
BS=`expr 256 \* 1024`
WDTBIN_OPTS="-buffer_size=$BS -num_sockets=8 -minloglevel 2 -sleep_ms 1 -max_retries 999"
WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"

BASEDIR=/dev/shm/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

pkill -x wdt

mkdir $DIR/src
mkdir $DIR/dst


cp -R wdt folly /usr/bin /usr/lib $DIR/src

# Various smaller tests if the bigger one fails and logs are too hard to read:
#cp wdt/wdtlib.cpp wdt/wdtlib.h $DIR/src
#cp wdt/*.cpp $DIR/src
#cp /usr/bin/* $DIR/src
#cp wdt/wdtlib.cpp $DIR/src/a
#cp wdt/wdtlib.h  $DIR/src/b
#head -30 wdt/wdtlib.cpp >  $DIR/src/c

# Can't have both client and server send to stdout in parallel or log lines
# get mangled/are missing - so we redirect the server one
$WDTBIN -directory $DIR/dst > $DIR/server.log 2>&1 &

# client now retries connects so no need wait for server to be up

# Only 1 socket (single threaded send/receive)
#$WDTBIN -num_sockets=1 -directory $DIR/src -destination ::1
# Normal
time $WDTBIN -directory $DIR/src -destination ::1 |& tee $DIR/client.log
# No need to wait for transfer to finish, client now exits when last byte is saved
echo "Checking for difference `date`"

NUM_FILES=`(cd $DIR/dst ; ( find . -type f | wc -l))`
echo "Transfered `du -ks $DIR/dst` kbytes across $NUM_FILES files"

(cd $DIR/src ; ( find . -type f | /bin/fgrep -v "/." | xargs md5sum | sort ) > ../src.md5s )
(cd $DIR/dst ; ( find . -type f | xargs md5sum | sort ) > ../dst.md5s )

echo "Should be no diff"
(cd $DIR; diff -u src.md5s dst.md5s)
STATUS=$?
#(cd $DIR; ls -lR src/ dst/ )

pkill -x wdt

echo "Server logs:"
cat $DIR/server.log

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
