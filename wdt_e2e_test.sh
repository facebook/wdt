#! /bin/sh


echo "Run from ~/fbcode - or fbmake runtests"

# Verbose:
#WDTBIN="_bin/wdt/wdt -minloglevel 0"
# Normal:
WDTBIN=_bin/wdt/wdt

DIR=`mktemp -d`
echo "Testing in $DIR"

pkill -x wdt

mkdir $DIR/src
mkdir $DIR/dst


cp -r wdt folly /usr/bin $DIR/src

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
$WDTBIN -directory $DIR/src -destination ::1 # > $DIR/client.log 2>&1
# No need to wait for transfer to finish, client now exits when last byte is saved
echo "Checking `date`"

(cd $DIR/src ; ( find . -type f | /bin/fgrep -v "/." | xargs md5sum) > ../src.md5s )
(cd $DIR/dst ; ( find . -type f | xargs md5sum ) > ../dst.md5s )

echo "Should be no diff"
(cd $DIR; diff -u src.md5s dst.md5s)
STATUS=$?
#(cd $DIR; ls -lR src/ dst/ )

pkill -x wdt

echo "Server logs:"
cat $DIR/server.log

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
