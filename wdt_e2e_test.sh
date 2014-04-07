#! /bin/sh


echo "Run from ~/fbcode - or fbmake runtests"
WDTBIN=_bin/wdt/wdt
DIR=`mktemp -d`
echo "Testing in $DIR"

pkill -x wdt

mkdir $DIR/src
mkdir $DIR/dst

#cp wdt/wdtlib.cpp wdt/wdtlib.h $DIR/src
#cp wdt/*.cpp $DIR/src
cp -r wdt folly /usr/bin $DIR/src
#cp /usr/bin/* $DIR/src
#cp wdt/wdtlib.cpp $DIR/src/a
#cp wdt/wdtlib.h  $DIR/src/b
#head -30 wdt/wdtlib.cpp >  $DIR/src/c

$WDTBIN -directory $DIR/dst &

# wait for server to be up
while [ `nc -z ::1 22356 > /dev/null; echo $?` -eq 1 ]
do
  echo "Server not up yet...`date`..."
  sleep 0.05
done

#$WDTBIN -num_sockets=1 -directory $DIR/src -destination ::1

$WDTBIN -directory $DIR/src -destination ::1
echo "Client is done `date`" 1>&2
sleep 0.5
echo "Checking `date`" 1>&2

(cd $DIR/src ; ( find . -type f | /bin/fgrep -v "/." | xargs md5sum) > ../src.md5s )
(cd $DIR/dst ; ( find . -type f | xargs md5sum ) > ../dst.md5s )

echo "Should be no diff"
(cd $DIR; diff -u src.md5s dst.md5s)
STATUS=$?
#(cd $DIR; ls -lR src/ dst/ )

pkill -x wdt
rm -rf $DIR

echo "Status is $STATUS"

exit $STATUS
