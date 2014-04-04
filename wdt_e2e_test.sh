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
cp wdt/*.* $DIR/src
#cp /usr/bin/* $DIR/src
#cp wdt/wdtlib.cpp $DIR/src/a
#cp wdt/wdtlib.h  $DIR/src/b
#head -30 wdt/wdtlib.cpp >  $DIR/src/c

$WDTBIN -directory $DIR/dst &

# wait for server to be up
while [ `/bin/true | nc ::1 22356; echo $?` -eq 1 ]
do
  echo "Server not up yet...`date`..."
  sleep 1
done

#$WDTBIN -num_sockets=1 -directory $DIR/src -destination ::1

$WDTBIN -directory $DIR/src -destination ::1

sleep 1
echo "Checking `date`" 1>&2

(cd $DIR/src ; md5sum * > ../src.md5s )
(cd $DIR/dst ; md5sum * > ../dst.md5s )

echo "Should be no diff"
(cd $DIR; diff -u src.md5s dst.md5s)
STATUS=$?
(cd $DIR; ls -l src/ dst/ )

pkill -x wdt
rm -rf $DIR

echo "Status is $STATUS"

exit $STATUS
