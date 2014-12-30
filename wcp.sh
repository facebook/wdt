#! /bin/bash

# Quick hack pending block/splitting support in wdt itself
# Source of this file is fbcode/wdt/wcp.sh
# This copies a single file - if you have a lot of files - use wdt directly
# (without the splitting step - don't make a compressed archive!)

if [ $# -ne 2 ] ; then
    echo "Usage $0 singlebigsrcfile [user@]desthost:directory"
    echo "The wdt binary should be in the path on both source and destination"
    echo "And wdt ports (22356-22364) should be available on the destination"
    exit 1
fi
SRCPATH=$1
if [ ! -r $SRCPATH ] ; then
    echo "First argument ($SRCPATH) must be a readable file"
    exit 2
fi


START_NANO=`date +%s%N`
START_MS=`expr $START_NANO / 1000000`
echo "Starting at `date` ($START_MS)"

DIR=`mktemp -d`
REMOTE=`echo $2|sed -e 's/^.*@//' -e 's/:.*//'`
SSHREMOTE=`echo $2|sed -e 's/:.*//'`
REMOTEDIR=`echo $2|sed -e 's/.*://'`
if [ -z "$REMOTEDIR" ] ; then
  REMOTEDIR=.
fi

FILENAME=`basename $SRCPATH`
SIZE=`stat -L -c %s $SRCPATH`
if [ $? -ne 0 ] ; then
  echo "Error stating $SRCPATH. aborting."
  exit 1
fi

echo "Copying $FILENAME ($SRCPATH) to $REMOTE (using $SSHREMOTE in $REMOTEDIR)"

# Here we try to start asynchronously - the effect is things may fail on the
# server side and we won't know - also pkill may not be fast enough and
# data may still be sent to the previous process
echo "Starting destination side server"
# LZ4 compression:
#ssh -f $SSHREMOTE "mkdir -p $REMOTEDIR; cd $REMOTEDIR; wdt && cat wdtTMP* | time lz4 -d > $FILENAME && rm wdtTMP* ; echo 'Complete!';date"
# Gzip compression:
ssh -f $SSHREMOTE "pkill wdt; mkdir -p $REMOTEDIR; cd $REMOTEDIR; rm -f wdtTMP; wdt  --minloglevel=2 && cat wdtTMP* | time gzip -d -c > $FILENAME && rm wdtTMP* ; echo 'Complete!';date"
# No compression:
#ssh -f $SSHREMOTE "mkdir -p $REMOTEDIR; cd $REMOTEDIR; wdt && cat wdtTMP* > $FILENAME && rm wdtTMP* ; echo 'Complete!';date"
#REMOTEPID=$!

# We compress so most likely we'll have less blocks than that
SPLIT=`expr $SIZE / 48 / 1024 + 1`

echo "Splitting file $1 ($SIZE) up to 48 ways -> $SPLIT kbyte chunks to $DIR"
# LZ4
#time lz4 -3 -z $1 - | (cd $DIR ; time split -b ${SPLIT}K - wdtTMP )
# Gzip
time gzip -1 -c $1 | (cd $DIR ; time split -b ${SPLIT}K - wdtTMP )
# No compression
#cat $1 | (cd $DIR ; time split -b ${SPLIT}K - wdtTMP )


date "+%s.%N"
time wdt --minloglevel=2 --directory $DIR --destination $REMOTE

echo -n "e" | nc $REMOTE 22356

echo "All done client side! cleanup..."
rm -r $DIR
END_NANO=`date +%s%N`
END_MS=`expr $END_NANO / 1000000`
DURATION=`expr $END_MS - $START_MS`
RATE=`expr 1000 \* $SIZE / 1024 / 1024 / $DURATION`

echo "Overall transfer @ $RATE Mbytes/sec ($DURATION ms for $SIZE uncompressed)"
