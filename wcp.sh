#! /bin/bash

# Quick hack pending block/splitting support in wdt itself

if [ $# -ne 2 ] ; then
    echo "Usage $0 srcfile [user@]desthost:directory"
    echo "The wdt binary should be in the path on both source and destination"
    echo "And the wdt port (22356-22364) should be available on the destination"
    exit 1
fi

DIR=`mktemp -d`
REMOTE=`echo $2|sed -e 's/^.*@//' -e 's/:.*//'`
SSHCMD=`echo $2|sed -e 's/:.*//'`
REMOTEDIR=`echo $2|sed -e 's/.*://'`
if [ -z "$REMOTEDIR" ] ; then
  REMOTEDIR=.
fi

SRCPATH=$1
FILENAME=`basename $SRCPATH`
SIZE=`stat -L -c %s $SRCPATH`
if [ $? -ne 0 ] ; then
  echo "Error stating $SRCPATH. aborting."
  exit 1
fi

echo "Copying $FILENAME ($SRCPATH) to $REMOTE (using $SSHCMD in $REMOTEDIR)"

echo "Starting destination side server"
ssh $SSHCMD "PATH=\$PATH:$PATH; mkdir -p $REMOTEDIR; cd $REMOTEDIR; wdt && cat wdtTMP* > $FILENAME && rm wdtTMP*" &

SPLIT=`expr $SIZE / 24 / 1024`

echo "Splitting file $1 ($SIZE) ~64 ways -> $SPLIT kbyte chunks to $DIR"
# if the file is less than 64k... tough luck/why use wdt
cat $1 | (cd $DIR ; split -b ${SPLIT}K - wdtTMP )


(cd $DIR ; wdt --destination $REMOTE ; echo -n "e" | nc $REMOTE 22356 )

echo "All done ! cleanup..."
rm -r $DIR
