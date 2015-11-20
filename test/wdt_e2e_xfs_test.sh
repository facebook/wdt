#! /bin/sh
# Invoke the regular script after finding an XFS dir to write to

XFS=`df -T 2> /dev/null | awk '$2=="xfs" {print $7}'|head -1`

if [ -z "$XFS" ] ; then
    echo "No XFS fs found. Skipping test."
    exit 0
fi

echo "Found XFS on $XFS"

if [ ! -w $XFS ] ; then
    XFS=$XFS/users/$USER
    if [ ! -w $XFS ] ; then
        echo "Can't write to either base xfs nor $XFS. Skipping test."
        exit 0
    fi
    DIR=$XFS/wdtTest
else
    DIR=$XFS/wdtTest_$USER
fi

set -x
exec `dirname $0`/wdt_e2e_simple_test.sh -d $DIR
