#! /bin/bash

#
# Smaller/minimal version of wdt_e2e_simple_test.sh
#

echo "Run from the cmake build dir (or ~/fbcode - or fbmake runtests)"

BASEDIR=/tmp/wdtTest_$USER
USE_ODIRECT=false
usage="
The possible options to this script are
-d base directory to use (defaults to $BASEDIR)
-o if the value is true, o_direct read is used
"

if [ "$1" == "-h" ]; then
  echo "$usage"
  exit 0
fi
while getopts ":d:h:o:" opt; do
  case $opt in
    d) BASEDIR="$OPTARG"
    ;;
    o)
    if [ "$OPTARG" == "true" ]; then
      echo "Testing with o_direct reads"
      USE_ODIRECT=true
    fi
    ;;
    h) echo "$usage"
       exit
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

# Set DO_VERIFY:
# to 1 : slow/expensive but checks correctness
# to 0 : fast for repeated benchmarking not for correctness
DO_VERIFY=1

if [ -z "$WDT_TEST_SYMLINKS" ] ; then
  WDT_TEST_SYMLINKS=1
fi
echo "WDT_TEST_SYMLINKS=$WDT_TEST_SYMLINKS"

# Verbose / to debug failure:
#WDTBIN="_bin/wdt/wdt -minloglevel 0 -v 99"
# Normal:
WDTBIN_OPTS="-minloglevel=0 -sleep_millis 1 -max_retries 999 -full_reporting "\
"-avg_mbytes_per_sec=3000 -max_mbytes_per_sec=3500 -start_port=0 "\
"-num_ports=4 -throttler_log_time_millis=200 -enable_checksum=true"
WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"
MD5SUM=`which md5sum`
STATUS=$?
if [ $STATUS -ne 0 ] ; then
  MD5SUM=`which md5`
fi
echo "Found md5sum as $MD5SUM"

if [ -z "$HOSTNAME" ] ; then
    echo "HOSTNAME not set, will try with 'localhost'"
    HOSTNAME=localhost
else
    echo "Will self connect to HOSTNAME=$HOSTNAME"
fi

mkdir -p $BASEDIR
DIR=`mktemp -d $BASEDIR/XXXXXX`
echo "Testing in $DIR"

#pkill -x wdt

mkdir $DIR/src
mkdir $DIR/extsrc

#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec /usr/share $DIR/src
#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec $DIR/src
#cp -R wdt $DIR/src

#for size in 1k 64K 512K 1M 16M 256M 512M 1G
#for size in 512K 1M 16M 256M 512M 1G
# Mac's dd doesn't understand K,M,G...
for size in 1024 65536 524288 1232896 19726336
do
    base=inp$size
    echo dd if=/dev/... of=$DIR/src/$base.1 bs=$size count=1
    dd if=/dev/urandom of=$DIR/src/$base.1 bs=$size count=1
#    dd if=/dev/zero of=$DIR/src/$base.1 bs=$size count=1
    for i in {2..8}
    do
        cp $DIR/src/$base.1 $DIR/src/$base.$i
    done
done
echo "done with setup"

if [ $WDT_TEST_SYMLINKS -eq 1 ]; then
  # test symlink issues
  (cd $DIR/src ; touch a; ln -s doesntexist badlink; dd if=/dev/zero of=c bs=1024 count=1; mkdir d; ln -s ../d d/e; ln -s ../c d/a)
  (cd $DIR/extsrc; mkdir TestDir; mkdir TestDir/test; cd TestDir; echo "Text1" >> file1; cd test; echo "Text2" >> file1; ln -s $DIR/extsrc/TestDir; cp -R $DIR/extsrc/TestDir $DIR/src)
fi


CMD="$WDTBIN -minloglevel=0 -directory $DIR/dst 2> $DIR/server.log | \
    $WDTBIN -directory $DIR/src -odirect_reads=$USE_ODIRECT - 2>&1 | \
    tee $DIR/client1.log"
echo "First transfer: $CMD"
eval $CMD
STATUS=$?
# TODO check for $? / crash... though diff will indirectly find that case

if [ $WDT_TEST_SYMLINKS -eq 1 ]; then
  CMD="$WDTBIN -minloglevel=0 -directory $DIR/dst_symlinks 2>> $DIR/server.log \
   | $WDTBIN -follow_symlinks -directory $DIR/src \
    -odirect_reads=$USE_ODIRECT - 2>&1 | tee $DIR/client2.log"
  echo "Second transfer: $CMD"
  eval $CMD
  # TODO check for $? / crash... though diff will indirectly find that case
fi

if [ $DO_VERIFY -eq 1 ] ; then
    echo "Verifying for run without follow_symlinks"
    echo "Checking for difference `date`"

    NUM_FILES=`(cd $DIR/dst && ( find . -type f | wc -l))`
    echo "Transfered `du -ks $DIR/dst` kbytes across $NUM_FILES files"

    (cd $DIR/src ; ( find . -type f -print0 | xargs -0 $MD5SUM | sort ) \
        > ../src.md5s )
    (cd $DIR/dst ; ( find . -type f -print0 | xargs -0 $MD5SUM | sort ) \
        > ../dst.md5s )

    echo "Should be no diff"
    (cd $DIR; diff -u src.md5s dst.md5s)
    STATUS=$?


  if [ $WDT_TEST_SYMLINKS -eq 1 ]; then
    echo "Verifying for run with follow_symlinks"
    echo "Checking for difference `date`"

    NUM_FILES=`(cd $DIR/dst_symlinks && ( find . -type f | wc -l))`
    echo "Transfered `du -ks $DIR/dst_symlinks` kbytes across $NUM_FILES files"

    (cd $DIR/src ; ( find -L . -type f -print0 | xargs -0 $MD5SUM | sort ) \
        > ../src_symlinks.md5s )
    (cd $DIR/dst_symlinks ; ( find . -type f -print0 | xargs -0 $MD5SUM \
        | sort ) > ../dst_symlinks.md5s )

    echo "Should be no diff"
    (cd $DIR; diff -u src_symlinks.md5s dst_symlinks.md5s)
    SYMLINK_STATUS=$?
    if [ $STATUS -eq 0 ] ; then
      STATUS=$SYMLINK_STATUS
    fi
  fi
else
    echo "Skipping independant verification"
fi


echo "Server logs:"
cat $DIR/server.log

if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r can make some unreadable dir
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
