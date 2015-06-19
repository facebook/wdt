#! /bin/bash

# Multiple benchmarks - set DO_VERIFY=0 and
#
# wdt/wdt_e2e_test.sh |& awk '/All data.*Total throughput/ \
# {print $30} /^(real|user|sys)/ {print $0}'

# 100 loops:
#
# for i in {1..100} ;do date; wdt/wdt_e2e_test.sh \
# |& awk '/All data.*Total throughput/ {print int($30+.5)}' \
# >> ~/wdt_perfRes100; done &
#
# $ histogram -offset 7000 -divider 100 -percentile1 25 -percentile2 50 < ~/wdt_perfRes100
# # count,avg,min,max,stddev,100,8433.43,6506,9561,690.083
# # range, mid point, percentile, count
# < 7100 , 7050 , 3, 3
# >= 7100 < 7200 , 7150 , 7, 4
# >= 7200 < 7300 , 7250 , 10, 3
# >= 7300 < 7400 , 7350 , 10, 0
# >= 7400 < 7500 , 7450 , 11, 1
# >= 7500 < 7600 , 7550 , 12, 1
# >= 7600 < 7700 , 7650 , 14, 2
# >= 7700 < 7800 , 7750 , 15, 1
# >= 7800 < 7900 , 7850 , 15, 0
# >= 7900 < 8000 , 7950 , 19, 4
# >= 8000 < 8100 , 8050 , 27, 8
# >= 8100 < 8200 , 8150 , 35, 8
# >= 8200 < 8400 , 8300 , 43, 8
# >= 8400 < 8600 , 8500 , 59, 16
# >= 8600 < 8800 , 8700 , 68, 9
# >= 8800 < 9000 , 8900 , 80, 12
# >= 9000 < 9500 , 9250 , 97, 17
# >= 9500 < 10000 , 9750 , 100, 3
# # target 25.0%,8075.0
# # target 50.0%,8487.5
usage="
The possible options to this script are
-t #threads
-s specify 1 to save client logs to local dir
-a Average transfer rate to achieve
-p Peak Rate for Token Bucket
-d Delay (seconds) to introduce at receiver
"

echo "Run from ~/fbcode - or fbmake runtests"

# Set DO_VERIFY:
# to 1 : slow/expensive but checks correctness
# to 0 : fast for repeated benchmarking not for correctness
DO_VERIFY=1
NC="nc -4" # ipv4 only
REALPATH=/mnt/vol/engshare/svnroot/tfb/trunk/www/scripts/bin/realpath

# Verbose:
#WDTBIN="_bin/wdt/wdt -minloglevel 0"
# Fastest:
BS=`expr 256 \* 1024`
threads=8
# set a default value for avg rate to test throttler
avg_rate=3000
max_rate=-1
keeplog=0
delay=0
if [ "$1" == "-h" ]; then
  echo "$usage"
  exit 0
fi
while getopts ":t:a:p:s:d:h:" opt; do
  case $opt in
    t) threads="$OPTARG"
    ;;
    s) keeplog="$OPTARG"
    ;;
    a) avg_rate="$OPTARG"
    ;;
    p) max_rate="$OPTARG"
    ;;
    d) delay="$OPTARG"
    ;;
    h) echo "$usage"
       exit
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done
printf "(Sockets,Average rate, Max_rate, Save local?, Delay)=%s,%s,%s,%s,%s\n" "$threads" "$avg_rate" "$max_rate" "$keeplog" "$delay"
#WDTBIN_OPTS="-buffer_size=$BS -num_sockets=8 -minloglevel 2 -sleep_ms 1 -max_retries 999"
WDTBIN_OPTS="-minloglevel=0 -sleep_millis 1 -max_retries 999 -full_reporting "\
"-avg_mbytes_per_sec=$avg_rate -max_mbytes_per_sec=$max_rate "\
"-num_ports=$threads -throttler_log_time_millis=200 -enable_checksum=true"
WDTBIN="_bin/wdt/wdt $WDTBIN_OPTS"

BASEDIR=/dev/shm/tmpWDT
#BASEDIR=/data/wdt/tmpWDT
mkdir -p $BASEDIR
DIR=`mktemp -d --tmpdir=$BASEDIR`
echo "Testing in $DIR"

#pkill -x wdt

mkdir $DIR/src
mkdir $DIR/extsrc

#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec /usr/share $DIR/src
#cp -R wdt folly /usr/bin /usr/lib /usr/lib64 /usr/libexec $DIR/src
cp -R wdt folly /usr/share $DIR/src
#cp -R wdt folly $DIR/src
# Removing symlinks which point to the same source tree
for link in `find -L $DIR/src -xtype l`
do
  real_path=`$REALPATH $link`;
  if [[ $real_path =~ ^$DIR/src/* ]]
  then
    echo "Removing symlink $link"
    rm $link
  fi
done
#cp -R wdt $DIR/src

#for size in 1k 64K 512K 1M 16M 256M 512M 1G
#for size in 512K 1M 16M 256M 512M 1G
for size in 1k 64K 512K 1M 16M 256M 512M
do
    base=inp$size
    echo dd if=/dev/... of=$DIR/src/$base.1 bs=$size count=1
#    dd if=/dev/urandom of=$DIR/src/$base.1 bs=$size count=1
    dd if=/dev/zero of=$DIR/src/$base.1 bs=$size count=1
    for i in {2..8}
    do
        cp $DIR/src/$base.1 $DIR/src/$base.$i
    done
done
echo "done with setup"

# test symlink issues
(cd $DIR/src ; touch a; ln -s doesntexist badlink; dd if=/dev/zero of=c bs=1024 count=1; mkdir d; ln -s ../d d/e; ln -s ../c d/a)
(cd $DIR/extsrc; mkdir TestDir; mkdir TestDir/test; cd TestDir; echo "Text1" >> file1; cd test; echo "Text2" >> file1; ln -s $DIR/extsrc/TestDir; cp -R $DIR/extsrc/TestDir $DIR/src)

# Various smaller tests if the bigger one fails and logs are too hard to read:
#cp wdt/wdtlib.cpp wdt/wdtlib.h $DIR/src
#cp wdt/*.cpp $DIR/src
#cp /usr/bin/* $DIR/src
#cp wdt/wdtlib.cpp $DIR/src/a
#cp wdt/wdtlib.h  $DIR/src/b
#head -30 wdt/wdtlib.cpp >  $DIR/src/c
# Can't have both client and server send to stdout in parallel or log lines
# get mangled/are missing - so we redirect the server one
$WDTBIN -minloglevel=1 -directory $DIR/dst > $DIR/server.log 2>&1 &
# client now retries connects so no need wait for server to be up
pidofreceiver=$!
# Only 1 socket (single threaded send/receive)
#$WDTBIN -num_sockets=1 -directory $DIR/src -destination ::1
# Normal

#time trickle -d 1000 -u 1000 $WDTBIN -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log
if [ $delay -gt 0 ] ; then
  (sleep 3; kill -STOP $pidofreceiver; sleep $delay; kill -CONT $pidofreceiver ) &
fi
echo "$WDTBIN -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log"
time $WDTBIN -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log

echo -n e | $NC localhost 22356

$WDTBIN -directory $DIR/dst_symlinks >> $DIR/server.log 2>&1 &
echo "$WDTBIN -follow_symlinks -directory $DIR/src -destination $HOSTNAME |& tee -a $DIR/client.log"
time $WDTBIN -follow_symlinks -directory $DIR/src -destination $HOSTNAME |& tee -a $DIR/client.log

echo -n e | $NC localhost 22356

# rsync test:
#time rsync --stats -v -W -r $DIR/src/ $DIR/dst/

# No need to wait for transfer to finish, client now exits when last byte is saved


if [ $DO_VERIFY -eq 1 ] ; then
    echo "Verifying for run without follow_symlinks"
    echo "Checking for difference `date`"

    NUM_FILES=`(cd $DIR/dst ; ( find . -type f | wc -l))`
    echo "Transfered `du -ks $DIR/dst` kbytes across $NUM_FILES files"

    (cd $DIR/src ; ( find . -type f -print0 | xargs -0 md5sum | sort ) \
        > ../src.md5s )
    (cd $DIR/dst ; ( find . -type f -print0 | xargs -0 md5sum | sort ) \
        > ../dst.md5s )

    echo "Should be no diff"
    (cd $DIR; diff -u src.md5s dst.md5s)
    STATUS=$?


    echo "Verifying for run with follow_symlinks"
    echo "Checking for difference `date`"

    NUM_FILES=`(cd $DIR/dst_symlinks; ( find . -type f | wc -l))`
    echo "Transfered `du -ks $DIR/dst_symlinks` kbytes across $NUM_FILES files"

    (cd $DIR/src ; ( find -L . -type f -print0 | xargs -0 md5sum | sort ) \
        > ../src_symlinks.md5s )
    (cd $DIR/dst_symlinks ; ( find . -type f -print0 | xargs -0 md5sum \
        | sort ) > ../dst_symlinks.md5s )

    echo "Should be no diff"
    (cd $DIR; diff -u src_symlinks.md5s dst_symlinks.md5s)
    SYMLINK_STATUS=$?
    if [ $STATUS -eq 0 ] ; then
      STATUS=$SYMLINK_STATUS
    fi
#(cd $DIR; ls -lR src/ dst/ )
else
    echo "Skipping independant verification"
    STATUS=0
fi


echo "Server logs:"
cat $DIR/server.log
if [ $keeplog -ne 0 ]; then
  cp $DIR/client.log client.log
fi
if [ $STATUS -eq 0 ] ; then
  echo "Good run, deleting logs in $DIR"
  find $DIR -type d | xargs chmod 755 # cp -r makes lib/locale not writeable somehow
  rm -rf $DIR
else
  echo "Bad run ($STATUS) - keeping full logs and partial transfer in $DIR"
fi

exit $STATUS
