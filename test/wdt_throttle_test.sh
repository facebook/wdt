# shellcheck disable=SC2148
# runs wdt with certain parameters for thread, average transfer rate, and peak rate
# peak rate is just a multiplier over average rate here
# This thread can be used by specifying a multiplier and the results of the
# logs for this experiment can be plotted.
BASE_DIR="/tmp/wdt_plots"
DATADIR=/dev/shm/tmpWDT
mkdir -p $BASE_DIR
DIR=`mktemp -d --tmpdir=$DATADIR`
echo "Testing in $DIR"
pkill -x wdt
mkdir $DIR/src
cp -R wdt folly /usr/share $DIR/src
# Removing symlinks which point to the same source tree
for link in `find -L $DIR/src -xtype l`
do
  real_path=`$REALPATH $link`;
  if [[ $real_path =~ ^$DIR/src/* ]]
  then
    rm $link
  fi
done
for size in 1k 64K 512K 1M 16M 256M 512M
do
    base=inp$size
    echo dd if=/dev/... of=$DIR/src/$base.1 bs=$size count=1
    dd if=/dev/zero of=$DIR/src/$base.1 bs=$size count=1
    for i in {2..8}
    do
        cp $DIR/src/$base.1 $DIR/src/$base.$i
    done
done
echo "done with setup"
printf "Starting the receiver"
WDTBIN="_bin/wdt/wdt -directory=$DIR/dst"
$WDTBIN -minloglevel=1 -directory $DIR/dst > $DIR/server.log 2>&1 &
pidofreceiver=$!
doit() {
  transfer_rate=$1
  peak_rate="$(echo "$transfer_rate*$2" | bc -l)"
  delay=$3
  printf "Running experiment with (Avg_Rate, Peak_Rate, Delay)=%s,%s,%s\n" "$transfer_rate" "$peak_rate" "$delay"
  echo $transfer_rate
  WDTBIN_OPTS="-minloglevel=0 -sleep_millis 1 -max_retries 999 -full_reporting -avg_mbytes_per_sec=$transfer_rate -max_mbytes_per_sec=$peak_rate -throttler_log_time_millis=200"
  if [ $delay -gt 0 ] ; then
    (sleep 3; kill -STOP $pidofreceiver; sleep $delay; kill -CONT $pidofreceiver ) &
  fi
  echo "$WDTBIN $WDTBIN_OPTS -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log"
  time $WDTBIN $WDTBIN_OPTS -directory $DIR/src -destination $HOSTNAME |& tee $DIR/client.log
  grep  Throttler:Transfer_Rates "$DIR/client.log" | awk -F '::' '{print $2}' > wdt_run_plot
  gnuplot << EOF
    set xlabel "Time(sec)"
    set ylabel "Throughput (MB)"
    set term png
    set output "plot_{$transfer_rate}_{$peak_rate}.png"
    plot "wdt_run_plot" using 1:2 with lines title "Avg Throughput", "wdt_run_plot" using 1:3 with lines title "Instant throughput"
EOF
  mv "plot_{$transfer_rate}_{$peak_rate}.png" "$BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.png"
  cp "$DIR/client.log" "$BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.logs"
  rm "wdt_run_plot"
  printf "Created the plot at $BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.png"
  rm -rf "$DIR/dst/*"
  sleep 2
}
if [[ ! -d "$BASE_DIR" ]] ; then
  mkdir "$BASE_DIR"
  printf "Creating a directory $BASE_DIR"
else
  rm -rf "$BASE_DIR/*"
  printf "Deleteing contents of $BASE_DIR"
fi
delay=5 #introduce delay of 5 seconds
max_runs=1 #number of runs for averaging
start=100
end=200
for (( num_runs=1; num_runs<=$max_runs; num_runs+=1 ))
do
  for (( t=$start; t<= $end; t+=100))
  do
    for mul in {1.1,1.2,1.3,1.5}
    do
      doit $t $mul $delay
    done
  done
done
printf "Killing the receiver"
kill -9 $pidofreceiver
rm -rf "$DIR/src"
rm -rf "$DIR/dst"
