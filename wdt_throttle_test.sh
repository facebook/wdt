# runs wdt with certain parameters for thread, average transfer rate, and peak rate
# peak rate is just a multiplier over average rate here 
# This thread can be used by specifying a multiplier and the results of the 
# logs for this experiment can be plotted.
BASE_DIR="/tmp/wdt_plots"
doit() {
  transfer_rate=$1
  peak_rate="$(echo "$transfer_rate*$2" | bc -l)"
  delay=$3
  printf "Running experiment with (Avg_Rate, Peak_Rate, Delay)=%s,%s,%s\n" "$transfer_rate" "$peak_rate" "$delay"
  num_sockets=1
  echo $transfer_rate
  wdt/wdt_e2e_test.sh -t $num_sockets -a $transfer_rate -p $peak_rate -s 1 -d $delay
  grep  Throttler:Transfer_Rates client.log | awk -F '::' '{print $2}' > wdt_run_plot
  gnuplot << EOF
    set xlabel "Time(sec)"
    set ylabel "Throughput (MB)"
    set term png
    set output "plot_{$transfer_rate}_{$peak_rate}.png"
    plot "wdt_run_plot" using 1:2 with lines title "Avg Throughput", "wdt_run_plot" using 1:3 with lines title "Instant throughput"
EOF
  mv "plot_{$transfer_rate}_{$peak_rate}.png" "$BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.png"
  cp "client.log" "$BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.logs"
  rm "wdt_run_plot"
  printf "Created the plot at $BASE_DIR/plot_{$transfer_rate}_{$peak_rate}.png"
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
