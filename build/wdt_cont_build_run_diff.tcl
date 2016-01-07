#! /usr/bin/env tclsh
# run _setup once and then this will run a loop and email results
# TODO: rewrite in python

set diff [lindex $argv 0]
puts "Working on diff $diff"
if {[string length $diff]==0} {
  puts stderr "Need DXXXXX argument"
  exit 1
}

# In order for /data/users/$USER to be different than default when creating
# wdtTest for xfs test but exists and so those directory which we rm
# don't conflict with the users' normal runs we hack the USER env var:
append ::env(USER) "_wdt_contbuild"

set userdir $::env(USER)
puts "Will run script with USER env = $userdir"

set maxTestDuration "15m"
set totalMaxDuration "35m"

puts "Max test duration: $maxTestDuration - Max total $totalMaxDuration"

# Set throughput - lower for now / there is some issue with kernel or env
# (or our code?)
set ::env(WDT_THROUGHPUT) 13000

set CDIR "/data/users/$userdir"

# path and ld library path
set ::env(PATH) "$CDIR/bin:$::env(PATH)"
if {[info exists ::env(LD_LIBRARY_PATH)]} {
    set ::env(LD_LIBRARY_PATH) "$CDIR/lib:$::env(LD_LIBRARY_PATH)"
} else {
    set ::env(LD_LIBRARY_PATH) "$CDIR/lib"
}

puts "PATH=$::env(PATH)  LD_LIBRARY_PATH=$::env(LD_LIBRARY_PATH)"

# extra stuff in the topic (where this is running/patch/.... etc)
set EXTRA ""
if {[info exists ::env(WDT_CONTBUILD_EXTRA_SUBJECT)]} {
    set EXTRA $::env(WDT_CONTBUILD_EXTRA_SUBJECT)
}

# Email to/from
set TO "wdt-builds@fb.com"
set FROM $TO
# start first build right away, helps testing, will use previous 10min TS/name
set DELTA 0
proc printDate {ts} {
   puts "[clock format $ts]"
}
proc sleep {time} {
    after [expr $time*1000] set end 1
    vwait end
}

# we will email for the first change
set last {}
# uncomment to force initial version update after restart
# set last "force"
# previous hg log for wdt (will cause email first too)
set hgprev {none}
# also email every x :
set emailEvery "6 hours"

proc nextEmail {} {
    global nextEmail emailEvery
    set nextEmail [clock scan $emailEvery]
    puts "Will email after [clock format $nextEmail]"
}

proc sendEmail {reason} {
    global type msg TO FROM LOGF EXTRA diff
    puts "Sending email to $TO because: $reason"
    set emailFileName ${LOGF}.email
    # Email headers
    set f [open $emailFileName w]
    puts $f "From: $FROM"
    puts $f "To: $TO"
    puts $f "Subject: WDT build: ${type}-${diff}${EXTRA}: $msg ($reason)"
    puts $f {Content-type: text/plain; charset="UTF-8"}
    puts $f ""; # seperate headers from body
    puts $f "filtered log, full log at https://fburl.com/wdt_${type}_builds"
    close $f
    # rest of the body of the email:
    # filters info and vlog and normal compilation times:
    exec egrep -v {^([IV]| [0-9].[0-9][0-9]s )} $LOGF >> $emailFileName
    # Sending both
    exec sendmail $TO < $emailFileName
    file delete $emailFileName
    nextEmail
}

nextEmail

cd $CDIR/fbsource/fbcode

# no auto versioning in this script
# only 2 types for now - either 'open source' on the mac or full otherwise
set os [exec uname]
if {$os == "Darwin"} {
    set type "mac"
    set extraCmds "echo done"
    set targetDir "/usr/local/var/www/wdt_builds/"
    set sudo ""
    set timeoutCmd "gtimeout"
} else {
    set type "unix"
    set timeoutCmd "timeout"
    set extraCmds "cd $CDIR/fbsource/fbcode &&\
     (sudo tc qdisc del dev lo root; sudo ip6tables --flush || true) &&\
     time fbconfig --clang -r wdt &&\
     time fbmake opt &&\
     time wdt/test/wdt_max_send_test.sh |& tail -50 &&\
     time wdt/test/wdt_max_send_test.sh _bin/wdt/fbonly/wdt_fb |& tail -50 &&\
     time fbconfig --sanitize address -r wdt &&\
     time fbmake dbg &&\
     time $timeoutCmd $maxTestDuration fbmake runtests --extended-tests --run-disabled --record-results --return-nonzero-on-timeouts &&\
     sudo tc qdisc add dev lo root netem delay 20ms 10ms \
     duplicate 1% corrupt 0.1% &&\
     echo rerunning tests with tc delays &&\
     time $timeoutCmd $maxTestDuration fbmake runtests --run-disabled --record-results --return-nonzero-on-timeouts &&\
     sudo tc qdisc del dev lo root"
    set targetDir "~/public_html/wdt_builds/"
    set sudo "sudo"
}

while {1} {
    # round the time to 10 minutes (in part so log files aren't growing forever)
    set now [clock seconds]
    printDate $now
    set tenmin [expr {($now+$DELTA*60)/600*600}]
    printDate $tenmin
    set sleep [expr $tenmin-$now]
    puts "Sleeping $sleep seconds"
    sleep $sleep
    # after first time, run X mins from now
    set DELTA 10; # minutes
    set LOGTS [clock format $tenmin -format %d%H%M]
    set LOGF "$CDIR/$LOGTS.log"
    puts "Logging to $LOGF"
    # cleanup previous builds failure - sudo not needed/asking for passwd on mac
    if {[catch {exec $timeoutCmd $totalMaxDuration sh -c "set -o pipefail;\
     set -x; date; uname -a;\
     $sudo rm -rf /tmp/wdtTest_$userdir /dev/shm/wdtTest_$userdir wdtTest &&\
     cd $CDIR/fbsource/fbcode && (hg book -d arcpatch-$diff || true) &&\
     time hg pull -r master -u --dest master && arc patch $diff &&\
     ( time hg rebase -d master || true ) &&\
     hg bookmark -v &&\
     hg log -l 2 && hg log -v -l 1 folly && hg log -v -l 2 wdt &&\
     cd $CDIR/cmake_wdt_build && time make -j 4 && \
     CTEST_OUTPUT_ON_FAILURE=1 time $timeoutCmd $maxTestDuration make test &&\
     $extraCmds" >& $LOGF < /dev/null} results options]} {
        set msg "BAD"
    } else {
        set msg "GOOD"
    }
    puts $msg
    catch {exec hg log -l 2 -T "{rev}\n" wdt | tail -1} hgout
    puts "wdt changeset now $hgout"
    if {[string length $last]==0} {
        sendEmail "contbuild restarted"
    } elseif {[string compare $hgout $hgprev]} {
        sendEmail "hg log wdt change"
    } elseif {[string compare $last $msg]} {
        # Build changed from $last to $msg
        sendEmail "was $last"
    } elseif {[clock seconds]>$nextEmail} {
        # periodic emails
        sendEmail "every $emailEvery email"
    }
    # works with fburl homedirs
    set target "${targetDir}${LOGTS}_${msg}.log"
    file copy -force $LOGF $target
    puts "Copied to $target"
    set last $msg
    set hgprev $hgout
}
