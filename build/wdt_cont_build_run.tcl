#! /usr/bin/env tclsh
# run _setup once and then this will run a loop and email results
# TODO: rewrite in python and less if/else/state mess for autoversion+autopkg
# (right now it will auto version successful new changes to wdt/ and then
# auto package if the subsequent auto version change still builds and test good)

# In order for /data/users/$USER to be different than default when creating
# wdtTest for xfs test but exists and so those directory which we rm
# don't conflict with the users' normal runs we hack the USER env var:
append ::env(USER) "_wdt_contbuild"

set userdir $::env(USER)
puts "Will run script with USER env = $userdir"

set maxTestDuration "20m"
set totalMaxDuration "50m"

puts "Max test duration: $maxTestDuration - Max total $totalMaxDuration"

# Set throughput - lower for now / there is some issue with kernel or env
# (or our code?)
set ::env(WDT_THROUGHPUT) 11000

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
    global type msg TO FROM LOGF EXTRA
    puts "Sending email to $TO because: $reason"
    set emailFileName ${LOGF}.email
    # Email headers
    set f [open $emailFileName w]
    puts $f "From: $FROM"
    puts $f "To: $TO"
    puts $f "Subject: WDT build: ${type}${EXTRA}: $msg ($reason)"
    puts $f {Content-type: text/plain; charset="UTF-8"}
    puts $f ""; # seperate headers from body
    puts $f "filtered log, full log at https://fburl.com/wdt_${type}_builds"
    puts $f "and $LOGF on the machine"
    close $f
    # rest of the body of the email:
    # filters info and vlog and normal compilation times:
    exec egrep -v {^([IV]| [0-9].[0-9][0-9]s |BUILT|CACHE|FOUND)} $LOGF >> $emailFileName
    # Sending both
    exec sendmail $TO < $emailFileName
    file delete $emailFileName
    nextEmail
}

nextEmail

cd $CDIR/fbsource/fbcode


# only 2 types for now - either 'open source' on the mac or full otherwise
set os [exec uname]
if {$os == "Darwin"} {
    set type "mac"
    set extraCmds "echo done"
    set targetDir "/usr/local/var/www/wdt_builds/"
    set sudo ""
    set timeoutCmd "gtimeout"
    set autoVersion 0
} else {
    set type "unix"
    set timeoutCmd "timeout"
    set extraCmds "df -h /dev/shm && cd $CDIR/fbsource/fbcode &&\
     (sudo tc qdisc del dev lo root; sudo ip6tables --flush || true) &&\
     time buck build @mode/opt wdt/... &&\
     time wdt/test/wdt_max_send_test.sh ./buck-out/gen/wdt/wdt |& tail -50 &&\
     EXTRA_WDT_OPTIONS=\"-scuba_dataset=wdt_transfer_report_test\" time wdt/test/wdt_max_send_test.sh ./buck-out/gen/wdt/fbonly/wdt_fb |& tail -50 &&\
     time buck build @mode/dbg-asan wdt/... &&\
     time $timeoutCmd $maxTestDuration buck test @mode/dbg-asan wdt/... -- --run-disabled --record-results --return-nonzero-on-timeouts --print-long-results &&\
     sudo tc qdisc add dev lo root netem delay 20ms 10ms \
     duplicate 1% corrupt 0.1% &&\
     echo rerunning tests with tc delays &&\
     time $timeoutCmd $maxTestDuration buck test wdt/... -- --run-disabled --record-results --return-nonzero-on-timeouts --print-long-results &&\
     sudo tc qdisc del dev lo root &&\
     time $timeoutCmd $maxTestDuration buck test stargate/... -- --print-long-results --return-nonzero-on-timeouts"
    set targetDir "~/public_html/wdt_builds/"
    set sudo "sudo"
    set autoVersion 1
}

set autoPkgNext 0
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
     $sudo rm -rf /tmp/wdtTest_$userdir /dev/shm/wdtTest_$userdir wdtTest \
     /dev/shm/stargate-* &&\
     cd $CDIR/fbsource/fbcode && time hg pull -r master -u --dest master &&\
     hg log -l 1 && hg log -v -l 1 folly && hg log -v -l 1 wdt &&\
     hg log -v -l 1 stargate &&\
     cd $CDIR/cmake_wdt_build && time make -j 4 && \
     CTEST_OUTPUT_ON_FAILURE=1 time $timeoutCmd $maxTestDuration make test &&\
     $extraCmds" >& $LOGF < /dev/null} results options]} {
        set msg "BAD"
        set good 0
    } else {
        set msg "GOOD"
        set good 1
    }
    puts $msg
    set autoBump $autoVersion; # only on unix build will it possibly trigger
    set firstLine "err"
    if {[catch {exec hg log -l 1 -T "{desc}" wdt} prevDesc]} {
        puts "Error getting desc: $prevDesc"
        set autoBump 0
        set autoPkgNext 0
    } else {
        set firstLine [lindex [split $prevDesc \n] 0];
        if {![string compare $firstLine "wdt/stargate version bump"]} {
            puts "Previous commit is auto commit, will not auto commit!"
            set autoBump 0
        } else {
            puts "Last commit not a version bump, resetting autoPkg"
            set autoPkgNext 0
        }
    }
    catch {exec hg log -l 1 -T "{rev}" wdt stargate} hgout
    puts "wdt changeset now $hgout (autoBump $autoBump, good $good, autoPkg $autoPkgNext) $firstLine"
    if {[string length $last]==0} {
        sendEmail "contbuild restarted"
    } elseif {[string compare $hgout $hgprev]} {
        sendEmail "hg log wdt change $firstLine"
        if {$good} {
            if {$autoBump} {
                puts "Auto updating version after good build for $firstLine:"
                set LOGF "$LOGF.auto_bump"
                if {[catch {exec wdt/build/auto_version.tcl >& $LOGF}]} {
                    puts "Unable to bump - see $LOGF"
                    set msg "auto version update failure"
                    sendEmail "exec error"
                } else {
                    puts "Setting autoPkgNext for next build"
                    set autoPkgNext 1
                }
            } elseif {$autoPkgNext} {
                set autoPkgNext 0
                set LOGF "$LOGF.fbpkg"
                puts "Auto fbpkg after auto bump"
                if {[catch {exec fbpkg build wdt >& $LOGF}]} {
                    puts "Unable to build fbpkg - see $LOGF"
                    set msg "auto fbpkg build failure"
                    sendEmail "exec error"
                } else {
                    catch {exec tail -1 $LOGF} pkgver
                    catch {exec ./buck-out/gen/wdt/fbonly/wdt_fb --version} ver
                    regexp {[0-9.]+} $ver shortver
                    puts "Package built: $pkgver contains v$shortver"
                    catch {exec fbpkg tag --yes $pkgver v$shortver >>& $LOGF}
                    set msg "new package built $shortver -> $pkgver"
                    sendEmail "new package"
                }
            }
            set msg "GOOD"; # otherwise we get a subsequent email about ->good
        } else {
            # reset autopkgnext
            set autoPkgNext 0
        }
    } elseif {[string compare $last $msg]} {
        # Build changed from $last to $msg
        sendEmail "was $last"
        set autoPkgNext 0
    } elseif {[clock seconds]>$nextEmail} {
        # periodic emails
        sendEmail "every $emailEvery email"
    }
    # works with fburl homedirs
    set target "${targetDir}${LOGTS}_${msg}.log"
    catch {file copy -force $LOGF $target} copyMsg
    puts "Copy to $target : $copyMsg"
    set last $msg
    set hgprev $hgout
}
