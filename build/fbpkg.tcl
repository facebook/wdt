#! /usr/bin/env tclsh
#
# Quick script/standalone version of what happens  in wdt_cont_build_run
# when 2 successful builds happen back to back: we build a package with the
# new versioned one. This script is to do that manually - make sure you are
# on a clean, tested ( buck test wdt/... -- --run-disabled --extended-tests  \
# --print-long-results --record-results ) label, and you ran
# build/version_update.tcl and auto_version.tcl before than, waited / pulled
# version change from trunk and tested at a clean point - then this will
# build a non ephemeral package (which will get picked up by unicorn for
# instance).
#
# If said package is confirmed to be good, you can use
# fbonly/fbkpg_to_opsfiles.sh to push it (that version should be stable and
# not change more than once a month or so)
#

# Email to/from
set TO "wdt-builds@fb.com"
set FROM $TO
set EXTRA {}
set type "manual fbpkg"

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
}

if {![file exists wdt/Wdt.h]} {
    puts "Must be run from fbcode/ directory"
    exit 1
}

set LOGF "/tmp/wdt.fbpkg.log"
puts "*** Auto fbpkg - warning, make sure you tested the current rev as this"
puts "*** ends up in production soon after it is built... ^C now to abort"
after 4000
puts "building..."
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
