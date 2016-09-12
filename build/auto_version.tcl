#! /usr/bin/env tclsh
# Quick Tcl script to auto commit bump in the version


proc find_base_dir {} {
    global argv0
    return [file dirname $argv0]
}

proc wdt_version_update {} {
    set curDir [pwd]
    set basedir [find_base_dir]
    cd [file join $basedir ".."]
    # that script expects to be run from wdt/
    source build/version_update.tcl
    cd $curDir
}

proc sg_version_update {} {
    set curDir [pwd]
    set basedir [find_base_dir]
    cd [file join $basedir ".." ".." "stargate"]
    # that script expects to be run from stargate/
    source version_update.tcl
    cd $curDir
}

proc auto_commit {} {
    set commit_msg {wdt/stargate version bump
Summary:
wdt/stargate version bump auto commit

Test Plan:
n/a

Reviewers: svcscm

Reviewed by: svcscm
}
    puts [exec hg commit -u svcscm@fb.com -m $commit_msg]
    puts "*** Committed"
}

proc auto_land {} {
    puts [exec hg push --to master -r . --pushvars "BYPASS_REVIEW=true"]
    puts "*** Landed, going back to master:"
    puts [exec hg update master]
}

proc check_clean {} {
    catch {exec hg status -m -a -r -d} status
    set status [string trim $status]
    if {[string length $status]} {
        puts "*** Error, aborting: not clean repo state: $status"
        exit 1
    }
}

proc switch_to_and_update_master {} {
    puts "*** Switching to and updating master:"
    puts [exec hg pull 2>@1]
    puts [exec hg update master]
}

# Put it all together:
switch_to_and_update_master
check_clean
wdt_version_update
sg_version_update
puts "*** WARNING - this will auto land:"
puts [exec hg status]
puts "Interrupt now if that's not what you want..."
after 2000
auto_commit
auto_land
