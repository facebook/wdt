#! /usr/bin/env tclsh
# Quick Tcl script to auto commit bump in the version


proc find_base_dir {} {
    global argv0
    return [file dirname $argv0]
}

proc version_update {} {
    set basedir [find_base_dir]
    cd [file join $basedir ".."]
    # that script expects to be run from wdt/
    source [file join build/version_update.tcl]
}


proc auto_commit {} {
    set commit_msg {wdt version bump
Summary:
wdt version bump auto commit

Test Plan:
n/a

Reviewers: ldemailly

Reviewed by: ldemailly
}
    puts [exec hg commit -m $commit_msg]
    puts "committed"
}

proc auto_land {} {
    puts [exec echo y | arc land --ninja]
    puts "landed"
}

proc check_clean {} {
    catch {exec hg status -m -a -r -d} status
    set status [string trim $status]
    if {[string length $status]} {
        puts "Error, aborting: not clean repo state: $status"
        exit 1
    }
}

check_clean
version_update
puts "WARNING - this will auto land:"
puts [exec hg status]
puts "Interrupt now if that's not what you want..."
after 2000
auto_commit
auto_land
