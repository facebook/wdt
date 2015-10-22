#! /usr/bin/env tclsh
# Quick Tcl script to update the buildlevel (which is date+1 digit increment)

set fname "CMakeLists.txt"
set f [open $fname r+]

set content [read $f]

set re {^(project.*VERSION )([0-9]+)\.([0-9]+)\.([0-9]+)\)$}

if {![regexp -lineanchor $re $content all begin major minor build]} {
    puts stderr "Searched for regular expression: $re"
    puts stderr "Unable to find project version string in CMakeLists.txt!"
    exit 1
}

puts "In $fname, found for $begin"
puts "Major V = $major"
puts "Minor V = $minor"
puts "Build L = $build"

if {[string length $build] != 7} {
    puts stderr "Unexpected length for build \"$build\"";
}
set date [string range $build 0 5]
set incr [string range $build 6 6]


set today [clock format [clock seconds] -format "%y%m%d"]

puts "Previous build date $date ($incr) vs today $today"

if {[string equal $date $today]} {
   incr incr
   if {$incr > 9} {
     puts stderr "Ran too many times (10) on same day, aborting"
     exit 1
   }
} else {
   set incr 0
}

set newBuild "$today$incr"

puts "*** So new build is $newBuild"

set replace "\\1\\2.\\3.$newBuild)"
if {![regsub -lineanchor $re $content $replace content]} {
    puts stderr "Error substituting new build - $re $replace"
    exit 1
}

seek $f 0
puts -nonewline $f $content
close $f
puts "*** Updated $fname"

# Same on WdtConfig.h

set fname "WdtConfig.h"
set f [open $fname r+]
set content {}
while {![eof $f]} {
    set line [gets $f]
    if {[string match "#define WDT_VERSION_MAJOR*" $line]} {
        puts "Changing $line to:"
        set line "#define WDT_VERSION_MAJOR $major"
        puts $line
    } elseif {[string match "#define WDT_VERSION_MINOR*" $line]} {
        puts "Changing $line to:"
        set line "#define WDT_VERSION_MINOR $minor"
        puts $line
    } elseif {[string match "#define WDT_VERSION_BUILD*" $line]} {
        puts "Changing $line to:"
        set line "#define WDT_VERSION_BUILD $newBuild"
        puts $line
    } elseif {[string match "#define WDT_VERSION_STR*" $line]} {
        puts "Changing $line to:"
        set line "#define WDT_VERSION_STR \"$major.$minor.$newBuild-fbcode\""
        puts $line
    }
    lappend content $line
}

seek $f 0
puts -nonewline $f [join $content "\n"]
close $f

puts "*** Updated $fname"
