#! /usr/bin/env python

import os
from common_utils import *

def run_test(name, receiver_extra_flags, sender_extra_flags):
    global wdtbin, test_count, root_dir
    print("{0}. Testing {1}".format(test_count, name))
    receiver_cmd = "{0} {1}".format(wdtbin, receiver_extra_flags)
    (receiver_process, connection_url) = start_receiver(
            receiver_cmd, root_dir, test_count)
    sender_cmd = ("{0} -connection_url \'{1}\' {2}").format(
        wdtbin, connection_url, sender_extra_flags)
    sender_status = run_sender(sender_cmd, root_dir, test_count)
    print("status for sender {0}".format(sender_status))
    receiver_status = receiver_process.wait()
    print("status for receiver {0}".format(receiver_status))
    test_count += 1
    return sender_status, receiver_status

def error(what):
    global broken
    print("ERR {0}".format(what))
    broken += 1

wdtbin = os.getcwd() + "/_bin/wdt/wdt"  # todo this should handled in common !

root_dir = create_test_directory("/tmp")
data_dir = root_dir + "dir1"
create_directory(data_dir)
os.chdir(data_dir)
fname = "existingfile"
f = open(fname, "w")
f.write("existing data")
f.close()
mtime = os.stat(fname).st_mtime
print("mtime is {0}".format(mtime))

test_count = 1

sender_st, rec_st = run_test("default - should fail", "", "")

broken = 0
if sender_st == 0:
    error("sender should have failed")
if rec_st == 0:
    error("receiver should have failed")
newmtime = os.stat(fname).st_mtime
if newmtime != mtime:
    error("file shouldn't have changed {0} -> {1}".format(mtime, newmtime))

mtime = newmtime

sender_st, rec_st = run_test("with -overwrite should succeed", "-overwrite", "")

if sender_st != 0:
    error("sender should have worked")
if rec_st != 0:
    error("receiver should have worked")
newmtime = os.stat(fname).st_mtime
if newmtime == mtime:
    error("file should have changed from {0}".format(mtime))

print("Total issues {0}".format(broken))
if not broken:
    print("Good run, deleting logs in " + root_dir)
    shutil.rmtree(root_dir)
exit(broken)
