#! /usr/bin/env python

from common_utils import *


def run_test(name, receiver_extra_flags, sender_extra_flags, expect_fail=False):
    start_test(name)
    start_receiver(receiver_extra_flags)
    run_sender(sender_extra_flags)
    check_transfer_status(expect_fail)


root_dir = create_test_directory("/tmp")
data_dir = root_dir + "/dir1"
create_directory(data_dir)
fname = data_dir + "/existingfile"
f = open(fname, "w")
f.write("existing data")
f.close()
mtime = os.stat(fname).st_mtime
print("mtime is {0}".format(mtime))

test_count = 1

sender_args = "-directory " + data_dir
receiver_args = sender_args

run_test("default - should fail", receiver_args, sender_args, True)
newmtime = os.stat(fname).st_mtime
if newmtime != mtime:
    error("file shouldn't have changed {0} -> {1}".format(mtime, newmtime))

mtime = newmtime

run_test(
    "with -overwrite should succeed", receiver_args + " -overwrite", sender_args
)

newmtime = os.stat(fname).st_mtime
if newmtime == mtime:
    error("file should have changed from {0}".format(mtime))

# md5sums checks and cleanup
exit(verify_transfer_success())
