#! /usr/bin/env python

from common_utils import *


def run_test(name, sender_extra_flags, fail_transfer=False):
    start_test(name)
    connection_url = start_receiver(wdt_receiver_arg)
    if fail_transfer is True:
        connection_url += "&id=blah1234"
    sender_arg = "{0} -manifest {1}/file_list {2}".format(
        wdt_sender_arg, root_dir, sender_extra_flags
    )
    run_sender(sender_arg, connection_url)
    check_transfer_status(fail_transfer)
    fail_errors = ["PROTOCOL_ERROR", "Bad file descriptor"]
    check_logs_for_errors(fail_errors)


root_dir = create_test_directory("/tmp")
# create random files
src_dir = generate_random_files(256 * 1024)

# generate file list
file_list_in = open(os.path.join(root_dir, "file_list"), 'wb')
for root, dirs, files in os.walk(src_dir):
    for file in files:
        if file == "file0":
            # skip that one
            continue
        if file == "file1":
            # add a size for file1
            file_list_in.write("{0}\t{1}".format(file, 1025))
        else:
            file_list_in.write(file)
        file_list_in.write('\n')
file_list_in.close()
print("Done with set-up")

wdtbin_opts = " -full_reporting --enable_perf_stat_collection "

wdt_receiver_arg = wdtbin_opts + " -num_ports 2"
wdt_sender_arg = wdtbin_opts

run_test("basic file list", "")
run_test("file list with direct reads", "-odirect_reads")
run_test("file list with all open early", "-open_files_during_discovery -1")
run_test("file list with 1 open early", "-open_files_during_discovery 1")
run_test(
    "file list with open early and direct reads",
    "-open_files_during_discovery -1 -odirect_reads"
)
run_test(
    "failed transfer with open early and direct reads",
    "-open_files_during_discovery -1 -odirect_reads", True
)

os.remove(os.path.join(src_dir, "file0"))
open(os.path.join(src_dir, "file1"), 'a').truncate(1025)

status = verify_transfer_success()
exit(status)
