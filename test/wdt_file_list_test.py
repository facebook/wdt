#! /usr/bin/env python

import os
from common_utils import *

def run_test(name, sender_extra_flags):
    global wdt_receiver, wdtbin, test_count, root_dir
    print("{0}. Testing {1}".format(test_count, name))
    receiver_cmd = "{0} -directory {1}/dst{2}".format(
            wdt_receiver, root_dir, test_count)
    (receiver_process, connection_url) = start_receiver(
            receiver_cmd, root_dir, test_count)
    sender_cmd = ("{0} -directory {1}/src -connection_url \'{2}\' -manifest "
                  "{1}/file_list {3}").format(
                          wdtbin, root_dir, connection_url, test_count,
                          sender_extra_flags)
    transfer_status = run_sender(sender_cmd, root_dir, test_count)
    transfer_status |= receiver_process.wait()
    check_transfer_status(transfer_status, root_dir, test_count)
    test_count += 1

def main():
    global wdt_receiver, wdtbin, test_count, root_dir
    root_dir = create_test_directory("/tmp")
    src_dir = root_dir + "/src"
    # create random files
    generate_random_files(src_dir, 256 * 1024)

    # generate file list
    file_list_in = open(os.path.join(root_dir, "file_list"), 'wb')
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if file == "file0":
                continue
            if file == "file1":
                # add a size for file1
                file_list_in.write("{0}\t{1}".format(file, 1025))
            else:
                file_list_in.write(file)
            file_list_in.write('\n')
    file_list_in.close()
    print("Done with set-up")

    wdtbin_opts = "-full_reporting"
    wdtbin = "_bin/wdt/wdt " + wdtbin_opts
    wdt_receiver = wdtbin + " -start_port 0 -num_ports 2"
    test_count = 0

    run_test("basic file list", "")
    run_test("file list with direct reads", "-odirect_reads")
    run_test("file list with open early", "-open_files_during_discovery")
    run_test("file list with open early and direct reads",
            "-open_files_during_discovery -odirect_reads")

    os.remove(os.path.join(src_dir, "file0"))
    open(os.path.join(src_dir, "file1"), 'a').truncate(1025)

    status = verify_transfer_success(root_dir, range(test_count))
    exit(status)

if __name__ == "__main__":
    main()
