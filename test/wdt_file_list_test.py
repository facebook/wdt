#! /usr/bin/env python

from common_utils import *

skip_tests = set()

def run_test(name, sender_extra_flags, fail_transfer=False):
    global wdt_receiver_arg, wdt_sender_arg, test_count, root_dir
    print("{0}. Testing {1}".format(test_count, name))
    receiver_arg = "{0} -directory {1}/dst{2}".format(
            wdt_receiver_arg, root_dir, test_count)
    (receiver_process, connection_url) = start_receiver_arg(
            receiver_arg, root_dir, test_count)
    if fail_transfer is True:
        connection_url += "&id=blah1234"
    sender_arg = ("{0} -directory {1}/src -connection_url \'{2}\' -manifest "
                  "{1}/file_list {3}").format(
                          wdt_sender_arg, root_dir, connection_url,
                      sender_extra_flags)
    transfer_status = run_sender_arg(sender_arg, root_dir, test_count)
    transfer_status |= receiver_process.wait()
    if fail_transfer is True:
        if not transfer_status:
            print("test was expected to fail but succeeded")
            exit(transfer_status + 1)
        else:
            skip_tests.add(test_count)
    else:
        check_transfer_status(transfer_status, root_dir, test_count)
    fail_errors = ["PROTOCOL_ERROR", "Bad file descriptor"]
    check_logs_for_errors(root_dir, test_count, fail_errors)
    test_count += 1


def main():
    global wdt_receiver_arg, wdt_sender_arg, test_count, root_dir
    set_binaries()
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

    wdtbin_opts = " -full_reporting --enable_perf_stat_collection "

    wdt_receiver_arg = wdtbin_opts + " -num_ports 2"
    wdt_sender_arg = wdtbin_opts
    test_count = 0

    run_test("basic file list", "")
    run_test("file list with direct reads", "-odirect_reads")
    run_test("file list with all open early", "-open_files_during_discovery -1")
    run_test("file list with 1 open early", "-open_files_during_discovery 1")
    run_test("file list with open early and direct reads",
            "-open_files_during_discovery -1 -odirect_reads")
    run_test("failed transfer with  open early and direct reads",
            "-open_files_during_discovery -1 -odirect_reads",
            True)

    os.remove(os.path.join(src_dir, "file0"))
    open(os.path.join(src_dir, "file1"), 'a').truncate(1025)

    status = verify_transfer_success(root_dir, range(test_count), skip_tests)
    exit(status)

if __name__ == "__main__":
    main()
