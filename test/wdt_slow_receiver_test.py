#! /usr/bin/env python

# In this test, the receiver is extremely slow. Sender should detect that and
# wait for the receiver to finish. Read timeout for sender is small to trigger
# tcp unacked bytes checking code.

from common_utils import *

def main():
    test_count = 0
    root_dir = create_test_directory("/tmp")
    src_dir = "{0}/src{1}" .format(root_dir, test_count)
    generate_random_files(src_dir, 140 * 1024 * 1024)

    receiver_cmd = ("_bin/wdt/wdt -directory {0}/dst{1} -start_port=0 "
                    "-num_ports=1 "
                    "-avg_mbytes_per_sec=10").format(
                            root_dir, test_count)
    (receiver_process, url) = start_receiver(receiver_cmd, root_dir, test_count)
    sender_cmd = ("_bin/wdt/wdt -directory {0}/src{1} -read_timeout_millis=500 "
                  "-num_ports=1 -enable_perf_stat_collection "
                  "-connection_url=\'{2}\'").format(root_dir, test_count, url)
    status = run_sender(sender_cmd, root_dir, test_count)
    status |= receiver_process.wait()

    check_transfer_status(status, root_dir, test_count)
    status = verify_transfer_success(root_dir, range(test_count))
    exit(status)


if __name__ == "__main__":
    main()
