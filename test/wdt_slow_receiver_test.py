#!/usr/bin/env python3

# In this test, the receiver is extremely slow. Sender should detect that and
# wait for the receiver to finish. Read timeout for sender is small to trigger
# tcp unacked bytes checking code.

from common_utils import (
    check_transfer_status,
    create_test_directory,
    generate_random_files,
    run_sender,
    start_receiver,
    start_test,
    verify_transfer_success,
)

# 1 time setup
create_test_directory("/tmp")
generate_random_files(140 * 1024 * 1024)

# test(s)
start_test("slow receiver")
start_receiver("-num_ports=1 -avg_mbytes_per_sec=10")
run_sender("-read_timeout_millis=300 -num_ports=1 -enable_perf_stat_collection")
check_transfer_status()
# md5 and cleanup at the end
exit(verify_transfer_success())
