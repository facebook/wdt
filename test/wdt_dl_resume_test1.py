#!/usr/bin/env python3

# the test is similar to wdt_dl_resume_test1.py
# we initiate a transfer and cancel it 2/3+ in flight
# we make sure the second transfer finishes the job
# but takes less than 50% of the first one
# 1/3 ~= 50% * 2/3
# in the end the test ensures that receiver imposes
# -enable_download_resumption on the sender

from time import time

from common_utils import (
    check_transfer_status,
    create_test_directory,
    generate_random_files,
    get_test_count,
    run_sender,
    start_receiver,
    start_test,
    verify_transfer_success,
)

root_dir = create_test_directory("/tmp")
generate_random_files(100 * 1024 * 1024)

start_test("receiver -enable_download_resumption imposes it on sender")
test_count = get_test_count()
start_time = time.time()
start_receiver(
    "-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption -abort_after_seconds=7 -delete_extra_files=true"
)
run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
check_transfer_status(expect_failed=True, check_receiver=True)
dur1 = time.time() - start_time
start_time = time.time()
start_receiver(
    "-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption -delete_extra_files=true"
)
run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
check_transfer_status()
dur2 = time.time() - start_time
# 1.9 = 2 with 5% tolerance
if dur1 < 1.9 * dur2:
    print("Aborted previously, this run should take < 50% of prev cycle")
    exit(1)
exit(verify_transfer_success())
