#! /usr/bin/env python

# the test ensures that when the receiver is with enable_download_resumption
# this will also impose it on the sender as well as set of files is kept
# the same as delete_extra_files is true on the receiver.
# there was a bug where we did not delete these files when
# sender/receiver mismatch on enable_download_resumption

from time import time
from common_utils import *
import os


def delete_one_dst_file():
    dst_dir = get_dest_dir()
    file_name = os.path.join(dst_dir, "file0")
    os.remove(file_name)


def rename_one_src_file():
    src_dir = get_source_dir()
    new_file_name = os.path.join(src_dir, "file55")
    src_file_name = os.path.join(src_dir, "file0")
    os.rename(src_file_name, new_file_name)


root_dir = create_test_directory("/tmp")
generate_random_files(100 * 1024 * 1024)

start_test("receiver -enable_download_resumption imposes it on sender")
# first we measure how long it takes for a full transfer
test_count = get_test_count()
start_time = time.time()
start_receiver("-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption -delete_extra_files=true")
run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
check_transfer_status()
dur1 = time.time() - start_time
# we delete one file from the dst directory and make sure
# the transfer takes a small portion of the previous cycle runtime
delete_one_dst_file()
start_time = time.time()
start_receiver("-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption -delete_extra_files=true")
run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
check_transfer_status()
dur2 = time.time() - start_time
if dur1 < 12 * dur2:
    print("Single deletion of a file < 8% wall clock of overall cycle")
    exit(1)
# next we rename one of the src file and check if
# the transfer takes less than 8% of the prev cycle
rename_one_src_file()
start_time = time.time()
start_receiver("-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption -delete_extra_files=true")
run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
check_transfer_status()
dur3 = time.time() - start_time
if dur1 < 12 * dur3:
    print("Single addition of a file < 8% wall clock of overall cycle")
    exit(1)
# the set of files should be identical
exit(verify_transfer_success())
