#!/usr/bin/env python3

from common_utils import *

# 1 time setup
create_test_directory("/dev/shm")
src_dir = get_source_dir()
create_directory(src_dir)
gen_files = get_gen_files()
cmd = "{} -directory={} -filename=testLarge1 -gen_size_mb={}".format(
    gen_files, src_dir, 7 * 1024
)
status = os.system(cmd)
if status:
    error(f"Failure generating data running {cmd}:{status}")

# test(s)
start_test("large 7Gb file and disk mode")
url = start_receiver("-option_type=disk")  # will get escaped
run_sender("-option_type=disk")
check_transfer_status()
exit(verify_transfer_success())
