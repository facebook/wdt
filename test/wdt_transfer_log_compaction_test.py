#! /usr/bin/env python

from common_utils import *
import os


def verify_log_parse_ok(log_dir):
    bin = get_wdt_binary()
    parse_cmd = "%s -parse_transfer_log -directory %s" % (bin, log_dir)
    stdout, stderr = run_command(parse_cmd)
    ok_str = "Transfer log parsing finished OK"

    if ok_str not in stdout and ok_str not in stderr:
        error("Compacted transfer log could not be parsed")


def test_enable_transfer_log_compaction():
    '''
    Test transfer log compaction works. We first transfer files without
     -enable_transfer_log_compaction and get the log size. Then we
     transfer the same set of files with -enable_transfer_log_compaction,
    the transfer log size should be smaller and the log should be parsed OK
    with -parse_transfer_log.
    '''
    create_test_directory("/tmp")
    generate_random_files(100 * 1024 * 1024)
    transfer_log_name = ".wdt.log"

    start_test("receiver -enable_transfer_log_compaction")
    transfer_log_path = os.path.join(get_dest_dir(), transfer_log_name)

    # -enable_download_resumption will make sure transfer log will be generated
    start_receiver("-num_ports=1 -avg_mbytes_per_sec=10 "
                   "-enable_download_resumption -delete_extra_files=true")
    run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
    check_transfer_status()

    # get transfer log size
    uncompacted_log_size = os.path.getsize(transfer_log_path)
    print("Uncompacted log (%s) size is %s" % (transfer_log_path, uncompacted_log_size))

    # enable transfer log compaction
    start_receiver("-num_ports=1 -avg_mbytes_per_sec=10 -enable_download_resumption"
                   " -enable_transfer_log_compaction -delete_extra_files=true")
    run_sender("-avg_mbytes_per_sec=10 -block_size_mbytes=1")
    check_transfer_status()
    # get transfer log size
    compacted_log_size = os.path.getsize(transfer_log_path)

    if compacted_log_size >= uncompacted_log_size:
        error(
            "Transfer log is not compacted. Uncompacted size: %s."
            " Compacted size: %s" % (uncompacted_log_size, compacted_log_size))

    verify_log_parse_ok(get_dest_dir())

    exit(verify_transfer_success())


test_enable_transfer_log_compaction()
