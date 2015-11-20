#!/usr/bin/env python
import shutil
from common_utils import *

def run_test(wdtbin, test_name, test_count, root_dir, connection_url):
    test_description = ("Test #{0}, [{1}]").format(test_count, test_name)
    print(test_description)
    sender_cmd = (wdtbin + " -directory {0}/src "
            "-connection_url \'{1}\'").format(root_dir, connection_url)
    transfer_status = run_sender(sender_cmd, root_dir, test_count)
    if transfer_status:
        print("Failed " + test_description + ". Check logs"
            + " in " + root_dir)
        exit(transfer_status)

def main():
    wdt_version = get_wdt_version()
    print("wdt protocol version " + wdt_version)
    root_dir = create_test_directory("/tmp")
    src_dir = root_dir + "/src"
    generate_random_files(src_dir, 256 * 1024)

    wdtbin_opts = "-full_reporting -num_ports 4"
    wdtbin = "_bin/wdt/wdt " + wdtbin_opts
    #receiver version should be one behind
    receiver_version = (int(wdt_version) - 1)
    receiver_cmd = (wdtbin + " -start_port 0 -run_as_daemon "
        "-skip_writes -protocol_version {0}").format(receiver_version)
    #start the receiver in long running mode
    print(receiver_cmd)
    (receiver_process, connection_url) = start_receiver(receiver_cmd,
            root_dir, 0)

    run_test(wdtbin, "sender 1 same version", 1, root_dir, connection_url)
    run_test(wdtbin, "sender 2 same version", 2, root_dir, connection_url)

    protocol_key = "recpv"
    prev_str = "{0}={1}".format(protocol_key, receiver_version)
    new_str = "{0}={1}".format(protocol_key, wdt_version)
    connection_url_new_version = connection_url.replace(prev_str, new_str)

    run_test(wdtbin, "sender 1 newer version", 3, root_dir,
            connection_url_new_version)
    run_test(wdtbin, "sender 2 newer version", 4, root_dir,
            connection_url_new_version)
    #since receiver is in long running mode, kill it
    receiver_process.kill()
    print("Tests successful! Removing logs and data from " + root_dir)
    shutil.rmtree(root_dir)

if __name__ == "__main__":
    main()
