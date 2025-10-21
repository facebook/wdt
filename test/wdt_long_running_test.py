#!/usr/bin/env python3

from common_utils import (
    check_transfer_status,
    create_test_directory,
    error,
    generate_random_files,
    get_receiver_process,
    get_wdt_version,
    good_run,
    run_sender,
    start_receiver,
    start_test,
)


def run_test(test_name, connection_url):
    start_test(test_name)
    run_sender("", connection_url)
    check_transfer_status(
        False,
        False,  # not expected to fail  # don't check/wait on receiver
    )


wdt_version = get_wdt_version()
print("wdt protocol version " + wdt_version)

create_test_directory("/tmp")

generate_random_files(256 * 1024)

wdtbin_opts = "-full_reporting -num_ports 4"

# receiver version should be one behind
receiver_version = int(wdt_version) - 1
receiver_args = (
    " -start_port 0 -run_as_daemon -skip_writes"
    + f" -protocol_version {receiver_version}"
)
# start the receiver in long running mode
start_test("receiver start")
connection_url = start_receiver(receiver_args)

run_test("sender 1 same version", connection_url)
run_test("sender 2 same version", connection_url)

protocol_key = "recpv"
prev_str = f"{protocol_key}={receiver_version}"
new_str = f"{protocol_key}={wdt_version}"

connection_url_new_version = connection_url.replace(prev_str, new_str)

if connection_url_new_version == connection_url:
    error("url not changing... test bug...")

run_test("sender 1 newer version", connection_url_new_version)
run_test("sender 2 newer version", connection_url_new_version)

# if we get this far the above tests passed

# since receiver is in long running mode, kill it
get_receiver_process().kill()
# cleanup
exit(good_run())
