#!/usr/bin/env python3

from common_utils import *
import re
import subprocess

# TODO: refactor more using common_utils


def testNegotiation(higher):
    receiver_cmd = get_receiver_binary() + " -skip_writes -max_accept_retries 10"
    print(receiver_cmd)
    receiver_process = subprocess.Popen(
        receiver_cmd.split(), stdout=subprocess.PIPE, universal_newlines=True
    )

    connection_url = receiver_process.stdout.readline().strip()

    protocol_key = "recpv"
    url_match = re.search(f"[?&]{protocol_key}=([0-9]+)", connection_url)
    protocol = url_match.group(1)
    if higher:
        new_protocol = int(protocol) + 1
    else:
        new_protocol = int(protocol) - 1

    prev_str = f"{protocol_key}={protocol}"
    new_str = f"{protocol_key}={new_protocol}"
    new_connection_url = connection_url.replace(prev_str, new_str)

    sender_cmd = "{1} -directory wdt/ -connection_url '{0}'".format(
        new_connection_url, get_sender_binary()
    )
    print(sender_cmd)
    status = os.system(sender_cmd)
    receiver_status = receiver_process.wait()
    # wait for receiver finish
    status |= receiver_status

    if status != 0:
        print(
            (
                "Protocol negotiation test failed, sender protocol {}, "
                "receiver protocol {}"
            ).format(new_protocol, protocol)
        )
        exit(status)


testNegotiation(False)
testNegotiation(True)
print("Protocol negotiation test passed")
