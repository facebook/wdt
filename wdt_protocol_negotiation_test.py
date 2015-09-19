#! /usr/bin/env python

import os
import re
import subprocess

def testNegotiation(higher):
    receiver_cmd = ("_bin/wdt/wdt -start_port 0 -skip_writes "
    "-max_accept_retries 10")
    print(receiver_cmd)
    receiver_process = subprocess.Popen(receiver_cmd.split(),
                                        stdout=subprocess.PIPE)

    connection_url = receiver_process.stdout.readline().strip()

    protocol_key = "recpv"
    url_match = re.search("[?&]{0}=([0-9]+)".format(protocol_key),
                          connection_url)
    protocol = url_match.group(1)
    if higher:
        new_protocol = int(protocol) + 1
    else:
        new_protocol = int(protocol) - 1

    prev_str = "{0}={1}".format(protocol_key, protocol)
    new_str = "{0}={1}".format(protocol_key, new_protocol)
    new_connection_url = connection_url.replace(prev_str, new_str)

    sender_cmd = ("_bin/wdt/wdt -directory wdt/ -connection_url "
                  "\'{0}\'").format(new_connection_url)
    print(sender_cmd)
    status = os.system(sender_cmd)
    receiver_status = receiver_process.wait()
    # wait for receiver finish
    status |= receiver_status

    if status != 0:
        print(("Protocol negotiation test failed, sender protocol {0}, "
              "receiver protocol {1}").format(new_protocol, protocol))
        exit(status)


def main():
    testNegotiation(False)
    testNegotiation(True)
    print("Protocol negotiation test passed")

if __name__ == "__main__":
    main()
