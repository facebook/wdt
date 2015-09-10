#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import subprocess
from time import sleep
from time import time
from urlparse import urlparse
from urlparse import parse_qs
from threading import Thread

receiver_end_time = 0
receiver_status = 0

def wait_for_receiver_finish(receiver_process):
    global receiver_end_time
    global receiver_status
    receiver_status = receiver_process.wait()
    receiver_end_time = time()

def main():
    global receiver_end_time
    global receiver_status
    environment_variable_name = 'WDT_TEST_IPV6_CLIENT'
    if (environment_variable_name in os.environ and
            os.environ[environment_variable_name] == "0"):
        print("Test with ipv6 client is disabled in this system")
        return
    receiver_start_delay = 0.01

    receiver_cmd = "_bin/wdt/wdt -start_port 0 -skip_writes -v 1"
    print(receiver_cmd)
    receiver_start_time = time()
    receiver_process = subprocess.Popen(receiver_cmd.split(),
                                        stdout=subprocess.PIPE)

    connection_url = receiver_process.stdout.readline().strip()
    parse_result = urlparse(connection_url)
    query_parse_result = parse_qs(parse_result.query)
    port_to_block = query_parse_result['ports'][0].split(',')[0]

    # this sleep is needed to allow receiver to start listening. Otherwise,
    # nc will not be able to connect and the port will not be blocked
    sleep(receiver_start_delay)

    # start a thread to wait for receiver finish
    thread = Thread(target=wait_for_receiver_finish,
                    args=[receiver_process])
    thread.start()

    sender_cmd = ("(sleep 20 | nc -4 localhost {0}) & "
                  "(sleep 20 | nc -4 localhost {0}) & "
                  "_bin/wdt/wdt -directory wdt/ -ipv6 "
                  "-connection_url \"{1}\"").format(
                          port_to_block, connection_url)
    print(sender_cmd)
    sender_start_time = time()
    status = os.system(sender_cmd)
    sender_end_time = time()

    # wait for receiver finish
    thread.join()
    status |= receiver_status

    if status != 0:
        exit(status)

    sender_duration = sender_end_time - sender_start_time
    receiver_duration = receiver_end_time - receiver_start_time

    receiver_duration -= receiver_start_delay
    diff = abs(sender_duration - receiver_duration) * 1000
    max_allowed_diff = 200
    if diff > max_allowed_diff:
        print(("Sender and Receiver duration difference {0} is more than "
               "allowed duration {1}").format(diff, max_allowed_diff))
        exit(1)
    print(("Test passed - Sender and Receiver"
           " duration diff {0} ms").format(diff))

if __name__ == "__main__":
    main()
