#! /usr/bin/env python

import re
from threading import Thread
from common_utils import *

# Todo: refactor using more of common_utils

receiver_end_time = 0
receiver_status = 0


def wait_for_receiver_finish(receiver_process):
    global receiver_end_time
    global receiver_status
    receiver_status = receiver_process.wait()
    receiver_end_time = time.clock()


def test(resumption):
    global receiver_end_time
    global receiver_status
    environment_variable_name = 'WDT_TEST_IPV6_CLIENT'
    if (
        environment_variable_name in os.environ and
        os.environ[environment_variable_name] == "0"
    ):
        print("Test with ipv6 client is disabled in this system")
        return

    receiver_cmd = get_receiver_binary() + " -skip_writes -num_ports=1 -v 1"
    print(receiver_cmd)
    receiver_process = subprocess.Popen(
        receiver_cmd.split(),
        stdout=subprocess.PIPE
    )

    connection_url = receiver_process.stdout.readline().strip()
    print(connection_url)
    # wdt url can be of two kinds :
    # 1. wdt://localhost?ports=1,2,3,4
    # 2. wdt://localhost:1?num_ports=4
    # the second kind of url is another way of expressing the first one
    url_match = re.search('\?(.*&)?ports=([0-9]+).*', connection_url)
    if not url_match:
        url_match = re.search(':([0-9]+)(\?.*)', connection_url)
        rest_of_url = url_match.group(2)
        port_to_block = url_match.group(1)
        start_port = ":" + port_to_block
    else:
        rest_of_url = url_match.group(0)
        start_port = ""
        port_to_block = url_match.group(2)
    print(rest_of_url + " " + port_to_block)

    # start a thread to wait for receiver finish
    thread = Thread(target=wait_for_receiver_finish, args=[receiver_process])
    thread.start()

    # we hack the url to be ::1 instead of hostname to increase chances
    # it works on machines which do have ipv6 but no dns entry
    sender_cmd = (
        "(sleep 20 | nc -4 localhost {0}) &> /dev/null & "
        "(sleep 20 | nc -4 localhost {0}) &> /dev/null & "
        "sleep 1; {3} -directory wdt/ -ipv6 "
        "-num_ports=1 "
        "-connection_url \"wdt://[::1]{1}{2}\""
    ).format(
        port_to_block, start_port, rest_of_url, get_sender_binary()
    )
    if resumption:
        sender_cmd = sender_cmd + " -enable_download_resumption"
    print(sender_cmd)
    status = os.system(sender_cmd)
    status >>= 8
    sender_end_time = time.clock()

    # wait for receiver finish
    thread.join()
    status |= receiver_status

    if status != 0:
        print("Test failed, exiting with {0}".format(status))
        exit(status)

    diff = abs(sender_end_time - receiver_end_time) * 1000
    max_allowed_diff = 200
    if diff > max_allowed_diff:
        print(
            (
                "Sender and Receiver end time difference {0} is more than "
                "allowed diff {1}"
            ).format(diff, max_allowed_diff)
        )
        exit(1)
    print(
        (
            "Test passed - Sender and Receiver"
            " end time diff {0} ms"
        ).format(diff)
    )


print("Testing without download resumption")
test(False)
print("Testing with download resumption")
test(True)
