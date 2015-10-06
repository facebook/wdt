#! /usr/bin/env python

import socket
from time import time
from common_utils import *

def start_server():
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(5)
    return s

def main():
    s = start_server()
    port = s.getsockname()[1]
    print("Started server at port {0}".format(port))

    read_timeout = 500
    abort_check_interval = 100
    retries = 3
    # start a wdt sender
    sender_cmd = ("_bin/wdt/wdt -directory wdt/ -ipv6 -start_port={0} "
                  "-num_ports=1 -destination=localhost "
                  "-max_transfer_retries={1} -read_timeout_millis={2} "
                  "-abort_after_seconds=5 "
                  "-abort_check_interval_millis={3}").format(
                          port,
                          retries,
                          read_timeout,
                          abort_check_interval)
    print(sender_cmd)
    start_time = time()
    os.system(sender_cmd)
    end_time = time()
    s.close()
    duration_millis = (end_time - start_time) * 1000
    # have to count first read_receiver_cmd
    num_retries = retries + 1
    expected_duration = (read_timeout + abort_check_interval) * num_retries
    # adding extra 500 millis delay because delay introduced during testing
    expected_duration += 500
    print("Transfer duration {0} millis, expected duration {1} millis").format(
            duration_millis,
            expected_duration)
    if duration_millis > expected_duration:
        exit(1)
    else:
        exit(0)

if __name__ == "__main__":
    main()
