#! /usr/bin/env python

# In this test, a dummy python server is created which reads everything send to
# it, but does not write anything back. When a WDT sender connects to it, it
# should detect that the other side is making no progress and return with
# NO_PROGRESS status.

import socket
import threading
from common_utils import *


def start_server():
    global bad_socket
    bad_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    bad_socket.settimeout(0.5)  # timeout of 500ms
    bad_socket.bind(("", 0))
    bad_socket.listen(5)


def start_server_thread():
    print("Before accept")
    (conn, addr) = bad_socket.accept()
    print("Connected with {0}:{1}".format(addr[0], addr[1]))
    while True:
        if stop:
            break
        try:
            conn.recv(1024)
            # sleep for 1 ms. This limits the rate to 1MBytes/sec or less
            time.sleep(0.001)
        except Exception as e:
            print(e)
    print("server thread stopped")
    bad_socket.close()


create_test_directory("/tmp")
# create 5mb random files
generate_random_files(5 * 1024 * 1024)

start_server()
port = bad_socket.getsockname()[1]
print("Started fake/bad server at port {0}".format(port))

stop = False
server_thread = threading.Thread(target=start_server_thread)
server_thread.start()

read_timeout = 500
# start a wdt sender
sender_status = run_sender(
    "-read_timeout_millis={0}".format(read_timeout),
    "wdt://[::1]:{0}?num_ports=1".format(port)
)
stop = True
server_thread.join()
if sender_status != 24:
    print(
        "Sender should exit with code NO_PROGRESS(24), but it exited "
        "with {0}. Logs at {1}".format(sender_status, root_dir)
    )
    exit(1)
else:
    good_run()
