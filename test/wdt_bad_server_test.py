#! /usr/bin/env python

# In this test, a dummy python server is created which reads everything send to
# it, but does not write anything back. When a WDT sender connects to it, it
# should detect that the other side is making no progress and return with
# NO_PROGRESS status.

import socket
import threading
import time
from common_utils import *

def start_server():
    global bad_socket
    bad_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    bad_socket.settimeout(0.5)  # timeout of 500ms
    bad_socket.bind(("", 0))
    bad_socket.listen(5)

def start_server_thread():
    global bad_socket, stop
    conn, addr = bad_socket.accept()
    print("Connected with {0}:{1}".format(
        addr[0], addr[1]))
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

def main():
    global bad_socket, stop

    root_dir = create_test_directory("/tmp")
    src_dir = root_dir + "/src"
    # create 5mb random files
    generate_random_files(src_dir, 5 * 1024 * 1024)

    start_server()
    port = bad_socket.getsockname()[1]
    print("Started server at port {0}".format(port))

    stop = False
    server_thread = threading.Thread(target=start_server_thread)
    server_thread.start()

    read_timeout = 500
    # start a wdt sender
    sender_cmd = ("_bin/wdt/wdt -directory {0} -ipv6 -start_port={1} "
                  "-num_ports=1 -destination=localhost -ipv6 "
                  "-read_timeout_millis={2}").format(
                          src_dir,
                          port,
                          read_timeout)
    sender_status = run_sender(sender_cmd, root_dir, 0)
    stop = True
    server_thread.join()
    if sender_status != 24:
        print("Sender should exit with code NO_PROGRESS(24), but it exited "
              "with {0}. Logs at {1}".format(sender_status, root_dir))
        exit(1)
    else:
        print("Test passed")
        shutil.rmtree(root_dir)
        exit(0)

if __name__ == "__main__":
    main()
