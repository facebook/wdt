from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import os
import hashlib
import difflib
import subprocess
from random import randint
import shutil
import tempfile

def start_receiver(receiver_cmd, root_dir, test_count):
    print("Receiver: " + receiver_cmd)
    server_log = "{0}/server{1}.log".format(root_dir, test_count)
    receiver_process = subprocess.Popen(receiver_cmd.split(),
                                        stdout=subprocess.PIPE,
                                        stderr=open(server_log, 'w'))
    connection_url = receiver_process.stdout.readline().strip()
    if not connection_url:
        print("ERR: Unable to get the connection url from receiver!")
    return (receiver_process, connection_url)

def run_sender(sender_cmd, root_dir, test_count):
    # TODO: fix this to not use tee, this is python...
    sender_cmd = "sh -c \"set -o pipefail; " + sender_cmd \
                + " 2>&1 | tee {0}/client{1}.log\"".format(root_dir, test_count)
    print("Sender: " + sender_cmd)
    return os.system(sender_cmd)

def check_transfer_status(status, root_dir, test_count):
    if status:
        with open("{0}/server{1}.log".format(root_dir, test_count), 'r') as fin:
            print(fin.read())
        print("Transfer failed {0}".format(status))
        exit(status)

def create_directory(root_dir):
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

def create_test_directory(prefix):
    user = os.environ['USER']
    base_dir = prefix + "/wdtTest_" + user
    create_directory(base_dir)
    root_dir = tempfile.mkdtemp(dir=base_dir)
    print("Testing in {0}".format(root_dir))
    return root_dir

def generate_random_files(root_dir, seed_size):
    create_directory(root_dir)
    cur_dir = os.getcwd()
    os.chdir(root_dir)
    for i in range(0, 4):
        file_name = "sample{0}".format(i)
        with open(file_name, 'wb') as fout:
            fout.write(os.urandom(seed_size))
    for i in range(0, 16):
        file_name = "file{0}".format(i)
        with open(file_name, 'wb') as fout:
            for j in range(0, 4):
                sample = randint(0, 3)
                sin = open("sample{0}".format(sample), 'rb')
                fout.write(sin.read())
    os.chdir(cur_dir)

def get_md5_for_file(file_path):
    return hashlib.md5(open(file_path, 'rb').read()).hexdigest()

def create_md5_for_directory(src_dir, md5_file_name):
    lines = []
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            full_path = os.path.join(root, file)
            md5 = get_md5_for_file(full_path)
            lines.append("{0} {1}".format(md5, file))
    lines.sort()
    md5_in = open(md5_file_name, 'wb')
    for line in lines:
        md5_in.write(line + "\n")

def verify_transfer_success(root_dir, test_ids):
    src_md5_path = os.path.join(root_dir, "src.md5")
    create_md5_for_directory(os.path.join(root_dir, "src"), src_md5_path)
    status = 0
    for i in test_ids:
        print("Verifying correctness for test {0}".format(i))
        print("Should be no diff")
        dst_dir = os.path.join(root_dir, "dst{0}".format(i))
        dst_md5_path = os.path.join(root_dir, "dst{0}.md5".format(i))
        create_md5_for_directory(dst_dir, dst_md5_path)
        diff = difflib.unified_diff(open(src_md5_path).readlines(),
                open(dst_md5_path).readlines())
        delta = ''.join(diff)
        if not delta:
            print("Found no diff for test {0}".format(i))
            if search_in_logs(root_dir, i, "PROTOCOL_ERROR"):
                status = 1
        else:
            print(delta)
            with open("{0}/server{1}.log".format(
                    root_dir, i), 'r') as fin:
                print(fin.read())
            status = 1
    if status == 0:
        print("Good run, deleting logs in " + root_dir)
        shutil.rmtree(root_dir)
    else:
        print("Bad run - keeping full logs and partial transfer in " + root_dir)
    return status

def search_in_logs(root_dir, i, str):
    found = False
    client_log = "{0}/client{1}.log".format(root_dir, i)
    server_log = "{0}/server{1}.log".format(root_dir, i)
    if str in open(client_log).read():
        print("Found {0} in {1}".format(str, client_log))
        found = True
    if str in open(server_log).read():
        print("Found {0} in {1}".format(str, server_log))
        found = True
    return found
