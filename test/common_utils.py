import os
import hashlib
import difflib
import subprocess
import shutil
import tempfile
import errno
import string
import random
import time


def get_env(name):
    if name in os.environ:
        return os.environ[name]

# Appease the linter gods:
test_count = root_dir = wdt_binary = receiver_binary = sender_binary = None
receiver_process = connection_url = test_name = server_log = None
sender_status = skip_tests = test_ids = None


def set_binaries():
    global wdt_binary, receiver_binary, sender_binary, \
           gen_files_binary, gen_files_bigrams
    binary = get_env('WDT_BINARY')
    if binary:
        wdt_binary = binary
        print("Set default binary from WDT_BINARY env var: " + wdt_binary)
    else:
        wdt_binary = "./buck-out/gen/wdt/wdt"
        if not os.path.exists(wdt_binary):
            wdt_binary = "./_bin/wdt/wdt"
    sender = get_env('WDT_SENDER')
    if sender:
        sender_binary = sender
        print("Set sender from WDT_SENDER env var: " + sender_binary)
    else:
        sender_binary = wdt_binary
    receiver = get_env('WDT_RECEIVER')
    if receiver:
        receiver_binary = receiver
        print("Set receiver from WDT_RECEIVER env var: " + receiver_binary)
    else:
        receiver_binary = wdt_binary
    gen_files = get_env('WDT_GEN_FILES')
    if gen_files:
        gen_files_binary = gen_files
        print("Set gen_files_binary from WDT_GEN_FILES env var: " + gen_files)
    else:
        gen_files_binary = "./buck-out/gen/wdt/bench/wdt_gen_files"
        if not os.path.exists(gen_files_binary):
            gen_files_binary = "./_bin/wdt/bench/wdt_gen_files"
    bigrams = get_env('WDT_GEN_BIGRAMS')
    if bigrams:
        gen_files_bigrams = bigrams
        print("Set gen_files_bigrams from WDT_GEN_BIGRAMS env var: " + bigrams)
    else:
        gen_files_bigrams = os.path.join(
            os.path.dirname(__file__), "../bench/book1.bigrams"
        )
    print(
        "Sender: " + sender_binary + " Receiver: " + receiver_binary +
        " gen_files " + gen_files_binary + " bigrams " + gen_files_bigrams
    )

# sets the file global wdt/sender/receiver binary paths from env or _bin default
set_binaries()


def run_command(cmd):
    print("Running %s" % cmd)
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    return p.communicate()

# note globals in python can be read without being declared global (!!)
def get_wdt_binary():
    return wdt_binary


def get_sender_binary():
    return sender_binary


def get_receiver_binary():
    return receiver_binary


def get_gen_files():
    return "{0} -stats_source={1} -seed_with_time".format(
        gen_files_binary, gen_files_bigrams
    )


def get_wdt_version():
    bin = get_wdt_binary()
    dummy_cmd = bin + " --version"
    dummy_process = subprocess.Popen(dummy_cmd.split(), stdout=subprocess.PIPE)
    protocol_string = dummy_process.stdout.readline().strip()
    print("Wdt " + bin + " version is " + protocol_string)
    return protocol_string.split()[4]


def extend_wdt_options(cmd):
    extra_options = get_env('EXTRA_WDT_OPTIONS')
    if extra_options:
        print("extra options " + extra_options)
        cmd = cmd + " " + extra_options
    encryption_type = get_env('ENCRYPTION_TYPE')
    if encryption_type:
        print("encryption_type " + encryption_type)
        cmd = cmd + " -encryption_type=" + encryption_type
    enable_checksum = get_env('ENABLE_CHECKSUM')
    if enable_checksum:
        print("enable_checksum " + enable_checksum)
        cmd = cmd + " -enable_checksum=" + enable_checksum
    return cmd


def start_receiver(extra_args):
    global connection_url, receiver_process, server_log
    receiver_cmd = extend_wdt_options(receiver_binary)
    receiver_cmd = "{0} -directory {1}/dst{2} {3}".format(
        receiver_cmd, root_dir, test_count, extra_args
    )
    print("Receiver: " + receiver_cmd)
    server_log = "{0}/server{1}.log".format(root_dir, test_count)
    receiver_process = subprocess.Popen(
        receiver_cmd.split(),
        stdout=subprocess.PIPE,
        stderr=open(server_log, 'w')
    )
    connection_url = receiver_process.stdout.readline().strip()
    if not connection_url:
        error("Unable to get the connection url from receiver!")
    return connection_url


def get_receiver_process():
    return receiver_process


def run_sender(extra_args, url=""):
    global sender_status
    sender_status = -1
    if not url:
        url = connection_url
    sender_cmd = extend_wdt_options(sender_binary)
    sender_cmd = "{0} -directory {1}/src -connection_url '{2}' {3}".format(
        sender_cmd, root_dir, url, extra_args
    )
    # TODO: fix this to not use tee, this is python...
    sender_cmd = "bash -c \"set -o pipefail; " + sender_cmd \
                + " 2>&1 | tee {0}/client{1}.log\"".format(root_dir, test_count)
    print("Sender: " + sender_cmd)
    # On unix return code of system is shifted by 8 bytes but lower bits are
    # set on signal too and sometimes it's not flipped so let's or it all
    sender_status = os.system(sender_cmd)
    sender_status = (sender_status >> 8) | (sender_status & 0xff)
    print("status for sender {0}".format(hex(sender_status)))
    return sender_status


def error(msg):
    print_server_log()
    print("FAILING Test #{0} ({1}) {2}".format(test_count, test_name, msg))
    exit(1)


def print_server_log():
    if server_log:
        with open(server_log, 'r') as fin:
            print(fin.read())

def check_transfer_status(expect_failed=False, check_receiver=True):
    global receiver_status
    if check_receiver:
        receiver_status = receiver_process.wait()
        print("status for receiver {0}".format(receiver_status))
    else:
        # hacky way to not change code below for rare case we don't care about
        # receiver
        receiver_status = sender_status
    if expect_failed is True:
        if sender_status == 0:
            error("was expected to fail but sender didn't")
        if receiver_status == 0:
            error("was expected to fail but receiver didn't")
        skip_tests.add(test_count)
    else:
        if sender_status != 0:
            error(
                "was expected to succeed but sender err {0}"
                .format(sender_status)
            )
        if receiver_status != 0:
            error(
                "was expected to succeed but sender err {0}"
                .format(sender_status)
            )


def check_logs_for_errors(fail_errors):
    log_file = "%s/server%s.log" % (root_dir, test_count)
    server_log_contents = open(log_file).read()
    log_file = "%s/client%s.log" % (root_dir, test_count)
    client_log_contents = open(log_file).read()

    for fail_error in fail_errors:
        if fail_error in server_log_contents:
            error("%s found in logs %s" % (fail_error, log_file))
        if fail_error in client_log_contents:
            error("%s found in logs %s" % (fail_error, log_file))


def create_directory(root_dir):
    # race condition during stress test can happen even if we check first
    try:
        os.mkdir(root_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
        pass


def next_test():
    global test_count
    test_count = test_count + 1


def create_test_directory(prefix):
    global root_dir, test_count, skip_tests, test_ids
    user = os.environ['USER']
    base_dir = prefix + "/wdtTest_" + user
    create_directory(base_dir)
    root_dir = tempfile.mkdtemp(dir=base_dir)
    print("Testing in {0}".format(root_dir))
    test_count = 0
    skip_tests = set()
    test_ids = set()
    return root_dir


def start_test(name):
    global test_count, test_ids, test_name
    test_count = test_count + 1
    test_ids.add(test_count)
    test_name = name
    print("Test #{0}: {1}".format(test_count, name))


def get_source_dir():
    return root_dir + "/src/"


def get_dest_dir():
    return os.path.join(root_dir, "dst{0}".format(test_count))


def get_test_count():
    return test_count


def generate_random_files(total_size):
    src_dir = get_source_dir()
    print(
        "Creating random files, size {0}, into {1}".format(total_size, src_dir)
    )
    create_directory(src_dir)
    seed_size = int(total_size / 70)
    gen_files = get_gen_files()
    for i in range(0, 4):
        file_name = "sample{0}".format(i)
        cmd = "{0} -directory={1} -filename={2} -gen_size_mb={3}".format(
            gen_files, src_dir, file_name, seed_size / 1024. / 1024.
        )
        status = os.system(cmd)
        if status:
            error("Failure generating data running {0}:{1}".format(cmd, status))
    for i in range(0, 16):
        file_name = "file{0}".format(i)
        status = os.system(
            "{0} -directory={1} -filename={2} -gen_size_mb={3}".format(
                gen_files, src_dir, file_name, 4 * seed_size / 1024. / 1024.
            )
        )
        if status:
            error("Failure generating data")
    return src_dir


def get_md5_for_file(file_path):
    return hashlib.md5(open(file_path, 'rb').read()).hexdigest()


def create_md5_for_directory(src_dir, md5_file_name):
    lines = []
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if file == ".wdt.log":
                continue
            full_path = os.path.join(root, file)
            md5 = get_md5_for_file(full_path)
            lines.append("{0} {1}".format(md5, file))
    lines.sort()
    md5_in = open(md5_file_name, 'wb')
    for line in lines:
        md5_in.write(line + "\n")


def verify_transfer_success():
    src_md5_path = os.path.join(root_dir, "src.md5")
    create_md5_for_directory(os.path.join(root_dir, "src"), src_md5_path)
    status = 0
    for i in test_ids:
        if i in skip_tests:
            print("Skipping verification of test %s" % (i))
            continue
        print("Verifying correctness for test {0}".format(i))
        print("Should be no diff")
        dst_dir = os.path.join(root_dir, "dst{0}".format(i))
        dst_md5_path = os.path.join(root_dir, "dst{0}.md5".format(i))
        create_md5_for_directory(dst_dir, dst_md5_path)
        diff = difflib.unified_diff(
            open(src_md5_path).readlines(), open(dst_md5_path).readlines()
        )
        delta = ''.join(diff)
        if not delta:
            print("Found no diff for test {0}".format(i))
            if search_in_logs(i, "PROTOCOL_ERROR"):
                status = 1
        else:
            print(delta)
            with open("{0}/server{1}.log".format(root_dir, i), 'r') as fin:
                print(fin.read())
            status = 1
    if status == 0:
        good_run()
    else:
        print("Bad run - keeping full logs and partial transfer in " + root_dir)
    return status


def good_run():
    print("Good run, deleting logs in " + root_dir)
    shutil.rmtree(root_dir)
    return 0


def search_in_logs(i, str):
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


def generate_encryption_key():
    return ''.join(random.choice(string.lowercase) for i in range(16))
