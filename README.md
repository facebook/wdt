`WDT` Warp speed Data Transfer
------------------------------

## Design philosophy/Overview

Goal:
Lowest possible total transfer time -  to be only hardware limited
(disc or network bandwidth not latency) and as efficient as possible
(low CPU/memory/resources utilization)

We keep dependencies minimal in order to maximize portability
and ensure a small binary size. As a bonus, this also minimizes compile time.

We aren't using exceptions for performance reasons and because using exceptions would
make it harder to reason about the control flow of the library.
We also believe the WDT library is easier to integrate as a result.
Our philosophy is to write moderately structured and encapsulated C code
as opposed to using every feature of C++.

We try to minimize the number of system calls, which is one of the reasons
we are using blocking thread IOs. We can maximize system throughput because
at any given point some threads are reading while others are writing, and data
is buffered on both paths - keeping each subsystem busy while minimizing
kernel to userspace switches.

## Example

While WDT is primarily a library, we also have a small command line tool
which we use for tests and which is useful by itself. Here is a quick example:

```
Receiver side: (starts the server indicating destination directory)

[ldemailly@devbig074]$ wdt -directory /data/users/ldemailly/transfer1

Sender side: (discover and sends all files in a directory tree to destination)

[root@dev443]$ wdt -directory /usr/bin -destination devbig074.prn2

[=================================================] 100% 588.8 Mbytes/s
I0720 21:48:08.446014 3245296 Sender.cpp:314] Total sender time = 2.68699
seconds (0.00640992 dirTime). Transfer summary : Transfer status = OK. Number
of files transferred = 1887. Data Mbytes = 1582.08. Header kBytes = 62.083
(0.00383215% overhead). Total bytes = 1658999858. Wasted bytes due to
failure = 0 (0% overhead). Total sender throughput = 588.816 Mbytes/sec
(590.224 Mbytes/sec pure transf rate)
```

Note that in this simple example with lots of small files (/usr/bin from
a linux distribution), but not much data (~1.5Gbyte), the maximum
speed isn't as good as it would with more data (as there is still a TCP ramp
up time even though it's faster because of parallelization) like when we use
it in our production use cases.

See "wcp.sh" (which installs as "wcp") for a more detailed example.

## Performance/Results

In an internal use at Facebook to transfer RocksDB snapshot between hosts
we are able to transfer data at a throttled 600 Mbytes/sec even across
long distance, high latency links (e.g. Sweden to Oregon). That's 3x the speed
of the previous highly optimized HTTP-based solution and with less strain on the
system. When not throttling, we are able to easily saturate a 40 Gbit/s NIC and
get near theoretical link speed (above 4 Gbytes/sec).

We have so far optimized WDT for servers with fast IOs - in particular flash
card or in-memory read/writes. If you use disks throughput won't be as good,
but we do plan on optimizing for disks as well in the future.

## Dependencies

CMake for building WDT - See BUILD.txt

gflags (google flags library) but only for the command line,  the library
doesn't depend on that

gmock and gtest (google testing) but only for tests

glog (google logging library)

Parts of the Facebook Folly open source library (as set in the CMakefile)
Mostly conv, threadlocal and checksum support.

You can build and embed wdt as a library with as little as a C++11 compiler and
glog - and you could macro away glog or replace it by printing to stderr if needed.

## Code layout

### Directories

* deps/
Dependencies (open source version)

* fbonly/
Stuff specific to facebook/ (not in open source version)

* bench/
Benchmark generation tools


### Main files

* CMakeLists.txt, BUILD.txt
Build definition file - use CMake to generate a Makefile or a project file for
your favorite IDE - details in BUILD.txt

* wdtCmdline.cpp

Main program which allows to have a server or client process to exercise
the library (for end 2 end test as well as a standalone utility)

* wcp.sh

A script to use wdt like scp for single big files - pending splitting support
inside wdt proper the script does the splitting for you. install as "wcp".

* WdtOptions.{h|cpp}

To specify the behavior of wdt. If wdt is used as a library, then the
caller get the mutable object of options and set different options accordingly.
When wdt is run in a standalone mode, behavior is changed through gflags in
wdtCmdLine.cpp

* WdtBase.{h|cpp}

Common functionality and settings between Sender and Receiver

* WdtResourceController.{h|cpp}

Optional factory for Sender/Receiver with limit on number being created.

### Producing/Sending

* ByteSource.h

Interface for a data element to be sent/transferred

* FileByteSource.{h|cpp}

Implementation/concrete subclass of ByteSource for a file identified as a
relative path from a root dir. The identifier (path) sent remotely is
the relative path

* SourceQueue.h

Interface for producing next ByteSource to be sent

* DirectorySourceQueue.{h|cpp}

Concrete implementation of SourceQueue producing all the files in a given
directory, sorted by decreasing size (as they are discovered, you can start
pulling from the queue even before all the files are found, it will return
the current largest file)


* Sender.{h|cpp}

Formerly wdtlib.cpp - main code sending files


### Consuming / Receiving

* FileCreator.{h|cpp}

Creates file and directories necessary for said file (mkdir -p like)

* Receiver.{h|cpp}

Formerly wdtlib.cpp - main code receiving files


### Low level building blocks

* ServerSocket.{h|.cpp}

Encapsulate a server socket listening on a port and giving a file descriptor
to be used to communicate with the client

* ClientSocket.{h|cpp}

Client socket wrapper - connection to a server port -> fd

* Protocol.{h|cpp}

Decodes/Encodes meta information needed to interpret the data stream:
the id (file path) and size (byte length of the data)

* SocketUtils.{h|cpp}

Common socket related utilities (both client/server, sender/receiver side use)

* Throttler.{h|cpp}

Throttling code

* ErrorCodes.h

Header file for error codes

* Reporting.{h|cpp}

Class representing transfer stats and reports

## Future development/extensibility

The current implementation works well and has high efficiency.
It is also extensible by implementing different byte sources both in and
out. But inserting processing units isn't as easy.

For that we plan on restructuring the code to use a Zero copy stream/buffer
pipeline: To maintain efficiency, the best overall total transfer time and
time to first byte we can see WDT's internal architecture as chainable units

[Disk/flash/Storage IO] -> [Compression] -> [Protocol handling]
-> [Encryption] -> [Network IO]

And the reverse chain on the receiving/writing end
The trick is the data is variable length input and some units can change length
and we need to process things by blocks
Constraints/Design:
- No locking / contention when possible
- (Hard) Limits on memory used
- Minimal number of copies/moving memory around
- Still works the same for simple
   read file fd -> control -> write socked fd current basic implementation

Possible Solution(?) API:
- Double linked list of Units
- read/pull from left (pull() ?)
- push to the right (push() ?)
- end of stream from left
- propagate last bytes to right

Can still be fully synchronous / blocking, works thanks to eof handling
(synchronous gives us lock free/single thread - internally a unit is
free to use parallelization like the compression stage is likely to want/need)

Another thing we touched on is processing chunks out of order - by changing
header to be ( fileid, offset, size ) instead of ( filename, size )
and assuming everything is following in 1 continuous block (will also help
the use case of small number of large files/chunks) : mmap'in
the target/destination file
The issue then is who creates it in what order - similar to the directory
creation problem - we could use a meta info channel to avoid locking/contention
but that requires synchronization

We want things to work with even up to 1 second latency without incurring
a 1 second delay before we send the first payload byte


## Submitting diffs/making changes

See CONTRIBUTING.md

Please run the tests (make test) and the manual tests (integration upcoming)
wdt_e2e_test.sh
wdt_download_resumption_test.sh
wdt_network_test.sh
wdt_max_send_test.sh

(facebook only:)
Make sure to do the following, before "arc diff":
```
 (cd wdt ; ./clangformat.sh )

 fbconfig  --clang --with-project-version clang:dev -r  wdt

 fbmake runtests
 fbmake runtests_opt
 fbmake opt

 wdt/wdt_max_send_test.sh
```

and check the output of the last step to make sure one of the 3 runs is
still above 20,000 Mbytes/sec (you may need to make sure you
/dev/shm is mostly empty to get the best memory throughput, as well
as not having a ton of random processes running during the test)

Also :

* Update this file
* Make sure your diff has a task
* Put (relevant) log output of sender/receiver in the diff test plan or comment
* Depending on the changes
  * Perf: wdt/wdt_e2e_test.sh has a mix of ~ > 700 files, > 8 Gbytes/sec
  * do run remote network tests (wdt/wdt_remote_test.sh)
  * do run profiler and check profile results (wdt/fbonly/wdt_prof.sh)
    80k small files at > 1.6 Gbyte/sec
