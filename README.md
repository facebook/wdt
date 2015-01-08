`WDT` Warp speed Data Transfer
------------------------------

## Design philosophy

Goal:
Lowest possible total transfer time - and when not using self imposed
resource limits to be only hardware limited (disc or network bandwidth
not latency) and as efficient as possible


Zero copy stream/buffer pipeline: To maintain efficiency, the best overall
total transfer time and time to first byte we can see WDT's internal
architecture as chainable units

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

## Dependencies

gflags (google flags library) but only for the command line,  the library
doesn't depend on that

gtest (google testing) but only for tests

glog (google logging library)

Parts of facebook Folly open source library (as set in the CMakefile)
Mostly conv and threadlocal

You can build and embed wdt as a library with as little as a C++11 compiler and
glog - and you could macro way glog or replace by printing to stderr if needed

## Code layout

### Directories

* cmake/
CMake files additionally to CMakeLists.txt

* deps/
Dependencies (open source version)

* fbonly/
Stuff specific to facebook/not open source yet

### Main files

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

Class represnting transfer stats and reports

## Submitting diffs/making changes

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
* Put (releveant) log output of sender/receiver in the diff test plan or comment
* Depending on the changes
  * Perf: wdt/wdt_e2e_test.sh has a mix of ~ > 700 files, > 8 Gbytes/sec
  * do run remote network tests (wdt/wdt_remote_test.sh)
  * do run profiler and check profile results (wdt/fbonly/wdt_prof.sh)
    80k small files at > 1.6 Gbyte/sec
