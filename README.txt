
= Code layout =

* wdtCmdline.cpp

Main program which allows to have a server or client process to exercise
the library (for end 2 end test as well as a standalone utility)


=== Producing/Sending ===

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



=== Consuming / Receiving ===

* FileCreator.{h|cpp}

Creates file and directories necessary for said file (mkdir -p like)

* Receiver.{h|cpp}


=== Low level building blocks ===

* ServerSocket.{h|.cpp}

Encapsulate a server socket listening on a port and giving a file descriptor
to be used to communicate with the client

* ClientSocket.{h|cpp}

Client socket wrapper - connection to a server port -> fd

* Protocol.{h|cpp}

Decodes/Encodes an id (file path) and size (byte length of the data)

* SocketUtils.{h|cpp}

Common socket related utilities (both client/server, sender/receiver side use)


=== Old ===

* wdtlib.{h|cpp}

Initial all in one hackathon version being refactored/cleaned up into above
components instead
