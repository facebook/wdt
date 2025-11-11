WDT opensource build uses CMake 3.2.x or later

This should build in a variety of environments (if it doesn't please open
an issue or even better, contribute a patch)

See also [README.md](../README.md#dependencies) Dependencies section
(Inside facebook see https://our.intern.facebook.com/intern/wiki/WDT/OpenSource)

Checkout the .travis.yml and travis_linux.sh and travis_osx.sh
for build bootstrap without root/sudo requirement

We don't have yet a native Windows port (please contribute!) but it does
build and runs with Cygwin64 using the Linux instructions (and static linking)

# Notes:
 On Ubuntu 14.04 - to get g++ 4.9
 ```
 sudo add-apt-repository ppa:ubuntu-toolchain-r/test
 sudo apt-get upgrade
 ```
If using a vmware image/starting fresh
```
 sudo vmware-config-tools.pl  # to setup shared folders
```

openssl should already be there but may need update to 1.0.x

# Build Instructions
__Install Cmake 3.2 or greater.__
*See directions below for Mac
(Cmake 3.3/head on a Mac is recommended for Xcode support)*
```
wget http://www.cmake.org/files/v3.2/cmake-3.2.3.tar.gz
tar xvfz cmake-3.2.3.tar.gz
cd cmake-3.2.3
./bootstrap --prefix=/usr --parallel=16 && make -j && sudo make install
```
__Get folly source tree__
```
git clone https://github.com/facebook/folly.git
```
__Install glog-dev (includes gflags, libunwind), boost system, double conversion
if you can find a binary distrubution for your variant of linux:__
*libjemalloc-dev dependency is optional*

```
sudo apt-get install libgoogle-glog-dev libboost-system-dev \
libdouble-conversion-dev libjemalloc-dev
```

__Otherwise, Build double-conversion, gflags and glog from source__

*It's important to build and configure gflags correctly for glog to pick it up
and avoid linking errors later or getting a wdt without flags working*
```
git clone https://github.com/schuhschuh/gflags.git
mkdir gflags/build
cd gflags/build
cmake -DGFLAGS_NAMESPACE=google -DBUILD_SHARED_LIBS=on ..
make -j && sudo make install
```

```
git clone https://github.com/google/glog.git
cd glog
./configure # add --with-gflags=whereyouinstalledgflags
# to avoid ERROR: unknown command line flag 'minloglevel' later
make -j && sudo make install
```

*If double-conversion isn't available via apt-get:*
```
git clone https://github.com/floitsch/double-conversion.git
cd double-conversion; cmake . -DBUILD_SHARED_LIBS=on
make -j && sudo make install
```


__Build wdt from source__
```
cmake pathtowdtsrcdir -DBUILD_TESTING=on  # skip -D... if you don't want tests
make -j
CTEST_OUTPUT_ON_FAILURE=1 make test
sudo make install
```
# Install on Mac OS X

Open the terminal application and type "git" or "make" to be prompted to install the developper command line tools.


## Using the contbuild script locally

This is quite heavy handed (manual/brew steps are better) but should work because travis works

```sh
git clone https://github.com/facebook/wdt.git
cd wdt
source build/travis_osx.sh
mkdir ../build && cd ../build
cmake ../wdt -DBUILD_TESTING=1
make -j
CTEST_OUTPUT_ON_FAILURE=1 make test
# optionally
make install
```

## Or manual/step by step using brew

__Install homebrew (http://brew.sh/)__


__Install Cmake__

```
brew install cmake
```

__Install glog and gflags and boost__
```sh
brew install glog gflags boost
```
__Install Double conversion__
```
brew install double-conversion
```

__libcrypto from openssl-1.0.x__

```
brew install openssl
# note OPENSSL is /usr/local/opt/openssl for  /usr/local/Cellar/openssl/1.0.2k
```


__Build wdt from source__

Get folly source for use during build
```
git clone https://github.com/facebook/folly.git
```
Fetch wdt source
```
git clone https://github.com/facebook/wdt.git
```

```
mkdir wdt-mac
cd wdt-mac
```
Using Xcode:
```
cmake ../wdt -G Xcode -DBUILD_TESTING=on -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl
```

Using Unix makefiles:
```
cmake ../wdt -G "Unix Makefiles" -DBUILD_TESTING=on -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl
make -j
CTEST_OUTPUT_ON_FAILURE=1 make test
sudo make install
```

Using Eclipse CDT:

*Follow instruction to import the project like on
http://www.cmake.org/Wiki/Eclipse_CDT4_Generator*
```
cmake ../wdt -G "Eclipse CDT4 - Unix Makefiles" -DBUILD_TESTING=on
```


# Troubleshooting

## MAC OSX Build Problems -- Catalina

## TLDR

1) Turn testing off in the cmake call
2) You need to manually change a version number in 2 wdt repo files from a fresh pull of a repo
3) You need to use system installed folly.  brew install it, and pass the flag to cmake informing it to skip the local build.
4) cmake ... make ... make install ... succcess!

* Following the manual instructions, not travis. I had the command line tools installed, but no Xcode Dev App. I followed the `cmake ../wdt -G "Unix Makefiles" -DBUILD_TESTING=on -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl` path. Cmake ran fine, but make crashed in several ways I had to find workarounds for.

### Tests Causing Errors

<pre>
make -j 8
...
ld: library not found for -lgtest
clang: error: linker command failed with exit code 1 (use -v to see invocation)
make[2]: *** [CMakeFiles/wdtbenchtestslib.dir/build.make:100: libwdtbenchtestslib.dylib] Error 1
make[1]: *** [CMakeFiles/Makefile2:466: CMakeFiles/wdtbenchtestslib.dir/all] Error 2
make[1]: *** Waiting for unfinished jobs....
[ 18%] Linking CXX executable _bin/wdt/wdt_gen_stats
[ 18%] Built target wdt_gen_stats
[ 19%] Linking CXX executable _bin/wdt/bench/wdt_gen_files
[ 19%] Built target wdt_gen_files
[ 20%] Linking CXX shared library libfolly4wdt.dylib
[ 20%] Built target folly4wdt
make: *** [Makefile:146: all] Error 2
</pre>

*solutions*
1) start with a fresh cmake call and turn testing off ` -DBUILD_TESTING=off`
2) Or, just re-run `make -j`  and it will proceed to the next bug
`

### ld: malformed 64-bit a.b.c.d.e version number: 1.32.1910230

Not being savvy w/make/cmake/clang/... this took a while to figure out. OSX appears to have slightly different rules around versioning numbering schemes than the rest of ULINUX land.  I'd do a poor job rehashing it and didn't save the reference links.... so here is the fix:
1) First, do not change the 1.32 prefix.  The '32' minor version needs to match some other library minor version or all hell breaks loose.  The third integer needs to change (1910230)
2) delete the wdt repo and re-pull a fresh copy.  delete your build directory too.
3) edit the wdt/WdtConfig.h file. This section

<pre>
#define WDT_VERSION_MAJOR 1
#define WDT_VERSION_MINOR 32
#define WDT_VERSION_BUILD 1910230
// Add -fbcode to version str
#define WDT_VERSION_STR "1.32.1910230-fbcode"
</pre>

Should be edited to look like this:

<pre>
#define WDT_VERSION_MAJOR 1
#define WDT_VERSION_MINOR 32
#define WDT_VERSION_BUILD 64
// Add -fbcode to version str
#define WDT_VERSION_STR "1.32.64-fbcode"
</pre>
* I chose the number 64 and it worked, I know there are rules and size limits for this part of the version, so stick with 64

4) And this file: wdt/CMakeLists.txt needs to be edited. From

<pre># There is no C per se in WDT but if you use CXX only here many checks fail
# Version is Major.Minor.YYMMDDX for up to 10 releases per day (X from 0 to 9)
# Minor currently is also the protocol version - has to match with Protocol.cpp
project("WDT" LANGUAGES C CXX VERSION 1.32.1910230)```
</pre>
To:
<pre>
# There is no C per se in WDT but if you use CXX only here many checks fail
# Version is Major.Minor.YYMMDDX for up to 10 releases per day (X from 0 to 9)
# Minor currently is also the protocol version - has to match with Protocol.cpp
project("WDT" LANGUAGES C CXX VERSION 1.32.64)
</pre>

5) Ok, save the files. move back to your fresh build dir start with cmake with testing offf:

<pre>
cmake ../wdt -G "Unix Makefiles" -DBUILD_TESTING=off -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl
make -j 8
...
...
[ 82%] Building CXX object CMakeFiles/wdt_min.dir/util/CommonImpl.cpp.o
[ 84%] Linking CXX shared library libwdt_min.dylib
Undefined symbols for architecture x86_64:
  "folly::detail::to_ascii_table<10ull, folly::to_ascii_alphabet<false> >::data", referenced from:
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in ClientSocket.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in EncryptionUtils.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in ErrorCodes.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in WdtTransferRequest.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in Sender.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in ServerSocket.cpp.o
  "folly::detail::to_ascii_powers<10ull, unsigned long long>::data", referenced from:
      unsigned long folly::to_ascii_size<10ull>(unsigned long long) in ClientSocket.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in ClientSocket.cpp.o
      unsigned long folly::to_ascii_size<10ull>(unsigned long long) in EncryptionUtils.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in EncryptionUtils.cpp.o
      unsigned long folly::to_ascii_size<10ull>(unsigned long long) in ErrorCodes.cpp.o
      unsigned long folly::to_ascii_with<10ull, folly::to_ascii_alphabet<false>, 20ul>(char (&) [20ul], unsigned long long) in ErrorCodes.cpp.o
      unsigned long folly::to_ascii_size<10ull>(unsigned long long) in WdtTransferRequest.cpp.o
      ...
ld: symbol(s) not found for architecture x86_64
clang: error: linker command failed with exit code 1 (use -v to see invocation)
make[2]: *** [CMakeFiles/wdt_min.dir/build.make:520: libwdt_min.1.32.64.dylib] Error 1
make[1]: *** [CMakeFiles/Makefile2:169: CMakeFiles/wdt_min.dir/all] Error 2
make: *** [Makefile:136: all] Error 2
</pre>

* New Error with the same lib, but this time involving folly.  This also took me forever to figure out, but CMKAE has the fix for this in place already, use the system install of folly.....

### Linking CXX shared library libwdt_min.dylib Undefined symbols for architecture x86_64:

The fix for this is to use the system folly and not the WDS minimal version.
So:
1) Delete build directory
2) Delete wdt repo, repull it, edit the version numbers as described above.
3) Install folly with brew `brew install folly`
4) Brew exits with the path to where it has installed folly.  Copy that so you can pass that to cmake in a momenmt.
5) mkdir wdt-mac (next to the wdt dir), and:

<pre>
cmake ../wdt -G "Unix Makefiles" -DBUILD_TESTING=off -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl -DWDT_USE_SYSTEM_FOLLY=/usr/local/Cellar/folly/2021.06.14.00/
make -j 8
sudo make install
</pre>

And there it is.


## IPv6 DNS entry missing
If the host you are running the tests on does not have an IPv6 address (either
missing a AAAA record or not having any IPv6 address) some tests will fail
(like port_block_test) if that is the case disable IPv6 related tests using:
```
WDT_TEST_IPV6_CLIENT=0 CTEST_OUTPUT_ON_FAILURE=1 make test
```
