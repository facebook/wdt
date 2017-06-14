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

## IPv6 DNS entry missing
If the host you are running the tests on does not have an IPv6 address (either
missing a AAAA record or not having any IPv6 address) some tests will fail
(like port_block_test) if that is the case disable IPv6 related tests using:
```
WDT_TEST_IPV6_CLIENT=0 CTEST_OUTPUT_ON_FAILURE=1 make test
```
