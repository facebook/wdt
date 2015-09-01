WDT opensource build uses CMake 3.2.x or later

This should build in a variety of environments (if it doesn't please open
an issue or even better, contribute a patch)

See also [README.md](README.md#dependencies) Dependencies section
(Inside facebook see https://our.intern.facebook.com/intern/wiki/WDT/OpenSource)

Checkout the .travis.yml and travis_linux.sh and travis_osx.sh
for build bootstrap without root/sudo requirement

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
cmake -D GFLAGS_NAMESPACE=google -D BUILD_SHARED_LIBS=on ..
make -j && sudo make install
```

```
svn checkout http://google-glog.googlecode.com/svn/trunk/ glog
cd glog
./configure # add --with-gflags=whereyouinstalledgflags
# to avoid ERROR: unknown command line flag 'minloglevel' later
make -j && sudo make install
```

*If double-conversion isn't available via apt-get:*
```
git clone https://github.com/floitsch/double-conversion.git
cd double-conversion; cmake . ; make -j && sudo make install
```


__Build wdt from source__
```
cmake pathtowdtsrcdir -DBUILD_TESTING=on  # skip -D... if you don't want tests
make -j
CTEST_OUTPUT_ON_FAILURE=1 make test
sudo make install
```
# Install on Mac OS X

__Install Xcode 6 and the command line tools__ (so
/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/clang++
exists)


## Using the contbuild script locally

```sh
source travis_osx.sh
mkdir ../build && cd ../build
cmake ../wdt -DBUILD_TESTING=1
make -j
CTEST_OUTPUT_ON_FAILURE=1 make test
# optionally
make install
```

## Or manual/step by step

__Install Cmake__

```
git clone http://cmake.org/cmake.git
cd cmake
./bootstrap --prefix=/usr --parallel=16
make -j && sudo make install
cd ../
```
__Install homebrew (http://brew.sh/)__

__Install glog and gflags and boost__
```sh
brew update
brew install glog gflags boost
```
__Install Double conversion__
```
git clone https://github.com/floitsch/double-conversion.git
cd double-conversion; cmake . ; make -j && sudo make install;
cd ../
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
cmake ../wdt -G Xcode -DBUILD_TESTING=on
```

Using Unix makefiles:
```
cmake ../wdt -G "Unix Makefiles" -DBUILD_TESTING=on
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

You may get certificate errors getting gmock and need to permanently
accept the certificate from the commandline

```
svn list https://googlemock.googlecode.com
# type "p", then ignore the output and make again
```
