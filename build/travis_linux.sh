#! /bin/bash
# Script to setup the travis build env - changes here are likely needed
# in wdt_cont_build_setup*.sh too
set -x
set -e

date
uname -a
echo $HOSTNAME
mkdir $HOME/bin || true
export PATH=$HOME/bin:$PATH
export LD_LIBRARY_PATH=$HOME/lib:$LD_LIBRARY_PATH
export CMAKE_PREFIX_PATH=$HOME
openssl version
cd ..
git clone https://github.com/google/double-conversion.git
(mkdir double-conversion-build; cd double-conversion-build; cmake -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=$HOME ../double-conversion; make -j 4 && make install)
git clone https://github.com/gflags/gflags.git
(mkdir gflags-build; cd gflags-build; cmake -DCMAKE_INSTALL_PREFIX=$HOME -DGFLAGS_NAMESPACE=google -DBUILD_SHARED_LIBS=on ../gflags && make -j 4 && make install)
git clone https://github.com/google/glog.git
( cd glog && ./autogen.sh && ./configure --with-gflags=$HOME --prefix=$HOME && make -j 4 && make install )
git clone https://github.com/facebook/folly.git
pwd ; ls -l
cd wdt
#set +e
set +x
