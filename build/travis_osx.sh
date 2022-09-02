#! /bin/bash
# Sets up MacOS build environment (for travis but can be used on other mac too)
set -x
#set -e

date
uname -a
echo $HOSTNAME
mkdir $HOME/bin || true
brew update
brew install openssl || true
cd ..
export PATH=/usr/local/opt/openssl/bin:$HOME/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:$HOME/lib:$LD_LIBRARY_PATH
export OPENSSL_ROOT_DIR=/opt/homebrew/Cellar/openssl@1.1/1.1.1q
export CMAKE_PREFIX_PATH=$HOME
openssl version -a
git clone https://github.com/google/double-conversion.git
(mkdir double-conversion-build; cd double-conversion-build; cmake -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=$HOME ../double-conversion; make -j 4 && make install)
git clone https://github.com/gflags/gflags.git
(mkdir gflags-build; cd gflags-build; cmake -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DCMAKE_INSTALL_PREFIX=$HOME -DGFLAGS_NAMESPACE=google -DBUILD_SHARED_LIBS=on ../gflags && make -j 4 && make install)
git clone https://github.com/google/glog.git
( mkdir glog-build; cd glog-build; cmake -DCMAKE_C_COMPILER=/usr/bin/gcc -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DCMAKE_INSTALL_PREFIX=$HOME -DGLOG_NAMESPACE=google -DBUILD_SHARED_LIBS=on ../glog &&  make -j 4 && make install )
git clone https://github.com/facebook/folly.git
pwd ; ls -l
cd wdt
#set +e
set +x
