#! /bin/bash

set -x
#set -e

date 
uname -a
echo $HOSTNAME
mkdir $HOME/bin || true
brew update
brew install openssl || true
brew link --force openssl || true
openssl version -a
CMAKE_BASE=cmake-3.3.2-Darwin-x86_64
cd ..
CMAKE_BIN_DIR=`pwd`/$CMAKE_BASE/CMake.app/Contents/bin
export PATH=$CMAKE_BIN_DIR:$HOME/bin:$PATH
export LD_LIBRARY_PATH=$HOME/lib:$LD_LIBRARY_PATH
wget https://www.cmake.org/files/v3.3/$CMAKE_BASE.tar.gz
tar xfz $CMAKE_BASE.tar.gz
which cmake
cmake --version
git clone https://github.com/floitsch/double-conversion.git
(cd double-conversion; cmake -DCMAKE_INSTALL_PREFIX=$HOME .; make -j 4 && make install)
git clone https://github.com/schuhschuh/gflags.git
(mkdir gflags/build; cd gflags/build; cmake -DCMAKE_INSTALL_PREFIX=$HOME -D GFLAGS_NAMESPACE=google -D BUILD_SHARED_LIBS=on .. && make -j 4 && make install)
svn checkout http://google-glog.googlecode.com/svn/trunk/ glog
( cd glog && ./configure --with-gflags=$HOME --prefix=$HOME && make -j 4 && make install )
git clone https://github.com/facebook/folly.git 
pwd ; ls -l  
cd wdt
# to avoid svn clone errors of gmock later:
( echo p | svn list https://googlemock.googlecode.com ) || true
#set +e
set +x
