#! /bin/bash

set -x
set -e

date
uname -a
echo $HOSTNAME
mkdir $HOME/bin || true
export PATH=$HOME/bin:$PATH
export LD_LIBRARY_PATH=$HOME/lib:$LD_LIBRARY_PATH
openssl version
which openssl
ldd `which openssl`
if [[ "$CXX" == "clang++" ]] ; then
  export CC=clang-3.6
  export CXX=clang++-3.6
else
  ln -s /usr/bin/g++-4.9 $HOME/bin/g++
  ln -s /usr/bin/gcc-4.9 $HOME/bin/gcc
fi
cd ..
# remove the dangerous no-check-certificate when travis is fixed...
# https://github.com/travis-ci/travis-ci/issues/5059
wget --no-check-certificate https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.sh
sh cmake-3.3.2-Linux-x86_64.sh --prefix=$HOME --skip-license
git clone https://github.com/floitsch/double-conversion.git
(cd double-conversion; cmake -DCMAKE_INSTALL_PREFIX=$HOME .; make -j 4 && make install)
git clone https://github.com/schuhschuh/gflags.git
(mkdir gflags/build; cd gflags/build; cmake -DCMAKE_INSTALL_PREFIX=$HOME -D GFLAGS_NAMESPACE=google -D BUILD_SHARED_LIBS=on .. && make -j 4 && make install)
svn checkout http://google-glog.googlecode.com/svn/trunk/ glog
( cd glog && ./configure --with-gflags=$HOME --prefix=$HOME && make -j 4 && make install )
git clone https://github.com/facebook/folly.git
pwd ; ls -l
cd wdt

#set +e
set +x
