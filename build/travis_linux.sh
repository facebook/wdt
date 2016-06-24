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
openssl version
if [[ "$CXX" == "clang++" ]] ; then
  export LLVM_VERSION=3.7.1
  wget http://llvm.org/releases/$LLVM_VERSION/clang+llvm-$LLVM_VERSION-x86_64-linux-gnu-ubuntu-14.04.tar.xz
  mkdir $HOME/clang
  tar xf clang+llvm-$LLVM_VERSION-x86_64-linux-gnu-ubuntu-14.04.tar.xz -C $HOME/clang --strip-components 1
  export PATH=$HOME/clang/bin:$PATH
  export LD_LIBARY_PATH=$HOME/clang/lib:$LD_LIBRARY_PATH
#  export CC=clang-3.7
#  export CXX=clang++-3.7
  which $CXX || true
#  ls -l $HOME/clang/bin
  $CXX --version
# having issue with clang and glog use of libunwind somehow:
  wget http://download.savannah.nongnu.org/releases/libunwind/libunwind-snap-070410.tar.gz
  tar xfz libunwind-snap-070410.tar.gz
  ( cd libunwind-0.99-alpha; ./configure --prefix=$HOME && make -j 4 && make install )
else
  ln -s /usr/bin/g++-4.9 $HOME/bin/g++
  ln -s /usr/bin/gcc-4.9 $HOME/bin/gcc
fi
cd ..
# remove the dangerous no-check-certificate when travis is fixed...
# https://github.com/travis-ci/travis-ci/issues/5059
wget --no-check-certificate https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.sh
sh cmake-3.3.2-Linux-x86_64.sh --prefix=$HOME --skip-license
OPENSSL_VERSION=openssl-1.0.1q
wget https://www.openssl.org/source/$OPENSSL_VERSION.tar.gz
tar xfz $OPENSSL_VERSION.tar.gz
ls -l $HOME/bin
( cd $OPENSSL_VERSION ; ./config --prefix=$HOME threads shared; make ; make install )
which openssl
$HOME/bin/openssl version
ldd $HOME/bin/openssl
export OPENSSL_ROOT_DIR=$HOME
git clone https://github.com/floitsch/double-conversion.git
(cd double-conversion; cmake -DBUILD_SHARED_LIBS=on -DCMAKE_INSTALL_PREFIX=$HOME .; make -j 4 && make install)
git clone https://github.com/schuhschuh/gflags.git
(mkdir gflags/build; cd gflags/build; cmake -DCMAKE_INSTALL_PREFIX=$HOME -DGFLAGS_NAMESPACE=google -DBUILD_SHARED_LIBS=on .. && make -j 4 && make install)
git clone https://github.com/google/glog.git
(mkdir glog/build; cd glog/build; cmake -DCMAKE_INSTALL_PREFIX=$HOME -DBUILD_SHARED_LIBS=on .. && make -j 4 && make install)
# ( cd glog && autoreconf --force --install && ./configure --with-gflags=$HOME --prefix=$HOME && make -j 4 && make install )
# -Dgflags_DIR=$HOME -Dgflags_NAMESPACE=google
git clone https://github.com/facebook/folly.git
pwd ; ls -l
cd wdt

#set +e
set +x
